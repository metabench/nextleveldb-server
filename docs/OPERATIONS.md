# NextLevelDB Server Operations & Developer Guide

This guide fills the gaps left by the existing architecture, protocol, and navigation docs. It explains how to install, run, configure, and validate the server in practice, and highlights operational hazards called out in `docs/CRITICAL_ISSUES.md`.

---

## Prerequisites
- Node.js `>=8.5.0` (repo-tested with modern Node; older versions are unmaintained).
- A platform toolchain that can build LevelDB via `leveldown` (native module). On Debian/Ubuntu, install `build-essential` and `python3`.
- Disk space for the LevelDB data directory; by default the server will create `~/NextLevelDB/dbs/default`.

Install dependencies:
```bash
npm install
```

Run the automated tests (lightweight, no DB I/O):
```bash
npm test
```

---

## Server Variants and Entry Points

The codebase exposes four server classes. All are exported from `module.js` and can also be run directly with `node <file> -p <path>`.

| File | Class | Purpose | Notes |
|------|-------|---------|-------|
| `nextleveldb-core-server.js` | `NextLevelDB_Core_Server` | Minimal LevelDB wrapper, loads model, WebSocket listener, no safety checks | Fastest startup; no index/record validation |
| `nextleveldb-server.js` | `NextLevelDB_Server` | Adds higher-level helpers (key subdivisions, selections) atop core | Default “standard” server |
| `nextleveldb-safer-server.js` | `NextLevelDB_Safer_Server` | Runs safety checks (index validity, malformed record detection) on startup | **Can block for minutes+ on large DBs** |
| `nextleveldb-p2p-server.js` | `NextLevelDB_P2P_Server` | Adds peer-to-peer sync helpers | Requires programmatic peer config |

### Running from the CLI
All server files accept an optional path flag:
```bash
node nextleveldb-safer-server.js -p /path/to/db  # recommended when you need validation
node nextleveldb-server.js -p /path/to/db        # faster startup, fewer checks
node nextleveldb-core-server.js -p /path/to/db   # lowest-level, minimal helpers
```

Defaults when flags are omitted:
- DB path: `~/NextLevelDB/dbs/default`
- Port: `420`

### Using as a Module
```javascript
const {
  NextLevelDB_Core_Server,
  NextLevelDB_Server,
  NextLevelDB_Safer_Server,
  NextLevelDB_P2P_Server
} = require('./module'); // or require('nextleveldb-server') when published

const server = new NextLevelDB_Safer_Server({ db_path: '/tmp/nldb', port: 420 });
server.start((err) => {
  if (err) throw err;
  console.log('Server started');
});
```

---

## Configuration and Authentication

The only built-in configuration file is `config/config.json`:
```json
{
  "nextleveldb_access": {
    "root": ["ROOTACCESS"]
  }
}
```

Usage and warnings:
- Tokens are **plaintext** and reused as cookies (`access_token`), making them vulnerable to theft/XSS.
- A single token can grant full root access; there is no expiry, rotation, or rate limiting.
- For production, replace tokens, secure transport, and add external controls (firewall, reverse proxy auth).

The server also reads `--path`/`-p` for DB location; other runtime settings (port, peers, safety behaviour) are hard-coded in code.

---

## Networking and Protocol Surface

- Transport: WebSocket (binary), default port `420`.
- Protocol: see `docs/BINARY_PROTOCOL.md` for command IDs, paging options, and message layout.
- Entry point: `handle-ws-binary.js` handles all commands; `handle-ws-utf8.js` is legacy/deprecated.
- HTTP: `handle-http.js` exists but is minimal and not integrated into the startup path.
- Message size: `maxReceivedFrameSize` is set to 512MB in `nextleveldb-core-server.js` (DoS risk; see Critical Issues).

---

## Data and Model Basics

- Keys are prefixed by a Key Prefix (KP) derived from the table ID: `table KP = table_id * 2 + 2`, `index KP = table_id * 2 + 3`.
- System KPs (0–9) store schema rows (tables, fields, indexes, incrementors) and are loaded into `this.model` on startup.
- Records are encoded/decoded with `nextleveldb-model` and `binary-encoding`; operations often stream as observables.
- The database directory is created automatically if missing (`~/NextLevelDB/dbs/<name>` by default).

---

## Operational Workflows

### Create or Open a Database
```bash
node nextleveldb-safer-server.js -p /data/nldb   # preferred for integrity checks
# or use nextleveldb-server.js for faster startup
```
The server auto-creates the path if needed. On first start, system tables are created as records are written.

### Programmatic Use (embed in another process)
Use `NextLevelDB_Server` or `NextLevelDB_Safer_Server` and call `start`. Provide `db_path` and optionally `port`.

### Peer-to-Peer Sync (manual wiring)
`NextLevelDB_P2P_Server` requires you to supply peer info and source DB names in the constructor (no CLI flags are defined). It then uses `nextleveldb-client` to diff and sync tables. See `nextleveldb-p2p-server.js` for expected fields: `source_dbs`, `peers_info`, `clients`, `map_client_indexes`.

### Sample Data
- `sample-data/` contains JSON examples.
- `sampledb.js` shows a usage sketch but references `nextleveldb-active` (not bundled); treat it as a conceptual example rather than a runnable script unless you add that dependency.

### Wiping Data
Binary protocol commands include `LL_WIPE`/`LL_WIPE_REPLACE`, but they are routed through `handle-ws-binary.js` and should be used cautiously; there is no additional confirmation.

---

## Safety, Integrity, and Known Pitfalls

Review `docs/CRITICAL_ISSUES.md` before deploying. Highlights:
- **Error handlers use undefined variables** in `nextleveldb-core-server.js` (range methods) and `handle-ws-binary.js` (callback references) — errors can crash the process.
- **Authentication is weak** (plaintext tokens, cookie-based, no expiry/rate limiting).
- **512MB WebSocket frame limit** enables DoS.
- **Safety checks block startup** (`nextleveldb-safer-server.js`), iterating every record/index on large DBs.
- **Async patterns are mixed** (callbacks, observables, promises); ensure both paths are exercised when modifying code.
- **No transaction/rollback**; partial batch failures can leave inconsistent state.
- **Massive handlers** (`handle-ws-binary.js`) are hard to reason about; changes are high-risk.

Operational recommendations:
- Use `NextLevelDB_Safer_Server` in development to surface index/record issues; switch to `NextLevelDB_Server` for faster local iteration if startup is too slow.
- Constrain inbound traffic (proxy, WAF, firewall) to mitigate the oversized frame limit.
- Add external auth in front of the WebSocket listener if running outside a trusted network.
- When debugging, you can toggle verbose logging in `handle-ws-binary.js` via `logging_enabled`.

---

## Testing and Validation

### Automated Tests
Run all tests:
```bash
npm test
```
Current coverage (Vitest):
- Module export wiring (`module.js` vs concrete server files).
- Core server defaults (`db_path`, `port`, KP buffers, model metadata accessors).
- Running means tracker (`running-means-per-second.js`) rolling averages and rotation behaviour.
- P2P helpers (`nextleveldb-p2p-server.js`) for diff orchestration and model loading/error paths.

### Suggested Manual Checks
- Start a server and connect with `nextleveldb-client`:
  - Create a table, insert records, verify index creation.
  - Exercise range reads and paged responses.
  - Test subscription commands (`LL_SUBSCRIBE_*`) and ensure events arrive.
- Verify safety checks:
  - Run `nextleveldb-safer-server.js` against a known dataset and watch for index/record validation output.
- Inspect logs for swallowed errors; many observables simply `console.log` errors.

### Adding New Tests
- Prefer small, fast unit tests; avoid spinning up real LevelDB in CI unless using tmp directories and small datasets.
- Use fixtures to test binary protocol parsing/encoding without opening sockets.
- Cover both callback and promise branches where `prom_or_cb`/`obs_or_cb` wrappers are used.

---

## Development Guidelines

- Read `docs/ARCHITECTURE.md`, `docs/CODE_NAVIGATION.md`, and `docs/CRITICAL_ISSUES.md` before changes.
- Favor `async/await` and clear error propagation; avoid adding new observable/callback hybrids unless necessary.
- Do not copy patterns from `handle-ws-binary.js` without scrutiny; many are fragile.
- Document magic numbers and KP math when touched; prefer constants.
- Keep tests fast and isolated; LevelDB-heavy tests should use temporary directories and small datasets.

---

## Troubleshooting Quick Answers
- **Server crashes on range operations**: See undefined `err` usage in `nextleveldb-core-server.js` error handlers (Critical Issue #1).
- **Startup is extremely slow**: Safety checks in `nextleveldb-safer-server.js` iterate all records; use standard server or disable safety for large DBs.
- **Auth appears insecure**: It is; tokens are plaintext and cookie-based. Add an upstream auth layer and rotate tokens manually.
- **Unexpected memory growth**: Subscription maps are not fully cleaned up on connection close (Critical Issue #10).
- **Large messages hang**: WebSocket frame limit is 512MB; reject untrusted clients or lower the limit in code.

