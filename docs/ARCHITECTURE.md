# NextLevelDB Server Architecture

## Executive Summary

NextLevelDB is a **relational database built on top of LevelDB** (a key-value store). It provides:
- Relational table abstractions over key-value storage
- Automatic secondary indexing
- Binary WebSocket protocol for efficient client-server communication
- Peer-to-peer synchronization capabilities
- Schema stored within the database itself

**Primary Use Case**: High-throughput time-series data collection (originally built for cryptocurrency market data from exchanges like Bittrex).

---

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLIENT APPLICATIONS                               │
│                    (nextleveldb-client npm package)                         │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  │ WebSocket Connection (Binary Protocol)
                                  │ Port 420 (default)
                                  │
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                        COMMUNICATION LAYER                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    handle-ws-binary.js                              │    │
│  │  • Parses binary protocol messages                                  │    │
│  │  • Routes commands to appropriate server methods                    │    │
│  │  • Handles paging, streaming responses                              │    │
│  │  • Manages subscriptions for real-time updates                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    HTTP Server (Node.js http)                       │    │
│  │  • Hosts WebSocket upgrade                                          │    │
│  │  • Basic HTTP handling (minimal)                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                         SERVER CLASS HIERARCHY                              │
│                                                                             │
│    ┌────────────────────────────────────────────────────────────────┐       │
│    │              NextLevelDB_P2P_Server                            │       │
│    │  • Multi-node synchronization                                  │       │
│    │  • Connects to peer databases                                  │       │
│    │  • Table/record syncing with progress tracking                 │       │
│    │  • Key range copying between nodes                             │       │
│    └────────────────────────┬───────────────────────────────────────┘       │
│                             │ extends                                       │
│    ┌────────────────────────▼───────────────────────────────────────┐       │
│    │              NextLevelDB_Safer_Server                          │       │
│    │  • Index validation and repair                                 │       │
│    │  • Auto-increment PK verification                              │       │
│    │  • Malformed record detection/logging                          │       │
│    │  • Safety checks on startup                                    │       │
│    └────────────────────────┬───────────────────────────────────────┘       │
│                             │ extends                                       │
│    ┌────────────────────────▼───────────────────────────────────────┐       │
│    │              NextLevelDB_Server                                │       │
│    │  • Table key subdivisions                                      │       │
│    │  • Field selection from records                                │       │
│    │  • Index maintenance operations                                │       │
│    │  • High-level query operations                                 │       │
│    └────────────────────────┬───────────────────────────────────────┘       │
│                             │ extends                                       │
│    ┌────────────────────────▼───────────────────────────────────────┐       │
│    │              NextLevelDB_Core_Server                           │       │
│    │  • LevelDB connection management                               │       │
│    │  • CRUD operations (put, get, delete, batch)                   │       │
│    │  • Key/record streaming with ranges                            │       │
│    │  • Model loading from database                                 │       │
│    │  • Subscription system for real-time updates                   │       │
│    │  • Table/index record retrieval                                │       │
│    └────────────────────────┬───────────────────────────────────────┘       │
│                             │ extends                                       │
│    ┌────────────────────────▼───────────────────────────────────────┐       │
│    │              Evented_Class (from lang-mini)                    │       │
│    │  • Event emission/subscription base                            │       │
│    │  • on(), off(), raise() methods                                │       │
│    └────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                           MODEL LAYER                                       │
│                    (nextleveldb-model npm package)                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  • Database schema definition                                       │    │
│  │  • Table/field/index definitions                                    │    │
│  │  • Binary encoding/decoding of records                              │    │
│  │  • Key prefix calculation                                           │    │
│  │  • Index record generation                                          │    │
│  │  • Model row diffing for sync                                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────────────────┐
│                          STORAGE LAYER                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    LevelDB (via levelup/leveldown)                  │    │
│  │  • Sorted key-value store                                           │    │
│  │  • Binary keys and values                                           │    │
│  │  • Range queries via iterators                                      │    │
│  │  • Batch write operations                                           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure and Responsibilities

### Core Server Files

| File | Lines | Purpose |
|------|-------|---------|
| `nextleveldb-core-server.js` | ~3200 | Core database operations, model loading, streaming |
| `nextleveldb-server.js` | ~1100 | Extended operations, table subdivisions, selections |
| `nextleveldb-safer-server.js` | ~2100 | Safety checks, index validation, data repair |
| `nextleveldb-p2p-server.js` | ~1100 | Peer-to-peer sync, multi-node operations |
| `handle-ws-binary.js` | ~3400 | Binary protocol handler, command routing |
| `module.js` | ~10 | Export aggregator |

### Supporting Files

| File | Purpose |
|------|---------|
| `handle-http.js` | HTTP request handling (minimal) |
| `handle-ws-utf8.js` | UTF-8 WebSocket (deprecated) |
| `running-means-per-second.js` | Performance metrics logging |
| `sampledb.js` | Sample database for testing |
| `config/config.json` | Server configuration, access tokens |

---

## Key Design Concepts

### 1. Key Prefix System

Every record in LevelDB has a key prefix that identifies its type:

```
Key Prefix (KP) Calculation:
- System/Core records: KP 0-9 (reserved)
- Table records:       KP = table_id * 2 + 2
- Index records:       KP = table_id * 2 + 3

Example for table_id = 5:
- Table records:  KP = 5 * 2 + 2 = 12
- Index records:  KP = 5 * 2 + 3 = 13
```

### 2. Binary Encoding

The system uses custom binary encoding (`binary-encoding` and `xas2` packages):

- **xas2**: Variable-length integer encoding (like protobuf varints)
- **binary-encoding**: Typed value encoding with type markers

```javascript
// Type markers in binary protocol
const XAS2 = 0;        // Variable-length integer
const DOUBLEBE = 1;    // 64-bit float big-endian
const DATE = 2;        // Timestamp
const STRING = 4;      // UTF-8 string
const BOOL_FALSE = 6;  // Boolean false
const BOOL_TRUE = 7;   // Boolean true
const NULL = 8;        // Null value
const BUFFER = 9;      // Raw binary buffer
const ARRAY = 10;      // Array of values
const OBJECT = 11;     // Key-value object
```

### 3. Message Protocol

Binary messages follow this structure:

```
┌──────────────┬───────────────┬─────────────┬──────────────┐
│ Message ID   │ Command Type  │ Paging Opts │ Payload      │
│ (xas2)       │ (xas2)        │ (varies)    │ (encoded)    │
└──────────────┴───────────────┴─────────────┴──────────────┘
```

**Command Types** (from handle-ws-binary.js):
```javascript
const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;
const LL_GET_ALL_KEYS = 2;
const LL_GET_ALL_RECORDS = 3;
const LL_GET_KEYS_IN_RANGE = 4;
const LL_GET_RECORDS_IN_RANGE = 5;
// ... 60+ command types
```

### 4. Paging System

Responses can be paged with different strategies:

```javascript
const NO_PAGING = 0;           // All results at once
const PAGING_RECORD_COUNT = 1; // N records per page
const PAGING_BYTE_COUNT = 3;   // N bytes per page
const PAGING_TIMED = 4;        // Periodic updates
```

Response message types indicate paging state:
```javascript
const BINARY_PAGING_NONE = 0;  // Single response
const BINARY_PAGING_FLOW = 1;  // More pages coming
const BINARY_PAGING_LAST = 2;  // Final page
```

### 5. Observable Pattern

The codebase heavily uses observables for streaming data:

```javascript
// Observable creation pattern
return obs_or_cb((next, complete, error) => {
    stream.on('data', data => next(data));
    stream.on('end', () => complete());
    stream.on('error', err => error(err));
    
    // Return [stop, pause, resume] functions
    return [
        () => stream.destroy(),
        () => stream.pause(),
        () => stream.resume()
    ];
}, callback);
```

### 6. Model System

The database schema (model) is stored within the database itself in system tables (KP 0-9):

- **Incrementors** (KP 0): Auto-increment counters
- **Tables** (KP 2): Table definitions
- **Fields** (KP 4): Field definitions  
- **Indexes** (KP 6): Index definitions
- And their corresponding index records (odd KPs)

---

## Data Flow Examples

### Writing a Record

```
1. Client encodes record using Model
2. Binary message sent via WebSocket
3. handle-ws-binary.js parses command (LL_PUT_RECORDS or INSERT_TABLE_RECORD)
4. Server validates/transforms data
5. If table has indexes, generate index records
6. Batch put to LevelDB (record + index records)
7. Emit 'db_action' event for subscribers
8. Send confirmation to client
```

### Reading Records in Range

```
1. Client sends LL_GET_RECORDS_IN_RANGE with key bounds
2. Server creates LevelDB ReadStream with range
3. For each record:
   a. Wrap in B_Record (buffer-backed record)
   b. Add to page buffer
   c. When page full, send PAGING_FLOW message
4. Send final PAGING_LAST message
5. Client reassembles pages
```

### P2P Sync Flow

```
1. P2P server connects to peer as client
2. Compare core models (diff_local_and_remote_models)
3. Compare structural tables (currencies, markets)
4. For large tables with FK+timestamp keys:
   a. Get key subdivisions from both sides
   b. Identify missing ranges
   c. Copy key ranges to local
5. For small tables:
   a. Full table sync with progress
```

---

## Configuration

### config/config.json

```json
{
    "nextleveldb_access": {
        "root": ["ACCESS_TOKEN_HERE"]
    },
    "source_dbs": ["data7", "data8"],
    "nextleveldb_connections": {
        "data7": {
            "server_address": "...",
            "server_port": 420
        }
    }
}
```

### Command Line Options

```bash
node nextleveldb-server.js --path /path/to/db
node nextleveldb-server.js -p /custom/path
```

Default path: `~/NextLevelDB/dbs/default`
Default port: `420`

---

## Dependencies Graph

```
nextleveldb-server
├── nextleveldb-model (schema, encoding)
├── nextleveldb-client (for P2P connections)
├── levelup + leveldown (storage)
├── binary-encoding (custom binary format)
├── xas2 (variable-length integers)
├── lang-mini (utilities, Evented_Class)
├── fnl + fnlfs (async utilities)
├── websocket (WebSocket server)
├── deep-diff + deep-equal (comparison)
├── rimraf (directory deletion)
├── command-line-args (CLI parsing)
└── my-config (configuration loading)
```

---

## Entry Points

### As Module
```javascript
const { 
    NextLevelDB_Core_Server,
    NextLevelDB_Server,
    NextLevelDB_Safer_Server,
    NextLevelDB_P2P_Server 
} = require('nextleveldb-server');

const server = new NextLevelDB_Safer_Server({
    db_path: '/path/to/db',
    port: 420
});

server.start((err, result) => {
    if (err) throw err;
    console.log('Server started');
});
```

### Direct Execution
Each server file can be run directly:
```bash
node nextleveldb-core-server.js -p ./mydb
node nextleveldb-safer-server.js -p ./mydb  # Recommended
node nextleveldb-p2p-server.js -p ./mydb    # For sync features
```

---

## Important Classes and Methods

### NextLevelDB_Core_Server

| Method | Description |
|--------|-------------|
| `start(callback)` | Initialize DB, WebSocket server, load model |
| `get(key)` | Get single record by key (Promise) |
| `batch_put(buffer, callback)` | Batch insert encoded records |
| `get_records_in_range(range, callback)` | Stream records in key range |
| `get_table_records(table, callback)` | Get all records from table |
| `ensure_table(definition, callback)` | Create table if not exists |
| `load_model(callback)` | Load schema from database |
| `ll_subscribe_all(callback)` | Subscribe to all DB changes |

### NextLevelDB_Safer_Server

| Method | Description |
|--------|-------------|
| `safety_check(fix, callback)` | Run all safety validations |
| `check_index_to_record_validity(callback)` | Verify indexes point to valid records |
| `check_record_to_index_validity(callback)` | Verify records have proper indexes |
| `check_autoincrementing_table_pk(table, callback)` | Fix incrementor mismatches |

### NextLevelDB_P2P_Server

| Method | Description |
|--------|-------------|
| `connect_to_source_dbs(callback)` | Connect to configured peers |
| `sync_db_table_data(db_name, table)` | Sync table from remote |
| `copy_key_range_to_local(db, l, u)` | Copy specific key range |
| `diff_local_and_remote_models(db, callback)` | Compare schemas |
