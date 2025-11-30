# NextLevelDB Server Documentation

Welcome to the NextLevelDB Server documentation. This documentation is designed to give both humans and AI agents a deep understanding of the codebase.

## Documentation Index

### 📐 [ARCHITECTURE.md](ARCHITECTURE.md)
Comprehensive system architecture documentation including:
- System diagrams
- Class hierarchy
- Data flow patterns
- Key design concepts
- Configuration reference
- API overview

### 🔴 [CRITICAL_ISSUES.md](CRITICAL_ISSUES.md)
**Read this first if you're modifying code!**

Brutally honest assessment of codebase problems:
- Critical bugs (crashes, security issues)
- High-severity issues (memory leaks, race conditions)
- Medium-severity technical debt
- Low-severity code smells
- Recommendations for AI agents

### 📡 [BINARY_PROTOCOL.md](BINARY_PROTOCOL.md)
Complete binary protocol reference:
- Message structure
- Command types
- Paging options
- Data encoding
- Example message flows

### 🧭 [CODE_NAVIGATION.md](CODE_NAVIGATION.md)
Quick reference for finding code:
- Function locations
- Common patterns
- Search patterns
- Debugging tips

### 🛠️ [OPERATIONS.md](OPERATIONS.md)
Hands-on setup and operations guide:
- Installation and prerequisites
- Running core/standard/safer/P2P servers
- Configuration, auth, and networking notes
- Safety caveats and troubleshooting
- Testing strategy and manual checks

---

## Quick Start for AI Agents

### Before Making Any Changes

1. **Read [CRITICAL_ISSUES.md](CRITICAL_ISSUES.md)** - Understand known bugs and anti-patterns
2. **Check [CODE_NAVIGATION.md](CODE_NAVIGATION.md)** - Find where functionality lives
3. **Review [ARCHITECTURE.md](ARCHITECTURE.md)** - Understand the system design

### Key Warnings

⚠️ **Do NOT copy existing patterns blindly** - Many contain bugs

⚠️ **Error handling is broken in many places** - Test error paths explicitly

⚠️ **Three async patterns are mixed** - Prefer async/await for new code

⚠️ **handle-ws-binary.js is 3400 lines** - Changes here affect everything

⚠️ **Safety checks run on startup** - Can block for minutes with large DBs

### Critical Files

| File | Risk Level | Notes |
|------|------------|-------|
| `handle-ws-binary.js` | 🔴 HIGH | All protocol handling, easy to break |
| `nextleveldb-core-server.js` | 🔴 HIGH | Core operations, inheritance root |
| `nextleveldb-safer-server.js` | 🟠 MEDIUM | Startup blocking, index validation |
| `module.js` | 🟠 MEDIUM | Has an export bug (see CRITICAL_ISSUES) |
| `config/config.json` | 🔴 HIGH | Contains auth tokens |

### Testing Changes

There are **no automated tests**. To verify changes:

1. Start server: `node nextleveldb-safer-server.js -p ./testdb`
2. Use `nextleveldb-client` to test operations
3. Check server logs for errors
4. Test error conditions explicitly

---

## Project Context

NextLevelDB was built for high-throughput time-series data collection, specifically cryptocurrency market data. The codebase shows signs of:

- Rapid feature development without refactoring
- Evolution from simple to complex requirements
- Performance optimizations that sacrificed readability
- Multiple developers with different styles
- Incomplete documentation and testing

The system works for its intended use case but has significant technical debt.

---

## External Dependencies

Core functionality depends on these packages (all by the same author):

- `nextleveldb-model` - Schema management and encoding
- `nextleveldb-client` - Client library (used for P2P sync)
- `binary-encoding` - Custom binary format
- `xas2` - Variable-length integers
- `lang-mini` - Utility library and base classes
- `fnl` / `fnlfs` - Async utilities

Understanding these packages is essential for deep codebase work.
