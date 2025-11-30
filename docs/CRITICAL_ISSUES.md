# NextLevelDB Server - Critical Issues and Technical Debt

> **For AI Agents**: This document is a brutally honest assessment of the codebase's problems. When working on this codebase, be aware that many fundamental architectural issues exist. Do not assume patterns you see are correct or should be followed.

---

## Severity Ratings

- 🔴 **CRITICAL**: Can cause data loss, security breaches, or system failures
- 🟠 **HIGH**: Significant bugs, performance issues, or maintainability blockers
- 🟡 **MEDIUM**: Technical debt, code smells, inconsistencies
- 🟢 **LOW**: Minor issues, style problems, documentation gaps

---

## 🔴 CRITICAL ISSUES

### 1. Undefined Variable Bugs in Error Handlers

**Location**: `nextleveldb-core-server.js` lines ~2140, ~2180

The error handlers have undefined variable bugs that will crash on any error:

```javascript
// BROKEN - 'err' is undefined, should be: err => reject(err)
get_first_key_in_range(arr_range, callback) {
    return prom_or_cb((resolve, reject) => {
        // ...
        }).on('error', reject(err))  // ❌ CRASHES - 'err' not defined
```

```javascript
get_last_key_in_range(arr_range, callback) {
    return prom_or_cb((resolve, reject) => {
        // ...
        }).on('error', reject(err))  // ❌ CRASHES - 'err' not defined
```

**Impact**: Any error during key range operations crashes the server instead of being handled.

**Fix Required**:
```javascript
.on('error', err => reject(err))  // or just: .on('error', reject)
```

---

### 2. Callback Reference Errors in handle-ws-binary.js

**Location**: `handle-ws-binary.js` - multiple locations

The callback variable `callback` is referenced but never defined in the WebSocket handler scope:

```javascript
// Line ~1300 and many others
.on('error', function (err) {
    callback(err);  // ❌ 'callback' IS NOT DEFINED IN THIS SCOPE
})
```

**Impact**: Errors in stream operations cause "callback is not defined" crashes instead of proper error handling.

---

### 3. No Input Validation on Binary Protocol

**Location**: `handle-ws-binary.js`

The binary protocol handler trusts all incoming data without validation:

```javascript
var handle_ws_binary = function (connection, nextleveldb_server, message_binary) {
    [message_id, pos] = x.read(message_binary, pos);
    [i_query_type, pos] = x.read(message_binary, pos);
    // No validation that message_id or i_query_type are valid
    // No bounds checking on buffer reads
    // No schema validation on decoded data
```

**Impact**: 
- Malformed messages can crash the server
- Potential buffer overflow/underflow attacks
- Invalid command types silently fail

---

### 4. Insecure Authentication System

**Location**: `nextleveldb-core-server.js` lines ~250-290, `config/config.json`

```javascript
// Tokens stored in plain text config
"nextleveldb_access": {
    "root": ["ROOTACCESS"]  // ❌ Plain text, easily compromised
}

// Token passed via cookie - vulnerable to XSS
let provided_access_token = map_cookies['access_token'];
```

**Issues**:
- Access tokens stored in plain text
- Single token grants full root access
- Token transmitted via cookies (XSS vulnerable)
- No token expiration or rotation
- No rate limiting on authentication failures
- No audit logging of authentication attempts

---

### 5. 512MB Message Size Limit - DoS Vector

**Location**: `nextleveldb-core-server.js` line ~210

```javascript
var wsServer = new WebSocketServer({
    maxReceivedFrameSize: 512000000,  // 512MB per message!
    autoAcceptConnections: false
});
```

**Impact**: Single client can send 512MB messages, potentially exhausting server memory.

---

### 6. No Transaction Support or Rollback

**Location**: Throughout the codebase

```javascript
// Batch operations have no rollback capability
db.batch(ops, (err) => {
    if (err) {
        // ❌ Some operations may have succeeded before failure
        // ❌ No way to rollback partial writes
        callback(err);
    }
});
```

**Impact**: Partial failures can leave database in inconsistent state.

---

## 🟠 HIGH SEVERITY ISSUES

### 7. Module.js Export Bug

**Location**: `module.js`

```javascript
const NextLevelDB_Safer_Server = require('./nextleveldb-safer-server');
const NextLevelDB_Server = require('./nextleveldb-safer-server');  // ❌ WRONG FILE!

module.exports = {
    NextLevelDB_Server  // Actually exports Safer_Server, not Server
}
```

**Impact**: Code expecting `NextLevelDB_Server` gets `NextLevelDB_Safer_Server` with its safety checks.

---

### 8. Inconsistent Async Patterns Throughout Codebase

The codebase randomly mixes three async patterns, often within the same function:

```javascript
// Pattern 1: Callbacks
this.get_table_id_by_name(table_name, (err, id) => { ... });

// Pattern 2: Observables
let obs = this.get_table_records(table);
obs.on('next', data => { ... });
obs.on('complete', () => { ... });

// Pattern 3: Promises
return prom_or_cb((resolve, reject) => { ... }, callback);

// Pattern 4: Hybrid observable-or-callback
return obs_or_cb((next, complete, error) => { ... }, callback);
```

**Impact**:
- Difficult to reason about control flow
- Error handling inconsistent
- Race conditions likely
- Cannot use modern async/await cleanly

---

### 9. Blocking Safety Checks on Startup

**Location**: `nextleveldb-safer-server.js` line ~1950

```javascript
start(callback) {
    super.start((err, res) => {
        // This can iterate through EVERY record in the database
        this.safety_check(fix_errors, callback);  // ❌ BLOCKS STARTUP
    });
}
```

**Impact**: Server may take minutes/hours to start with large databases while it validates every record and index.

---

### 10. Memory Leaks in Subscription System

**Location**: `nextleveldb-core-server.js`

```javascript
this.map_b64kp_subscription_put_alerts = {};
this.map_b64kp_subscription_put_alert_counts = {};
// Subscriptions are registered but cleanup on connection close is incomplete
```

```javascript
connection.on('close', (reasonCode, description) => {
    // Tries to cancel subscriptions but map_b64kp_subscription_put_alerts
    // may still hold references
    each(connection.subscription_handlers, (handler, event_name) => {
        that.off(event_name, handler);
    });
    // ❌ Doesn't clean up map_b64kp_subscription_put_alerts properly
});
```

---

### 11. Race Conditions in Parallel Observable Operations

**Location**: `nextleveldb-server.js` `get_table_key_subdivisions`

```javascript
obs_tks.on('next', key => {
    count_in_progress++;
    this.get_first_and_last_keys_beginning(search_key, false, false, (err, keys) => {
        count_in_progress--;
        // ❌ Results can arrive in different order than requested
        // ❌ No ordering guarantee maintained
        if (obs_complete && count_in_progress === 0) {
            res.raise('complete');
        }
    });
});
```

---

### 12. Error Events Not Always Propagated

**Location**: Multiple files

```javascript
// Errors often just logged and swallowed
obs_call.on('error', err => {
    console.log('err', err);  // ❌ Error logged but not propagated
    // Caller never knows error occurred
});
```

---

## 🟡 MEDIUM SEVERITY ISSUES

### 13. Massive Function Size and Complexity

**handle-ws-binary.js** is a 3400+ line function with one giant if/else chain:

```javascript
var handle_ws_binary = function (connection, nextleveldb_server, message_binary) {
    // 3400 lines of:
    if (i_query_type === LL_WIPE) { ... }
    if (i_query_type === LL_PUT_RECORDS) { ... }
    if (i_query_type === LL_COUNT_RECORDS) { ... }
    // ... 60+ more command handlers
}
```

**Impact**: 
- Unmaintainable
- Cannot test individual handlers
- Difficult to understand flow
- High cyclomatic complexity

---

### 14. Code Duplication Across Files

Same utility functions defined multiple times:

```javascript
// Defined in nextleveldb-server.js
let obs_map = (obs, fn_data) => { ... }

// Identical definition in nextleveldb-core-server.js
let obs_map = (obs, fn_data) => { ... }

// And again in nextleveldb-safer-server.js
let obs_map = (obs, fn_data) => { ... }
```

This applies to: `obs_map`, `obs_filter`, `obs_arrayified_call`, `kp_to_range`, etc.

---

### 15. Magic Numbers Throughout Codebase

```javascript
// What is 420? (It's the default port, but not documented)
this.port = spec.port || 420;

// What does * 2 + 2 mean?
let kp = table_id * 2 + 2;
let ikp = kp + 1;

// Core prefix bounds - why 0-9?
const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;
```

---

### 16. Inconsistent Error Message Formats

```javascript
// Sometimes Error objects
callback(new Error('Table ' + table_name + ' does not exist locally'));

// Sometimes strings
throw 'NYI';
throw 'get_first_and_last_keys_beginning unexpected signature:' + sig;

// Sometimes just the string 'stop'
throw 'stop';
```

---

### 17. Commented-Out Code and Dead Code

Extensive commented-out code throughout:

```javascript
/*
if (process.argv.length === 2) {
    //db_path = process.argv[1];
}
*/

// And TODO comments from years ago:
// 29/03/2018 - Want to get syncing from remote servers done today.
```

---

### 18. Console Logging as Primary Debugging

```javascript
// Production code full of console.log debugging
console.log('key', key);
console.log('data', data);
console.log('obs_call complete');
// No proper logging framework with levels
```

---

### 19. Hardcoded Assumptions About Key Structure

```javascript
// Assumes first 2 elements are always table_id and index_id
let key_field_ids_with_table_id_and_index_id = index.kv_field_ids[0];
let indexed_field_id = index.kv_field_ids[0][2];

// Assumes single PK field in many places
if (pk.fields.length === 1) { ... }
```

---

### 20. No Connection Pooling or Limits

```javascript
wsServer.on('request', request => {
    // No limit on number of connections
    // No connection pooling
    // No timeout handling
    var connection = request.accept('echo-protocol', request.origin);
});
```

---

## 🟢 LOW SEVERITY ISSUES

### 21. Inconsistent Naming Conventions

```javascript
// Sometimes camelCase
let table_id = ...
let tableId = ...

// Sometimes snake_case  
let arr_table_records = ...

// Sometimes abbreviated
let buf = ...
let arr_buf_idx_res = ...

// Sometimes full names
let buffer_key_beginning = ...
```

---

### 22. Missing Type Information

No TypeScript, no JSDoc annotations for most functions:

```javascript
// What types? What does it return? What exceptions?
get_table_key_subdivisions(table_id, remove_kp = true, decode = true, callback) {
```

---

### 23. No Unit Tests

The codebase has zero automated tests. Testing is done via:
- Manual execution
- Inline test functions in main blocks:

```javascript
if (require.main === module) {
    // Manual test code here
    let test_observe_table_records = () => { ... }
    let test_get_all_index_records = () => { ... }
}
```

---

### 24. Outdated Dependencies

`package.json` pins old versions:
```json
"leveldown": "^6.1.1",  // Current is 6.x but released in 2023
"websocket": "^1.0.34"  // Very old
```

---

### 25. No Request Timeout Handling

Long-running requests have no timeouts:

```javascript
// A query that scans millions of records has no timeout
db.createReadStream({}).on('data', function (data) {
    // Could run forever
});
```

---

## Recommendations for AI Agents

When working on this codebase:

1. **Never assume existing patterns are correct** - Copy-paste will propagate bugs
2. **Test every code path** - Many error handlers are broken
3. **Prefer async/await** over the existing callback/observable mix when adding new code
4. **Add input validation** before processing any external data
5. **Check for undefined** - Many functions assume parameters exist without checking
6. **Verify error propagation** - Follow errors through to make sure they reach callers
7. **Be cautious with paging code** - Very complex and error-prone
8. **Consider safety server startup time** - May need to disable safety checks during development
9. **Test with small databases first** - Large DBs expose performance issues
10. **Check both callback and Promise paths** - Often only one is actually tested

### Priority Refactoring Order

1. Fix critical undefined variable bugs
2. Add input validation to handle-ws-binary.js
3. Standardize on async/await
4. Split handle-ws-binary.js into command modules
5. Add proper error handling throughout
6. Add authentication improvements
7. Add connection limits and timeouts
8. Add comprehensive testing
