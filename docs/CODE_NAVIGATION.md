# NextLevelDB Server - Code Navigation Guide

> **For AI Agents**: This guide helps you navigate the codebase quickly. Use these patterns to locate specific functionality.

---

## Quick Reference: Where to Find Things

### Server Initialization
- **WebSocket server setup**: `nextleveldb-core-server.js:start()` (~line 180-350)
- **Authentication check**: `nextleveldb-core-server.js:check_access_token()` (~line 280)
- **Config loading**: `nextleveldb-core-server.js:load_config_access_tokens()` (~line 240)
- **Model loading**: `nextleveldb-core-server.js:load_model()` (~line 760)

### Command Handling
- **Binary protocol parser**: `handle-ws-binary.js` (entire file)
- **Command type constants**: `handle-ws-binary.js:200-280`
- **Paging constants**: `handle-ws-binary.js:80-130`

### Database Operations
- **Put operations**: `nextleveldb-core-server.js:batch_put()`, `ll_batch_put()`, `batch_put_kvpbs()`
- **Get operations**: `nextleveldb-core-server.js:get()`, `get_records_in_range()`
- **Delete operations**: `nextleveldb-core-server.js:delete_by_key()`, `delete_rows_by_keys()`
- **Count operations**: `nextleveldb-core-server.js:count()`, `ll_count_keys_in_range()`

### Table Operations
- **Ensure table**: `nextleveldb-core-server.js:ensure_table()` (~line 1150)
- **Get table records**: `nextleveldb-core-server.js:get_table_records()` (~line 2460)
- **Get table keys**: `nextleveldb-core-server.js:get_table_keys()` (~line 2470)
- **Table subdivisions**: `nextleveldb-server.js:get_table_key_subdivisions()` (~line 200)

### Index Operations
- **Get index records**: `nextleveldb-core-server.js:ll_get_table_index_records()` (~line 600)
- **Index validation**: `nextleveldb-safer-server.js:check_index_to_record_validity()` (~line 550)
- **Index repair**: `nextleveldb-safer-server.js:check_record_to_index_validity()` (~line 220)

### Subscription System
- **Subscribe all**: `nextleveldb-core-server.js:ll_subscribe_all()` (~line 400)
- **Subscribe key prefix**: `nextleveldb-core-server.js:ll_subscribe_key_prefix_puts()` (~line 420)
- **Event raising**: Search for `this.raise('db_action'`

### P2P/Sync Operations
- **Connect to peers**: `nextleveldb-p2p-server.js:connect_to_source_dbs()` (~line 520)
- **Sync table data**: `nextleveldb-p2p-server.js:sync_db_table_data()` (~line 400)
- **Copy key range**: `nextleveldb-p2p-server.js:copy_key_range_to_local()` (~line 260)
- **Model diff**: `nextleveldb-p2p-server.js:diff_local_and_remote_models()` (~line 170)

### Safety/Validation
- **Full safety check**: `nextleveldb-safer-server.js:safety_check()` (~line 880)
- **Auto-increment check**: `nextleveldb-safer-server.js:check_autoincrementing_table_pk()` (~line 200)
- **Invalid record detection**: `nextleveldb-safer-server.js:get_all_invalid_table_records()` (~line 1100)

---

## Key Classes and Inheritance

```
Evented_Class (lang-mini)
    └── NextLevelDB_Core_Server (nextleveldb-core-server.js)
            └── NextLevelDB_Server (nextleveldb-server.js)
                    └── NextLevelDB_Safer_Server (nextleveldb-safer-server.js)
                            └── NextLevelDB_P2P_Server (nextleveldb-p2p-server.js)
```

### Class Properties

**NextLevelDB_Core_Server**:
- `this.db` - LevelDB instance
- `this.db_path` - Database directory path
- `this.port` - WebSocket port
- `this.model` - Database schema (Model_Database instance)
- `this.db_options` - LevelDB configuration
- `this.map_access_tokens` - Auth token map
- `this.map_b64kp_subscription_put_alerts` - Subscription tracking
- `this.running_means_per_second` - Performance metrics

**NextLevelDB_P2P_Server**:
- `this.clients` - Array of connected peer clients
- `this.map_client_indexes` - Map of peer names to client array indexes
- `this.peers_info` - Peer connection configuration
- `this.source_dbs` - List of source database names to sync from

---

## Common Patterns to Recognize

### Signature Checking Pattern

The codebase uses a custom signature system for function overloading:

```javascript
let a = arguments, sig = get_a_sig(a);
// sig is a string like '[n,b,b,f]' representing:
// n = number, b = boolean, f = function, s = string, a = array, B = Buffer

if (sig === '[n,b,b,f]') {
    // Handle (number, bool, bool, callback)
} else if (sig === '[n,f]') {
    callback = a[1];
    decode = true;
    remove_kp = true;
} else {
    throw 'unexpected signature ' + sig;
}
```

### Observable Pattern

```javascript
// Creating an observable result
let res = new Evented_Class();
res.on('next', handler);      // Data events
res.on('complete', handler);  // Completion
res.on('error', handler);     // Errors

// Adding flow control
res.pause = () => stream.pause();
res.resume = () => stream.resume();
res.stop = () => stream.destroy();

// Observable-or-callback wrapper
return obs_or_cb((next, complete, error) => {
    // Return [stop, pause, resume] functions
    return [stopFn, pauseFn, resumeFn];
}, callback);
```

### Promise-or-Callback Pattern

```javascript
return prom_or_cb((resolve, reject) => {
    // Async operation
    someOperation((err, result) => {
        if (err) reject(err);
        else resolve(result);
    });
}, callback);

// Or with async/await inside:
return prom_or_cb(async (resolve, reject) => {
    try {
        const result = await someAsyncOperation();
        resolve(result);
    } catch (err) {
        reject(err);
    }
}, callback);
```

### Key Prefix Range Pattern

```javascript
// Convert key prefix to range bounds for iteration
let kp_to_range = buf_kp => {
    let buf_0 = Buffer.alloc(1);
    buf_0.writeUInt8(0, 0);
    let buf_255 = Buffer.alloc(1);
    buf_255.writeUInt8(255, 0);
    return [
        Buffer.concat([buf_kp, buf_0]),    // Lower bound
        Buffer.concat([buf_kp, buf_255])   // Upper bound
    ];
};
```

### Stream Processing Pattern

```javascript
this.db.createReadStream({
    'gte': lower_bound,
    'lte': upper_bound,
    'limit': limit
})
.on('data', data => {
    // Process each record
    next(new B_Record([data.key, data.value]));
})
.on('error', err => error(err))
.on('close', () => { /* Stream closed */ })
.on('end', () => complete());
```

---

## Important External Dependencies

### nextleveldb-model
- `Model.Database` / `Model_Database` - Schema management
- `Model.BB_Record` / `B_Record` - Buffer-backed record wrapper
- `Model.BB_Key` / `B_Key` - Buffer-backed key wrapper
- `Model.Key_List` - Collection of keys
- `Model.Record_List` / `B_Record_List` - Collection of records
- `Model.Index_Record_Key` - Index key wrapper
- `Model.encoding` / `database_encoding` - Encoding utilities
- `Model.Command_Message` - Binary message parser
- `Model.Command_Response_Message` - Binary response builder
- `Model.Paging` - Paging options parser

### binary-encoding
- `Binary_Encoding.encode_to_buffer()` - Encode values to binary
- `Binary_Encoding.decode_buffer()` - Decode binary to values
- `Binary_Encoding.split_length_item_encoded_buffer()` - Split encoded buffer
- `Binary_Encoding.join_buffer_pair()` - Combine key-value buffers

### xas2
- `xas2(number).buffer` - Encode number to varint buffer
- `xas2.read(buffer, position)` - Read varint from buffer

### lang-mini
- `Evented_Class` - Base class with event system
- `tof()` - Type-of function
- `each()` - Iteration helper
- `get_a_sig()` - Get argument signature string
- `Fns` - Function queue for sequential execution

### fnl / fnlfs
- `observable()` - Create observable
- `obs_or_cb()` - Observable or callback wrapper
- `prom_or_cb()` - Promise or callback wrapper
- `sig_obs_or_cb()` - Signature-aware observable/callback
- `fnlfs.ensure_directory_exists()` - Directory creation
- `fnlfs.load()` - JSON file loading

---

## Search Patterns for Common Tasks

### Find all places where records are written to DB
```
Search: this.db.batch\(|this.db.put\(|batch_put
```

### Find all WebSocket message handlers
```
Search: i_query_type === 
Files: handle-ws-binary.js
```

### Find all observable event handlers
```
Search: \.on\('next'|\.on\('complete'|\.on\('error'
```

### Find all places errors are raised
```
Search: res\.raise\('error'|reject\(|callback\(err
```

### Find all model-related operations
```
Search: this\.model\.|Model_Database|Model\.
```

### Find all key prefix calculations
```
Search: \* 2 \+ 2|\* 2 \+ 3|table_id \*|\.kp|buf_kp
```

---

## Test Data and Examples

### Sample Database Location
- `sampledb.js` - Example database setup
- `sample-data/sample-data.json` - Sample data file

### Inline Test Functions

Each server file has test functions in the `if (require.main === module)` block:

```javascript
// In nextleveldb-core-server.js:
let test_get_table_keys = () => { ... }
let test_count_core = () => { ... }

// In nextleveldb-server.js:
let test_observe_table_records = () => { ... }
let test_get_all_index_records = () => { ... }
```

---

## Debugging Tips

### Enable Logging in handle-ws-binary.js
```javascript
// At top of file:
const logging_enabled = true;  // Change to true for verbose logging
```

### Check Model State
```javascript
console.log('Model tables:', this.model.tables.map(t => t.name));
console.log('Model description:', this.model.description);
```

### Inspect Record Contents
```javascript
// For B_Record instances:
console.log('Decoded:', record.decoded);
console.log('Key:', record.key);
console.log('Value:', record.value);
console.log('Table ID:', record.table_id);
console.log('KP:', record.kp);
```

### Check Binary Encoding
```javascript
const Binary_Encoding = require('binary-encoding');
console.log('Encoded:', Binary_Encoding.encode_to_buffer(data));
console.log('Decoded:', Binary_Encoding.decode_buffer(buffer));
```
