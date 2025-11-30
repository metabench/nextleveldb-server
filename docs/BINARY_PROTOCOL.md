# NextLevelDB Binary Protocol Reference

This document describes the binary WebSocket protocol used for client-server communication.

---

## Message Structure

All messages follow this structure:

```
┌─────────────────────────────────────────────────────────────────┐
│                       MESSAGE HEADER                            │
├──────────────────┬──────────────────┬───────────────────────────┤
│   Message ID     │   Command Type   │   Paging Options          │
│   (xas2 varint)  │   (xas2 varint)  │   (varies by command)     │
├──────────────────┴──────────────────┴───────────────────────────┤
│                       MESSAGE PAYLOAD                           │
│                  (binary-encoded data)                          │
└─────────────────────────────────────────────────────────────────┘
```

### xas2 Encoding

Variable-length integer encoding (similar to Protocol Buffers varints):
- 7 bits of data per byte
- High bit indicates continuation
- Little-endian ordering

```javascript
// Examples:
// 127 → [0x7F]           (1 byte)
// 128 → [0x80, 0x01]     (2 bytes)
// 300 → [0xAC, 0x02]     (2 bytes)
```

---

## Data Type Markers

When encoding values, each is prefixed with a type marker:

| Marker | Value | Description |
|--------|-------|-------------|
| `XAS2` | 0 | Variable-length integer |
| `DOUBLEBE` | 1 | 64-bit float, big-endian |
| `DATE` | 2 | Timestamp (ms since epoch) |
| `STRING` | 4 | UTF-8 string (length-prefixed) |
| `BOOL_FALSE` | 6 | Boolean false |
| `BOOL_TRUE` | 7 | Boolean true |
| `NULL` | 8 | Null value |
| `BUFFER` | 9 | Raw binary buffer |
| `ARRAY` | 10 | Array of values |
| `OBJECT` | 11 | Key-value object |
| `COMPRESSED_BUFFER` | 12 | Compressed data |

---

## Command Types

### Core Operations (0-10)

| Command | Value | Description |
|---------|-------|-------------|
| `LL_COUNT_RECORDS` | 0 | Count all records in database |
| `LL_PUT_RECORDS` | 1 | Batch insert records |
| `LL_GET_ALL_KEYS` | 2 | Stream all keys |
| `LL_GET_ALL_RECORDS` | 3 | Stream all records |
| `LL_GET_KEYS_IN_RANGE` | 4 | Get keys in key range |
| `LL_GET_RECORDS_IN_RANGE` | 5 | Get records in key range |
| `LL_COUNT_KEYS_IN_RANGE` | 6 | Count keys in range |
| `LL_GET_FIRST_LAST_KEYS_IN_RANGE` | 7 | Get boundary keys in range |
| `LL_GET_RECORD` | 8 | Get single record by key |
| `LL_COUNT_KEYS_IN_RANGE_UP_TO` | 9 | Count with limit |
| `LL_GET_RECORDS_IN_RANGE_UP_TO` | 10 | Get records with limit |

### Table Operations (11-30)

| Command | Value | Description |
|---------|-------|-------------|
| `LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH` | 11 | Count index matches |
| `INSERT_TABLE_RECORD` | 12 | Insert single table record |
| `INSERT_RECORDS` | 13 | Insert multiple records |
| `ENSURE_RECORD` | 14 | Insert if not exists |
| `ENSURE_TABLE_RECORD` | 15 | Ensure table record exists |
| `GET_TABLE_RECORD_BY_KEY` | 16 | Get record by primary key |
| `GET_TABLE_RECORDS_BY_KEYS` | 17 | Get multiple records by keys |
| `DELETE_RECORDS_BY_KEYS` | 18 | Delete by keys |
| `ENSURE_TABLE` | 20 | Create table if not exists |
| `ENSURE_TABLES` | 21 | Create multiple tables |
| `TABLE_EXISTS` | 22 | Check table existence |
| `TABLE_ID_BY_NAME` | 23 | Get table ID from name |
| `GET_TABLE_FIELDS_INFO` | 24 | Get table schema |
| `GET_TABLE_KEY_SUBDIVISIONS` | 25 | Get key range divisions |
| `GET_RELATED_RECORDS` | 26 | Get FK-related records |
| `LL_GET_FIRST_LAST_RECORDS_IN_RANGE` | 27 | Get boundary records |

### Key Operations (36-38)

| Command | Value | Description |
|---------|-------|-------------|
| `LL_GET_FIRST_LAST_KEYS_BEGINNING` | 36 | Keys starting with prefix |
| `LL_GET_FIRST_KEY_BEGINNING` | 37 | First key with prefix |
| `LL_GET_LAST_KEY_BEGINNING` | 38 | Last key with prefix |

### Selection Operations (40-50)

| Command | Value | Description |
|---------|-------|-------------|
| `SELECT_FROM_RECORDS_IN_RANGE` | 40 | Select fields from range |
| `SELECT_FROM_TABLE` | 41 | Select fields from table |
| `LL_GET_RECORDS_IN_RANGES` | 50 | Get records in multiple ranges |

### Subscription Operations (60-62)

| Command | Value | Description |
|---------|-------|-------------|
| `LL_SUBSCRIBE_ALL` | 60 | Subscribe to all DB changes |
| `LL_SUBSCRIBE_KEY_PREFIX_PUTS` | 61 | Subscribe to prefix puts |
| `LL_UNSUBSCRIBE_SUBSCRIPTION` | 62 | Cancel subscription |

### Administrative Operations (100+)

| Command | Value | Description |
|---------|-------|-------------|
| `LL_WIPE` | 100 | Delete all data |
| `LL_WIPE_REPLACE` | 101 | Delete all and replace |
| `LL_SEND_MESSAGE_RECEIPT` | 120 | Acknowledge message |
| `LL_MESSAGE_STOP` | 121 | Stop streaming operation |
| `LL_MESSAGE_PAUSE` | 122 | Pause streaming |
| `LL_MESSAGE_RESUME` | 123 | Resume streaming |

---

## Paging Options

### Request Paging Types

| Type | Value | Following Data |
|------|-------|----------------|
| `NO_PAGING` | 0 | None |
| `PAGING_RECORD_COUNT` | 1 | xas2: records per page |
| `PAGING_KEY_COUNT` | 2 | xas2: keys per page (deprecated) |
| `PAGING_BYTE_COUNT` | 3 | xas2: bytes per page |
| `PAGING_TIMED` | 4 | xas2: milliseconds between updates |

### Response Message Types

| Type | Value | Description |
|------|-------|-------------|
| `BINARY_PAGING_NONE` | 0 | Single complete response |
| `BINARY_PAGING_FLOW` | 1 | More pages coming |
| `BINARY_PAGING_LAST` | 2 | Final page |
| `RECORD_PAGING_NONE` | 3 | Single record response |
| `RECORD_PAGING_FLOW` | 4 | More records coming |
| `RECORD_PAGING_LAST` | 5 | Final record page |
| `RECORD_UNDEFINED` | 6 | Record not found |
| `KEY_PAGING_NONE` | 7 | Single key response |
| `KEY_PAGING_FLOW` | 8 | More keys coming |
| `KEY_PAGING_LAST` | 9 | Final key page |
| `ERROR_MESSAGE` | 10 | Error response |

---

## Response Structure

### Non-Paged Response

```
┌──────────────┬───────────────────┬──────────────────────────────┐
│ Message ID   │ BINARY_PAGING_NONE│ Encoded Result               │
│ (xas2)       │ (xas2 = 0)        │ (binary-encoded)             │
└──────────────┴───────────────────┴──────────────────────────────┘
```

### Paged Response (Flow)

```
┌──────────────┬───────────────────┬──────────────┬───────────────┐
│ Message ID   │ BINARY_PAGING_FLOW│ Page Number  │ Page Data     │
│ (xas2)       │ (xas2 = 1)        │ (xas2)       │ (encoded)     │
└──────────────┴───────────────────┴──────────────┴───────────────┘
```

### Paged Response (Last)

```
┌──────────────┬───────────────────┬──────────────┬───────────────┐
│ Message ID   │ BINARY_PAGING_LAST│ Page Number  │ Page Data     │
│ (xas2)       │ (xas2 = 2)        │ (xas2)       │ (encoded)     │
└──────────────┴───────────────────┴──────────────┴───────────────┘
```

---

## Record Encoding

### Key-Value Pair

Records are stored as length-prefixed key-value pairs:

```
┌─────────────────┬─────────────────┬─────────────────┬─────────────┐
│ Key Length      │ Key Data        │ Value Length    │ Value Data  │
│ (xas2)          │ (bytes)         │ (xas2)          │ (bytes)     │
└─────────────────┴─────────────────┴─────────────────┴─────────────┘
```

### Key Structure

Keys include a key prefix (KP) identifying the table:

```
┌─────────────────┬────────────────────────────────────────────────┐
│ Key Prefix (KP) │ Primary Key Fields (encoded)                   │
│ (xas2)          │ (type-prefixed values)                         │
└─────────────────┴────────────────────────────────────────────────┘

KP Calculation:
- Table records: KP = table_id * 2 + 2
- Index records: KP = table_id * 2 + 3
- System records: KP = 0-9 (reserved)
```

### Index Record Key Structure

```
┌──────────────┬──────────────┬─────────────────┬────────────────────┐
│ Index KP     │ Index ID     │ Indexed Fields  │ Reference to PK    │
│ (xas2)       │ (xas2)       │ (encoded)       │ (encoded)          │
└──────────────┴──────────────┴─────────────────┴────────────────────┘
```

---

## Example Message Flows

### Count All Records

**Request:**
```
[msg_id=1] [cmd=0] [paging=0]
   │         │        │
   │         │        └── NO_PAGING
   │         └── LL_COUNT_RECORDS
   └── Message ID
```

**Response:**
```
[msg_id=1] [type=0] [count=12345]
   │         │         │
   │         │         └── Record count (xas2)
   │         └── BINARY_PAGING_NONE
   └── Message ID
```

### Get Records In Range (Paged)

**Request:**
```
[msg_id=2] [cmd=5] [paging=1] [page_size=100] [lower_key] [upper_key]
   │         │        │           │              │           │
   │         │        │           │              │           └── Upper bound key
   │         │        │           │              └── Lower bound key
   │         │        │           └── 100 records per page
   │         │        └── PAGING_RECORD_COUNT
   │         └── LL_GET_RECORDS_IN_RANGE
   └── Message ID
```

**Response (Page 0):**
```
[msg_id=2] [type=4] [page=0] [record1] [record2] ... [record100]
   │         │        │        │
   │         │        │        └── 100 encoded records
   │         │        └── Page number
   │         └── RECORD_PAGING_FLOW
   └── Message ID
```

**Response (Final Page):**
```
[msg_id=2] [type=5] [page=3] [record1] [record2] ... [record25]
   │         │        │        │
   │         │        │        └── Final 25 records
   │         │        └── Page number
   │         └── RECORD_PAGING_LAST
   └── Message ID
```

---

## Subscription Messages

### Subscribe to Key Prefix

**Request:**
```
[msg_id=10] [cmd=61] [key_prefix_buffer]
    │          │           │
    │          │           └── Key prefix to watch
    │          └── LL_SUBSCRIBE_KEY_PREFIX_PUTS
    └── Message ID (reused for subscription updates)
```

**Update Notifications:**
```
[msg_id=10] [sub_type=1] [batch_data]
    │           │            │
    │           │            └── Batch of put records
    │           └── SUB_RES_TYPE_BATCH_PUT
    └── Original subscription message ID
```

---

## Error Handling

Errors are returned as:

```
[msg_id] [type=10] [error_data]
   │        │          │
   │        │          └── Error details (encoded)
   │        └── ERROR_MESSAGE
   └── Original message ID
```

**Note**: Error handling is inconsistent in the codebase. Many errors cause server-side crashes rather than proper error responses. See CRITICAL_ISSUES.md for details.
