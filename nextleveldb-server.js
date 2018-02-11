var jsgui = require('jsgui3');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
var Fns = jsgui.Fns;

var Evented_Class = jsgui.Evented_Class;

const http = require('http');
const url = require('url');
const os = require('os');
const path = require('path');

const rimraf = require('rimraf');

var WebSocket = require('websocket');
var WebSocketServer = WebSocket.server;

var xas2;
//var encodings = require('./encodings-core');
var x = xas2 = require('xas2');

var fs = require('fs');

// Extra fs tools?
//  That could be worth separating.

var fs2 = jsgui.fs2;

//var handle_http = require('./handle-http');
//var handle_ws_utf8 = require('./handle-ws-utf8');

var handle_ws_binary = require('./handle-ws-binary');

// handle_ws_binary

//var Binary_Encoding = require('binary-encoding');
var Binary_Encoding = require('binary-encoding');
var Binary_Encoding_Record = Binary_Encoding.Record;

var recursive_readdir = require('recursive-readdir');
var Running_Means_Per_Second = require('./running-means-per-second');

var levelup = require('level');

var Model = require('nextleveldb-model');
var Model_Database = Model.Database;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;

class NextLevelDB_Server extends Evented_Class {
    constructor(spec) {
        super();
        this.db_path = spec.db_path;
        this.port = spec.port;
    }
    start(callback) {
        //console.log('this.db_path', this.db_path);
        var options = this.db_options = {
            'keyEncoding': 'binary',
            'valueEncoding': 'binary'
        };

        var that = this;

        this.using_prefix_put_alerts
        this.map_b64kp_subscription_put_alerts = {};
        this.map_b64kp_subscription_put_alert_counts = {};


        var db;

        //if (db_already_exists) {
        //    db = that.db = replace_db_put(levelup(that.db_path, options), that);
        //}

        //var fns_ws = {};

        var running_means_per_second = that.running_means_per_second = new Running_Means_Per_Second();
        running_means_per_second.start_single_line_log();

        var server = http.createServer(function(request, response) {
            handle_http(request, response);
        });

        server.listen(that.port, function() {
            //console.log((new Date()) + ' Server is listening on port 8080');
            console.log("Server is listening on port " + that.port + ', using database path ' + path.normalize(that.db_path));
            //callback(null, that.port);
        });

        var wsServer = new WebSocketServer({
            httpServer: server,
            maxReceivedFrameSize: 512000000,
            autoAcceptConnections: false
        });

        function originIsAllowed(origin) {
            console.log('originIsAllowed origin', origin);
            // put logic here to detect whether the specified origin is allowed.
            return true;
        };

        //db = db || (that.db = replace_db_put(levelup(that.db_path, options), that));
        db = db || levelup(this.db_path, this.db_options);
        this.db = db;

        var db_already_exists = false;

        //console.log('this.db_path', this.db_path);

        fs.exists(this.db_path, (db_dir_exists) => {

            if (db_dir_exists) {

                fs2.dir_contents(this.db_path, (err, res_contents) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('res_contents', res_contents);

                        if (res_contents.files) {
                            db_already_exists = true;

                        } else {
                            //db_already_exists = false;
                        }
                        proceed();
                    }
                });
            } else {
                proceed();
            }
        });

        var proceed = () => {
            if (db_already_exists) {
                proceed_2();
            } else {

                //that.fns_ws.initial_db_setup(callback);
                // rest of the init
                proceed_2();
            }
        }
        var next_connection_id = 0;

        var proceed_2 = () => {
            wsServer.on('request', function(request) {

                if (!originIsAllowed(request.origin)) {
                    // Make sure we only accept requests from an allowed origin
                    request.reject();
                    console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
                    return;
                }
                var connection = request.accept('echo-protocol', request.origin);
                connection.id = next_connection_id++;

                //
                //console.log((new Date()) + ' Connection accepted.');
                connection.on('message', function(message) {
                    console.log('message', message);
                    if (message.type === 'utf8') {
                        throw 'deprecating http interface';

                        //handle_ws_utf8(connection, that, that.fns_ws, message);
                    } else if (message.type === 'binary') {
                        console.log('Received Binary Message of ' + message.binaryData.length + ' bytes');
                        handle_ws_binary(connection, that, message.binaryData);


                        //connection.sendBytes(message.binaryData);
                    }
                });
                connection.on('close', function(reasonCode, description) {
                    console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.\n');
                    console.log('reasonCode, description', reasonCode, description);
                    // Then need to unsubscribe from event handler.

                    // Cancel the subscriptions.

                    var cancel_subscriptions = function() {
                        each(connection.subscription_handlers, (subscription_handler, event_name) => {
                            that.off(event_name, subscription_handler);
                        })
                    };
                    cancel_subscriptions();
                });
            });
            callback(null, true);
        }

        //var proceed = function () {

        //};
    }

    ll_wipe(callback) {
        // disconnect from the database
        var that = this;

        this.db.close((err) => {
            if (err) { callback(err); } else {
                // Then rimraf the db directory

                rimraf(this.db_path, (err) => {
                    if (err) { callback(err); } else {
                        // Create a new db in place

                        this.db = levelup(that.db_path, that.db_options);

                        callback(null, this.db);

                    }
                })
            }
        })
    }

    ll_subscribe_all(callback_update) {

        var sub_msg_id = 0;
        var that = this;

        var process_subscription_event = (e) => {
            //console.log('server process_subscription_event', e);
            e.sub_msg_id = sub_msg_id++;
            callback_update(e);
        }

        this.on('db_action', process_subscription_event);

        // could return an unsubscribe function.

        var unsubscribe = () => {
            that.off('db_action', process_subscription_event);
            // return
            // why the increment here?
            return sub_msg_id++;

        }

        process_subscription_event({
            'type': 'connected'
        });

        return unsubscribe;

    }

    ll_subscribe_key_prefix_puts(buf_kp, callback_update) {

        var sub_msg_id = 0;
        var that = this;
        console.log('** buf_kp', buf_kp);

        var b64_kp = buf_kp.toString('hex');
        console.log('b64_kp', b64_kp);
        //throw 'stop';

        // Working on batching these

        var process_subscription_event = (e) => {
            //console.log('server process_subscription_event', e);
            e.sub_msg_id = sub_msg_id++;

            callback_update(e);
        }

        var evt_name = 'put_kp_batch_' + b64_kp;

        this.on(evt_name, process_subscription_event);

        if (this.map_b64kp_subscription_put_alert_counts[b64_kp]) {
            this.map_b64kp_subscription_put_alert_counts[b64_kp]++;
            //this.map_b64kp_subscription_put_alerts[b64_kp].push(process_subscription_event);
        } else {
            this.map_b64kp_subscription_put_alert_counts[b64_kp] = 1;
            //this.map_b64kp_subscription_put_alerts[b64_kp] = [process_subscription_event];
        }


        this.using_prefix_put_alerts = true;

    }

    load_model(callback) {
        // only loads the core system db rows for the moment.
        var that = this;

        this.get_system_db_rows((err, system_db_rows) => {
            // These system db rows could be wrong.
            //  Could be a problem with the existing DB's model records.



            if (err) { callback(err); } else {
                that.model = Model_Database.load(system_db_rows);
                callback(null, that.model);
            }
        });

    }

    ll_count_keys_in_range(buf_l, buf_u, callback) {
        var count = 0;
        //var res = [];
        this.db.createKeyStream({
            'gt': buf_l,
            'lt': buf_u
        }).on('data', function(key) {
            //arr_res.push(x(key.length).buffer);
            //console.log('key', key);
            //arr_res.push(key);
            count++;
        }).on('error', function(err) {
            //console.log('Oh my!', err)
            callback(err);
        }).on('close', function() {
            //console.log('Stream closed')
        }).on('end', function() {
            //callback(null, res);
            //console.log('*** count', count);
            callback(null, count);
        });
    }

    get core_lu_buffers() {
        var res = [xas2(CORE_MIN_PREFIX).buffer, xas2(CORE_MAX_PREFIX).buffer];
        return res;
    }

    count_core(callback) {
        var [bl, bu] = this.core_lu_buffers;
        this.ll_count_keys_in_range(bl, bu, callback);
        //this.
    }

    ll_count(callback) {
        var count = 0;
        this.db.createKeyStream({}).on('data', function(key) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                //res_keys.push(encodeURI(key));
                count++;
            }).on('error', function(err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function() {
                //console.log('Stream closed')
            })
            .on('end', function() {
                callback(null, count);
            })
    }

    // No paging on this one.
    get_all_db_keys(callback) {
        var res = [];
        this.db.createKeyStream({}).on('data', function(key) {
                res.push(key);
            })
            .on('error', function(err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function() {
                //console.log('Stream closed')
            })
            .on('end', function() {
                callback(null, res);
            })
    }

    get_all_db_rows(callback) {
        // This has got not means to cancel it. Simple version.
        var res_records = [];

        this.db.createReadStream({}).on('data', function(record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                res_records.push([record.key, record.value]);
            })
            .on('error', function(err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function() {
                //console.log('Stream closed')
            })
            .on('end', function() {
                callback(null, res_records);
            })
    }

    // Get system db rows...

    get_system_db_rows(callback) {
        // tables ids 0, 1, 2, 3
        // tables, native types, table fields, table indexes

        // so, the very start of the key space between 0 and 7 (1 + 2 * 3)  1 being a 0 indexed 2
        //  tables key space starts at 2, each table has got 2 key spaces
        var res_records = [];

        this.db.createReadStream({
                'gte': xas2(CORE_MIN_PREFIX).buffer,
                'lte': xas2(CORE_MAX_PREFIX).buffer
            }).on('data', function(record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                res_records.push([record.key, record.value]);
            })
            .on('error', function(err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function() {
                //console.log('Stream closed')
            })
            .on('end', function() {
                callback(null, res_records);
            })
    }

    get_system_db_buffer(callback) {
        // tables ids 0, 1, 2
        // tables, native types, table fields


        // so, the very start of the key space between 0 and 7 (1 + 2 * 3)  1 being a 0 indexed 2
        //  tables key space starts at 2, each table has got 2 key spaces

        // May be nice to continuously add to a large buffer?
        //  Encode each row as buffer...

        // Build up the buffer in the simple way with row lengths.

        var arr_buf_res = [];

        this.db.createReadStream({
                'gte': xas2(0).buffer,
                'lte': xas2(9).buffer
            }).on('data', function(record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                //res_records.push([record.key, record.value]);

                arr_buf_res.push(Binary_Encoding.join_buffer_pair([record.key, record.value]));

            })
            .on('error', function(err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function() {
                //console.log('Stream closed')
            })
            .on('end', function() {
                //console.log('arr_buf_res', arr_buf_res);

                callback(null, Buffer.concat(arr_buf_res));
            })
    }


    // an array batch put too.
    //  Would not need to decode the buffer.

    arr_batch_put(arr_bufs, callback) {

        // Need to do more to standardise the key prefix subscriptions
        //  and db put notifications to subscribers.

        let ops = [],
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;
        let buf_empty = Buffer.alloc(0);

        each(arr_bufs, item => {
            if (Array.isArray(item)) {

                if (this.using_prefix_put_alerts) {
                    //prefix_put_alerts_batch = [];
                    var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                    b64_key = item[0].toString('hex');

                    // Better to use a map and array.
                    //  Maybe the standard event based system would be fine.
                    //  Do more work on subscription handling.

                    for (key in this.map_b64kp_subscription_put_alert_counts) {
                        //console.log('key', key);
                        if (b64_key.indexOf(key) === 0) {
                            //console.log('found matching put alert key prefix', key);
                            map_key_batches[key] = map_key_batches[key] || [];
                            map_key_batches[key].push(arr_row);
                        }
                    }
                }

                ops.push({
                    'type': 'put',
                    'key': item[0],
                    'value': item[1]
                });
            } else {
                ops.push({
                    'type': 'put',
                    'key': item,
                    'value': buf_empty
                });
            }
        });

        var that = this;
        db.batch(ops, function(err) {
            if (err) {
                callback(err);
            } else {


                that.raise('db_action', {
                    'type': 'arr_batch_put',
                    'value': arr_bufs
                });

                /*
                each(map_key_batches, (map_key_batch, key) => {
                    //console.log('1) key', key);
                    //console.log('map_key_batch', map_key_batch);


                    var buf_encoded_batch = Model_Database.encode_model_rows(map_key_batch);
                    that.raise('put_kp_batch_' + key, {
                        'type': 'batch_put',
                        'buffer': buf_encoded_batch
                    });
                });
                */
                callback(null, true);
            }
        })
    }

    batch_put(buf, callback) {
        var ops = [],
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;

        Binary_Encoding.evented_get_row_buffers(buf, (arr_row) => {
            if (this.using_prefix_put_alerts) {
                //prefix_put_alerts_batch = [];
                var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                b64_key = arr_row[0].toString('hex');

                // Better to use a map and array.
                //  Maybe the standard event based system would be fine.
                //  Do more work on subscription handling.

                for (key in this.map_b64kp_subscription_put_alert_counts) {
                    //console.log('key', key);
                    if (b64_key.indexOf(key) === 0) {
                        //console.log('found matching put alert key prefix', key);
                        map_key_batches[key] = map_key_batches[key] || [];
                        map_key_batches[key].push(arr_row);
                    }
                }
            }
            ops.push({
                'type': 'put',
                'key': arr_row[0],
                'value': arr_row[1]
            });
        });

        //console.log('ops', JSON.stringify(ops, null, 2));
        //throw 'stop';
        var that = this;
        db.batch(ops, function(err) {
            if (err) {
                callback(err);
            } else {

                that.raise('db_action', {
                    'type': 'batch_put',
                    'buffer': buf
                });

                each(map_key_batches, (map_key_batch, key) => {
                    //console.log('1) key', key);
                    console.log('map_key_batch', map_key_batch);
                    //var buf_encoded_batch = Model_Database.encode_arr_rows_to_buf(map_key_batch);
                    //var buf_encoded_batch = Binary_Encoding.encode_to_buffer(map_key_batch);

                    //var buf_encoded_batch = Model_Database.encode_arr_rows_to_buf(map_key_batch);
                    var buf_encoded_batch = Model_Database.encode_model_rows(map_key_batch);


                    //console.log('buf_encoded_batch', buf_encoded_batch);
                    that.raise('put_kp_batch_' + key, {
                        'type': 'batch_put',
                        'buffer': buf_encoded_batch
                    });
                });
                callback(null, true);
            }
        })

    }
}

// Run it from the command line with a path?

let custom_path = '';

custom_path = 'D:\\NextlevelDB\\DB1'

if (require.main === module) {
    var user_dir = os.homedir();
    //console.log('user_dir', user_dir);
    //var docs_dir =

    var path_dbs = user_dir + '/NextLevelDB/dbs'

    // Would also be worth being able to choose db names


    fs2.ensure_directory_exists(user_dir + '/NextLevelDB', (err, exists) => {
        if (err) { throw err } else {

            fs2.ensure_directory_exists(path_dbs, (err, exists) => {
                if (err) { throw err } else {
                    //console.log('dir now exists');
                    //throw 'stop';

                    var db_path = custom_path || path_dbs + '/default';

                    //var db_path = 'db';
                    var port = 420;

                    // Is the first one the node executable?

                    console.log('process.argv.length', process.argv.length);
                    console.log('process.argv', process.argv);

                    if (process.argv.length === 2) {
                        //db_path = process.argv[1];
                    }
                    if (process.argv.length === 3) {
                        db_path = path_dbs + '/' + process.argv[2];
                    }
                    if (process.argv.length === 4) {
                        //db_path = process.argv[2];
                        db_path = path_dbs + '/' + process.argv[2];
                        port = parseInt(process.argv[3]);
                    }

                    var ls = new NextLevelDB_Server({
                        'db_path': db_path,
                        'port': port
                    });

                    // There could be a web admin interface too.

                    ls.start((err, res_started) => {
                        if (err) {
                            console.trace();
                            throw err;
                        } else {
                            console.log('NextLevelDB Started');

                            var start_with_core_model = () => {
                                // Could do an initial db setup...

                                ls.count_core((err, count) => {
                                    if (err) {
                                        throw err;
                                    }
                                    console.log('\n1) count', count);

                                    if (count === 0) {

                                    } else {
                                        // Get all of the records...
                                        //  Non-streaming.

                                        // Should probably have a Binary_Query processor.
                                        /*
                                        ls.get_all_db_rows((err, arr_db_rows) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('arr_db_rows', arr_db_rows);
                                            }
                                        });
                                        */

                                        ls.load_model((err, model) => {
                                            if (err) { throw err; } else {
                                                //console.log('model', model);
                                                console.log('model loaded');

                                                // get_arr_table_ids_and_names

                                                console.log('ls.model.get_arr_table_ids_and_names', ls.model.arr_table_ids_and_names);
                                                console.log('ls.model.description', ls.model.description);


                                            }
                                        })
                                    }
                                });
                            }
                            start_with_core_model();
                        }
                    });
                }
            });
        }
    });
} else {
    //console.log('required as a module');
}


module.exports = NextLevelDB_Server;