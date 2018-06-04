const jsgui = require('jsgui3');
const tof = jsgui.tof;
const each = jsgui.each;
const is_array = jsgui.is_array;
const arrayify = jsgui.arrayify;
const get_a_sig = jsgui.get_a_sig;
const Fns = jsgui.Fns;
const def = jsgui.is_defined;
//const clone = jsgui.clone;



const Evented_Class = jsgui.Evented_Class;
const crypto = require('crypto');
const http = require('http');
const url = require('url');
const os = require('os');
const path = require('path');
const rimraf = require('rimraf');
const WebSocket = require('websocket');
const WebSocketServer = WebSocket.server;

const commandLineArgs = require('command-line-args');
const deep_diff = require('deep-diff').diff;

let xas2;
const x = xas2 = require('xas2');
const fs = require('fs');


const deep_equal = require('deep-equal');
// Extra fs tools?
//  That could be worth separating.

const fs2 = jsgui.fs2;
const handle_ws_binary = require('./handle-ws-binary');
//var Binary_Encoding = require('binary-encoding');
const Binary_Encoding = require('binary-encoding');
const Binary_Encoding_Record = Binary_Encoding.Record;

const recursive_readdir = require('recursive-readdir');
const Running_Means_Per_Second = require('./running-means-per-second');

const levelup = require('level');

const Model = require('nextleveldb-model');
const B_Record_List = Model.Record_List;
const B_Record = Model.BB_Record;
const B_Key = Model.BB_Key;
const Key_List = Model.Key_List;

const fnl = require('../fnl/fnl');
const observable = fnl.observable;
const execute_q_obs = fnl.seq;
const sig_obs_or_cb = fnl.sig_obs_or_cb;
const prom_or_cb = fnl.prom_or_cb;
//const



const Model_Database = Model.Database;
const encoding = Model.encoding;
const Index_Record_Key = Model.Index_Record_Key;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;


const NextlevelDB_Core_Server = require('./nextleveldb-core-server');


const obs_to_cb = (obs, callback) => {
    let arr_all = [];
    obs.on('next', data => arr_all.push(data));
    obs.on('error', err => callback(err));
    obs.on('complete', () => callback(null, arr_all));
}

const obs_or_cb = (obs, callback) => {
    let _obs;
    if (typeof obs === 'function') {
        _obs = observable(obs);
    } else {
        _obs = obs;
    }

    // then if a[0] is a function, make a new Observable with that function.


    if (callback) {
        obs_to_cb(_obs, callback);
    } else {
        return _obs;
    }
}

let obs_map = (obs, fn_data) => {
    let res = new Evented_Class();
    fn_data.on('next', data => {
        res.raise('next', fn_data(data));
    })
    fn_data.on('complete', () => res.raise('complete'));
    fn_data.on('error', err => res.raise('error', err));
    if (obs.stop) res.stop = obs.stop;
    if (obs.pause) res.pause = obs.pause;
    if (obs.resume) res.resume = obs.resume;
    return res;
}

let obs_filter = (obs, fn_select) => {
    let res = new Evented_Class();
    fn_data.on('next', data => {
        if (fn_select(data)) {
            res.raise('next', data);
        }
    })
    fn_data.on('complete', () => res.raise('complete'));
    fn_data.on('error', err => res.raise('error', err));
    if (obs.stop) res.stop = obs.stop;
    if (obs.pause) res.pause = obs.pause;
    if (obs.resume) res.resume = obs.resume;
    return res;
}

let obs_arrayified_call = (caller, fn, arr_params) => {
    // Just execure one at a time.
    let res = new Evented_Class();
    //throw 'stop';
    let c = 0,
        l = arr_params.length,
        ctu = true;

    let process = () => {
        if (ctu) {
            if (c < l) {
                let arr_call_params = arr_params[c];
                c++;
                console.log('arr_call_params', arr_call_params);
                //throw 'stop';
                let process_obs = fn.call(caller, arr_call_params);
                process_obs.on('next', data => {
                    //console.log('data', data);
                    //console.trace();
                    res.raise('next', data);
                });
                process_obs.on('error', err => {
                    res.raise('error', err);
                    ctu = false;
                    // And stop processing?
                    //  Raising 'complete'?
                });
                process_obs.on('complete', () => {
                    //res.raise('complete')
                    process();
                });
            } else {
                // All are complete.
                res.raise('complete');
            };
        } else {
            // Been an error.
        }
    }
    process();
    return res;
}


let kp_to_range = buf_kp => {
    let buf_0 = Buffer.alloc(1);
    buf_0.writeUInt8(0, 0);
    let buf_1 = Buffer.alloc(1);
    buf_1.writeUInt8(255, 0);
    // and another 0 byte...?

    return [Buffer.concat([buf_kp, buf_0]), Buffer.concat([buf_kp, buf_1])];
}

let differencing = Model_Database.model_rows_diff;

let get_map_cookies = request_cookies => {
    let res = {};
    each(request_cookies, request_cookie => {
        res[request_cookie.name] = request_cookie.value
    })
    return res;
}


// bufs_lu = (buf_kp)

// Inherit from core-server
//  Will then use mixin functionality

class NextLevelDB_Core_Server extends Evented_Class {
    constructor(spec) {
        super();
        this.db_path = spec.db_path || spec.path;
        this.port = spec.port;

        // Core / ll does not use model?
        // Would make sense for core functions to use a model?
        //  Or just to send them direct to the server and get the result.

    }

    get core_lu_buffers() {
        var res = [xas2(CORE_MIN_PREFIX).buffer, xas2(CORE_MAX_PREFIX).buffer];
        return res;
    }

    get all_table_ids() {
        let res = [];
        each(this.model.tables, table => res.push(table.id));
        return res;
    }

    // get this from the model.
    //  the model is isomorphic.


    get table_ids_with_indexes() {
        return this.model.table_ids_with_indexes;
    }
    start(callback) {
        return prom_or_cb((resolve, reject) => {
            var options = this.db_options = {
                'keyEncoding': 'binary',
                'valueEncoding': 'binary'
            };

            var that = this;
            this.using_prefix_put_alerts; // ???
            this.map_b64kp_subscription_put_alerts = {};
            this.map_b64kp_subscription_put_alert_counts = {};
            var db;

            var running_means_per_second = that.running_means_per_second = new Running_Means_Per_Second();
            running_means_per_second.start_single_line_log();

            var server = http.createServer(function (request, response) {
                handle_http(request, response);
            });

            server.listen(that.port, function () {
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
                console.log('\noriginIsAllowed origin', origin);
                //throw 'stop';
                // put logic here to detect whether the specified origin is allowed.
                return true;
            };
            // need to load access tokens from the config

            this.map_access_tokens = {};

            let load_config_access_tokens = () => {
                var config = require('my-config').init({
                    path: path.resolve('../../config/config.json') //,
                    //env : process.env['NODE_ENV']
                    //env : process.env
                });
                let root_tokens = config.nextleveldb_access.root;
                console.log('root_tokens', root_tokens);
                //this.map_access_tokens.root = root_tokens;
                each(root_tokens, root_token => {
                    this.map_access_tokens[root_token] = 'root';
                })
                console.log('this.map_access_tokens', this.map_access_tokens);
                //this.map_access_tokens[]
                //console.log('this.map_access_tokens.root', this.map_access_tokens.root);
            }
            load_config_access_tokens();

            let check_access_token = access_token => {
                let username = this.map_access_tokens[access_token];
                console.log('username', username);
                let allowed = false;
                if (username === 'root') {
                    allowed = true;
                }
                return allowed;
            }
            //db = db || (that.db = replace_db_put(levelup(that.db_path, options), that));

            var db_already_exists = false;
            //console.log('this.db_path', this.db_path);
            fs.exists(this.db_path, (db_dir_exists) => {
                if (db_dir_exists) {
                    fs2.dir_contents(this.db_path, (err, res_contents) => {
                        if (err) {
                            //callback(err);
                            reject(err);
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

                db = db || levelup(this.db_path, this.db_options);
                this.db = db;

                if (db_already_exists) {

                    // Load the model from the database.

                    this.load_model((err, model) => {
                        if (err) {
                            //throw err;
                            reject(err);
                        } else {
                            //console.log('model', model);
                            //console.log('model rows', model.get_model_rows());
                            //throw 'stop';
                            this.model = model;
                            proceed_2();
                        }
                    })
                } else {
                    // need to create the model, then ll_put those records into the db.
                    let model = new Model_Database();
                    //console.log('model', model);
                    let buf = model.get_model_rows_encoded();
                    // Could move away from 'model rows' and use these BB_Rows or a Row_List.
                    //  Row_List can represent all of them, and 

                    //console.log('buf.length', buf.length);
                    //console.log('decoded new model', Model_Database.decode_model_rows(buf));

                    var arr_core = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf);
                    //console.log('arr_core', arr_core);
                    // 13/03/2018 The table incremntor is OK here.
                    //throw 'stop';

                    // Now, since we have it serialised as binary from the Model_Database, we should be able to use a (new) low level function to put a binary ll record block/array into the DB.
                    //  This is getting on for a very large amount of functionality since the last update.
                    //  Moving more functionality from the client side into the db server, then will make it available through appropriate APIs.


                    this.batch_put(buf, () => {
                        // A problem decoding the buffer on the server side?
                        //  Seems not, it's got the correct ops.

                        this.get_all_db_rows((err, all_db_rows) => {
                            if (err) {
                                //callback(err);
                                reject(err);
                            } else {
                                //console.log('all_db_rows', all_db_rows);
                                this.model = model;
                                proceed_2();
                            }
                        })
                    });
                }
            }
            var next_connection_id = 0;

            var proceed_2 = () => {

                wsServer.on('request', function (request) {

                    //console.log('request', request);
                    //console.log('request.origin', request.origin);

                    //console.log('Object.keys(request)', Object.keys(request));
                    //console.log('Object.keys(request.socket)', Object.keys(request.socket));
                    // console.log('(request.socket)', (request.socket));
                    //console.log('Object.keys(request.socket.server)', Object.keys(request.socket.server));
                    //console.log('Object.keys(request.socket._peername)', Object.keys(request.socket._peername));
                    //console.log('(request.socket._peername)', (request.socket._peername));

                    //console.log('request.cookies', request.cookies);

                    let map_cookies = get_map_cookies(request.cookies);
                    //console.log('map_cookies', map_cookies);

                    let provided_access_token = map_cookies['access_token'];
                    // Check the provided access token to see if it's allowed.

                    let access_allowed = check_access_token(provided_access_token);
                    //console.log('access_allowed', access_allowed);
                    if (!access_allowed) {
                        request.reject();
                        console.log((new Date()) + ' Valid access token required.');
                        return;
                    } else {

                        if (!originIsAllowed(request.origin)) {
                            request.reject();
                            console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
                            return;
                        }

                        // // This is a possible place to check authentication and authorisation.
                        // Could just check for a valid access token. If a valid token is provided, then we can continue.
                        //  Worth handing it to an auth module. Authenticates a user has the access token, authorises them to connect.

                        var connection = request.accept('echo-protocol', request.origin);
                        connection.id = next_connection_id++;
                        //console.log((new Date()) + ' Connection accepted.');
                        connection.on('message', function (message) {
                            //console.log('message', message);
                            if (message.type === 'utf8') {
                                throw 'deprecating utf8 interface';
                            } else if (message.type === 'binary') {
                                handle_ws_binary(connection, that, message.binaryData);
                            }
                        });
                        connection.on('close', function (reasonCode, description) {
                            console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.\n');
                            console.log('reasonCode, description', reasonCode, description);
                            // Then need to unsubscribe from event handler.
                            // Cancel the subscriptions.
                            var cancel_subscriptions = function () {
                                each(connection.subscription_handlers, (subscription_handler, event_name) => {
                                    that.off(event_name, subscription_handler);
                                })
                            };
                            cancel_subscriptions();
                        });
                    }
                });
                resolve(true);
                //callback(null, true);
            }
        }, callback);
        //console.log('this.db_path', this.db_path);

    }

    ll_wipe(callback) {
        // disconnect from the database
        var that = this;

        this.db.close((err) => {
            if (err) {
                callback(err);
            } else {
                // Then rimraf the db directory
                rimraf(this.db_path, (err) => {
                    if (err) {
                        callback(err);
                    } else {
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
        //console.log('** buf_kp', buf_kp);

        var b64_kp = buf_kp.toString('hex');
        //console.log('b64_kp', b64_kp);
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

    // could have the limit option too, and have an observable result.

    // with a limit too?
    ll_count_keys_in_range(buf_l, buf_u, limit = -1, callback) {
        // gets more complex with an observable.
        //  should ping the count every 1000ms

        // This completes with its value.
        //  A last value.

        // or just a function o(...)

        // a general purpose observable (paradigm) function

        // 

        // A function to get the current value too?

        return sig_obs_or_cb(arguments, (a, sig, next, complete, error, l) => {
            if (l === 1) {
                [buf_l, buf_u] = a[0];
            } else if (l === 2) {
                if (sig === '[B,B]') {

                } else {
                    throw 'NYI';
                }
                // array and number
                //console.log('buf_l, buf_u', buf_l, buf_u);
            }

            // A timer providing interim updates?
            //  Still will need the last result.

            var count = 0;
            //var res = [];
            let stream = this.db.createKeyStream({
                'gt': buf_l,
                'lt': buf_u,
                'limit': limit
            }).on('data', function (key) {
                //arr_res.push(x(key.length).buffer);
                //console.log('key', key);
                //arr_res.push(key);
                count++;
            }).on('error', error).on('close', function () {
                //console.log('Stream closed')
            }).on('end', function () {
                complete(count);
            });

            return [() => {
                read_stream.destroy();
                //res.raise('complete');
                // or stopped without being completed?
                complete();
            }, () => {
                if (!read_stream.isPaused()) {
                    read_stream.pause();
                    //return res.resume;
                }
            }, () => {
                if (read_stream.isPaused()) {
                    read_stream.resume();
                }
            }];
        });
    }

    // May be obselete, new and possibly more flexible version.

    ll_get_records_in_range(arr_buf_range, limit = -1, callback) {
        let sig = get_a_sig(arguments);
        if (sig === '[a,f]') {
            callback = a[1];
            limit = -1;
            //throw 'stop';
        } else if (sig === '[a]') {

        } else if (sig === '[a,n]') {

        } else {
            throw 'NYI stop';
        }

        return obs_or_cb(observable((next, complete, error) => {
            let read_stream = this.db.createReadStream({
                'gte': arr_buf_range[0],
                'lte': arr_buf_range[1],
                'limit': limit
            }).on('data', record => {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                //res_records.push([record.key, record.value]);
                //console.log('record', record);
                next(new B_Record([record.key, record.value]));
            }).on('error', error).on('close', () => {
                //console.log('Stream closed')
            }).on('end', complete);
            return [() => {
                read_stream.destroy();
                //res.raise('complete');
                // or stopped without being completed?
                complete();
            }, () => {
                if (!read_stream.isPaused()) {
                    read_stream.pause();
                    //return res.resume;
                }
            }, () => {
                if (read_stream.isPaused()) {
                    read_stream.resume();
                }
            }];
        }), callback);
    }
    // The keys are each a buffer.
    ll_delete_records_by_keys(arr_keys, callback) {
        // Could have a batch limit?
        // Promise
        return prom_or_cb((resolve, reject) => {
            let ops = [];
            // encode these keys...

            // If the keys are not already encoded.
            //let encoded_keys = encoding.encode_keys(arr_keys);

            each(arr_keys, key => {
                ops.push({
                    type: 'del',
                    key: key.buffer || key
                });
            });
            this.db.batch(ops, (err) => {
                if (err) {
                    //callback(err);
                    reject(err);
                } else {
                    this.raise('db_action', {
                        'type': 'arr_batch_delete',
                        'value': arr_keys
                    });
                    //callback(null, true);
                    resolve(true);
                }
            })
        }, callback);
    }
    // optional paging as well.
    //  paging would require an observable.

    // This will be used to get the table records.
    //  Iterating over the table records will be a useful way of getting the records.
    //  Nice to have each record in an observable type callback (in some cases). Further from low level speed though.

    // Should get rid of decoding option,
    //  but return records that can decode themselves, but stay as buffered data normally.
    //   should be a decent mix of fast and convenient

    ll_get_records_with_kp(kp, callback) {
        return obs_or_cb(this.ll_get_records_in_range(kp_to_range(xas2(kp).buffer)), callback);
    }
    // Because counting can take a while, need to use observable count.

    // will have limit as well
    //  no range specified.

    // just call it count.
    //  it's the core count now.
    //  coult be overridden.
    //   will be changed to accept params / range

    count(callback) {

        // change to observable


        // how about an inner function that gets the argument sig, and has the internal definition for observable.
        // sig_obs
        //  and if the last one is a function, it returns using a callback.
        //console.log('count !!callback', !!callback);
        return obs_or_cb((next, complete, error) => {
            var count = 0;
            var delay = 1000;
            let repeater = setInterval(() => {
                //res.raise('next', count);
                next(count);

                //console.log('count', count);

            }, delay);
            //console.log('counting');

            let stream = this.db.createKeyStream({}).on('data', function (key) {
                count++;
            }).on('error', error)
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    clearInterval(repeater);
                    complete(count);
                });

            return [];
        }, callback);
    }

    // Could be possible to have decoding option here too.
    //  Decoding option could be further down the stack and passed through.

    // May be a good usage of js iterators or generators.
    //

    // Likely change this.
    //  ll version won't decode
    //  Will return records as buffer-backed

    // Won't have decoding here.
    ll_get_table_records(table_id, opt_cb) {
        let kp = table_id * 2 + 2;
        let obs = this.ll_get_records_with_kp(xas2(kp).buffer);
        return obs_or_cb(obs, opt_cb);
    }
    // No decoding

    ll_get_table_index_records(table_id, opt_cb) {

        let a = arguments,
            sig = get_a_sig(a);
        let decode = false;
        //console.log('ll_get_table_index_records sig', sig);
        if (sig === '[n,b]') {
            decode = a[1];
            opt_cb = null;
        }
        //throw 'stop';

        let kp = table_id * 2 + 2;
        let ikp = kp + 1;

        let obs = this.ll_get_records_with_kp(xas2(ikp).buffer);
        //console.log('obs', obs);
        //console.log('decode', decode);
        return obs_or_cb(obs, opt_cb);

    }

    delete(record, callback) {
        // B_Record
        if (record instanceof B_Record) {
            return this.delete_by_key(record.kvp_bufs[0], callback);
        } else {
            throw 'NYI';
        }
    }

    delete_by_key(buf_key, callback) {
        // want this to work as a promise.
        //return prom_or_cb()
        if (callback) {
            this.db.del(buf_key, callback);
        } else {
            return this.db.del(buf_key);
        }
    }

    load_model(callback) {
        // only loads the core system db rows for the moment.
        var that = this;
        // 
        this.get_system_db_rows((err, system_db_rows) => {
            // These system db rows could be wrong.
            //  Could be a problem with the existing DB's model records.
            if (err) {
                callback(err);
            } else {
                this.model = Model_Database.load(system_db_rows);
                let model_rows = this.model.rows;
                //  getting the model rows missing some of them out for some reason?
                if (true || model_rows.length !== system_db_rows.length) {
                    //console.log('system_db_rows.length', system_db_rows.length);
                    //console.log('model_rows.length', model_rows.length);
                    // 13/03/2018 - This is a newly discovered bug where the model does not make every table (missing the native types table) when it gets reconstructed.
                    // do a diff here?

                    //console.log('system_db_rows', Model_Database.decode_model_rows(system_db_rows));
                    //console.log('system_db_rows');

                    //each(system_db_rows, row => console.log('row.decoded', row.decoded));

                    //console.log('model_rows', Model_Database.decode_model_rows(model_rows));
                    // I think the model is not generating the index rows.
                    //throw 'stop';
                    // Not so sure that an index of incrementors is that useful....
                    //  However, missing such an index (when expected) will cause a crash here.
                    //  Could remove that index of incrementors.
                    //let models_diff = this.model.diff();
                    // Need to do db kv row diff.

                    //let diff = deep_diff(model_rows, system_db_rows);
                    //
                    //console.log('Model_Database.decode_model_rows(system_db_rows)', Model_Database.decode_model_rows(system_db_rows));
                    //let diff = Model_Database.diff_model_rows(Model_Database.decode_model_rows(system_db_rows), Model_Database.decode_model_rows(model_rows));
                    let diff = Model_Database.diff_model_rows(system_db_rows, model_rows);
                    //console.log('diff', JSON.stringify(diff, null, 2));

                    console.log('diff.changed.length', diff.changed.length);
                    console.log('diff.added.length', diff.added.length);
                    console.log('diff.deleted.length', diff.deleted.length);


                    //console.log('diff.added', diff.added);

                    //each(diff.changed, changed => console.log('changed', changed));
                    each(diff.added, added => {
                        //console.log('added', added);
                        console.log('added', added.decoded);
                    });

                    each(diff.changed, changed => {
                        console.log('changed[0]', changed[0].decoded);
                        console.log('changed[1]', changed[1].decoded);
                    });

                    each(diff.deleted, deleted => {
                        console.log('deleted', deleted.decoded);
                    });


                    if (diff.same) {
                        callback(null, that.model);
                    } else {
                        console.log('Object.keys(diff)', Object.keys(diff));

                        // Though this error is very annoying, it will help to keep things in sync and prevent it from getting worse.
                        //  This checks that the model rows have been loaded properly from the DB

                        console.log('system_db_rows.length', system_db_rows.length);
                        console.log('model_rows.length', model_rows.length);
                        callback(new Error('Mismatch between core db rows and core rows obtained from model. '));
                    }
                } else {
                    callback(null, that.model);
                }
            }
        });
    }

    count_core(callback) {
        var [bl, bu] = this.core_lu_buffers;
        return this.ll_count_keys_in_range(bl, bu, callback);
        //this.
    }

    // needed in the core.
    persist_row_diffs(row_diffs, callback) {
        // Shouldn't use the batch put system I think?
        //  If it did, listeners would be able to respond to the events.
        // row_diffs will now deal with the buffer backed row and record types.

        return prom_or_cb((resolve, reject) => {
            let ops = [];
            // For the moment, will batch it up into ops.
            each(row_diffs.deleted, record => {
                // these records map have kvp_bufs
                //console.log('del record', record.decoded);
                ops.push({
                    'type': 'del',
                    'key': record[0] || record.kvp_bufs[0]
                });
            })
            each(row_diffs.added, record => {
                //console.log('put record', record.decoded);
                ops.push({
                    'type': 'put',
                    'key': record[0] || record.kvp_bufs[0],
                    'value': record[1] || record.kvp_bufs[1]
                });
            })
            each(row_diffs.changed, record_pair => {
                //console.log('changed record \nbefore', record_pair[0].decoded, '\nafter', record_pair[1].decoded);
                let new_record = record_pair[1];
                // delete at the old keys
                ops.push({
                    'type': 'del',
                    'key': record_pair[0][0] || record_pair[0].kvp_bufs[0]
                });
                ops.push({
                    'type': 'put',
                    'key': new_record[0] || new_record.kvp_bufs[0],
                    'value': new_record[1] || new_record.kvp_bufs[1]
                });
            });
            //console.log('ops', ops);
            //throw 'stop';
            this.db.batch(ops, (err) => {
                if (err) {
                    //callback(err);
                    reject(err);
                } else {
                    resolve();

                    /*
                    this.raise('db_action', {
                        'type': 'arr_batch_put',
                        'value': ops
                    });
                    */

                    //callback(null, true);
                }
            })
        }, callback);
    }



    // Records with incomplete keys...
    //  Could be a fine format for when we know the record will be assigned an id by the table.

    // Model will have some details about that.
    //  want to be able to create records (for testing) without changing the incrementor or adding them to the model.
    //  Then after passing tests the record could be added.

    // Will also have ensure_record check against indexes.

    /*
    ensure_record(record, callback) {
        return prom_or_cb((resolve, reject) => {

            // Do no overwrite records

            // Not changing records

            // Do we already have the record, based on key

            // Are any of the key fields undefined?

            // At the moment, can only generate one autoincrementing field

            // need to get the table id from the record.

            let table_id = record.table_id;
            let model_table = this.model.map_tables_by_id[table_id];
            // Then the number of autoincrementing fields in the pk.
            let model_pk = model_table.pk;

        }, callback);
    }
    */


    has(record_or_key, callback) {
        return prom_or_cb((resolve, reject) => {
            let key = record_or_key.key;
            let buf_key = key.buffer;
            this.db.get(buf_key, function (err, value) {
                if (err) {
                    if (err.notFound) {
                        // handle a 'NotFoundError' here
                        resolve(false);
                    } else {
                        reject(err);
                    }
                    // I/O or other error, pass it up the callback chain
                    //return callback(err)
                } else {
                    resolve(true);
                }
                // .. handle `value` here
            })
        }, callback);
    }

    // May get more complicated?
    //  ensure_table_record

    // where we are told what table its for.


    // table index lookup
    //  table id, index id, data

    // Table record lookup

    table_record_lookup(table_id, data, callback) {
        return prom_or_cb((resolve, reject) => {

            (async () => {
                //console.log('table_record_lookup table_id, data', table_id, data);
                let model_table = this.model.map_tables_by_id[table_id];
                // calculate lookups from model table and data
                //  could be in the Model even.

                let kv_fields = model_table.kv_fields;
                //console.log('new_active_record kv_fields', kv_fields);

                // then make a map of these

                let map_fields = {};
                each(kv_fields[0], ((x, i) => map_fields[x] = [0, i]));
                each(kv_fields[1], ((x, i) => map_fields[x] = [1, i]));

                //console.log('map_fields', map_fields);

                // Then are there any fields in the kv that are not used in the data
                //  Could that be all the fields missing from the PK?


                //let map_fields_missing_from_data = {};
                let map_fields_from_data = {};

                each(data, (value, name) => {
                    // look for the name

                    let ref = map_fields[name];
                    //console.log('name ' + name + ' ref', ref);

                    map_fields_from_data[name] = true;
                });

                // key fields missing

                let key_fields_missing = [];
                let value_fields_missing = [];

                each(kv_fields[0], (x, i) => {
                    if (!map_fields_from_data[x]) {
                        key_fields_missing.push(x);
                    }
                })

                each(kv_fields[1], (x, i) => {
                    if (!map_fields_from_data[x]) {
                        value_fields_missing.push(x);
                    }
                })

                //console.log('key_fields_missing', key_fields_missing);
                //console.log('value_fields_missing', value_fields_missing);


                // get indexed field names from the model table as an operation.

                //let single_indexed_field_names = 

                let indexed_field_names_and_ids = model_table.indexed_field_names_and_ids;
                // indexed fields names and ids

                //console.log('indexed_field_names_and_ids', indexed_field_names_and_ids);
                //  


                // then do two separate lookups by that field.




                // Then if there is just one key field missing, it's an autoincrementing id.

                // Active_Record is a place that can handle some complexities of changing between JS data and DB data.
                //


                //  Also, check for indexed fields present.
                //  Lookup according to these index fields, using OR.
                //   Treat them as though they are unique, despite that not having been specified.

                // then get the field values to lookup

                let to_lookup = {};
                let to_lookup_kv = {};

                if (key_fields_missing.length === 1 && value_fields_missing.length === 0) {
                    // Check that the single missing key field is an autoincrementing primary key
                    each(indexed_field_names_and_ids, ifnid => {
                        if (typeof data[ifnid[0]] !== 'undefined') {
                            //to_lookup[ifnids[0]] = data[ifn];
                            to_lookup[ifnid[1]] = data[ifnid[0]];
                            to_lookup_kv[ifnid[0]] = data[ifnid[0]];

                        }
                    })
                } else {
                    throw 'table_record_lookup NYI';
                }

                //console.log('to_lookup', to_lookup);
                //console.log('to_lookup_kv', to_lookup_kv);

                //console.log('model_table.record_def.map_indexes_by_field_names', model_table.record_def.map_indexes_by_field_names);

                let m = model_table.record_def.map_indexes_by_field_names;
                let arr_lookups = [];

                each(to_lookup_kv, (v, i) => {
                    //
                    let idx = m[JSON.stringify([i])];
                    //console.log('!!idx', !!idx);
                    //console.log('idx.id', idx.id);

                    arr_lookups.push([idx.id, [v]]);

                });
                // 
                // not sure about the lookup function.
                //  more a case for the core db.

                // multiple searches by different (unique) indexes
                //  though resistant to putting more into core now.
                //  maybe table_index_lookup
                // About finding which index corresponds to which item of data we have been given.

                // Map of indexes by fields definitely sounds useful.
                //  That would be part of the Model.

                // database.table.map_indexes_by_fields

                // we have
                // map_indexes_by_field_names

                //each(to_lookup, (v, i) => {
                //    arr_lookups.push([i, v]);
                //})
                //console.log('arr_lookups', arr_lookups);
                // iterate the indexes?
                //  Want to see which indexes to use for which fields we have the data for.


                //  Then will need to make sure lookup work for other types too...
                //   Should be quite general.
                // then do the individual field value lookups.
                // would be nice to have a map of indexes by their field names.
                //  Some indexes would only be for one field.


                let found;

                for (let lookup of arr_lookups) {
                    //console.log('lookup', lookup);
                    let res_lookup = await this.table_index_pk_lookup(table_id, lookup[0], lookup[1]);
                    //console.log('res_lookup', res_lookup);

                    if (def(res_lookup)) {
                        if (def(found)) {
                            // found again
                            if (res_lookup === found) {
                                // return two records?

                                reject(new Error('More than one record matches indexes using data given'));
                            }
                        } else {
                            found = res_lookup;
                        }
                    }
                }
                resolve(found);
            })();
        }, callback);
    }


    /*
    table_record_exists(table_id, b_record, callback) {
        return prom_or_cb((resolve, reject) => {

            // if it's 


        })
    }
    */


    // Maybe more checking / lookups will be done here rather than in Active_Record.


    ensure_table_record(table_id, b_record, callback) {
        //console.log('core ensure_table_record');
        return prom_or_cb((resolve, reject) => {

            (async () => {
                //let console = { log: () => null };

                // The b_record could be missing its key.

                // If it's not missing its key, we can search for it based on its primary key.


                //console.log('ensure_table_record table_id', table_id);
                //console.log('b_record', b_record);
                //console.log('b_record.decoded', b_record.decoded);
                let [key, value] = b_record.decoded;

                //console.log('[key, value]', [key, value]);





                // Then use the fields from the model table kv fields to put together the data for the lookup
                let table = this.model.map_tables_by_id[table_id];
                //console.log('table.kv_field_names', table.kv_field_names);
                let [key_field_names, value_field_names] = table.kv_field_names;
                // then we put together the loookup





                if (!def(key)) {
                    let lookup = {};
                    each(value, (value_item, i) => {
                        lookup[value_field_names[i]] = value_item;
                    });

                    //console.log('lookup', lookup);
                    let res_lookup = await this.table_record_lookup(table_id, lookup);
                    //console.log('res_lookup', res_lookup);

                    // So if it's not there already (has an empty key)
                    //  Need to generate a new key for the record.
                    //  

                    // Don't want the new key in the model (only)
                    //  Model would update its own incrementor then update the DB.
                    // Using the model here does make sense.
                    //  sometimes it depends on the 
                    // model_table.generate_key()
                    // then we need to update the incrementor.
                    // more like
                    // server.generate_table_key(table_id)


                    if (!res_lookup) {

                        //let new_record = new B_Record()

                        //let resolve = await this.put_table_record(table_id)

                        let new_key = await this.generate_table_key(table_id);
                        //console.log('new_key', new_key);

                        // want to be able to set / replace the key of the record.
                        //  maybe it's worth treating these records as immutable, and having Active_Records able to be changed.

                        b_record.key = new_key._buffer;
                        //console.log('b_record.decoded', b_record.decoded);

                        // Then need to put that record, including with its various index records.
                        //  Already have that code, I think.

                        let res_put = await this.put_table_record(table_id, b_record);

                        //console.log('res_put', res_put);
                        resolve(res_put);
                    } else {
                        resolve(res_lookup);
                    }
                } else {
                    // or just put the record

                    // Yup... need to write simple code for this.

                    // try has by key
                    //console.log('have been given the key');

                    let exists = await this.has(b_record.key);
                    //console.log('exists', exists);

                    if (!exists) {

                        let res_put = await this.put_table_record(table_id, b_record);

                        //console.log('res_put', res_put);
                        resolve(res_put);
                    } else {
                        resolve(b_record);
                    }
                }
            })();
            // 
        }, callback);
    }

    // get_db_incrementor_value
    //  does not use the Model for this, updates the Model if necessary

    //get_db_table_pk_incrementor_value


    // A whole increment function at once

    // will get the new incrementor value.
    //  does fresh read from db rather than relying on model.
    //  I suppose this is a small part of syncing.

    // Only keeping the core model in sync is necessary for most operations.


    db_table_pk_increment(table_id, callback) {
        return prom_or_cb((resolve, reject) => {
            (async () => {
                const model_table = this.model.map_tables_by_id[table_id];
                const model_pk_incrementor = model_table.pk_incrementor;
                const buf_inc_key = model_pk_incrementor.key.buffer;
                const inc_value = xas2.read(await this.db.get(buf_inc_key));
                const next_inc_value = inc_value + 1;
                model_pk_incrementor.value = next_inc_value;
                const buf_next_inc_value = xas2(next_inc_value).buffer;
                //console.log('pre put');
                const res_put_next_inc_value = await this.db.put(buf_inc_key, buf_next_inc_value);
                //console.log('res_put_next_inc_value', res_put_next_inc_value);
                resolve(inc_value);
            })();
        });
    }


    // Others would have their keys made out of their data
    //  Will be easier to find as well.
    //  Or at least not need index lookups to find them.

    // Then will have facility to ensure / lookup multiple records at once / quickly
    //  Active_Record in the crypto-data-collector will mean it knows when the records are in the DB.
    //  Records that are referred to in foreign keys will have 


    // table record key
    //  but not giving a record. It's just generating another key.

    generate_table_key(table_id, callback) {
        return prom_or_cb((resolve, reject) => {
            (async () => {
                const model_table = this.model.map_tables_by_id[table_id];
                const pk = model_table.pk;
                //console.log('pk.fields.length', pk.fields.length);

                if (pk.fields.length === 1) {
                    // need to see if its autoincrementing.
                    //console.log('pk.fields[0]', pk.fields[0]);

                    if (model_table.pk_incrementor) {
                        let new_id = await this.db_table_pk_increment(table_id);
                        let encoded_key = encoding.encode_key(model_table.kp, [new_id]);
                        resolve(new B_Key(encoded_key));
                    }
                } else {
                    reject(new Error('generate_table_key only works on tables with a single PK field'))
                }
            })();
        }, callback);
    }


    ensure(record, callback) {
        return prom_or_cb((resolve, reject) => {
            //let key = record_or_key.key;
            //let buf_key = key.buffer;
            (async () => {
                let exists = await this.has(record);
                //console.log('exists', exists);
                if (!exists) {
                    let put_res = await this.put(record);
                    resolve(put_res); // Which should be the record.
                } else {
                    resolve(record);
                }
            })();
        }, callback);
    }

    // would be nice to make an observable
    //  could send log-level updates about what it is doing
    //  could get other observables on the way to 

    ensure_table(arr_table, callback) {
        //console.log('ensure_table');

        let table_name, table_def;


        // observable with last
        //  so can be used with await


        // and get the sig

        return sig_obs_or_cb(arguments, (a, sig, next, complete, error, l) => {
            // not sure 
            //console.log('sig', sig);
            if (sig === '[s,a]') {
                [table_name, table_def] = a;
            } else {
                console.log('a', a);
                console.trace();
                throw 'NYI';
            }

            (async () => {
                let exists = !!this.model.map_tables[table_name];
                //console.log('exists', exists);

                if (exists) {
                    // but this result is maybe not ready yet.
                    //  as in the 'then' has not been called.
                    // a small delay before calling this?
                    console.log('pre complete');
                    // could check its structure is the same
                    //complete();
                    setImmediate(complete);
                    //complete();
                } else {
                    //console.log('does not exist');
                    // let old_model_rows = this.model.get_model_rows();
                    // model.rows

                    // a rows property / iterator would be useful.
                    //  nice if rows were an iterable object.
                    //  just an array will be fine.
                    let old_model_rows = this.model.rows;

                    //console.log('model.rows', this.model.rows);

                    // that works OK now, at least externally.
                    //  will use this kind of buffer-backed row more, and as a default.
                    //  allows use of both encoded and decoded data, decode on demand.
                    //console.log('table_name, table_def', table_name, table_def);
                    //throw 'stop';

                    let new_table = this.model.add_table(table_name, table_def);
                    //console.log('new_table.id', new_table.id);
                    //console.log('new_table.indexes.length', new_table.indexes.length);
                    //throw 'stop';

                    //let new_model_rows = this.model.get_model_rows();
                    let new_model_rows = this.model.rows;
                    let diff = Model_Database.diff_model_rows(old_model_rows, new_model_rows);
                    //console.log('diff ' + JSON.stringify(diff, null, 4));
                    //console.log('diff ', diff);
                    //console.log('diff.deleted.length', diff.deleted.length);
                    //console.log('diff.changed.length', diff.changed.length);
                    //console.log('diff.changed.length', diff.changed.length);
                    // then each of the changes...


                    //console.log('diff.added.length', diff.added.length);

                    each(diff.added, x => console.log('added', x.decoded));
                    each(diff.changed, x => console.log('changed', x[0].decoded, x[1].decoded));
                    let persisted = await this.persist_row_diffs(diff);
                    //console.log('post persist');
                    //throw 'stop';

                    // //complete(new Active_Table())
                    // complete(this.active_table(table_name));

                    // Should not need to reload the model, as work is done in the model here.
                    //let reloaded_model = await this.load_model();
                    complete();

                }
            })();
            return [];
        });
    }
    // This one can definitely be improved greatly.
    // Definitely would like a sample / test database which does not have all that many records.
    ensure_tables(arr_tables, callback) {
        // Optional callback, otherwise return observable
        //  Could do this as an observable, but use optional_observable()
        // For of over the array of tables
        let a = arguments,
            sig = get_a_sig(arguments);
        //console.log('ensure_tables sig', sig);
        //console.log('arr_tables', arr_tables);
        //throw 'stop';
        if (sig === '[a]') {
            // Function calling observable...
            //  Function call queue / fn_queue / fnq
            // Observable that is calling multiple functions.
            //  Can make this code much more concise.
            let res = new Evented_Class();
            let fns = Fns();
            each(arr_tables, arr_table => {
                //console.log('1) arr_table', arr_table);
                fns.push([this, this.ensure_table, [arr_table], (err, res) => {
                    if (err) {
                        // Not sure if an error here stops all subsequent function calls.
                        res.raise('error', err);

                    } else {
                        res.raise('next', res);
                    }
                }]);
            });
            fns.go((err, res_all) => {
                if (err) {
                    //callback(err);
                    res.raise('error', err);
                } else {
                    //callback(res_all);
                    res.raise('complete');
                }
            });
            return res;
        }

        if (sig === '[a,f]') {
            let fns = Fns();
            each(arr_tables, arr_table => {
                //console.log('arr_table', arr_table);
                fns.push([this, this.ensure_table, arr_table]);
            });
            fns.go((err, res_all) => {
                if (err) {
                    callback(err);
                } else {
                    callback(null, res_all);
                }
            });
        }
        // using an observable to return all the results along the way would be useful.
        //  observable function sequence
    }


    table_index_pk_lookup(table_id, idx_id, arr_values, callback) {
        return prom_or_cb((resolve, reject) => {


            (async () => {
                let table_kp = table_id * 2 + 2;
                let table_ikp = table_kp + 1;

                var buf_key_beginning = Model_Database.encode_index_key(
                    table_ikp,
                    idx_id, arr_values
                );
                //console.log('buf_key_beginning', buf_key_beginning);

                let arr_buf_idx_res = await this.ll_get_records_with_kp(buf_key_beginning);
                //console.log('arr_buf_idx_res', arr_buf_idx_res);

                if (!arr_buf_idx_res) {
                    resolve(undefined);
                } else {
                    if (arr_buf_idx_res.length === 0) {
                        // Callback with a new error saying 'Table Not Found'.
                        //callback(new Error(''));

                        resolve(undefined);
                    } else {


                        let decoded_key = arr_buf_idx_res[0].decoded[0];

                        //console.log('table_index_pk_lookup decoded_key', decoded_key);
                        //console.log('3) decoded', decoded);

                        throw 'stop';

                        /*
    
                        let t_return_field = tof(return_field);
                        if (t_return_field === 'number') {
                            let res = decoded_key[return_field];
                            callback(null, res);
                        }
                        */
                    }
                }

            })();





        }, callback);
    }


    // an array of values, not a single one?
    //  Can be multiple items, so array makes sense.
    //  Possibly make this handle single value too?


    // Need more of a look at this.
    table_index_value_lookup(table_id, idx_id, arr_values, return_field, opt_cb) {
        // Change to Promise
        //console.log('table_index_value_lookup');
        // Should get a single value.
        // Without the callback being used, it will return a promise.

        let inner = (callback) => {
            let table_kp = table_id * 2 + 2;
            let table_ikp = table_kp + 1;

            var buf_key_beginning = Model_Database.encode_index_key(
                table_ikp,
                idx_id, arr_values
            );
            // then lookup all those keys beginning x, return first.
            //  could make error if there are more?
            //let arr_buf_range = kp_to_range(buf_key_beginning);

            // then go get the range.
            //console.log('arr_buf_range', arr_buf_range);

            // Called with a callback - should return all of the results at once.

            // .first_only
            //  would be an observable feature

            this.ll_get_records_with_kp(buf_key_beginning, (err, arr_buf_idx_res) => {
                if (err) {
                    callback(err);
                } else {
                    // decode the key
                    //console.log('arr_buf_idx_res', arr_buf_idx_res);

                    if (arr_buf_idx_res.length === 0) {
                        // Callback with a new error saying 'Table Not Found'.
                        //callback(new Error(''));

                        callback(null, undefined);
                    } else {

                        let decoded_key = arr_buf_idx_res[0].decoded[0];

                        //console.log('decoded_key', decoded_key);
                        //console.log('3) decoded', decoded);

                        //throw 'stop';

                        let t_return_field = tof(return_field);
                        if (t_return_field === 'number') {
                            let res = decoded_key[return_field];
                            callback(null, res);
                        }
                    }
                }
            });
        }
        if (opt_cb) {
            inner(opt_cb);
        } else {
            // Will return a promise / promisy observable.
            throw 'table_index_value_lookup NYI';
        }
    }

    get_table_id_by_name(table_name, opt_cb) {
        let that = this;
        let inner = callback => {
            // could have an error if the table isn't found.

            let tbl_tables = that.model.map_tables['tables'];
            //console.log('tbl_tables', tbl_tables);
            // then do a lookup against that.
            let id = tbl_tables.id;
            that.table_index_value_lookup(id, 0, [table_name], 3, callback);
        }

        if (opt_cb) {
            inner.call(this, opt_cb);
        } else {
            return new Promise((resolve, reject) => {
                //setTimeout(resolve, 100, 'foo');
                inner((err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                });
            });
        }
    }


    get(key) {
        // and a promise, not callback
        //console.log('get key', key);

        let res = new Promise((accept, reject) => {

            this.db.get(key, (err, value) => {
                if (err) {
                    //console.log('err', err);
                    //reject(err);

                    //
                    accept(undefined);
                } else {

                    // gets the record.

                    let rec = new B_Record([key, value]);
                    //console.log('rec', rec);
                    //console.log('value', value);
                    accept(rec);
                }
            });
        })
        return res;
    }
    // get table records of tables with indexes

    // get_table_records_by_table_ids

    // Then will have some functions to shift some records around if something is not where it should be / misplaced.
    //  Maybe use a bit of reasoning to work out the way to shift things.

    // Soon will be able to provide valid, up-to-date datasets.

    // May well be (have been) worth setting up a 9th server just in case.
    // Then will be worth setting up a DB structure that links together the same coin on different exchanges,

    get_table_index_records_by_arr_table_ids(arr_table_ids) {

        let q_obs = [];
        each(arr_table_ids, table_id => {
            q_obs.push([this, this.ll_get_table_index_records, [table_id]]);
        });
        // but with simplest return style...
        // Then can load these up into Index_Key objects.
        //  will make new buffer-backed class to handle index keys.
        // xas2 table index pk, xas2 index id, index fields all available through decode_buffer
        // ll_get_table_index_records - is that pausable?
        return execute_q_obs(q_obs);
    }


    get_table_records_by_arr_table_ids(arr_table_ids) {
        let q_obs = [];
        each(arr_table_ids, table_id => {
            q_obs.push([this, this.ll_get_table_records, [table_id]]);
        });
        let obs_all = execute_q_obs(q_obs);
        return obs_all;
    }

    // get table_id_table_records

    get_all_table_records_where_tables_are_indexed() {
        return this.get_table_records_by_arr_table_ids(this.table_ids_with_indexes);
    }

    get_all_table_records() {
        return this.get_table_records_by_arr_table_ids(this.all_table_ids);
    }

    get_all_index_records() {
        return this.get_table_index_records_by_arr_table_ids(this.all_table_ids);
    }

    get_all_index_record_keys() {
        // obs_map
        //  where we give a function that applies to the data.
        return obs_map(this.get_all_index_records, data => new B_Key(data));
    }

    // No paging on this one.

    // Would be nice to make observable, standard. Stoppable, pausable.

    get_all_db_keys(callback) {
        var res = [];
        this.db.createKeyStream({}).on('data', function (key) {
            res.push(key);
        })
            .on('error', function (err) {
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                callback(null, res);
            })
    }

    get_all_db_rows(callback) {
        // This has got not means to cancel it. Simple version.
        var res_records = [];

        this.db.createReadStream({}).on('data', function (record) {
            //console.log('record', record);
            res_records.push([record.key, record.value]);
        })
            .on('error', function (err) {
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                callback(null, res_records);
            })
    }

    get_system_db_rows(callback) {
        //console.log('get_system_db_rows');
        //throw 'stop';
        // could do this as an observable, with optional callback
        return obs_or_cb((next, complete, error) => {
            this.db.createReadStream({
                'gte': xas2(CORE_MIN_PREFIX).buffer,
                'lte': xas2(CORE_MAX_PREFIX).buffer
            }).on('data', function (record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                next(new B_Record([record.key, record.value]));
            }).on('error', function (err) {
                //console.log('Oh my!', err)
                //callback(err);
                error(err);
            }).on('close', function () {
                //console.log('Stream closed')
            }).on('end', function () {
                //callback(null, res_records);
                complete();
            })
            return [];
        }, callback);
    }

    get_first_and_last_keys_in_buf_range(buf_l, buf_u, remove_kp, decode, callback) {

        let a = arguments,
            sig = get_a_sig(a);

        //console.log('get_first_and_last_keys_in_buf_range sig', sig);
        //console.log('[buf_l, buf_u, remove_kp, decode]', buf_l, buf_u, remove_kp, decode);
        //console.log('');
        // more flexible params to handle removal of KPs from the results.

        if (sig === '[B,B,f]') {

        } else if (sig === '[B,B]') {

        } else if (sig === '[B,B,b]') {
            remove_kp = a[2];
            callback = null;
        } else if (sig === '[B,B,b,b]') {
            //remove_kp = a[2];
            //callback = null;
        } else {
            throw 'get_first_and_last_keys_in_buf_range unexpected signature ' + sig;
        }

        // Probably don't want to decode in many cases

        // Resolve 2 fns at once.
        //  Will be easier if those two are made into promises.

        // An inner promise would be better than inner callback.
        let res = new Promise((resolve, reject) => {
            let fns = Fns();
            fns.push([this, this.get_first_key_in_range, [
                [buf_l, buf_u], remove_kp, decode
            ]]);
            fns.push([this, this.get_last_key_in_range, [
                [buf_l, buf_u], remove_kp, decode
            ]]);
            fns.go(2, (err, res_all) => {
                if (err) {
                    reject(err);
                } else {
                    //console.log('* res_all', res_all);
                    resolve(res_all);
                    //throw 'stop';
                }
            })
        });

        if (callback) {
            res.then(res => callback(null, res), err => callback(err));
        } else {
            return res;
        }

    }

    // get rid of remove_pks = false, decode = false to make simpler syntax

    get_first_and_last_keys_beginning(key_beginning, remove_pks = false, decode = false, callback) {

        // change to a promise.


        let a = arguments;
        let sig = get_a_sig(a);
        let buf_key_beginning;
        // Yet more arguments... will have decode (or not) option
        //console.log('get_first_and_last_keys_beginning sig', sig);
        if (sig === '[B,b]') {
            buf_key_beginning = key_beginning;
        } else if (sig === '[a,b]') {
            buf_key_beginning = Model.encoding.encode_key(key_beginning);
        } else if (sig === '[B,f]') {
            buf_key_beginning = key_beginning;
            callback = a[1];
            remove_pks = false;

        } else if (sig === '[a,f]') {
            buf_key_beginning = Model.encoding.encode_key(key_beginning);
            callback = a[1];
            remove_pks = false;
        } else if (sig === '[a,b,f]') {
            buf_key_beginning = Model.encoding.encode_key(key_beginning);
            callback = a[2];
            decode = false;
        } else if (sig === '[B,b,f]') {
            buf_key_beginning = key_beginning;
            callback = a[2];
            decode = false;
        } else if (sig === '[B,b,b,f]') {
            buf_key_beginning = key_beginning;
        } else {
            throw 'get_first_and_last_keys_beginning unexpected signature:', sig;
        }

        let [buf_l, buf_u] = kp_to_range(buf_key_beginning);

        let pr_res = this.get_first_and_last_keys_in_buf_range(buf_l, buf_u, remove_pks, decode);

        if (callback) {
            pr_res.then(res => callback(null, res), err => callback(err)).catch(err => {
                callback(err);
            });
        } else {
            return pr_res;
        }
    }
    // key_beginning_to_range = kp_to_range

    get_first_key_beginning(buf_beginning, callback) {
        let range = kp_to_range(buf_beginning);
        return this.get_first_key_in_range(range, callback);
    }

    get_last_key_beginning(buf_beginning, callback) {
        let range = kp_to_range(buf_beginning);
        return this.get_last_key_in_range(range, callback);
    }

    // Assumes decoding

    // These two functions below could do with decoding options.
    //  Decoding should be optional

    // no kp removal either
    //  no decoding - but the Key is a class that can decode the buffer.


    get_first_key_in_range(arr_range, callback) {
        return prom_or_cb((resolve, reject) => {
            let res;
            this.db.createKeyStream({
                'gte': arr_range[0],
                'lte': arr_range[1],
                'limit': 1
            }).on('data', function (key) {
                //console.log('key', key);
                res = key;
            }).on('error', reject(err)).on('close', function () {
                //console.log('Stream closed');
            }).on('end', function () {
                if (res) {
                    resolve(B_Key(res));
                } else {
                    reject();
                }
            });
        }, callback);
    }

    // Should make decoding faslse by default in various places.

    get_last_key_in_range(arr_range, callback) {
        // again, promisify, remove remove_kp, decode, cb optional
        //console.log('buf_l', buf_l);
        //console.log('buf_u', buf_u);

        return prom_or_cb((resolve, reject) => {
            let res;
            this.db.createKeyStream({
                'gte': arr_range[0],
                'lte': arr_range[1],
                'limit': 1
            }).on('data', function (key) {
                //console.log('key', key);
                res = key;
            })
                //.on('error', reject(err))
                .on('close', function () {
                    //console.log('Stream closed');
                })
                .on('end', function () {
                    if (res) {
                        resolve(B_Key(res));
                    } else {
                        //callback(null, undefined);
                        // key not found.
                        reject();
                    }
                });
        }, callback);
    }

    // can use above fn because we know range

    get_last_key_in_table(table, callback) {


        // find the table key range

        //let [buf_l, buf_u] = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        let range = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        return this.get_last_key_in_range(range, callback);

    }

    // Basically the ll version but expressed differently, using sig_obs_or_cb to make the observable.

    get_records_in_range(buf_l, buf_u, limit = -1, callback) {
        // may want to call this using a single arr to hold the ranges.
        // remove options decoding, remove_kp
        // Make this use an Observable(...)

        return sig_obs_or_cb(arguments, (a, sig, next, complete, error, l) => {
            //console.log('get_records_in_range sig', sig);
            //console.log('a', a);
            let buf_l, buf_u;
            if (sig === '[a]') {
                [buf_l, buf_u] = a[0];
                //console.log('a[0]', a[0]);
            } else if (sig === '[a,n]') {
                [buf_l, buf_u] = a[0];
                limit = a[1];
            } else if (sig === '[B,B]') {
                [buf_l, buf_u] = a;
            } else if (sig === '[B,B,n]') {
                [buf_l, buf_u, limit] = a;
            } else {
                console.trace();
                console.log('sig', sig);
                throw 'stop';
            }

            let stream = this.db.createReadStream({
                'gt': buf_l,
                'lt': buf_u,
                'limit': limit
            }).on('data', data => {
                next(new B_Record([data.key, data.value]));
            }).on('error', error)
                .on('close', function () {
                    //console.log('2) Stream closed')
                }).on('end', complete)

            return [() => {
                stream.destroy();
                //res.raise('complete');
                // or stopped without being completed?
                complete();
            }, () => {
                if (!stream.isPaused()) {
                    stream.pause();
                    //return res.resume;
                }
            }, () => {
                if (stream.isPaused()) {
                    stream.resume();
                }
            }];
        })
    }

    get_records_in_ranges(arr_ranges) {
        let res = obs_arrayified_call(this, this.ll_get_records_in_range, arr_ranges);
        res.response_type = res.response_type || 'records';
        return res;
    }

    get_keys_in_range(range, limit = -1, callback) {

        // A good candidate for being written as an observable


        // may not have the best syntactic sugar yet.
        //  would like to specify functions to assign the parameters
        //  or even better do it quickly and automatically
        //  want to call a different function depending on the sig, then could call other main function?
        //  there will be some tricks that get data in the right closures.

        return sig_obs_or_cb(arguments, (a, sig, next, complete, error, l) => {
            //console.log('sig', sig);
            //console.log('a', a);
            let buf_l, buf_u;
            if (sig === '[a]') {
                [buf_l, buf_u] = a[0];
                //console.log('a[0]', a[0]);
            } else if (sig === '[a,n]') {
                [buf_l, buf_u] = a[0];
                limit = a[1];
            } else if (sig === '[B,B]') {
                [buf_l, buf_u] = a;
            } else if (sig === '[B,B,n]') {
                [buf_l, buf_u, limit] = a;
            } else {
                console.trace();
                console.log('sig', sig);
                throw 'stop';
            }

            //console.log('[buf_l, buf_u, limit]', [buf_l, buf_u, limit]);

            let stream = this.db.createKeyStream({
                'gt': buf_l,
                'lt': buf_u,
                'limit': limit
            }).on('data', data_key => {
                //console.log('data_key', data_key);
                next(new B_Key(data_key));
            }).on('error', error).on('close', function () {
                //console.log('2) Stream closed')
            }).on('end', complete)

            return [() => {
                stream.destroy();
                //res.raise('complete');
                // or stopped without being completed?
                complete();
            }, () => {
                if (!stream.isPaused()) {
                    stream.pause();
                    //return res.resume;
                }
            }, () => {
                if (stream.isPaused()) {
                    stream.resume();
                }
            }];
        });
    }

    get_table_record_by_key(table, key, callback) {
        let table_id = this.model.table_id(table);
        let table_kp = table_id * 2 + 2;

        // get single record in range?

        // 

        let arr_record_key = [table_kp].concat(key);

        // seems we don't have a simple get call in use

        let buf_key = Binary_Encoding.encode_to_buffer_use_kps(arr_record_key, 1);
        this.db.get(buf_key, (err, res) => {
            if (err) {
                if (err.notFound) {
                    // handle a 'NotFoundError' here
                    callback(null, undefined);
                } else {
                    callback(err);
                }
            } else {
                //console.log('db.get res', res);
                callback(null, [buf_key, res]);
            }
        })
    }


    get_table_records(table, callback) {
        let range = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        return this.get_records_in_range(range, callback);
    }

    // Inner observable here
    //  Get rid of excess options.
    get_table_keys(table, callback) {
        //let buf_key_prefix = xas2(this.model.table_id(table) * 2 + 2).buffer;
        let range = kp_to_range(xas2(this.model.table_id(table) * 2 + 2).buffer);
        return this.get_keys_in_range(range, callback);
    }

    get_table_records_hash(table, callback) {
        return prom_or_cb((resolve, reject) => {
            let obs_records = this.get_table_records(table);
            let hash = crypto.createHash('sha256');
            obs_records.on('next', record => hash.update(record));
            obs_records.on('complete', () => resolve(hash.digest('hex')));
        });
    }


    // table.fields_info


    get_table_fields_info(table, callback) {
        let _table = this.model.map_tables_by_id[this.model.table_id(table)];
        if (callback) {
            callback(null, table.fields_info);
        } else {
            return table.fields_info;
        }
    }





    // could use an observable creator function.
    //  the handler gets called with 3 functions available for it to call.


    // can just be get_records or get_records_by_keys
    //  observable is becoming more standard.

    // Could just use 'get' command.

    // Want some kind of functional optimised fp.



    // get_records_by_keys

    obs_get_records(key_list) {
        return observable((next, complete, error) => {
            (async () => {
                for (key of key_list) {
                    let record = await this.get(key);
                    //next(await this.get(key));
                    if (record) {
                        next(record)
                    } else {
                        error(key);
                    }
                }
                complete();
            })();
        });
    }

    // maintain / scan
    obs_records_not_found(records) {
        return observable((next, complete, error) => {
            (async () => {
                for (key of key_list) {
                    let record = await this.get(key);
                    //next(await this.get(key));
                    if (record) {
                        //next(record)
                    } else {
                        //error(key);
                        next(key);
                    }
                }
                complete();
            })();
        });
    }

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
        //var that = this;
        db.batch(ops, (err) => {
            if (err) {
                callback(err);
            } else {
                this.raise('db_action', {
                    'type': 'arr_batch_put',
                    'value': arr_bufs
                });

                callback(null, true);
            }
        })
    }

    put_table_record(table, record, callback) {
        let model_table;
        if (typeof table === 'number') {
            model_table = this.model.map_tables_by_id[table];
        }
        return prom_or_cb(async (resolve, reject) => {
            let indexes = model_table.indexes;
            // So it's worked out the record from the Model.

            //console.log('put_table_record', record);
            //console.log('record instanceof B_Record', record instanceof B_Record);
            //console.log('Array.isArray(record)', Array.isArray(record));

            if (record instanceof B_Record) {
                let m_record = model_table.new_record(record);
                let b_records = m_record.to_b_records();
                let res = await this.ll_batch_put(b_records);
                resolve(b_records[0]);
            } else if (Array.isArray(record)) {
                let old_model_pk_inc_val = model_table.pk_incrementor.value;
                let m_record = model_table.add_record(record);
                let new_model_pk_inc_val = model_table.pk_incrementor.value;
                let b_records = m_record.to_b_records();
                if (old_model_pk_inc_val !== new_model_pk_inc_val) {
                    b_records.push(model_table.pk_incrementor.record);
                }
                let res = await this.ll_batch_put(b_records);
                resolve(res);
            }

        }, callback);
    }



    // This one splices the kp into it.
    //  Decoding the records has removed the kp.

    // This is about batch putting rows
    batch_put_table_records(table_name, records, callback) {
        var ops = [],
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;

        // Maybe table does not exist locally
        let table = this.model.map_tables[table_name];
        //console.log('table', table);
        // Maybe it's OK when restarting anew because the local system has already copied the core model?

        //throw 'stop';
        if (table) {
            let kp = table.id * 2 + 2;

            each(records, row => {
                row.splice(0, 0, kp);

                if (this.using_prefix_put_alerts) {
                    //prefix_put_alerts_batch = [];
                    //var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                    b64_key = arr_row[0].toString('hex');
                    // Better to use a map and array.
                    //  Maybe the standard event based system would be fine.
                    //  Do more work on subscription handling.

                    for (key in this.map_b64kp_subscription_put_alert_counts) {
                        if (b64_key.indexOf(key) === 0) {
                            map_key_batches[key] = map_key_batches[key] || [];
                            map_key_batches[key].push(arr_row);
                        }
                    }
                }
                ops.push({
                    'type': 'put',
                    'key': row[0],
                    'value': row[1]
                });
            });

            db.batch(ops, err => {
                if (err) {
                    callback(err);
                } else {

                    this.raise('db_action', {
                        'type': 'batch_put',
                        'arr': records
                    });

                    each(map_key_batches, (map_key_batch, key) => {
                        //console.log('1) key', key);
                        console.log('map_key_batch', map_key_batch);
                        var buf_encoded_batch = Model_Database.encode_model_rows(map_key_batch);
                        //console.log('buf_encoded_batch', buf_encoded_batch);
                        this.raise('put_kp_batch_' + key, {
                            'type': 'batch_put',
                            'buffer': buf_encoded_batch
                        });
                    });
                    callback(null, true);
                }
            })
        } else {
            callback(new Error('Table ' + table_name + ' does not exist locally'));
        }
    }

    batch_put_decoded_arr(arr, callback) {
        var ops = [],
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;

        each(arr, row => {
            if (this.using_prefix_put_alerts) {
                //prefix_put_alerts_batch = [];
                var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                b64_key = arr_row[0].toString('hex');
                // Better to use a map and array.
                //  Maybe the standard event based system would be fine.
                //  Do more work on subscription handling.
                for (key in this.map_b64kp_subscription_put_alert_counts) {
                    if (b64_key.indexOf(key) === 0) {
                        map_key_batches[key] = map_key_batches[key] || [];
                        map_key_batches[key].push(arr_row);
                    }
                }
            }
            ops.push({
                'type': 'put',
                'key': row[0],
                'value': row[1]
            });
        });
        db.batch(ops, err => {
            if (err) {
                callback(err);
            } else {
                this.raise('db_action', {
                    'type': 'batch_put',
                    'arr': arr
                });
                each(map_key_batches, (map_key_batch, key) => {
                    //console.log('1) key', key);
                    console.log('map_key_batch', map_key_batch);
                    var buf_encoded_batch = Model_Database.encode_model_rows(map_key_batch);
                    //console.log('buf_encoded_batch', buf_encoded_batch);
                    this.raise('put_kp_batch_' + key, {
                        'type': 'batch_put',
                        'buffer': buf_encoded_batch
                    });
                });
                callback(null, true);
            }
        })
    }

    batch_put_kvpbs(arr_pairs, callback) {
        let ops,
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;
        // Unable to raise the buffer batch put event.
        let put_using_prefix_alerts = () => {
            ops = [];
            each(arr_pairs, pair => {
                //prefix_put_alerts_batch = [];
                var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                b64_key = pair[0].toString('hex');
                // Better to use a map and array.
                //  Maybe the standard event based system would be fine.
                //  Do more work on subscription handling.

                for (key in this.map_b64kp_subscription_put_alert_counts) {
                    if (b64_key.indexOf(key) === 0) {
                        map_key_batches[key] = map_key_batches[key] || [];
                        map_key_batches[key].push(pair);
                    }
                }
                ops.push({
                    'type': 'put',
                    'key': pair[0],
                    'value': pair[1]
                });
            })
        }
        let put_without_prefix_alerts = () => {
            let l = arr_pairs.length;
            let c;
            ops = new Array(arr_pairs.length);
            for (c = 0; c < l; c++) {
                ops[c] = ({
                    'type': 'put',
                    'key': arr_pairs[c][0],
                    'value': arr_pairs[c][1]
                });
            }
        }
        if (this.using_prefix_put_alerts) {
            put_using_prefix_alerts();
        } else {
            put_without_prefix_alerts();
        }
        var that = this;
        db.batch(ops, function (err) {
            if (err) {
                callback(err);
            } else {
                that.raise('db_action', {
                    'type': 'batch_put',
                    'items': arr_pairs
                });
                each(map_key_batches, (map_key_batch, key) => {
                    //console.log('1) key', key);
                    console.log('map_key_batch', map_key_batch);
                    var buf_encoded_batch = Model_Database.encode_model_rows(map_key_batch);
                    //console.log('buf_encoded_batch', buf_encoded_batch);
                    that.raise('put_kp_batch_' + key, {
                        'type': 'batch_put',
                        'buffer': buf_encoded_batch
                    });
                });
                callback(null, ops.length);
            }
        });
    }

    // Different types of batch put
    //  Some batches may also require generation / checking / putting index records.

    // low level batch put
    //  needs to be a simple put operation.

    // Can have other function to read / respond to key ranges.

    // With invisible callbacks too here....
    //  can it read the arguments (being an arrow function)
    ll_batch_put(arr_items, callback) {

        return prom_or_cb(async (resolve, reject) => {
            //console.log('ll_batch_put arr_items', arr_items);
            let ops = arr_items.map(item => {
                //console.log('item', item);
                if (item instanceof B_Record) {
                    return {
                        'type': 'put',
                        'key': item.kvp_bufs[0],
                        'value': item.kvp_bufs[1]
                    }
                }
            });
            resolve(await this.db.batch(ops));
            //return this.db.batch(ops);
        }, callback);
    }


    batch_put(buf, callback) {

        // buf is an array by default? An array of buffers?
        this.batch_put_kvpbs(Model.encoding.buffer_to_row_buffer_pairs(buf), callback);
    }

    table_exists(table_name, callback) {
        // Does not need to be async when checking the model.





        // Should probably consult the index records??? Error was not there.
        //console.log('table_name', table_name);
        //console.log('this.model.map_tables', this.model.map_tables);

        let exists = !!this.model.map_tables[table_name];
        //console.log('exists', exists);
        //throw 'stop';
        callback(null, exists);

    }

}

// Run it from the command line with a path?

let custom_path;

// Custom path will be within local app config.
//  That way it will work better on Linux too.

// Have a look in the config to find the db path.
//custom_path = 'D:\\NextlevelDB\\DB1';
// Loading it from local config would be best.


// may get back to creating 'll' functions, could put them in the ll server part.
//  Then the higher level functions can wrap the ll versions in observables that decode or remove kps according to params/

// May also be worth having ll client calls that don't handle client-side decoding.
//  Then the appropriate wrappers can be used to 




if (require.main === module) {

    // Want to be able to get a full path from the command line

    const option_definitions = [{
        name: 'path',
        alias: 'p',
        type: String
    }]

    const options = commandLineArgs(option_definitions);

    console.log('options', options);

    //throw 'stop';

    var user_dir = os.homedir();
    console.log('OS User Directory:', user_dir);
    //var docs_dir =
    var path_dbs = user_dir + '/NextLevelDB/dbs';
    // Would also be worth being able to choose db names

    fs2.ensure_directory_exists(user_dir + '/NextLevelDB', (err, exists) => {
        if (err) {
            throw err
        } else {
            fs2.ensure_directory_exists(path_dbs, (err, exists) => {
                if (err) {
                    throw err
                } else {
                    var db_path = options.path || path_dbs + '/default';
                    //var db_path = 'db';
                    var port = 420;
                    // Is the first one the node executable?

                    console.log('db_path', db_path);

                    //console.log('process.argv.length', process.argv.length);
                    //console.log('process.argv', process.argv);
                    /*

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
                    */

                    var ls = new NextLevelDB_Core_Server({
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

                            let test_observe_table_records = () => {
                                let o_all = ls.get_all_table_records();
                                o_all.next(data => {
                                    console.log('data', data);
                                    //throw 'stop';
                                });

                                // OK, so delaying works.
                                //  It's more like a direct wrapper to the data source, an observable data source, 

                                setTimeout(() => {
                                    o_all.delay(5000);
                                }, 3000);
                            }



                            let test_get_malformed_index_records = () => {
                                console.log('test_get_malformed_index_records');
                                // but that's in safety.
                                //  it's a much easier function now though.

                                //let obs_iir = ls.get_in
                            }
                            //test_get_malformed_index_records();

                            let test_get_all_index_records = () => {

                                obs_ir = ls.get_all_index_records();
                                obs_ir.on('next', data => {
                                    console.log('get_all_index_records data', data);

                                })

                            }
                            //test_get_all_index_records();

                            let test_count_core = () => {
                                console.log('test_count_core');

                                /*
                                ls.count_core((err, count) => {
                                    if (err) {
                                        throw err;
                                    }
                                    console.log('count', count);
                                });
                                */

                                (async () => {
                                    let count = await ls.count_core();
                                    console.log('awaited count', count);
                                })();
                                // and also an observable that looks at them one by one.
                            }
                            //test_count_core();

                            let test_get_table_keys = () => {
                                console.log('test_get_table_keys');

                                (async () => {
                                    let keys = await ls.get_table_keys('tables');
                                    console.log('awaited keys', keys);
                                })();


                            }
                            test_get_table_keys();







                            // looks good so far.


                            // let's go through every record.


                            // //observe(ls.get_all_records, )

                            //ls.get_all_records.next(data => ...).catch(error => ...).complete(() => ...)
                            //ls.get_all_records.data(data => ...).error(error => ...).done(() => ...)


                            // DB could create its own core model when it first starts.
                            //  Simpler to save the client from having to do it.

                            // The server component itself will start with its model loaded.

                            //callback(null, true);


                            // select_from_table_by_field_ids
                            //  or just select_from_table
                            //  call it over the API.

                            // Will be useful for comparing data from different DBs.


                            // Then select from could be made to work on the binary protocol.
                            //  Where clause could definitely be useful.
                            //  Could encode a SELECT query as an object within the Model.
                            //   Could do this via encoding / decoding an array, and move to OO select if it becomes more complex.
                            //    The advantage is having the same code on both the server and the client, an object which handles decoding.


                            // Could select from a key range.
                            //  The table gets converted to a key range.




                            // Need decoding option.
                            //  By default it will be decoded.

                            //let decode = true;

                            /*

                            let obs = ls.select_from_table('bittrex currencies', [0, 1], decode);
                            obs.on('next', data => {



                                console.log('obs data', data);
                            })
                            obs.on('complete', () => {
                                console.log('obs complete');
                            })

                            */


                            /*
                            let obs_trh = ls.get_table_records_hash('bittrex currencies');
                            obs_trh.on('next', data => {
                                console.log('obs_trh data', data);
                            })
                            obs_trh.on('complete', () => {
                                console.log('obs_trh complete');
                            })
                            */

                            /*

                            let pr_trh = ls.get_table_records_hash('bittrex currencies');
                            pr_trh.then(res => {
                                console.log('pr res', res);
                            }, err => {
                                console.log('pr err', err);
                            });


                            let buf_0 = Buffer.alloc(1);
                            buf_0.writeUInt8(0, 0);
                            let buf_255 = Buffer.alloc(1);
                            buf_255.writeUInt8(255, 0);

                            let range_1 = [Buffer.concat([xas2(0).buffer, buf_0]), Buffer.concat([xas2(0).buffer, buf_255])];
                            let range_2 = [Buffer.concat([xas2(2).buffer, buf_0]), Buffer.concat([xas2(2).buffer, buf_255])];


                            console.log('range_1', range_1);


                            let obs_get_records_in_ranges = ls.get_records_in_ranges([range_1, range_2]);

                            */


                            // This will be used to greatly improve the sync speed, when syncing record ranges.
                            //  Want to test this over the client too, with the same API.




                            // testing get_records_in_ranges

                            // could test getting the incrementors (kp 0) and the kp3



                            /*

                            var start_with_core_model = (callback) => {
                                // Could do an initial db setup...

                                ls.count_core((err, count) => {
                                    if (err) {
                                        throw err;
                                    }
                                    if (count === 0) {
                                        callback(null, true);
                                    } else {

                                        ls.load_model((err, model) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                ls.model = model;

                                                callback(null, true);


                                            }
                                        })
                                    }
                                });
                            }



                            start_with_core_model(() => {
                                console.log('DB started');


                            });
                            */
                        }
                    });
                }
            });
        }
    });
} else {
    //console.log('required as a module');
}

let p = NextLevelDB_Core_Server.prototype;
p.get_table_records = p.ll_get_table_records;


module.exports = NextLevelDB_Core_Server;