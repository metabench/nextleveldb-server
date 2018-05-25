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
var WebSocket = require('websocket');
var WebSocketServer = WebSocket.server;

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

// looks like this can go in fnl
//  but it maybe changed and generalised and optimised in some ways.
//  may have a call function

// promise => observable


// observables list
//  This may be tough to generalise, but could try.

// A queue of observables, exewcuted sequentially.

// seq_obs
// seq


/*

module.exports = {
    'observable': observable,
    'seq': seq,
    'sequence': seq,
    'sig_obs_or_cb': sig_obs_or_cb,
    'cb_to_prom_or_cb': cb_to_prom_or_cb,
    'prom_or_cb': prom_or_cb
}



*/





const NextlevelDB_Core_Server = require('./nextleveldb-core-server');

// Private system DBs could be useful - they could best be used by subclasses of this.
//  Could provide auth.
//  Could keep log data on what maintenance / some other operations such as syncing have taken place
//  Could keep a table of the sync processes that are going on at present. This table could then be queried to give sync updates.





// Authentication is the next core feature of the server.
//  Could make it so that there is just an admin user for the moment with full permissions.
//  For some DB things, could also open it up so that any user can read, and there is a rate limiter and / or DOS protector.

// Authentication would enable this DB to run a CMS website.

// For the moment, need to get this deployed onto remote servers
//  Could also work on error logging in case of failure.
//  Possibly logging the errors to the DB itself.


// Then a multi-client would be useful, to monitor the status of these various servers.
//  Get servers better at tracking number of records put/got per second again.

// Need to separate out different collectors.

// Bittrex -> DB
// Others -> DB

// Crypto-Data-Collector seems OK for one machine.
//  Maybe for coordinating a network too.

// Try collecting data for about 5 or so exchanges soon.
// Need it so that the collectors can be coded separately, and then started up to collect data for the given DB.

// A&A is probably the highest priority though.




// The database server will start by loading its Model, or creating a new one if there are no records in the database.


// In general will unload code and complexity from the client-side.
//  Will make use of observables, and flexible functions with optional callbacks.
//  Will also have decoding options in a variety of places. Sometimes the data that gets processed on the server will need to be decoded. 



// Got plenty more to do on the server level to ensure smooth data acquisition and sharing.


// Want to have server-side re-encoding of records, where it takes the records in one format, and if necessary reencodes them so that they go into the DB well.
//  That could involve foreign -> primary key lookups for some records, eg live asset data.
//  



// Crypto data collector should ensure a few tables according to some definitions.
//  Doing that when it starts up would be an effective way of doing it, and adding new tables will be incremental.

// 05/03/2018 - Being expanded greatly to provide functionality that will make a variety of processes easier to the client.
//  More advanced commands will be runnable on the server.
//  The server will make more use of the Model in order to act with more understanding.


// 18/03/2018
//  Commands have been greatly expanded.
//  Finding that no field ids get persisted properly.
//   Need it so that a field with a '+' is (an autoincrementing) xas2 integer.


// 26/03/2018
//  Have done more work on binary commands.
//  Have the foundation for a server connecting to another server to download all records in a table.

// 27/03/2018
//  Getting to the stage where cumulative hashes of records could be useful.
//   Or key comparison tasks?
//   Could pause whichever stream gets ahead, and stream the output.

// Could check consistency with record range hashes.
//  Could be useful for comparing table structure in the core
//  Also for comparing tables such as currencies and markets to check for consistency.

// Could turn to JSON and then just compare the strings in JS to start with.
// With the normalised records, need to check they are based on the same values.

// 31/03/2018
//  Noticed that incrementors had not been correctly updated in some cases. That means new rows could have been created with a PK value of 0, overwriting other records.
//  May need some data recovery / checking if we are to use the data?
//  Maybe it ruined the Bitcoin data, as that is at index 0.
//  Could have startup checks on tables with autoincrementing keys, to see what the highest value is, and compare that with the incrementor field.
//   If (on startup) the incrementor is less than the highest key value, it sets it to the highest key value + 1
//   Have clients listen to changes in the model (all the DB's core), so it can increment / update the index on the client side when it changes on the server.
//    Could reload the model, or process model updates by row.
//  Keeping the incrementors synced seems like a bit of a challenge.

// 12/05/2018 - Code can be simplified through better use of observables
//  fewer params (no decoding option)
//  could make lower level core functionality.



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

            //if (db_already_exists) {
            //    db = that.db = replace_db_put(levelup(that.db_path, options), that);
            //}

            //var fns_ws = {};

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
                    //  



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

                    //callback(null, count);
                    //console.log('* count', count);
                    // Should be able to just send a number through.

                    //res.raise('next', {
                    //    'count': count
                    //});
                    //res.raise('complete', {
                    //    'count': count
                    //});

                    //res.raise('next', count);
                    //res.raise('complete', count);
                    clearInterval(repeater);
                    complete(count);
                });

            return [];
        }, callback);


        /*

        var count = 0;
        var delay = 1000;

        let a = arguments;
        a.l = a.length;

        let sig = get_a_sig(a);

        //console.log('server ll_count');
        //console.log('sig', sig);

        if (sig === '[n]') {
            delay = a[0];
            callback = null;
        }

        //throw 'stop';

        if (callback) {
            this.db.createKeyStream({}).on('data', function (key) {
                count++;
            }).on('error', function (err) {
                callback(err);
            })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    callback(null, count);
                })
        } else {
            // return an observable that gives count updates.

            let res = new Evented_Class();

            // use observable instead

            // Then use setInterval to send back messages saying how the count has progressed.

            let repeater = setInterval(() => {
                res.raise('next', count);


            }, delay);

            this.db.createKeyStream({}).on('data', function (key) {
                count++;

            }).on('error', function (err) {
                //callback(err);
                res.raise('error', err);
            })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, count);
                    //console.log('* count', count);
                    // Should be able to just send a number through.

                    //res.raise('next', {
                    //    'count': count
                    //});
                    //res.raise('complete', {
                    //    'count': count
                    //});

                    res.raise('next', count);
                    res.raise('complete', count);
                    clearInterval(repeater);
                });
            return res;
        }

        */
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
        /*
        return sig_obs_or_cb(arguments, (a, sig, next, complete, error))
        // don't think we need this.
        */

        /*

        let a = arguments,
            sig = get_a_sig(a);
        let decode = false;

        // Don't want decoding option here. It's easy to do from the result objects anyway now.


        if (sig === '[n,b]') {
            decode = a[1];
            opt_cb = null;
        }
        */

        let kp = table_id * 2 + 2;
        let obs = this.ll_get_records_with_kp(xas2(kp).buffer);


        return obs_or_cb(obs, opt_cb);

        //return obs;

        /*

        if (decode) {

            let res = new Evented_Class();
            obs.on('next', data => {
                let decoded = Model_Database.decode_model_row(data);
                //console.log('decoded', decoded);

                res.raise('next', decoded);
            })
            obs.on('complete', () => {
                //console.log('ll_get_table_index_records obs complete');
                res.raise('complete');
            })
            return res;
        } else {
            return obs;
        }
        */
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


        //throw 'stop';
        // Observe them, having got the kp right for the indexes

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
                //console.log('system_db_rows', system_db_rows);

                // Looks like it did not add the incrementors when ensuring the db.



                //throw 'atop';
                // The table incrementor value should be at least about 4.

                //let decoded_system_db_rows = Model_Database.decode_model_rows(system_db_rows);
                //console.log('system_db_rows', system_db_rows);
                //console.log('system_db_rows.length', system_db_rows.length);
                //throw 'stop';
                //throw 'stop';

                // Seems the model db had not loaded all the right info.
                //  Misses the new table definitions. It has put the new incrementors in.
                //   Seems like the added table rows need to specifically be generated and added to the model.
                //   Could / should have this automatic upon making new tables that get (successfully) added to the model.

                // Then it won't be long until we are able to store a large amount of data, with high performance.
                //  It's the part where new tables get added to the model.
                //   If the model is not in initialisation mode, or early_init mode, we add the table records and index records as the tables get added


                // Not reading those rows in the same way?
                //  Some db rows not loaded into the model properly....


                // Null rather than an empty array being better everywhere?


                this.model = Model_Database.load(system_db_rows);

                // Check that the model rows from the db are the same length as those re-obtained from the model db.


                // Leaving out the index of incrementors.??
                // 
                let model_rows = this.model.rows;
                //  getting the model rows missing some of them out for some reason?




                if (model_rows.length !== system_db_rows.length) {
                    //console.log('system_db_rows.length', system_db_rows.length);
                    //console.log('model_rows.length', model_rows.length);

                    // 13/03/2018 - This is a newly discovered bug where the model does not make every table (missing the native types table) when it gets reconstructed.

                    // do a diff here?


                    //console.log('system_db_rows', Model_Database.decode_model_rows(system_db_rows));
                    console.log('system_db_rows');
                    each(system_db_rows, row => console.log('row.decoded', row.decoded));
                    console.log('model_rows', Model_Database.decode_model_rows(model_rows));

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

                    //each(diff.changed, changed => console.log('changed', changed));

                    each(diff.added, added => {
                        console.log('added', added.decoded);
                    });

                    each(diff.changed, changed => {
                        console.log('changed[0]', changed[0].decoded);
                        console.log('changed[1]', changed[1].decoded);
                    });

                    each(diff.deleted, deleted => {
                        console.log('deleted', deleted.decoded);
                    });

                    // Though this error is very annoying, it will help to keep things in sync and prevent it from getting worse.
                    //  This checks that the model rows have been loaded properly from the DB

                    console.log('system_db_rows.length', system_db_rows.length);
                    console.log('model_rows.length', model_rows.length);

                    callback(new Error('Mismatch between core db rows and core rows obtained from model. '));



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





    ensure_record(arr_record, callback) {

        // an inner promise / observable

        // will work differently when the key is automatically generated.
        throw 'NYI';
        let table_pk = arr_record[0][0];

        let table_id = (table_pk - 2) / 2




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

                    //throw 'stop';

                    // then persist that diff.
                    // The table indexes...


                    //console.log('pre persist');
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




        // next({
        //    type: 'update',
        //    data: 'difference persisted    
        //})



        // could take the table name
        //  as well as the table definition?

        // cloned model add table
        // model.Table(arr_table);

        // Optional callback, otherwise return observable


        // Ensure the table matched the given structure.

        // Do we already have the table?


        // A reliable way of comparing a table def against the actual table looks like it would be useful.
        //  Checking the fields and indexes would be useful.

        // Parsing the table def to fields and indexes definitions seems like it would help.
        //  There is a fair bit more complexity en route to having the fully working crypto and other db system.
        //  They do not seem like huge algorythms, but quite a lot of supporting structure for the table and indexing system.

        // Want to be sure, piece by piece, that crypto data can be collected in a robust, efficient, secure and flexible way.

        // Making it so that tables can be added while DB is running is a fairly standard DB feature.
        //  Just quite a lot of coding to do here and in a few other places.


        // If the table doesn't exist, then create it according to the spec.


        /*

        let inner = (cb) => {
            //console.log('ensure_table arr_table', arr_table);
            let table_name = arr_table[0];
            //console.log('table_name', table_name);
            this.table_exists(table_name, (err, exists) => {
                if (err) {
                    cb(err);
                } else {
                    //console.log('table_name exists', exists);
                    if (exists) {
                        console.log('table already exists');
                        // maybe return the id
                        cb(null, true);
                    } else {
                        // Would need to clone the existing model first


                        //let old_model = Object.assign(Object.create(Object.getPrototypeOf(this.model)), this.model)
                        let old_model_rows = this.model.get_model_rows();
                        //console.log('old_model_rows', old_model_rows);

                        //let old_model = clone(this.model);
                        //let old_model = this.model.clone();

                        let model_table = this.model.add_table(arr_table);
                        //console.log('model_table.id', model_table.id);

                        // The new table has the wrong id (0)
                        //  It should make use of the table incrementor to get the id.
                        //   The table incrementor had not been set correctly when initially creating the model, or not saved properly into the DB.

                        //throw 'stop';
                        // Then do the whole model diff...

                        //console.log('old_model', old_model);

                        // diff on the rows.


                        let new_model_rows = this.model.get_model_rows();

                        //let diff = old_model.diff(this.model);

                        // And we put whichever rows are different.
                        // console.log('diff', diff);


                        //let model_table = new Model.Table(arr_table);
                        //console.log('model_table', model_table);

                        //console.log('old_model_rows decoded', Model_Database.decode_model_rows(old_model_rows));
                        //console.log('new_model_rows decoded', Model_Database.decode_model_rows(new_model_rows));

                        //console.log('old_model_rows.length', old_model_rows.length);
                        //console.log('new_model_rows.length', new_model_rows.length);

                        // Simple array diff.
                        //  Any array rows that are new / deleted / changed.

                        // Need our own differencing algorythm here.

                        // Keys that appear in both.
                        //  With the same values, or with different values.
                        // Keys that appear in one and not the other

                        // Changed
                        // Added
                        // Deleted

                        let diff = Model_Database.diff_model_rows(old_model_rows, new_model_rows);
                        //console.log('diff ' + JSON.stringify(diff, null, 4));

                        //console.log('diff.changed.length', diff.changed.length);
                        //console.log('diff.added.length', diff.added.length);
                        //console.log('diff.deleted.length', diff.deleted.length);
                        // Then persist all of the changed rows to the database.

                        //throw 'stop';

                        this.persist_row_diffs(diff, cb);

                        // Then a diff of them both.

                        //throw 'stop';
                    }
                }
            })

            // Check to see if we already have the table.
            // Using await would be good for getting that value.


            // Use something in the model to parse the table...
            //  Some effective server-side tech will require use of the Model to carry out operations in a more OO way.


        }
        if (callback) {
            inner(callback);
        } else {
            // Return a promise / observable (resolvable)

            throw 'NYI';
        }

        */
    }


    // This one can definitely be improved greatly.

    // Definitely would like a sample / test database which does not have all that many records.



    ensure_tables(arr_tables, callback) {
        // Optional callback, otherwise return observable
        //  Could do this as an observable, but use optional_observable()


        // For of over the array of tables

        let a = arguments,
            sig = get_a_sig(arguments);

        console.log('ensure_tables sig', sig);
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
                console.log('1) arr_table', arr_table);
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
                console.log('arr_table', arr_table);
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



    // A variety of ll functions will have a lot more complexity involving observable results, flexible calling, polymorphism, calling of optimised inner functions.



    // Then get records by prefix limit....


    // Maybe we would want to specify limit = 0 meaning it's -1 in level terms.
    //  We just won't use limit of actually 0 records... just don't do the query.



    // Then a server function to get all ll records with key beginning ...

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
                        //console.log('table_id', table_id);

                        console.log('arr_buf_idx_res', arr_buf_idx_res);

                        //console.log('arr_buf_idx_res[0][0]', arr_buf_idx_res[0][0]);
                        //console.trace();



                        //let decoded = Model_Database.decode_key(arr_buf_idx_res[0][0]);

                        let decoded_key = arr_buf_idx_res[0].decoded[0];

                        console.log('decoded_key', decoded_key);
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
            //let kp = id * 2 + 2;
            //let ikp = kp + 1;
            //console.log('id', id);
            //console.log('kp', kp);
            //console.log('ikp', ikp);

            // What if it can't find the value?
            //  Undefined probably is better than an Error.

            console.log('table_name', table_name);



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


        // This should use a promise-or-callback mechanism.

        // Need to reference the 'tables' table.
        // that should have been loaded into the model.



        // Should be able to get the table id from the server's loaded model.
        //  This could be regarded as a caching layer that prevents round trips.

        // Do this with a DB lookup though.

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


    // get table system db rows
    //  

    // Get system db rows...
    // Will change this to get records in their normal way
    //  will return an observable by default.

    // However, using an array directly could be quicker for building directly, with less overhead.


    // Start changing terminology to 'records' in various places, and use the BB_Record from Model

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
        }, callback)



        // tables ids 0, 1, 2, 3
        // tables, native types, table fields, table indexes

        // so, the very start of the key space between 0 and 7 (1 + 2 * 3)  1 being a 0 indexed 2
        //  tables key space starts at 2, each table has got 2 key spaces

    }




    // less need for this, a record-list would be fine, easy to make.
    /*
    get_system_db_buffer(callback) {
        // tables ids 0, 1, 2
        // tables, native types, table fields

        // so, the very start of the key space between 0 and 7 (1 + 2 * 3)  1 being a 0 indexed 2
        //  tables key space starts at 2, each table has got 2 key spaces

        // May be nice to continuously add to a large buffer?
        //  Encode each row as buffer...

        // Build up the buffer in the simple way with row lengths.

        var arr_buf_res = [];
        // probly better to stream the records than get them all at once?

        this.db.createReadStream({
            'gte': xas2(0).buffer,
            'lte': xas2(9).buffer
        }).on('data', function (record) {
            arr_buf_res.push(Binary_Encoding.join_buffer_pair([record.key, record.value]));
        }).on('error', function (err) {
            //console.log('Oh my!', err)
            callback(err);
        }).on('close', function () {
            //console.log('Stream closed')
        }).on('end', function () {
            //console.log('arr_buf_res', arr_buf_res);
            callback(null, Buffer.concat(arr_buf_res));
        })
    }
    */

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
        // An inner promise would probably work best.=

        // Then first and last keys based on that.

        // Already have something in the binary handler that does this.
        //  Will do first and last keys by range


        /*
        let buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        let buf_255 = Buffer.alloc(1);
        buf_255.writeUInt8(255, 0);
        //console.log('kp', kp);
        //console.log('buf_key_beginning', buf_key_beginning);
        let buf_l = Buffer.concat([buf_key_beginning, buf_0]);
        let buf_u = Buffer.concat([buf_key_beginning, buf_255]);
        // This will also need a decode option.
        */


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

        // range query, limit 0. can use short code route here.


        // This is worth promisifying
        //  Would help elsewhere in the codebase.
        //  with no remove_kp, decode
        //   returns a Key object which itself can remove its KP if necessary and its easy to decode.



        /*

        let a = arguments;



        if (a.length === 3) {
            callback = a[2];
            decode = true;
        }
        if (a.length === 4) {

        } else {

            console.log('a.length', a.length);
            console.trace();

            throw 'get_first_key_in_range NYI';
        }
        */
        // return prom_or_cb




        //console.log('get_first_key_in_range [remove_kp, decode]', remove_kp, decode);

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

        /*
        let a = arguments;
        if (a.length === 2) {
            callback = a[1];
            decode = false;
            remove_kp = false;
        } else if (a.length === 3) {
            callback = a[2];
            decode = true;
        } else if (a.length === 4) {

        } else {
            console.log('a.length', a.length);
            throw 'get_last_key_in_range NYI';
        }
        let res;

        this.db.createKeyStream({
            'gte': arr_range[0],
            'lte': arr_range[1],
            'limit': 1,
            'reverse': true
        }).on('data', function (key) {
            //console.log('key', key);
            res = key;
        })
            .on('error', function (err) {
                //console.log('Oh my!', err);
            })
            .on('close', function () {
                //console.log('Stream closed');
            })
            .on('end', function () {
                // decode
                if (res) {
                    //console.log('get_last_key_in_range res', res);
                    if (decode) {
                        let decoded = Model_Database.decode_key(res);
                        if (remove_kp) {
                            decoded.shift();
                        }
                        callback(null, decoded);
                    } else {
                        // Can still remove the kp from the encoded buffer.
                        if (remove_kp) {
                            callback(null, Binary_Encoding.remove_kp(res));
                        } else {
                            callback(null, res);
                        }
                    }
                } else {
                    callback(null, undefined);
                }
            });

            */
    }

    // can use above fn because we know range

    get_last_key_in_table(table, callback) {


        // find the table key range

        //let [buf_l, buf_u] = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        let range = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        return this.get_last_key_in_range(range, callback);


        /*

        let table_id, table_name;
        let t_table = tof(table);
        //console.log('table', table);
        if (t_table === 'number') {
            table_id = table;
        }
        if (t_table === 'string') {
            table_name = table;
        }
        let proceed = () => {
            let i_kp = table_id * 2 + 2;
            //console.log('i_kp', i_kp);
            let kp = xas2(i_kp).buffer;
            let buf_0 = Buffer.alloc(1);
            buf_0.writeUInt8(0, 0);
            let buf_255 = Buffer.alloc(1);
            buf_255.writeUInt8(255, 0);
            //console.log('kp', kp);

            let buf_l = Buffer.concat([kp, buf_0]);
            let buf_u = Buffer.concat([kp, buf_255]);
            //console.log('buf_l', buf_l);
            //console.log('buf_u', buf_u);

            let res;

            this.db.createKeyStream({
                'gte': buf_l,
                'lte': buf_u,
                'limit': 1,
                'reverse': true
            }).on('data', function (key) {
                //console.log('key', key);
                res = key;

            })
                .on('error', function (err) {
                    //console.log('Oh my!', err);
                })
                .on('close', function () {
                    //console.log('Stream closed');
                })
                .on('end', function () {
                    // decode
                    if (res) {
                        //console.log('res', res);
                        let decoded = Model_Database.decode_key(res);
                        callback(null, decoded);
                    } else {
                        callback(null, undefined);
                    }
                    //console.log('Stream ended')
                });
        }
        if (def(table_id)) {
            // do a range query.
            proceed();
        } else {
            this.get_table_id_by_name(table_name, (err, _table_id) => {
                if (err) {
                    callback(err);
                } else {
                    table_id = _table_id;
                    proceed();
                }
            })
        }
        */
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


    _get_records_in_range(buf_l, buf_u, decoding = false, remove_kp = true, limit = -1, callback) {
        // may want to call this using a single arr to hold the ranges.
        // remove options decoding, remove_kp
        // Make this use an Observable(...)
        let a = arguments,
            l = a.length,
            sig = get_a_sig(a);

        console.log('get_records_in_range sig', sig);

        if (sig === '[a]') {
            [buf_l, buf_u] = a[0];
            decoding = a[1] || false;
            remove_kp = a[2] || false;
            limit = -1;
        }

        let inner = () => {
            let res = new Evented_Class();
            res.response_type = 'records';
            res.decoded = decoding;

            // a record decoding wrapper could work, not sure about speed.
            // but with decoding option

            if (!decoding) {
                this.db.createReadStream({
                    'gt': buf_l,
                    'lt': buf_u,
                    'limit': limit
                }).on('data', function (data) {
                    // maybe remove the pk from the field.
                    //if (remove_kp) {

                    //}
                    let buf_combined = Binary_Encoding.join_buffer_pair([(remove_kp) ? Binary_Encoding.remove_kp(data.key) : data.key, data.value]);
                    res.raise('next', buf_combined);
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('next', err);
                    })
                    .on('close', function () {
                        //console.log('1) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);
                        res.raise('complete', {});
                    })
            } else {
                this.db.createReadStream({
                    'gt': buf_l,
                    'lt': buf_u
                }).on('data', function (data) {
                    console.log('data', data);

                    let decoded = Model_Database.decode_model_row([data.key, data.value]);
                    //console.log('decoded', decoded);
                    if (remove_kp) decoded[0].shift();
                    //console.log('decoded', decoded);
                    res.raise('next', decoded);
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('error', err);
                    })
                    .on('close', function () {
                        //console.log('2) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);

                        //console.log('completed');
                        res.raise('complete', {});
                    })
            }
            return res;
        }

        if (callback) {
            let obs = inner();
            let res = [];
            obs.on('next', data => res.push(data));
            obs.on('error', err => callback(err));
            obs.on('complete', () => callback(null, res));
        } else {
            return inner();
        }
    }

    get_records_in_ranges(arr_ranges) {
        // No decoding or kp removal here.
        //  That would be an option / obs transformer for when the results 


        // probably just an observable producing the results is best.
        //  while faster results could be available by batching the responses here, it's messier code, and I'm going for simpler code where possible.

        //console.log('get_records_in_ranges', arr_ranges);

        // multiple observables, carry them out in sequence.
        //  unlikely that making parallel calls here will give much of an advantage, could mess up ordering.

        // get records in range, called as a sequence of observables.
        // obs_arrayified_call(this, this.get_records_in_range, arr_ranges);
        //  just pass through all of the results.

        // for of the ranges, then get the records for them


        let res = obs_arrayified_call(this, this.ll_get_records_in_range, arr_ranges);
        res.response_type = res.response_type || 'records';
        return res;
    }


    // An options object may be the right way
    //  Could make a decoding 
    //  get rid of decoding and remove kp options
    //  could happen later on
    //  will return oo buffer backed Key classes, not buffers.

    // don't decode or remove kps here

    // make this return observable by default, but can result in callback.

    // obs_cb
    //  creates the observable but raises callback if there is one.

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
        })


        //obs_or_cb_sig(arguments, )
        /*
        let a = arguments,
            l = a.length;
        if (l === 5) {
            callback = a[4];
            limit = -1;
        }
        */


        // Will be possible to remove the key prefixes while not decoding them.


        /*
        let inner = () => {
            let res = new Evented_Class();
            res.response_type = 'keys';
            res.decoded = decoding;

            // but with decoding option

            if (!decoding) {
                this.db.createKeyStream({
                    'gt': buf_l,
                    'lt': buf_u,
                    'limit': limit
                }).on('data', key => {
                    if (remove_kp) {
                        res.raise('next', Binary_Encoding.remove_kp(key));
                    } else {
                        res.raise('next', key)
                    }
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('next', err);
                    })
                    .on('close', function () {
                        //console.log('1) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);
                        res.raise('complete', {});
                    })
            } else {
                this.db.createKeyStream({
                    'gt': buf_l,
                    'lt': buf_u,
                    'limit': limit
                }).on('data', function (key) {
                    //console.log('data', data);
                    let decoded = encoding.decode_key(key);
                    //console.log('decoded', decoded);
                    if (remove_kp) decoded.shift();
                    res.raise('next', decoded);
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('error', err);
                    })
                    .on('close', function () {
                        //console.log('2) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);

                        //console.log('completed');
                        res.raise('complete', {});
                    })
            }
            return res;
        }

        if (callback) {
            let obs = inner();
            let res = [];
            obs.on('next', data => res.push(data));
            obs.on('error', err => callback(err));
            obs.on('complete', () => callback(null, res));
        } else {
            return inner();
        }
        */
    }



    _get_keys_in_range(buf_l, buf_u, decoding = false, remove_kp = true, limit = -1, callback) {

        let a = arguments,
            l = a.length;
        if (l === 5) {
            callback = a[4];
            limit = -1;
        }


        // Will be possible to remove the key prefixes while not decoding them.

        let inner = () => {
            let res = new Evented_Class();
            res.response_type = 'keys';
            res.decoded = decoding;

            // but with decoding option

            if (!decoding) {
                this.db.createKeyStream({
                    'gt': buf_l,
                    'lt': buf_u,
                    'limit': limit
                }).on('data', key => {
                    if (remove_kp) {
                        res.raise('next', Binary_Encoding.remove_kp(key));
                    } else {
                        res.raise('next', key)
                    }
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('next', err);
                    })
                    .on('close', function () {
                        //console.log('1) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);
                        res.raise('complete', {});
                    })
            } else {
                this.db.createKeyStream({
                    'gt': buf_l,
                    'lt': buf_u,
                    'limit': limit
                }).on('data', function (key) {
                    //console.log('data', data);
                    let decoded = encoding.decode_key(key);
                    //console.log('decoded', decoded);
                    if (remove_kp) decoded.shift();
                    res.raise('next', decoded);
                })
                    .on('error', function (err) {
                        //callback(err);
                        res.raise('error', err);
                    })
                    .on('close', function () {
                        //console.log('2) Stream closed')
                    })
                    .on('end', function () {
                        //callback(null, res);
                        //console.log('arr_res', arr_res);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);

                        //console.log('completed');
                        res.raise('complete', {});
                    })
            }
            return res;
        }

        if (callback) {
            let obs = inner();
            let res = [];
            obs.on('next', data => res.push(data));
            obs.on('error', err => callback(err));
            obs.on('complete', () => callback(null, res));
        } else {
            return inner();
        }
    }


    get_table_record_by_key(table, key, callback) {
        let table_id = this.model.table_id(table);
        let table_kp = table_id * 2 + 2;

        // get single record in range?

        // 

        let arr_record_key = [table_kp].concat(key);

        // seems we don't have a simple get call in use

        let buf_key = Binary_Encoding.encode_to_buffer_use_kps(arr_record_key, 1);
        //console.log('buf_key', buf_key);

        //console.log('pre db.get');

        //let c = 0;
        this.db.get(buf_key, (err, res) => {
            //c++;
            //console.log('c', c);
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

    // model.prefix_to_range


    // a limit property would be cool too.
    //  but then would need to work on param parsing more.

    // Decoding while removing the key prefix could be useful... but we don't have it right now


    // get keys in range


    get_table_records(table, callback) {
        let range = this.model.map_tables_by_id[this.model.table_id(table)].key_range;
        return this.get_records_in_range(range, callback);
    }

    /*
    get_table_records(table, decode = false, remove_kp = false, callback) {
        console.log('get_table_records table', table);
        // Should have option to remove the kps.
        //  That would be the default when decoding.
        let inner = () => {
            let buf_key_prefix = xas2(this.model.table_id(table) * 2 + 2).buffer;

            let buf_0 = Buffer.alloc(1);
            buf_0.writeUInt8(0, 0);
            let buf_1 = Buffer.alloc(1);
            buf_1.writeUInt8(255, 0);
            // and another 0 byte...?

            let buf_l = Buffer.concat([buf_key_prefix, buf_0]);
            let buf_u = Buffer.concat([buf_key_prefix, buf_1]);

            // While removing the key prefixes?
            //  That would be expected when it's specific to the table
            //  Would be nice (but extra work) to hava as an option

            // With remove kp
            //let remove_kp = true;
            //console.log('buf_l, buf_u', buf_l, buf_u);
            let res = this.get_records_in_range(buf_l, buf_u, decode, remove_kp);
            return res;
        }


        if (callback) {
            let obs = inner();
            let res = [];
            obs.on('next', data => res.push(data));
            obs.on('error', err => callback(err));
            obs.on('complete', () => callback(null, res));
        } else {
            return inner();
        }
        // could have a callback
        //let res = new Evented_Class();
    }
    */


    // Inner observable here
    //  Get rid of excess options.
    get_table_keys(table, callback) {

        //let buf_key_prefix = xas2(this.model.table_id(table) * 2 + 2).buffer;
        let range = kp_to_range(xas2(this.model.table_id(table) * 2 + 2).buffer);
        return this.get_keys_in_range(range, callback);

        /*

        let inner = () => {
            //console.log('this.model.table_id(table)', this.model.table_id(table));
            let buf_key_prefix = xas2(this.model.table_id(table) * 2 + 2).buffer;
            //console.log('buf_key_prefix', buf_key_prefix);
            //throw 'stop';
            let buf_0 = Buffer.alloc(1);
            buf_0.writeUInt8(0, 0);
            let buf_1 = Buffer.alloc(1);
            buf_1.writeUInt8(255, 0);
            // and another 0 byte...?

            let buf_l = Buffer.concat([buf_key_prefix, buf_0]);
            let buf_u = Buffer.concat([buf_key_prefix, buf_1]);


            // While removing the key prefixes?
            //  That would be expected when it's specific to the table
            //  Would be nice (but extra work) to hava as an option

            // With remove kp
            //let remove_kp = true;
            //let res = this.get_keys_in_range(buf_l, buf_u, decode, remove_kp);

            let res = this.get_keys_in_range(buf_l, buf_u);
            return res;
        }
        if (callback) {
            let obs = inner();
            let res = [];
            obs.on('next', data => res.push(data));
            obs.on('error', err => callback(err));
            obs.on('complete', () => callback(null, res));
        } else {
            return inner();
        }
        */
    }


    // get all table keys

    // fn_all_tables...


    // rather we iterate the tables, get their keys easily.





    // Would be nice to get this working from the client.
    //  When syncing smaller tables can look for that hash value to compare.

    // Get records_in_range_hash
    //  Though we could have items in the API that operate on tables, it's a bit higher level.
    //  Don't have get_table_records though.

    // Maybe hashes of records should be similar. Expose that through the API, then the client can get table record hashes that way
    //  As well as hashes of other key selections of records.


    // Watch an observable and return promise or callback.
    get_table_records_hash(table, callback) {

        return prom_or_cb((resolve, reject) => {
            let obs_records = this.get_table_records(table);
            let hash = crypto.createHash('sha256');
            obs_records.on('next', record => hash.update(record));
            obs_records.on('complete', () => resolve(hash.digest('hex')));
        });

        /*

        console.log('table', table);
        let pr_inner = () => {
            let res = new Promise((resolve, reject) => {
                let obs_records = this.get_table_records(table, false, false);
                let hash = crypto.createHash('sha256');
                obs_records.on('next', record => hash.update(record));
                obs_records.on('complete', () => resolve(hash.digest('hex')));
            });
            return res;
        }
        if (callback) {
            throw 'NYI';
        } else {
            return pr_inner();
        }
        */
    }


    // table.fields_info


    get_table_fields_info(table, callback) {


        let _table = this.model.map_tables_by_id[this.model.table_id(table)];


        if (callback) {
            callback(null, table.fields_info);
        } else {
            return table.fields_info;
        }



        /*


        // May be better by far to use the model here, not interpreting DB rows.
        let inner = (callback) => {
            let table_id, table_name;
            let t_table = tof(table);
            if (t_table === 'number') {
                table_id = table;
            }
            if (t_table === 'string') {
                table_name = table;
            }
            let proceed = () => {
                let table = this.model.map_tables_by_id[table_id];
                //console.log('table', table);
                let fields = table.fields;
                //console.log('fields', fields);
                let res = [];

                each(fields, field => {
                    let id = field.id;
                    let name = field.name;
                    let fk_to_table = field.fk_to_table;
                    let type_id = field.type_id;
                    let obj_res = {
                        'id': id,
                        'name': name,
                        'type_id': type_id
                    }
                    if (fk_to_table) {
                        let fk_pk = fk_to_table.pk;
                        let fk_pk_fields = fk_pk.fields;
                        let fk_to_fields = [];
                        each(fk_pk_fields, fk_to_field => {
                            fk_to_fields.push([fk_to_field.id, fk_to_field.name, fk_to_field.type_id]);
                        })
                        obj_res.fk_to = {
                            'table_name': fk_to_table.name,
                            'table_id': fk_to_table.id,
                            'fields': fk_to_fields
                        }
                    }
                    res.push(obj_res);
                });
                //console.log('res', res);
                callback(null, res);
            }

            if (table_id === undefined && table_name) {
                // look up the table id
                this.get_table_id_by_name(table_name, (err, id) => {
                    if (err) {
                        callback(err);
                    } else {
                        table_id = id;
                        proceed();
                    }
                });
            } else {
                if (table_id !== undefined) {
                    proceed();
                }
            }
        }

        inner((err, res) => {
            if (err) {
                callback(err);
            } else {
                //console.log('res', res);
                callback(null, res);
            }
        });
        */
    }





    // could use an observable creator function.
    //  the handler gets called with 3 functions available for it to call.


    // can just be get_records or get_records_by_keys
    //  observable is becoming more standard.

    // Could just use 'get' command.

    // Want some kind of functional optimised fp.



    // get_records_by_keys

    obs_get_records(key_list) {
        // would be nice to do for of with key list
        //console.log('obs_get_records key_list', key_list);
        // But keys that are not there?

        // Could return an Observable result.


        // for of the key list



        //return observable(())




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


        /*

        let res = new Evented_Class();

        let inner = async () => {
            //console.log('key_list.length', key_list.length);
            for (let key of key_list) {
                //console.log('key_list key', key);
                try {
                    let record = await this.get(key);

                    // could use this.has for nicer syntax.


                    //console.log('** record', record);

                    if (record) {
                        res.raise('next', record);
                    } else {
                        //res.raise('error', key);
                    }

                    //
                    //res.raise('next', record);
                } catch (err) {

                    // Don't use an error like this with observable


                    // not found ???

                    //console.log('err', err);

                    res.raise('error', key);
                }

            }
            res.raise('complete');
        }
        inner();
        return res;

        */
    }


    // maintain / scan
    obs_records_not_found(records) {
        //obs_keys_not_found
        // as in the keys that are not found.


        // go through each record

        // can we do for-of?
        //  for of with an async function would be best

        // need a for of the records.

        // Have an observable that searches for these records
        //  As in looks for them separately

        // could get the records by keys
        //  with an observable

        //console.log('records', records);


        // observer get records...
        //  and then raise when they are not found.



        /*
        let res = new Evented_Class();

        let kl = new Key_List(records);

        // We would want to be able to yield keys as we want them too?


        // get_all_recordss

        let obs = this.obs_get_records(kl);

        console.log('kl.length', kl.length);
        console.log('kl', kl);

        obs.on('next', record => {
            //console.log('** found record');
        })
        obs.on('error', key => {

            console.log('error', key);

            res.raise('next', key);
        })
        obs.on('complete', () => {
            res.raise('complete');
        });

        return res;
        */

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







    // non-core, not completed yet. ensure_index_records




    // Simplify putting of data
    //  Use the buffer-backed data


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

        // Seems like a problem with ensuring unique indexes while putting the record.

        // Want to be able to put a record while being sure it does not make a collision.
        //  Would also need a client-side version of this, as it is core.







        // ensure model is up-to-date
        //  at least in the section concerning the table


        // 

        // 


        // For the moment, could assume all indexes are unique indexes
        //  Later on, completely get rid of that, and use unique constraints, by table.
        //   Any valid record needs to satisfy the constraints, and those constraints are stored as constraints.


        let model_table;
        if (typeof table === 'number') {
            model_table = this.model.map_tables_by_id[table];
        }



        console.log('table', table);

        // model table

        // Will deal with the indexing as expected.

        return prom_or_cb(async (resolve, reject) => {

            let indexes = model_table.indexes;

            // So it's worked out the record from the Model.


            console.log('put_table_record', record);
            console.log('record instanceof B_Record', record instanceof B_Record);
            console.log('Array.isArray(record)', Array.isArray(record));

            if (record instanceof B_Record) {
                // may need to put index records too

                console.log('indexes.length', indexes.length);
                console.trace();
                throw 'NYI';

                if (indexes.length > 0) {

                } else {
                    // Put that single record.

                    // ll_put makes sense.

                    // want a smallish number of ll_functions, doing some essential tasks relatively simply.



                    //resolve()
                }
            } else if (Array.isArray(record)) {
                //let b_records = model_table.


                // get the pk inc value directly from the db?
                //  ensure that part of the model is up-to-date.

                // Active incrementors will help considerably.
                //  Better if increment happens on the DB, synced quickly


                // do an index lookup on the unique fields.
                //  fail to overwrite if any unique fields are already there.


                // Can't get the unique fields from the table.
                //  Unique field constraints not implemented.


                // Could make ensure_record
                //  it looks it up using indexes info (not relying on unique constraint)

                console.log('pre look for unique fields');
                console.log('model_table.unique_fields', model_table.unique_fields);
                console.log('post look for unique fields');

                // type_id




                let old_model_pk_inc_val = model_table.pk_incrementor.value;

                console.log('m_record.pk_incrementor', model_table.pk_incrementor);
                let m_record = model_table.add_record(record);
                console.log('m_record', m_record);

                console.log('m_record.pk_incrementor', model_table.pk_incrementor);

                let new_model_pk_inc_val = model_table.pk_incrementor.value;

                console.log('old_model_pk_inc_val', old_model_pk_inc_val);
                console.log('new_model_pk_inc_val', new_model_pk_inc_val);

                let b_records = m_record.to_b_records();


                if (old_model_pk_inc_val !== new_model_pk_inc_val) {
                    b_records.push(model_table.pk_incrementor.record);
                }

                console.log('b_records', b_records);

                // Could use the type id to represent if the field is unique as well as the type.
                //  if type_id > 128  subtract 128 and say it's a unique field.

                // This lack of unique field status is a challenge right now.
                //  don't want to assume all indexed fields have to be unique.


                // Getting unique fields (and unique indexes) right seems important.
                //  The distinction between unique and non-unique indexes.

                // Could have multiple crypto-trades at the same timestamp on the same exchange.
                //  May want to index them in a way that does not assume uniqueness.

                // Uniqueness could be done as a constraint.
                //  Not sure how much internal change to the DB it would require.

                // Unique constraints may be the way to go about it.
                //  loads the constraints from a field constraints table
                //  says which field it applies to.
                // Old DBs simply would not support or have any unique constraints that way.
                //  However, may need to have / make some reserved system space in the DBs.

                // Mass-shifting records + indexes would be useful.
                //  Being able to move a record's id.


                // Not doing unique record / constraint checking on put.
                //  For the moment no constraint checking makes sense, as there are no constraints to check.


                // Being able to move a table would make sense.
                //  Indexes refer to a table by id, so would need to change all index records that point towards it.
                //  The records themselves. FKs that make reference to 

                // Changing a whole bunch of records at once...
                //  Key definitions refer to the table.

                // Being able to shift tables (all) along would make sense.
                //  Seems like a somewhat complicated / involved function.

                // May be best just to use indexes as they are for the moment.
                //  Don't have a way to mark / check if fields are unique though.

                // Going through every LL record, parsing it, interpreting it will help to be able to change the index values.
                //  LL_Active_Record? So it refers to a row in the DB?

                // Row - low level leveldb row
                // Record - logical unit including its row, and all indexes.
                //  Shifting KPs will be part of it.

                // Could change a table id in the model, then see how it is reflected in the core rows.
                //  

                // Getting back on with the saving of crypto data...
                //  Could be done without unique constraints.

                // The next big change to the DB will have further space for system tables.
                //  Reserving space (by id) for 6 more system tables would be of use.

                // Need to save the crypto data properly now.
                //  Looks like the crypto saving system broke, it was not collecting data.

                // Seems like going for the crypto collecing reliability is important.
                //  Very likely to have lost some data from the last few daya (not collected it in the first place).


                // Really this should be simple logging, but it's turned into a quite complex DB project.
                //  Getting info about movement over the last few hours will certainly be useful.
                //  Really need to have this sitting there gathering info.

                // Actually, saved data collection so far seems OK.
                //  Want to work on the crypto-data-collector to give it a nicer interface.
                //  Bittrex watcher could do its repeated collect, and then raise events. The collector would then watch the watcher.

                // worth getting running on data11, data12
                //  data13 could even amalgamate data from the other servers.

                // copying all the backup dbs would help.
                //  should be able to sort through them and import data.


                // Definitely worth getting another crypto collector running soon, and sync the data from it.
                //  Being able to download a ZIP of all records? All price info records?
                //  Getting this in a normalised form?
                //  Records grouped together by month and by exchange?

                // Making graphs of the data will definitely help to tell if it's full / valid.

                // Worth setting up data11 with improved collector.
                //  Then get the data back from it.

                // 







                //console.log('model_table.fields', model_table.fields.map(x => x.type_id));


                //throw 'stop';

                let res = await this.ll_batch_put(b_records);
                resolve(res);


                // get the incrementor record for the table id.
                //  worth getting that from the model,
                //  checking that it matches the server's current value
                //  

                //console.log('Object.keys(model_table)' + Object.keys(model_table));


            }

        }, callback);
    }



    // This one splices the kp into it.
    //  Decoding the records has removed the kp.

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
        return prom_or_cb((resolve, reject) => {
            //console.log('arr_items', arr_items);

            let ops = arr_items.map(item => {
                //console.log('item', item);

                if (item instanceof B_Record) {
                    return {
                        'type': 'put',
                        'key': item.kvp_bufs[0],
                        'value': item.kvp_bufs[1]
                    }
                }

                /*
                return {
                    'type': 'put',
                    'key': item[0],
                    'value': item[1]
                }
                */
            });

            //console.log('ops', ops);

            /*

            db.batch(ops, function (err) {
                if (err) {
                    reject(err);
                } else {

                    resolve();

                }
            });
            */

            return this.db.batch(ops);








            //let ops = [];
            //each()

            // Could load it up into a Record_List to ensure the format
            //  



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


    // increment_incrementor (incrementor_id)


    // Could ensure multiple tables with one command from the client.
    //  Would need to encode the table definitions on the client, and send them to the server.
    //  The server having its own copy of the model makes it more efficient.

    // non-core persist_row_diffs


    // Setting removal of kp to false here may help.
    //  It could be done in a later processing stage. May be less efficient that way.




    // non-core select_from_records_in_range

    // select from records in range
    //  again may need a remove_kp option



    // validate index records against records
    // validate records against index records.

    // get all index records


    // A generator to yield records definitely looks like a nice way of doing it.
    //  However, pausing and resuming the db getting could be a bit tricky.
    //  Could write something, but not worth it right now.

    // An observable that gets all index rows (could be in specified tables) seems like the way.

    // Get all index rows for all tables.
    //  Would get the index row for each table.

    // Will be better moving to more async code.

    // get_all_index_records

    // use an observable sequencer to read all index records.

    // Having decoding options would be useful here.

    //  The seelction of data when it's encoded could be a bit more efficient.
    //   Decoding would take place at the last step.

    // want parameter parsing to be much shorter.
    //  don't have decode, callback is optional.
    //  only 2 or 3 params.





    // WIP
    //  Reads from the server-side model db.

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