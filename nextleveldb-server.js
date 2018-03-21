const jsgui = require('jsgui3');
const tof = jsgui.tof;
const each = jsgui.each;
const is_array = jsgui.is_array;
const arrayify = jsgui.arrayify;
const get_a_sig = jsgui.get_a_sig;
const Fns = jsgui.Fns;
//const clone = jsgui.clone;



const Evented_Class = jsgui.Evented_Class;

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
const Model_Database = Model.Database;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;


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



let kp_to_range = buf_kp => {
    let buf_0 = Buffer.alloc(1);
    buf_0.writeUInt8(0, 0);
    let buf_1 = Buffer.alloc(1);
    buf_1.writeUInt8(255, 0);
    // and another 0 byte...?

    return [Buffer.concat([buf_kp, buf_0]), Buffer.concat([buf_kp, buf_1])];
}

let differencing = (orig, current) => {
    let changed = [],
        added = [],
        deleted = [];

    let map_orig = {},
        map_current = {},
        map_orig_records = {};


    each(orig, (record) => {
        let [key, value] = record;
        map_orig[key.toString('hex')] = [value];
        map_orig_records[key.toString('hex')] = record;
    })
    //console.log('map_orig', map_orig);

    each(current, (record) => {
        let [key, value] = record;
        map_current[key.toString('hex')] = [value];

        // does it appear in orig?

        if (map_orig[key.toString('hex')]) {
            if (deep_equal(map_orig[key.toString('hex')][0], value)) {

            } else {
                //changed.push([record]);
                changed.push([map_orig_records[key.toString('hex')], record]);
            }
        } else {
            added.push(record);
        }


    })

    each(orig, (record) => {
        let [key, value] = record;
        //map_orig[key] = value;
        if (map_current[key.toString('hex')]) {

        } else {
            deleted.push(record);
        }
    })

    let res = {
        changed: changed,
        added: added,
        deleted: deleted
    }

    return res;



}

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
            console.log('originIsAllowed origin', origin);
            // put logic here to detect whether the specified origin is allowed.
            return true;
        };





        //db = db || (that.db = replace_db_put(levelup(that.db_path, options), that));

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

            db = db || levelup(this.db_path, this.db_options);
            this.db = db;

            if (db_already_exists) {

                // Load the model from the database.

                this.load_model((err, model) => {
                    if (err) {
                        throw err;
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
                console.log('model', model);
                let buf = model.get_model_rows_encoded();
                console.log('buf.length', buf.length);
                //console.log('decoded new model', Model_Database.decode_model_rows(buf));

                var arr_core = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf);
                console.log('arr_core', arr_core);


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
                            callback(err);
                        } else {
                            console.log('all_db_rows', all_db_rows);
                            this.model = model;

                            proceed_2();
                        }
                    })

                });






                //throw 'stop';


                // Initial db setup fn call here?

                // Or just 2 lines to create the core model, and then to put those rows into the DB.

                // Do this if there are 0 rows in the database.
                //  Could do a db count records up to, with limit being 1.

                // Seems a fair few functions will be moved to a lower level in the DB, and will have more flexibility at that level.
                //  Setting up and ensuring the setup of the db will be one of the major tasks to get on with.
                //  Want it to be easy to run a scan on the db to see that all records have their correct index records, and there are no incorrect or orphan index records.








                //that.fns_ws.initial_db_setup(callback);
                // rest of the init
                //
            }
        }
        var next_connection_id = 0;

        var proceed_2 = () => {

            wsServer.on('request', function (request) {

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
            });
            callback(null, true);
        }
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

    load_model(callback) {
        // only loads the core system db rows for the moment.
        var that = this;

        this.get_system_db_rows((err, system_db_rows) => {
            // These system db rows could be wrong.
            //  Could be a problem with the existing DB's model records.
            if (err) {
                callback(err);
            } else {
                //console.log('system_db_rows', system_db_rows);
                // The table incrementor value should be at least about 4.

                let decoded_system_db_rows = Model_Database.decode_model_rows(system_db_rows);
                //console.log('decoded_system_db_rows', decoded_system_db_rows);
                //console.log('decoded_system_db_rows.length', decoded_system_db_rows.length);
                //throw 'stop';
                //throw 'stop';

                // Seems the model db had not loaded all the right info.
                //  Misses the new table definitions. It has put the new incrementors in.
                //   Seems like the added table rows need to specifically be generated and added to the model.
                //   Could / should have this automatic upon making new tables that get (successfully) added to the model.

                // Then it won't be long until we are able to store a large amount of data, with high performance.
                //  It's the part where new tables get added to the model.
                //   If the model is not in initialisation mode, or early_init mode, we add the table records and index records as the tables get added





                this.model = Model_Database.load(system_db_rows);

                // Check that the model rows from the db are the same length as those re-obtained from the model db.


                let model_rows = this.model.get_model_rows();
                //console.log('system_db_rows.length', system_db_rows.length);
                //console.log('model_rows.length', model_rows.length);

                if (model_rows.length !== system_db_rows.length) {
                    console.log('system_db_rows.length', system_db_rows.length);
                    console.log('model_rows.length', model_rows.length);

                    // 13/03/2018 - This is a newly discovered bug where the model does not make every table (missing the native types table) when it gets reconstructed.

                    // do a diff here?

                    console.log('model_rows', Model_Database.decode_model_rows(model_rows));
                    console.log('system_db_rows', Model_Database.decode_model_rows(system_db_rows));



                    callback(new Error('Mismatch between core db rows and core rows obtained from model'));
                } else {
                    callback(null, that.model);
                }


            }
        });
    }

    ll_count_keys_in_range(buf_l, buf_u, callback) {
        var count = 0;
        //var res = [];
        this.db.createKeyStream({
            'gt': buf_l,
            'lt': buf_u
        }).on('data', function (key) {
            //arr_res.push(x(key.length).buffer);
            //console.log('key', key);
            //arr_res.push(key);
            count++;
        }).on('error', function (err) {
            //console.log('Oh my!', err)
            callback(err);
        }).on('close', function () {
            //console.log('Stream closed')
        }).on('end', function () {
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



    // A variety of ll functions will have a lot more complexity involving observable results, flexible calling, polymorphism, calling of optimised inner functions.

    ll_get_records_in_range(arr_buf_range, callback) {


        // Optional decoding here would be useful.

        // When checking records against indexes, it will be useful to have those records decoded.
        //  For this, as well as other reasons, record decoding will be moved further down the stack.
        //   The client will be able to get the server to decode records on the server, process them, and send encoded results to the client.






        // Could have an option here for paging, returning single records, or buffering them all together and returning them all at once.

        // Default option with a callback gives the records all at once. Observable will be one at a time.


        let sig = get_a_sig(arguments);
        //console.log('ll_get_records_in_range sig', sig);





        // This function being an intrinsic part of the server means it should be done well here.
        //  Providing multiple interfaces is the way to go here.

        // Can work with observables
        // Can use paging
        // Can return the entire result set in a callback (though in some cases it would use too much RAM)

        // This is quite a significant function. May have multiple inner versions of the function.


        // Returning an observable by default would be nice here.


        // An inner observable would be cool.

        // Could have a variety of inner functions, some run very quickly with fewer options (get compiled separately)



        //throw 'll_get_records_in_range stop';

        // Then a paging object.

        if (sig === '[a,f]') {
            throw 'stop';
        } else if (sig === '[a]') {
            // Return an observable, which will also function much like a promise.
            //  No paging option has been specified here.

            // Some kind of Paging_Observer would be quite useful.
            //  Gets given results one at a time (maybe with observer / events), and then it buffers them up into a fixed size.
            //  Would be a useful thing to apply to a function call or an existing Observer to turn into a Paged_Observer.

            // For the moment though, return an Observer with no paging.



            let res = new Evented_Class();

            /*
            let res_records = [];

            this.db.createReadStream({
                    'gte': arr_buf_range[0],
                    'lte': arr_buf_range[1]
                }).on('data', function (record) {
                    //console.log('key', key);
                    //console.log('key.toString()', key.toString());
                    res_records.push([record.key, record.value]);
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    //callback(err);
                    res.raise('error', err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res_records);
                    res.raise('complete', res_records);
                });
                */

            this.db.createReadStream({
                    'gte': arr_buf_range[0],
                    'lte': arr_buf_range[1]
                }).on('data', function (record) {
                    //console.log('key', key);
                    //console.log('key.toString()', key.toString());
                    //res_records.push([record.key, record.value]);
                    //console.log('record', record);
                    res.raise('next', [record.key, record.value]);


                    // Should return every single record as this type of kv array.


                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    //callback(err);
                    res.raise('error', err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res_records);
                    //console.log('end');
                    res.raise('complete');
                });

            return res;

        } else {
            throw 'NYI stop';
        }


    }


    // optional paging as well.
    //  paging would require an observable.


    // This will be used to get the table records.
    //  Iterating over the table records will be a useful way of getting the records.
    //  Nice to have each record in an observable type callback (in some cases). Further from low level speed though.



    ll_get_records_with_kp(kp, decode, callback) {

        // Optional decoding here would be useful.

        let a = arguments,
            sig = get_a_sig(a);

        //console.log('sig', sig);


        if (sig === '[B,f]') {
            callback = a[1];
            decode = false;
        }

        //throw 'stop';



        // Need to look at the Paging options here.
        //  When using paging, it can't return data with a single callback, though multiple callbacks could work.
        //   The better (more widely accepted) pattern is observables.





        // This could possibly be extended to return an observer if the callback is not given.



        // But the inner function may be better using observable.
        //  Could have an inner observable.

        // With an observable, this could have paging options too.

        // An inner observable here may do the job better.
        //  Could use that for calling a lower level observable in ll_get_records_in_range and just return that.


        // An inner promisy observable would make sense here.

        /*

        let inner = (callback) => {
            let range = kp_to_range(kp);
            console.log('* range', range);



            //this.
        }
        */





        let range = kp_to_range(xas2(kp).buffer);
        //console.log('range', range);
        let obs_get = this.ll_get_records_in_range(range);

        //console.log('obs_get', obs_get);

        if (callback) {
            //inner(callback);
            //console.log('ll_get_records_with_kp called with callback');

            // Won't have any paging when using a callback.
            let res = [];

            obs_get.on('next', data => {
                //console.log('data', data);
                res.push(data);
            });
            obs_get.on('complete', end_res => {
                //console.log('complete');
                //console.log('*res', res);
                callback(null, res);

            });
            obs_get.on('error', err => {
                callback(err);
            });

        } else {
            // With an observable (no callback), will return the results one by one.
            /*
            let res = new Evented_Class();
            obs_get.on('next', data => {
                //console.log('data', data);
                //res.push(data);
                res.raise('next', data);
            });
            obs_get.on('complete', end_res => {
                //console.log('complete');
                //console.log('res', res);
                //callback(null, res);
                res.

            });
            obs_get.on('error', err => {
                callback(err);
            });
            */
            return obs_get;
            //throw 'NYI';
        }

        //throw 'stop';


    }


    // Because counting can take a while, need to use observable count.

    ll_count(callback) {
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
    }

    // Then a server function to get all ll records with key beginning ...

    table_index_value_lookup(table_id, idx_id, arr_values, return_field, opt_cb) {
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

                        //console.log('arr_buf_idx_res[0][0]', arr_buf_idx_res[0][0]);
                        //console.trace();
                        let decoded = Model_Database.decode_key(arr_buf_idx_res[0][0]);
                        //console.log('3) decoded', decoded);

                        //throw 'stop';
                        let t_return_field = tof(return_field);
                        if (t_return_field === 'number') {
                            let res = decoded[return_field];
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

    // Maintenance of a table and its indexes looks like a good piece of functionality to have within the DB.



    maintain_table_indexes(table_name) {

        // Not sure what this should return - callback, observer, promise.

        // promise would work well with await.

        // obs_maintain_table_indexes for more detail with an observable?


        // promise version of the function that gets the 

        // for the moment, could use promise / callback flexible system.

        // For every table record, will generate its index records, and check that they are in the DB.
        //  This will be server-side, so the process will not take many requests from the client.


        // A basic server function, with the API, to lookup the table id would be very useful.
        //  Could have that as a basic server function.




        let inner = () => {

        }


    }



    // Could be possible to have decoding option here too.
    //  Decoding option could be further down the stack and passed through.




    ll_get_table_index_records(table_id, opt_cb) {

        let a = arguments,
            sig = get_a_sig(a);


        let decode = false;
        //console.log('sig', sig);


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
        if (decode) {

            // Apply a filter to the observable?
            //  A new observable around the old one that encludes decoding events?
            //  Sounds like a job for Observable Map.
            let res = new Evented_Class();

            obs.on('next', data => {
                //console.log('data', data);

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
        //throw 'stop';
        // Observe them, having got the kp right for the indexes

    }


    // Then get the table indexes



    check_table_indexes(table, opt_cb) {
        let sig = get_a_sig(arguments);
        let table_id;

        // parameterise being an async function (with callback?)
        //  getting the initial parameters in order could require a DB lookup (just maybe) if the table ID isn't loaded into the model.

        // could give table id or table name

        let parameterise = (callback) => {
            if (sig === '[s]') {
                opt_cb = null;
                this.get_table_id_by_name((err, id) => {
                    if (err) {
                        //opt_cb(err);
                        // return a failed promise or observable with immediate error.
                        callback(err);
                    } else {
                        table_id = id;
                        callback(null, true);
                    }
                });
            }
            if (sig === '[n]') {
                table_id = table;
                opt_cb = null;
                callback(null, true);

            }
        }


        parameterise((err, ready) => {
            if (err) {



                // Hard to know how to return this parameterisation error, maybe async parameterisation is not the best way.
                // Been called earlier. :)
            } else {

                // return an Observable that we use for observing the table index checks.

                // Observable for iterating records.
                // Not an iterator (at present) because we don't control the rate they come from the DB. Process them as fast as they arrive.

                let res = new Evented_Class();




                // get a new observable for iterating the table records.









            }
        })








    }

    // ll_count with progress / observable

    // observable would be the best API for this.




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
                console.log('record', record);
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
            }).on('data', function (record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                res_records.push([record.key, record.value]);
            })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
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
            }).on('data', function (record) {
                arr_buf_res.push(Binary_Encoding.join_buffer_pair([record.key, record.value]));
            })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
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

    batch_put(buf, callback) {
        var ops = [],
            db = this.db,
            b64_key, c, l, map_key_batches = {},
            key;



        // Operating with an observable (this seems better than promise) would be a nice option here.
        //  Maybe using Async would be possible if it has both the Observable and Promise APIs.

        // A ll version of this that does not do any checking / event raising would be faster.
        //  Thing is, it's important to have events be able to listen to the data that gets added.

        // Will use callback for the moment.


        // Maybe this should have a maximum size.


        // Seems like a problem here with buffer encoding and decoding for the put records.
        //  Next problem that needs to be solved.
        //  Maybe the client has skipped the keys when making the buffers of the records including their indexes.





        //console.log('buf', buf);



        Binary_Encoding.evented_get_row_buffers(buf, (arr_row) => {
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
                'key': arr_row[0],
                'value': arr_row[1]
            });
        });

        //console.log('ops', ops);

        //throw 'stop';

        var that = this;
        db.batch(ops, function (err) {
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


    ensure_table(arr_table, callback) {

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






                        let diff = differencing(old_model_rows, new_model_rows);
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










        // 

        // The model table could have a function that compares itself to a table definition.
        //  Maybe could do with a parse_table_def function that can be reused more. 

        // Or create a new Table that is disconnected from the Model.
        //  This sounds like a decent way of doing it, then compare that table to the one in the Model.
        //   




        // If so, get the fields from the model.






        // This could load up table definions on the server


        /*

        this.load_model((err, model) => {
            if (err) {
                callback(err);
            } else {

                let table_name = arr_table[0];
                if (model.table_exists(table_name)) {

                } else {
                    let new_table = model.add_table(arr_table);

                    // A function to get every single lower level DB record from the table, then add this to the database



                    // And the model would have some db changes too.
                    //  Changes to an incrementor



                    // Then get all of the db rows including incrementor rows



                }

                //if (model.table_exists)

                //model.ensure_table()

            }
        })
        */

    }


    ensure_tables(arr_tables, callback) {
        // Optional callback, otherwise return observable

        let a = arguments,
            sig = get_a_sig(arguments);

        console.log('ensure_tables sig', sig);
        //console.log('arr_tables', arr_tables);

        //throw 'stop';

        if (sig === '[a]') {

            // Function calling observable...
            //  Function call queue / fn_queue / fnq


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
                fns.push([this, this.ensure_table, [arr_table]]);
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

    persist_row_diffs(row_diffs, callback) {
        // Shouldn't use the batch put system I think?
        //  If it did, listeners would be able to respond to the events.

        let ops = [];

        // For the moment, will batch it up into ops.

        each(row_diffs.deleted, record => {
            ops.push({
                'type': 'del',
                'key': record[0]
            });
        })
        each(row_diffs.added, record => {
            ops.push({
                'type': 'put',
                'key': record[0],
                'value': record[1]
            });
        })
        each(row_diffs.changed, record_pair => {
            let new_record = record_pair[1];
            ops.push({
                'type': 'put',
                'key': new_record[0],
                'value': new_record[1]
            });
        })

        this.db.batch(ops, (err) => {
            if (err) {
                callback(err);
            } else {

                /*
                this.raise('db_action', {
                    'type': 'arr_batch_put',
                    'value': ops
                });
                */

                callback(null, true);
            }
        })




    }



    // WIP
    //  Reads from the server-side model db.
    get_table_fields_info(table, callback) {

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

                // Get the table fields info by id.

                // Doing this in the model would make a lot of sense.
                // Getting the fields info in a convenient format will assist transforming records so that they fit into the DB.

                // Want to refactor code so that there is less Bittrex specific code for the Bittrex gathering to work, and then get it working for many other exchanges quickly.

                let table = this.model.map_tables_by_id[table_id];
                //console.log('table', table);

                let fields = table.fields;
                //console.log('fields', fields);

                let res = [];

                each(fields, field => {
                    //console.log('field', field);
                    //console.log('keys: field', Object.keys(field));

                    let id = field.id;
                    let name = field.name;
                    let fk_to_table = field.fk_to_table;
                    let type_id = field.type_id;

                    /*
                    console.log('id', id);
                    console.log('name', name);
                    console.log('!!fk_to_table', !!fk_to_table);
                    console.log('type_id', type_id);
                    console.log('');
                    */

                    let obj_res = {
                        'id': id,
                        'name': name,
                        'type_id': type_id
                    }

                    // Seems like it could be more important to keep the type ids in more cases.
                    //  Specifically telling which fields are fk lookups, to integer values.
                    //  Positive autoincrementing incrementors.

                    // Look up the primary key type to the table any fk links to.

                    if (fk_to_table) {
                        let fk_pk = fk_to_table.pk;
                        //console.log('fk_pk', fk_pk);

                        let fk_pk_fields = fk_pk.fields;
                        // pk can be composite, made of more than one field.



                        //console.log('fk_pk_fields.length', fk_pk_fields.length);




                        let fk_to_fields = [];
                        each(fk_pk_fields, fk_to_field => {
                            fk_to_fields.push([fk_to_field.id, fk_to_field.name, fk_to_field.type_id]);

                        })

                        obj_res.fk_to = {
                            'table_name': fk_to_table.name,
                            'table_id': fk_to_table.id,

                            'fields': fk_to_fields
                        }

                        //console.log('fk_to_fields', fk_to_fields);

                        //console.log('obj_res', obj_res);



                        // Want to get back enough info to the clients to be able to construct the records properly.

                        // Could just return the field IDs right now.
                        //  Want to be able to tell that a record (such as Bittrex snapshots) have got references to another table's PK.
                        //  Would be nice to have data types that 


                        //  Don't have to get info about field types if that info is not available.
                        //   Don't want to have to require info about field types all over the place either.

                        // Could measure field types, or get_field_types_by_sample

                        // In many cases, we won't need to know the types, as we can retrieve the data from a lookup, and get it back in the right type.

                        // Want useful functionality here that will help with data transformation of bittrex snapshot records.
                        //  Want to be able to get the results here, and use that to make it easier or automatic to map records from an input format (such as output from an exchange) into the DB record format.

                        // Could maybe leave this function for the moment and do more of the mapping code myself?

                        //  Want it so we can define the mapping, and have it convert the records.

                        //   For the moment though, a function just to map the particular records could work.
                        //    Server side though, it could save in communication because it would not need to communicate the lookups of the ids over the network.

                        // A better made client-side cache of some data could help - though server side lookup seems important.
                        //  Defining a record mapping on the server and then using it seems like a more long-winded process.

                        // Doing the client-side lookups of code / named fields would be the quicker way to get those db records in place.
                        //  Could use a very simple JS client-side cache of known records (recxords that will not change in the meantime).


                        // This function would be good to get working so I can be more sure that what I am doing manually is fine.
                        //  Then write data transformation functions that takes data from the Bittrex API, and puts it into the right format for the DB.
                        //  Then get it set up and running on a couple more servers.
                        // Stop the data1 server, and copy over the data, could run it in a 'restore' mode if necessary. I think the records have stayed encoded in the same way though, and recently changed
                        //  was the interface to those records. Could try the data1 data upon restart to see if it works OK.

                        // Getting a local db syncing with the remote dbs would also be very useful too.
                        //  Can think in terms of a Workstation DB - where the data gets downloaded to the workstation, with the idea that the workstation won't be on all the time,
                        //   and also about having tolerable syncing times.

                        // Could have DB utility machines that (such as Raspberry Pi or Tinkerboard) that store a recent subset of the total data on the local network, so it can be synced to the workstation quickly.

                        // Then soon need to expand it to cover other exchanges, as well as storing full trading history including order books.

                        // Need to work through this for a while to put the Bittrex markets system into being.
                    }
                    res.push(obj_res);
                });
                //console.log('res', res);

                callback(null, res);


                // Then encode that result as an object in a buffer

                // Think binary encoding needs to be improved to hold objects.
                //let encoded_res = Binary_Encoding.flexi_encode_item(res);
                //console.log('encoded_res', encoded_res);
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



    }
}



// Run it from the command line with a path?

let custom_path;

// Custom path will be within local app config.
//  That way it will work better on Linux too.



// Have a look in the config to find the db path.


//custom_path = 'D:\\NextlevelDB\\DB1';

// Loading it from local config would be best.



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

                            // DB could create its own core model when it first starts.
                            //  Simpler to save the client from having to do it.

                            // The server component itself will start with its model loaded.

                            //callback(null, true);


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


module.exports = NextLevelDB_Server;