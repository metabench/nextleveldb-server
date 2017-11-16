/*
    Get fully working server implementation deployed.
        Server-side model gets loaded, so 
    Leave the current data1 instance running, and see about importing data from it.
    Currently not streaming records from the server fully.

    // Or look on the server to correct the model.
    //  Facility to directly edit rows would be useful.

    // Could have been generating some records incorrectly on the server.

    // Maybe functionality to fix the existing server's data would be useful.
    //  View existing server's data too.

    


    





*/

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
// Definitely want this up and running on a server soon.
//  Need to have HTML output of data tables too.
//  Maybe viewing pages.
//   Downloading decent sized data sets
//   Large amounts of data within HTML tables made available too.

// Need to do scans / reports to see how much data there is.
//  Function(s) to estimate the disk space used based on key prefixes.




//  Harder to do with slow count facilities in the DB.
//   Could have record counts by table, as well as indexed record counts.
//    As in, a record count stored for each day.
// Could have new counts table
//  Or get it working on a lower level.


// Will soon get data back from the server through a subscription system.









//var Model_Database = Model.Database;

// Could be worth redoing most of this whole thing.
//  DB access could further be provided by connected model objects.

// Such connected model objects could get themselves from the db, persist themselves.


// Making a web caching module for this seems quite important.
//  Could maybe benefit from some more user-friendly functions regarding tables and record types.
//   Defining them, saving them to the DB, and getting them back as JSON, displaying them in a GUI.

// What form of normalisation or compression of the keys of the URLs?
//  A table of URLs would make sense.
//   Then a table of when those URLs get accessed, as well as the rsult of accessing them

// This could maybe do with a basic UI that shows the tables... or have it optional.


// DB put could be expanded quite a lot to enable records to be put into the DB.

//  For the moment, it seems easier to expand the functionality of records.
//  Would be a more OO approach.


// Don't want nearly as many db access functions.
//  Use the model, including oo instances that are made specifically, to interact with the DB's data.

// A barrier between the server and the DB itself?


// A cache of the client's subscriptions?
//  The clients have their ids.





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

            // Can split this file up further into different processors.
            //  Process HTTP.
            //  Process Websocket
            //   Process JSON
            //   Process Binary
        });

        server.listen(that.port, function () {
            //console.log((new Date()) + ' Server is listening on port 8080');
            console.log("Server is listening on port " + that.port + ', using database path ' + path.normalize(that.db_path));
            //callback(null, that.port);
        });

        var wsServer = new WebSocketServer({
            httpServer: server,
            // You should not use autoAcceptConnections for production
            // applications, as it defeats all standard cross-origin protection
            // facilities built into the protocol and the browser.  You should
            // *always* verify the connection's origin and decide whether or not
            // to accept it.
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

            //console.log('pre callback');
            //console.log(exists ? 'it\'s there' : 'no passwd!');
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

        // Probably best to have some kind of query execution system.
        //  Need some low level functions that get called.
        //   Client could request data between various keys, after having constructed the query.




        // Todo : make sure each connection has an ID.
        //  May be worth issuing connection security tokens to be sent with requests?


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
                        throw 'deprecating http interface';

                        //handle_ws_utf8(connection, that, that.fns_ws, message);
                    }
                    else if (message.type === 'binary') {
                        //console.log('Received Binary Message of ' + message.binaryData.length + ' bytes');
                        handle_ws_binary(connection, that, message.binaryData);


                        //connection.sendBytes(message.binaryData);
                    }
                });
                connection.on('close', function (reasonCode, description) {
                    console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.\n');
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
        // Will have multiple callbacks.

        // Should produce a subscription id.
        //  Each client has their own subscription id?

        // subscription message id

        // But there should be a client subscription id.

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
        // not sure we should listen to every db_action event
        //  event name: kp_put_[base64kp]
        //   map_b64kp_subscription_alerts
        //  

        // have an array for all of the alerts?
        //  Maybe using events would make more sense.
        //   With specific event names.
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

        // Done using evented class like the subscribe all function seems best.


        
        if (this.map_b64kp_subscription_put_alert_counts[b64_kp]) {
            this.map_b64kp_subscription_put_alert_counts[b64_kp]++;
            //this.map_b64kp_subscription_put_alerts[b64_kp].push(process_subscription_event);
        } else {
            this.map_b64kp_subscription_put_alert_counts[b64_kp] = 1;
            //this.map_b64kp_subscription_put_alerts[b64_kp] = [process_subscription_event];
        }
        

        this.using_prefix_put_alerts = true;


        // the alert counts and the alerts themselves.
        //  a map of alert functions looks best here.
        //   don't need to use evented functionalty. Maybe would make sense though.


    }

    
    // Possibly tell it other table names to load.
    //  Could also have as a field of the table record
    //   keep_in_memory
    //    would also keep the indexing in memory.
    //    for the moment, getting the normal server index lookup working is more important.
    //     will also be able to do index ranges and some selection searches too.

    // But the web socket service version won't return the model itself.

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

    ll_count(callback) {
        var count = 0;
        this.db.createKeyStream({}).on('data', function (key) {
            //console.log('key', key);
            //console.log('key.toString()', key.toString());
            //res_keys.push(encodeURI(key));
            count++;
        }).on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                callback(null, count);
            })
    }

    // No paging on this one.
    get_all_db_keys(callback) {
        var res = [];
        this.db.createKeyStream({}).on('data', function (key) {
            res.push(key);
        })
            .on('error', function (err) {
                //console.log('Oh my!', err)
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

    // Get system db rows...

    get_system_db_rows(callback) {
        // tables ids 0, 1, 2, 3
        // tables, native types, table fields, table indexes

        // so, the very start of the key space between 0 and 7 (1 + 2 * 3)  1 being a 0 indexed 2
        //  tables key space starts at 2, each table has got 2 key spaces
        var res_records = [];

        this.db.createReadStream({
            'gte': xas2(CORE_MIN_PREFIX).buffer,
            'lte': xas2(CORE_MAX_PREFIX).buffer}).on('data', function (record) {
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
            //console.log('key', key);
            //console.log('key.toString()', key.toString());
            //res_records.push([record.key, record.value]);

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

    batch_put(buf, callback) {
        var ops = [];
        var db = this.db;

        // Search through that buffer to extract those records with a given key prefix buf.
        //  We can do that for any key prefixes there are subscription alerts for.

        // map_b64kp_subscription_alerts

        // map_b64kp_subscription_alerts

        //var b64_

        //console.log('batch_put');

        var b64_key, c, l;

        // Alerts of multiple rows to multiple clients.

        // rows by subscription.

        
        // Find out which keys are being listened to.
        //  Then raise the appropriate events for them.




        //var matching_alerts = [];

        // matching subscriptions

        // a batch for each key

        var map_key_batches = {};


        //var prefix_put_alerts_batch = [];
        //var prefix_put_alerts_batch = [];

        // evented_get_row_buffers as kvp buffers?
        Binary_Encoding.evented_get_row_buffers(buf, (arr_row) => {
            //console.log('arr_row', arr_row);

            // Does it begin with any of the key prefixes we have alerts for?
            //  Maybe we need an alert tree to check this.

            // Or could scan according to the different lengths of key prefixes, each encoded as base64 strings.
            //  Simple use of a Tree sounds useful.
            //console.log('\narr_row[0]', arr_row[0]);

            

            if (this.using_prefix_put_alerts) {
                //prefix_put_alerts_batch = [];
                var map_b64kp_subscription_put_alert_counts = this.map_b64kp_subscription_put_alert_counts;
                //var map_b64kp_subscription_put_alerts = this.map_b64kp_subscription_put_alerts;

                b64_key = arr_row[0].toString('hex');
                //console.log('b64_key', b64_key);


                //console.log('using_prefix_put_alerts');
                //console.log('keys: this.map_b64kp_subscription_put_alert_counts', Object.keys(this.map_b64kp_subscription_put_alert_counts));

                //each(Object.keys(this.map_b64kp_subscription_put_alerts))

                // Looks like we need to store the put alerts by key with the subscription id.
                


                for (var key in this.map_b64kp_subscription_put_alert_counts) {
                    //console.log('key', key);
                    if (b64_key.indexOf(key) === 0) {
                        //console.log('found matching put alert key prefix', key);

                        map_key_batches[key] = map_key_batches[key] || [];
                        map_key_batches[key].push(arr_row);







                        //match_subscribers[]

                        //prefix_put_alerts_batch.push(row);

                        // 

                        // if its found then process the subscription alerts.
                        ////  could also have 

                        // call each of these alerts.

                        // Would be better to batch these in the near future.



                        //matching_alerts = map_b64kp_subscription_put_alerts[key];

                        //l = matching_alerts.length;

                        
                        //console.log('l', l);
                        //for (c = 0; c < l; c++) {

                            /*
                            matching_alerts[c]({
                                'type': 'batch_put',
                                'buffer': Binary_Encoding.join_buffer_pair(arr_row)
                            });
                            */

                            //prefix_put_alerts_batch.push(arr_row);
                            // or after the actual put.
                        //}
                    }
                }
            }
            

            
            // 
            
            


            


            ops.push({
                'type': 'put',
                'key': arr_row[0],
                'value': arr_row[1]
            });
            //db.put(
            //var decoded_row = decode_model_row(arr_row);
            //console.log('decoded_row', decoded_row);

        });

        

        

        //console.log('ops', JSON.stringify(ops, null, 2));
        //throw 'stop';
        var that = this;
        db.batch(ops, function (err) {
            if (err) {
                callback(err);
            } else {

                // Then we notify relevant subscribers that these records have been put into the db.
                //  We could send back the whole collection of records....

                // If there are subscribers to a particular table or key prefix, may need to do some record decoding to recognise which subscribers to send the data to.
                //  Key prefix matching should be fairly quick.

                // Can use events, and have subscribers plug into the events.

                // has_table_subscribers
                // has_alldata_subscribers

                // subscribe_all_updates

                // That seems like the best to start with.

                // Subscriptions to all updates would help with sharding and replication.


                // Then there could be index prefix subscriptions.





                // could raise an action event.
                //  then with a type 


                // One system subscruibes to all puts / batch puts.
                that.raise('db_action', {
                    'type': 'batch_put',
                    'buffer': buf
                });


                //if (prefix_put_alerts_batch.length > 0) {
                //    
                //}

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
                })

                // then if there are specific events that get triggered for individual kep prefixes.


                // Raise an event with a buffer that contains those key prefixes.




                // Then the subscriptions would have an event that gets raised on this object.













                callback(null, true);
            }
        })

    }
}


if (require.main === module) {
    var user_dir = os.homedir();
    //console.log('user_dir', user_dir);
    //var docs_dir =

    var path_dbs = user_dir + '/NextLevelDB/dbs'

    fs2.ensure_directory_exists(user_dir + '/NextLevelDB', (err, exists) => {
        if (err) { throw err } else {

            fs2.ensure_directory_exists(path_dbs, (err, exists) => {
                if (err) { throw err } else {
                    //console.log('dir now exists');
                    //throw 'stop';

                    var db_path = path_dbs + '/default';


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


                            // Worth loading a model on start?
                            //  Could just be done as instructions through the API.

                            // Load model instruction
                            //  Once the model is loaded, rows can be put including indexing
                            //   not LL_PUT, PUT_WITH_INDEXING, maybe LL_PUT_WITH_INDEXING because its still a somewhat low level operation?
                            //    probably not for the moment, it's higher level than particularly LL.



                            // 




                            var start_with_core_model = () => {
                                // Could do an initial db setup...



                                ls.count_core((err, count) => {
                                    if (err) {
                                        throw err;
                                    }
                                    console.log('\n1) count', count);

                                    // No, probably best not to do this initial setup to start with.
                                    //  Want this running so that clients can give it the initial structure (Model).

                                    // Essentially, clients will put large-ish chunks of data into the db at once, and carry out index range queries
                                    //  The code on the client will translate programming API instructions to the lower level DB commands.

                                    if (count === 0) {
                                        // Ensuring the initial setup could involve using the Model to encode the initial records.
                                        // A standard instance of the Model will allow us to get the records quickly.

                                        // It would be useful to put all of the ll records into the db with one function call.

                                        /*

                                        var mdb = new Model.Database();
                                        mdb.create_db_core_model();


                                        var buf_records = mdb.get_model_rows_encoded();
                                        //console.log('buf_records', buf_records);
                                        //console.log('buf_records.length', buf_records.length);


                                        ls.batch_put(buf_records, (err, res_batch_put) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                //console.log('res_batch_put', res_batch_put);

                                                ls.ll_count((err, count) => {
                                                    if (err) {
                                                        throw err;
                                                    }
                                                    console.log('\n2) count', count);

                                                });
                                            }
                                        });

                                        */
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


                                                // get_str_info
                                                //  or a getter




                                            }
                                        })


                                        /*
                                        ls.get_system_db_buffer((err, buf_system) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('buf_system', buf_system);
                                                console.log('buf_system.length', buf_system.length);

                                                var m_system_db = Model.Database.from_buffer(buf_system);
                                                console.log('mdb', m_system_db);
                                            }
                                        })
                                        */


                                        //ls.get_all_decoded(
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