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

// Definitely want this up and running on a server soon.
//  Need to have HTML output of data tables too.
//  Maybe viewing pages.
//   Downloading decent sized data sets
//   Large amounts of data within HTML tables made available too.

// Need to do scans / reports to see how much data there is.
//  Harder to do with slow count facilities in the DB.
//   Could have record counts by table, as well as indexed record counts.
//    As in, a record count stored for each day.
// Could have new counts table
//  Or get it working on a lower level.










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

        var proceed_2 = () => {
            wsServer.on('request', function (request) {

                if (!originIsAllowed(request.origin)) {
                    // Make sure we only accept requests from an allowed origin
                    request.reject();
                    console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
                    return;
                }
                var connection = request.accept('echo-protocol', request.origin);
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
            if (err) { callback(err); } else {
                that.model = Model_Database.load(system_db_rows);

                callback(null, that.model);



            }
        });

    }

    ll_count(callback) {
        var count = 0;
        this.db.createKeyStream({}).on('data', function (key) {
            //console.log('key', key);
            //console.log('key.toString()', key.toString());
            //res_keys.push(encodeURI(key));
            count++;
        })
            .on('error', function (err) {
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

        this.db.createReadStream({
            'gte': xas2(0).buffer,
            'lte': xas2(9).buffer}).on('data', function (record) {
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
        Binary_Encoding.evented_get_row_buffers(buf, (arr_row) => {
            //console.log('arr_row', arr_row);

            ops.push({
                'type': 'put',
                'key': arr_row[0],
                'value': arr_row[1]
            });
            //db.put(
            //var decoded_row = decode_model_row(arr_row);
            //console.log('decoded_row', decoded_row);

        })
        //console.log('ops', JSON.stringify(ops, null, 2));
        //throw 'stop';
        db.batch(ops, function (err) {
            if (err) {
                callback(err);
            } else {
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
                                ls.ll_count((err, count) => {
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
                                        //ls.get_all_decoded(
                                    }
                                });
                            }

                            //start_with_core_model();

                            

                            
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