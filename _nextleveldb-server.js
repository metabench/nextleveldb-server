
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


var WebSocket = require('websocket');
var WebSocketServer = WebSocket.server;

var xas2;
var encodings = require('./encodings-core');
var x = xas2 = require('xas2');

var fs = require('fs');
var fs2 = jsgui.fs2;

var handle_http = require('./handle-http');
var handle_ws_utf8 = require('./handle-ws-utf8');

//var Binary_Encoding = require('binary-encoding');
var Binary_Encoding = require('binary-encoding');
var Binary_Encoding_Record = Binary_Encoding.Record;

var recursive_readdir = require('recursive-readdir');
var Running_Means_Per_Second = require('./running-means-per-second');

var levelup = require('level');

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








var replace_db_put = function (db, nextleveldb_server) {



	var old_db_put = db.put;

    var new_db_put = function (key, value, callback) {



        if (arguments.length === 2) {
            // [key, value]
            new_db_put(arguments[0][0], arguments[0][1], arguments[1]);
        }


		var b0_key, b0_value;
		var b1_key, b1_value;
		var b_key, b_value;

		if (key instanceof Buffer) {
			//old_db_put()
			b_key = key;
		} else {
			b0_key = key.buffer;
			if (b0_key) {
				b_key = b0_key;
			}
			b1_key = key._buffer;
			if (b1_key) {
				b_key = b1_key;
			}
		}

		if (value instanceof Buffer) {
			//old_db_put()
			b_value = value;
		} else {
			b0_value = value.buffer;
			if (b0_value) {
				b_value = b0_value;
			}
			b1_value = value._buffer;
			if (b1_value) {
				b_value = b1_value;
			}
		}
		//console.log('pre raise put');
		nextleveldb_server.raise.call(nextleveldb_server, 'put', [b_key, b_value]);
        old_db_put.call(db, b_key, b_value, callback);


	}
	db.put = new_db_put;
	return db;
};



class NextLevelDB_Server extends Evented_Class {
	constructor(spec) {
		super();
		this.db_path = spec.db_path;
		this.port = spec.port;
	}
	start(callback) {
		//console.log('this.db_path', this.db_path);
		var options = {
			'keyEncoding': 'binary',
			'valueEncoding': 'binary'
		};
        var that = this;

        this.encodings = {};

        Object.assign(this.encodings, require('./encodings-core'));

		// Directory exists and is not empty

        that.fns_ws = {};

        var proceed = function() {
            var db;

            if (db_already_exists) {
                db = that.db = replace_db_put(levelup(that.db_path, options), that);
            }

            //var fns_ws = {};

            var running_means_per_second = that.running_means_per_second = new Running_Means_Per_Second();
            running_means_per_second.start_single_line_log();

            var server = http.createServer(function(request, response) {
                handle_http(request, response);

                // Can split this file up further into different processors.
                //  Process HTTP.
                //  Process Websocket
                //   Process JSON
                //   Process Binary
            });



            server.listen(that.port, function() {
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

            db = db || (that.db = replace_db_put(levelup(that.db_path, options), that));

            // Plugins add functions to an object that contains many functions.
            //  Plugins could either be distributed over npm, or copied over disk / distributed into a user's directory.
            //  For the moment, distributing over NPM seems like the most convenient.




            var load_plugins = function(callback) {
                // go through the plugins directories, putting together a list of the files there.

                // within each level, load in alphabetical order.
                // read the directory names within  the plugins directory

                var pl_core = require('./plugin-core');
                var pl_timeseries = require('./plugin-timeseries');

                //pl_core(fns_ws, db, running_means_per_second);
                //pl_timeseries(fns_ws, db, running_means_per_second);

                pl_core(that, db, running_means_per_second);
                console.log('Loaded core plugin.');
                pl_timeseries(that, db, running_means_per_second);
                console.log('Loaded timeseries plugin.');

                // Then the other plugins... they will load their encodings too.

                callback(null, true);

                
            };

            // Finding the holes in the time series would be a useful thing.
            //  Then loading in data from some lower resolution or alternative sources.
            //  Having a multi-layer approach makes sense in terms of getting the answers, but it's more complicated too.

            // Plugins will be loaded with require?
            //  Plugins stored in other directory, such as the user directory?
            //  Could make plugins available over the web too.

            // Plugins would definitely be nice to have loaded from directories.

            load_plugins((err, res_loaded_plugins) => {
                if (err) {
                    callback(err);
                } else {

                    //console.log('Object.keys(res_loaded_plugins)', Object.keys(res_loaded_plugins));
                    //console.log('Object.keys(fns_ws)', Object.keys(fns_ws));

                    var initial_db_setup = that.fns_ws.initial_db_setup;
                    var starting_db_setup = that.fns_ws.starting_db_setup;
                    //var output_transform_to_hex = fns_ws.output_transform_to_hex;
                    //var get_table_key_prefix_by_name = fns_ws.get_table_key_prefix_by_name;

                    //throw 'stop';

                    //console.log('res_loaded_plugins', res_loaded_plugins);
                    // Then do the initial db setup

                    //console.log('db_already_exists', db_already_exists);
                    //throw 'stop';
                    if (db_already_exists) {
                        //
                        // Initialise this variable elsewhere, automatically?
                        //  Have it loaded as part of the setup makes sense.

                        //global_prefix_table_table = 1;

                        // starting_db_setup

                        // needs to load a few variables.
                        //  May need to load a table of table IDs - though lazy loading could work there.

                        that.fns_ws.starting_db_setup(callback);

                        //callback(null, server);
                    } else {

                        that.fns_ws.initial_db_setup(callback);
                        // rest of the init
                    }

                    wsServer.on('request', function(request) {



                        if (!originIsAllowed(request.origin)) {
                            // Make sure we only accept requests from an allowed origin
                            request.reject();
                            console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
                            return;
                        }
                        var connection = request.accept('echo-protocol', request.origin);
                        //
                        //console.log((new Date()) + ' Connection accepted.');
                        connection.on('message', function(message) {

                            if (message.type === 'utf8') {
                                handle_ws_utf8(connection, that, that.fns_ws, message);


                            }
                            else if (message.type === 'binary') {


                                //console.log('Received Binary Message of ' + message.binaryData.length + ' bytes');
                                connection.sendBytes(message.binaryData);
                            }
                        });
                        connection.on('close', function(reasonCode, description) {
                            console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.\n');
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
                }
            });
        };

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
	}
}


if (require.main === module) {
	// Require a database path / choose a default path
	//  Easiest setup possible is best, allow config options too

	//console.log('process.argv', process.argv);

    // Can try a different database path.

    // This could maybe make use of OO connected-model objects.
    //  connected-database
    //  connected-table
    //  connected-record
    //  connected-index

    //  Each of them would connect to the database to sync their db representation with their logical representation.
    //   Check if they are in the DB or not, if necessary add themselves.






    // var db_path = 'D:\\data\\db';
    // Would be nice to have a nextleveldb directory within the user's directory

    var user_dir = os.homedir();
    //console.log('user_dir', user_dir);

    //var docs_dir =

    var path_dbs = user_dir + '/NextLevelDB/dbs'

    fs2.ensure_directory_exists(user_dir + '/NextLevelDB', (err, exists) => {
        if (err) { throw err } else {

            // And a dbs directory.
            //  Want to be able to store different databases on disk.
            //  Could have multiple instances of NextLevelDB running, each using a different DB directory.

            // Have a default database / database 0.
            //  When gathering real-time data, it may be worth ending the creation of one database, and moving onto another.
            //   Send the complete database elsewhere for backup, and possible recombination into a larger full history database.

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

                    //console.log('db_path', db_path);

                    // Could have default port.
                    //  Question about where to store data file when the server is installed.
                    //  Should be possible to keep it in the local node module's directory.
                    //   However, that could make it very big...
                    //    But its best not to copy large amounts of modules anyway.
                    //     Storing it somewhere within the node modules / global node modules makes sense.

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
                            // console.log('Started Level_Server, pre get_all_keys');

                            //ls.streaming_paged_get_all_keys((err, page) => {
                            //    console.log('page', page);
                            //})

                            //ls.count_records((err, count) => {
                            //    console.log('count', count);
                            //})

                            // Probably worth starting again with a new database.
                            //  

                            /*
                            
                            */


                            // runs out of memory with large DBs.
                            
                            ls.fns_ws.get_all_keys((err, all_keys) => {
                                if (err) {

                                } else {
                                    console.log('all_keys', all_keys);

                                    console.log('pre get table names');
                                    ls.fns_ws.get_table_names((err, table_names) => {
                                        if (err) {
                                            console.trace();
                                            throw err;
                                        } else {
                                            //console.log('table_names', JSON.stringify(table_names));
                                            console.log('table_names:', table_names);


                                            // Worth providing encoding with the definition of the table?
                                            //  Table is much concerned with keeping a separate initial key space.


                                            /*

                                            ls.ensure_table('people', (err, res_table) => {
                                                if (err) {
                                                    throw err;
                                                } else {
                                                    console.log('res_table', res_table);

                                                    // Then can add records to the people table
                                                    //  Would be useful to define an index, so that when records get put in that table, we know how they get indexed, and the indexing can be done automatically.

                                                    // The key encoding and the record encoding should be enough for the moment.



                                                    // It may be useful to have a system that automatically puts a time on records that enter the system.
                                                    //  That would make a useful generalised system. Or opens a kind of bucket for all records that appear at that one span of time.




                                                }
                                            })
                                            */

                                            //console.log('------------');
                                            //console.log('');

                                            //each(table_names, (record) => {
                                            //    console.log(record[0], record[1].toString());
                                            //});
                                            //throw 'stop';

                                            // Then have it avoid 
                                        }
                                    });
                                }
                            });
                            


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