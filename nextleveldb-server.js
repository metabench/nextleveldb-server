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
const Model_Database = Model.Database;
const encoding = Model.encoding;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;


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


// Want client-side function (or on server?) to get the last record in any table.
//  This could be used at start-up to assign what the incrementor value should be

// A version of NextLevelDB_Server with safety checks upon start looks like it will be the next stage.



// Core - Would handle net io and opening the DB on disk.
// (Standard) - Would have most of the functionality.
// Safety
//  Seems to deserve its own file. Not sure about using its own class. Safety checking on startup seems like distinctive functionality.
// P2P








// A server could have a number of remote connections.
//  Being able to initiate and use remote connections would be a useful server-side piece of functionality.
//  Then make it available to the client.
// Want to be able, through a client, to get one server to copy table records from another server
// That will be a useful way to start and test the sync.
// Then there will be other sync modes, where a server will automatically sync from another server.

// May be good to use a single Amazon server for that, or another cloud provider.



// A table of completed syncing operations would help.
//  Also, syncing operations in progress.
//  Could contain data about estimates.


// May need to address changing table numbers / updating all the relevant records for that.
//  Introduce more flexibility about core / system tables?

// DB migration of table IDs seems necessary in order to have increased core table space.
//  As an earlier work-around, could have it as non-core.
//   That makes sense because it's specific to that system.

// Core records will be distributed over the system. It's the structure of the DB.
//  All core records are system records I think.

// Then there will be node-level non-distributed records.
//  Prime example being the current idea of sync tracking records. That is similar to server-level logs. Server-level info about what data other clients hold. Data about other peers on the network.
//   Useful to keep it in the DB so that it can get resumed on start-up.
//  Want to make this without breaking the current system.
//   Adding the syncing table itself will change the core.
//  Be able to add a syncing table to the remote dbs as part of maintenance
//   But they don't need it.

// Rather than checking for identical models, could check for relevant values being the same for each table.
//  Specific table IDs being the same.

// Could load up the servers so that they create the sync tables.
//  Could work around the models being different by not doing such low level syncing, or doing further tests first.

// Could keep it out of the core.
//  That way the core comparisons stay the same. That's the distributed core.
//  The trouble is that it would be referenced within the core because it exists in the DB.
//   Could check for those specific differences and OK them. We really don't want differences in the tables that are about to be synced on a low level.
//    That seems like a decent way of doing it. Still ll_sync when there are some core differences, so long as the differences won't cause problems.

// Though, an identical distributed core makes sense.
//  That would mean the same tables on all machines. Would mean we could not use incrementors there, or not the core kind.
//   Local_Incrementor?

// Separating out the distributed parts from non-distributed parts.

// Making another db could work.
//  a new sub-db called local. Keep things very separate.

// Possibly globally shared core tables would work OK.
//  Incrementors would not be such an issue.

// Changing table ids would be cool.
//  Shifting all table IDs up by one. Would need to know which field is ever a table id.
//  That seems like it would be a decent way to have a new table added at a lower ID, or for syncing when the table id changed.

// Go through every single ll record, including indexes and incrementors, and update (+1 or +n) every table id that is at or above a certain number.
//  Think this would need to suspend db writes, and then inform all clients that its model has changed.
// Notification to clients of db model changes would be useful.
//  Only send it when the changes have been completed.
//   model_change_update_subscribing_clients();
//    clients would need to specifically open the subscription to db model changes.
//     then the issue of changing the db records in correspondance to the model changes.

// A new version that has got more table space for system tables would be useful.
//  The separate db could handle records such as syncing records, and per-server security.

// Handling DB upgrades would be nice.
//  All DBs would get this syncing table.

// Have another field within the table record saying if it is a dist-core table, if it is a node-core table.
//  An unconnected sub-database would be useful for recording local logs. Wont be available through the normal API.
//  The p2p version would have the local database. Would list which ranges which have been downloaded.


// Local_System.
//  That would be a part of the p2p server.
//  Sync operations table
//  Could log read frequencies to arrange caching - though I think Level handles that anyway.
//  Could store blocks which are put into the local db / have been put into the local db.
//   Soring row range blocks in a separate local db would definitely be cool.
//  A task queue, including completed tasks and task status would definitely be of use.
//   Having it in a separate but accessible DB would be very useful.
//    It would have its own OO interface, it would not be synced with other DBs.

// Tasks
//  Completed already - timestamp completed
//  Running - timestamp started
//  Yet to run / queued. Definitely have the queued items going in sequence.
//   Not so sure about different priorities for the queue. Priorities mean some tasks could jump ahead of others.
//   The queue could be more about monitoring the tasks that are set. May want various different sequences, with blocks of tasks to have in the queue.
//   Probably just stick with an order of addition queue, and keep track of it.
//    That should be enough.

// Tasks would cover the sync operations.
//  If it has another sync operation to do, it could note the ranges of records synced in previous sync operations.
//  Could do more work on the partial syncing without this, checking the latest key values.

// Definitely will do more syncing of tables.
//  Will change the way bittrex data is added to make it more general.

// Generalising the bittrex case to other cases.
//  Probably worth re-doing some code, specifically the tables.
//  Maybe retire crypto-data-model, as we now use declarations that are loaded into the normal model.

// Returning hashes of data could be an output transformation / encoding.
//  That way we could get hashes as output for any query, or one hash that covers all results given.

// Daily record blocks could be of a lot of use.
//  Would log that it has downloaded all of the records for a given day.

// Client connection status, depending on how syncing is going?

// nextleveldb-sync-client
//  would keep track of the sync status to some extent.

// Getting inter-table ranges would definitely be of use for syncing.







// Binance, Bitfinex, HitBTC.

// exchange_id, exchange_trade_id, currency_id, value, volume, was_buy

// market snapshot data
// trade data
// candlestick data

























































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
                //console.log('model', model);
                let buf = model.get_model_rows_encoded();
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
                            callback(err);
                        } else {
                            //console.log('all_db_rows', all_db_rows);
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
                console.log('map_cookies', map_cookies);

                let provided_access_token = map_cookies['access_token'];


                // Check the provided access token to see if it's allowed.

                let access_allowed = check_access_token(provided_access_token);
                console.log('access_allowed', access_allowed);


                if (!access_allowed) {
                    request.reject();
                    console.log((new Date()) + ' Valid access token required.');
                    return;
                } else {

                    if (!originIsAllowed(request.origin)) {
                        // Make sure we only accept requests from an allowed origin

                        // Could check for the correct authorisation cookies.



                        request.reject();
                        console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
                        return;
                    }

                    // // This is a possible place to check authentication and authorisation.
                    // Could just check for a valid access token. If a valid token is provided, then we can continue.
                    //  Worth handing it to an auth module. Authenticates a user has the access token, authorises them to connect.





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
                }

                // Make the request using the right server access token.
                //  Access token and IP address, assess them and log them.

                // For the moment, want to restrict access to any client that does not have the access token.
                //  Read-only will log IP addresses (when it's time to do so);



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
                console.log('system_db_rows', system_db_rows);
                // The table incrementor value should be at least about 4.

                //let decoded_system_db_rows = Model_Database.decode_model_rows(system_db_rows);
                //console.log('system_db_rows', system_db_rows);
                console.log('system_db_rows.length', system_db_rows.length);
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


                // Leaving out the index of incrementors.??
                // 

                let model_rows = this.model.get_model_rows();
                //console.log('system_db_rows.length', system_db_rows.length);
                //console.log('model_rows.length', model_rows.length);

                if (model_rows.length !== system_db_rows.length) {
                    //console.log('system_db_rows.length', system_db_rows.length);
                    console.log('model_rows.length', model_rows.length);

                    // 13/03/2018 - This is a newly discovered bug where the model does not make every table (missing the native types table) when it gets reconstructed.

                    // do a diff here?

                    //console.log('model_rows', Model_Database.decode_model_rows(model_rows));
                    //console.log('system_db_rows', Model_Database.decode_model_rows(system_db_rows));


                    // Not so sure that an index of incrementors is that useful....
                    //  However, missing such an index (when expected) will cause a crash here.
                    //  Could remove that index of incrementors.

                    //let models_diff = this.model.diff();

                    // Need to do db kv row diff.



                    //let diff = deep_diff(model_rows, system_db_rows);

                    //

                    let diff = Model_Database.diff_model_rows(Model_Database.decode_model_rows(model_rows), Model_Database.decode_model_rows(system_db_rows));
                    console.log('diff', JSON.stringify(diff, null, 2));

                    console.log('diff.changed.length', diff.changed.length);
                    console.log('diff.added.length', diff.added.length);
                    console.log('diff.deleted.length', diff.deleted.length);

                    //each(diff.changed, changed => console.log('changed', changed));

                    each(diff.changed, changed => {
                        console.log('changed[0]', changed[0]);
                        console.log('changed[1]', changed[1]);



                    });


                    // Though this error is very annoying, it will help to keep things in sync and prevent it from getting worse.
                    //  This checks that the model rows have been loaded properly from the DB

                    callback(new Error('Mismatch between core db rows and core rows obtained from model. '));



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



    // Records with incomplete keys...
    //  Could be a fine format for when we know the record will be assigned an id by the table.

    // Model will have some details about that.
    //  want to be able to create records (for testing) without changing the incrementor or adding them to the model.
    //  Then after passing tests the record could be added.

    // Will also have ensure_record check against indexes.





    ensure_record(arr_record, callback) {

        // an inner promise / observable

        // will work differently when the key is automatically generated.

        let table_pk = arr_record[0][0];

        let table_id = (table_pk - 2) / 2;




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


    // get table system db rows
    //  



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


    batch_put(buf, callback) {

        // buf is an array by default? An array of buffers?

        var ops,
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


        // Could have a faster version without function call events here.
        //  

        // Simpler split to array row buffers would be faster.

        /*

        Binary_Encoding.evented_get_row_buffers(buf, (arr_row) => {

            //console.log('arr_row', arr_row);

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

        */


        let arr_pairs = Model.encoding.buffer_to_row_buffer_pairs(buf);


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
            /*
            each(arr_pairs, pair => {
                ops.push({
                    'type': 'put',
                    'key': pair[0],
                    'value': pair[1]
                });
            })
            */
        }

        if (this.using_prefix_put_alerts) {
            put_using_prefix_alerts();
        } else {
            put_without_prefix_alerts();
        }




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
                callback(null, ops.length);
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

    // Will be useful for partial syncing.

    // And a decode option going into this function.
    //  This will enable retrieval of this data (could be plenty of it) without needing to be decoded server-side.
    //   This will hopefully boost performance a decent amount.
    //    Or just be part of an efficient system.
    // Overhead of sig parsing and more polymorphism?




    get_first_and_last_keys_in_buf_range(buf_l, buf_u, remove_kp, decode, callback) {



        let a = arguments,
            sig = get_a_sig(a);

        console.log('get_first_and_last_keys_in_buf_range sig', sig);
        console.log('[buf_l, buf_u, remove_kp, decode]', buf_l, buf_u, remove_kp, decode);
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
                    //console.log('res_all', res_all);

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



    get_first_and_last_keys_beginning(key_beginning, remove_pks = false, decode = false, callback) {
        let a = arguments;
        let sig = get_a_sig(a);
        let buf_key_beginning;

        // Yet more arguments... will have decode (or not) option

        console.log('get_first_and_last_keys_beginning sig', sig);

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
        // An inner promise would probably work best.






        // Then first and last keys based on that.

        // Already have something in the binary handler that does this.
        //  Will do first and last keys by range

        let buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        let buf_255 = Buffer.alloc(1);
        buf_255.writeUInt8(255, 0);
        //console.log('kp', kp);

        let buf_l = Buffer.concat([buf_key_beginning, buf_0]);
        let buf_u = Buffer.concat([buf_key_beginning, buf_255]);


        // This will also need a decode option.

        let pr_res = this.get_first_and_last_keys_in_buf_range(buf_l, buf_u, remove_pks, decode);

        if (callback) {
            pr_res.then(res => callback(null, res), err => callback(err)).catch(err => {
                callback(err);
            });
        } else {
            return pr_res;
        }







    }


    // Assumes decoding

    // These two functions below could do with decoding options.
    //  Decoding should be optional

    get_first_key_in_range(arr_range, remove_kp, decode, callback) {

        let a = arguments;

        if (a.length === 3) {
            callback = a[2];
            decode = true;
        }
        if (a.length === 4) {

        } else {
            throw 'NYI';
        }

        //console.log('buf_l', buf_l);
        //console.log('buf_u', buf_u);

        let res;

        this.db.createKeyStream({
                'gte': arr_range[0],
                'lte': arr_range[1],
                'limit': 1
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
                    //console.log('get_first_key_in_range res', res);

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
    }

    get_last_key_in_range(arr_range, remove_kp, decode, callback) {
        //console.log('buf_l', buf_l);
        //console.log('buf_u', buf_u);

        let a = arguments;

        if (a.length === 3) {
            callback = a[2];
            decode = true;
        }
        if (a.length === 4) {

        } else {
            throw 'NYI';
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
    }

    get_last_key_in_table(table, callback) {
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

    }

    arr_fields_to_arr_field_ids(table_id, arr_fields, callback) {
        // May well be able to do this by consulting the model.
        console.log('arr_fields', arr_fields);
        let res = [];
        each(arr_fields, field => {
            let t_field = tof(field);
            console.log('t_field', t_field);
            if (t_field === 'number') {
                res.push(field);
            } else if (t_field === 'string') {
                let field_id = this.model.map_tables_by_id[table_id].map_fields[field].id;
                console.log('field_id', field_id);
                res.push(field_id);

            }
        })
        return res;


    }

    get_records_in_range(buf_l, buf_u, decoding = false, remove_kp = true, callback) {

        let inner = () => {
            let res = new Evented_Class();

            // but with decoding option

            if (!decoding) {
                this.db.createReadStream({
                        'gt': buf_l,
                        'lt': buf_u
                    }).on('data', function (data) {
                        let buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
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
                        //console.log('data', data);

                        let decoded = encoding.decode_model_row([data.key, data.value]);
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

    get_keys_in_range(buf_l, buf_u, decoding = false, remove_kp = true, callback) {

        // Will be possible to remove the key prefixes while not decoding them.

        let inner = () => {
            let res = new Evented_Class();

            // but with decoding option

            if (!decoding) {
                this.db.createKeyStream({
                        'gt': buf_l,
                        'lt': buf_u
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
                        'lt': buf_u
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


    // Decoding while removing the key prefix could be useful... but we don't have it right now
    get_table_records(table, decode = false, remove_kp = false, callback) {


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


    get_table_keys(table, decode = false, remove_kp = false, callback) {

        let inner = () => {
            console.log('this.model.table_id(table)', this.model.table_id(table));


            let buf_key_prefix = xas2(this.model.table_id(table) * 2 + 2).buffer;
            //console.log('buf_key_prefix', buf_key_prefix);
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

            let res = this.get_keys_in_range(buf_l, buf_u, decode, remove_kp);
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



    // Would be nice to get this working from the client.
    //  When syncing smaller tables can look for that hash value to compare.


    // Get records_in_range_hash
    //  Though we could have items in the API that operate on tables, it's a bit higher level.
    //  Don't have get_table_records though.

    // Maybe hashes of records should be similar. Expose that through the API, then the client can get table record hashes that way
    //  As well as hashes of other key selections of records.






    get_table_records_hash(table, callback) {
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
    }


    // select from records in range
    //  again may need a remove_kp option

    select_from_records_in_range(arr_pair_buf_range, arr_i_fields, callback) {
        // probably need to decode?
        //  or we get the decoded selections, then re-encode them.

        // A select encoded would help too, as it would be faster. Not needing decoding server-side for this selection.

        //  And the number of key prefixes
        //  Whether to include the key prefixes in the fields, and how many of them to skup
        // Binary_Encoding.buffer_select_from_buffer(buf, selection_indexes)

        // read_buffer

        // What to do with the record key prefixes.

        //  This does not consult indexes
        //   That would be used with a 'where' condition.

        // go through the range, and would need to do selections on both the keys and the values.


        let skip_table_kp = true;
        // means we need to process the table kp while reading.

        let fields_from_kv = (buf_key, buf_value) => {
            // 
            let res = new Array(arr_i_fields.length);
            // extract the fields from the key.

            // let buffer_select_from_buffer = (buf, arr_int_indexes, num_kps_encoded, num_kps_to_skip) => {


            // Want to select from the key while counting the number in the key

            let [selected_from_key, num_key_fields] = Binary_Encoding.buffer_select_from_buffer(buf_key, arr_i_fields, 1, 1);
            console.log('selected_from_key', selected_from_key);


            console.log('num_key_fields', num_key_fields);

            // buffer_starting_index

            // number of fields in the key.

            //let buffer_starting_index = 

            let selected_from_value = Binary_Encoding.buffer_select_from_buffer(buf_value, arr_i_fields, 1, 1, num_key_fields);
            console.log('selected_from_value', selected_from_value);



        }


        // go through that key range.







    }


    // Having decoding options would be useful here.

    //  The seelction of data when it's encoded could be a bit more efficient.
    //   Decoding would take place at the last step.

    select_from_table(table, arr_fields, decode = true, callback) {

        let a = arguments,
            sig = get_a_sig(a);
        console.log('sig', sig);

        let table_id;
        if (sig === '[s,a,b]') {
            table_id = this.model.table_id(table, true);
        } else if (sig === '[n,a,b]') {
            table_id = table;
        } else {
            throw 'Unexpected select_from_table signature ' + sig;
        }
        //throw 'stop';

        // Will use inner observable, but put them in a callback if it's been called with one.




        let inner = () => {

            console.log('arr_fields', arr_fields);

            // need to ensure we have the fields as ids.
            let arr_field_ids = this.arr_fields_to_arr_field_ids(table_id, arr_fields);
            console.log('arr_field_ids', arr_field_ids);

            // then go through all of the records.

            // What a about an observable get_table_records?
            //  Would need to decode the records server-side.



            // Don't decode, as we use the skipping decode to get the indexes we want.

            // The get table records without decoding - that could join the results into kv pairs.
            //  Not sure of the point apart from consistency though.


            let obs_tr = this.get_table_records(table, false);
            // Encode each as a kvp buffer?


            let res = new Evented_Class();
            let selected_fields;
            let num_fields = arr_fields.length;


            if (decode) {
                obs_tr.on('next', data => res.raise('next', Binary_Encoding.decode(encoding.select_indexes_buffer_from_kv_pair_buffer(data, 1, arr_field_ids))));
            } else {
                obs_tr.on('next', data => res.raise('next', encoding.select_indexes_buffer_from_kv_pair_buffer(data, 1, arr_field_ids)));
            }



            obs_tr.on('complete', () => res.raise('complete'));

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


        // Maybe a where condition too? 






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


    // This is a rather niche function that will be used to help syncing more efficiently.
    //  Will be used to get the latest records throughout the different sub-keys of a table.

    // Callbackify / promisify for resolve all?

    // Removing key prefixes from results may be worthwhile.

    // remove_kp could be irrelevant?
    get_table_key_subdivisions(table_id, remove_kp, decode, callback) {

        let a = arguments,
            sig = get_a_sig(a);

        if (sig === '[n,b,b,f]') {

        } else if (sig === '[n,b,b]') {

        } else if (sig === '[n,f]') {
            callback = a[1];
            decode = true;
            remove_kp = true;
        } else {
            console.trace();
            throw 'get_table_key_subdivisions unexpected signature ' + sig;
        }

        // This should be extended for server-side use so that it does minimal decoding, and returns the encoded results.
        //  It will be of 'array' response type. That somewhat determines how the messages are sent back to the client.

        // Getting this working without / with minimal decoding of records will be useful.



        let res = new Evented_Class();

        if (decode) {
            res.response_type = 'array';
        } else {
            res.response_type = 'binary';
        }


        // look at the pk fields.
        //  if there is just one pk field that references another table...
        //   or do it for the first.

        // Generate keys with all the values for what is referred to.





        let pk_fk_count = 0;
        let pk_fks = [];

        console.log('table_id', table_id);
        console.log('!!this.model.map_tables_by_id[table_id]', !!this.model.map_tables_by_id[table_id]);

        let model_table = this.model.map_tables_by_id[table_id];
        each(model_table.pk.fields, pk_field => {
            if (pk_field.fk_to_table) {
                pk_fk_count++;
                pk_fks.push(pk_field);
            }
        })

        //console.log('pk_fk_count', pk_fk_count);
        if (pk_fk_count > 0) {
            // If the very first pk field is the fk...
            let first_pk_fk_field_id = pk_fks[0].id;
            //console.log('first_pk_fk_field_id', first_pk_fk_field_id);

            if (first_pk_fk_field_id === 0) {
                // can do this.
                //  get all of the ids (pks) of that table.

                // get table keys
                //  and could do this as an observable - that way we can give back the results quickly


                // The full table keys.
                //  Probably is best to remove KPs from this?

                // Want to create key prefixes / ranges to search for records under.
                /// Could get the counts of each of them.




                // Can we remove the KPs without decoding?
                //  Binary_Encoding.remove_kp
                //  

                let obs_tks = this.get_table_keys(pk_fks[0].fk_to_table, decode, true);

                // Need to avoid finishing this too soon.
                //  Can't call complete until all the results are in.

                let count_in_progress = 0;
                let obs_complete = false;


                if (decode) {
                    obs_tks.on('next', key => {
                        //console.log('obs_tks key', key);
                        let search_key;
                        if (key.length > 1) {
                            search_key = [model_table.id * 2 + 2].concat([key]);
                        } else {
                            search_key = [model_table.id * 2 + 2].concat(key);
                        }
                        count_in_progress++;

                        this.get_first_and_last_keys_beginning(search_key, remove_kp, (err, keys) => {
                            if (err) {
                                count_in_progress--;
                                res.raise('error', err);
                            } else {
                                count_in_progress--;
                                res.raise('next', keys);
                                if (obs_complete && count_in_progress === 0) {
                                    res.raise('complete');
                                }
                            }
                        })
                    })
                } else {

                    // need to encode those within an array possibly.
                    //  Seems like more simple Binary_Encoding functions will help.
                    //   Then need to encode these xas2 numbers, as a buffer, into an array that contains them.


                    obs_tks.on('next', key => {
                        console.log('obs_tks key', key);
                        let search_key = Buffer.concat([xas2(table_id * 2 + 2).buffer, Binary_Encoding.xas2_sequence_to_array_buffer(key)]);
                        //console.log('search_key', search_key);
                        count_in_progress++;

                        // and decode option to this function...

                        this.get_first_and_last_keys_beginning(search_key, remove_kp, false, (err, keys) => {
                            if (err) {
                                console.trace(err);

                                // raise complete after error?
                                count_in_progress--;
                                res.raise('error', err);
                            } else {
                                console.log('keys', keys);
                                console.log('count_in_progress', count_in_progress);
                                count_in_progress--;
                                res.raise('next', keys);
                                if (obs_complete && count_in_progress === 0) {
                                    console.log('all get_first_and_last_keys_beginning complete');
                                    res.raise('complete');
                                }
                            }
                        })
                    })





                    //throw 'NYI'
                }



                obs_tks.on('complete', () => {
                    console.log('obs_tks complete');
                    //throw 'stop';
                    //res.raise('complete');
                    obs_complete = true;
                })



            }
        }

        //throw 'stop';

        if (callback) {
            let res_all = [];
            res.on('next', data => res_all.push(data));
            res.on('error', err => callback(err));
            res.on('complete', () => callback(null, res_all));
        } else {
            return res;
        }




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

                            let decode = true;

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

                            let pr_trh = ls.get_table_records_hash('bittrex currencies');
                            pr_trh.then(res => {
                                console.log('pr res', res);
                            }, err => {
                                console.log('pr err', err);
                            });


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