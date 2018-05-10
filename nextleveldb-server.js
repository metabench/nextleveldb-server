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

const Model_Database = Model.Database;
const encoding = Model.encoding;
const Index_Record_Key = Model.Index_Record_Key;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;

const execute_q_obs = (q_obs) => {
    let res = new Evented_Class();
    let c = 0;
    let process = () => {
        if (c < q_obs.length) {
            let q_item = q_obs[c];
            let obs_q_item = q_item[1].apply(q_item[0], q_item[2]);
            obs_q_item.on('next', data => {
                res.raise('next', data);
            });
            obs_q_item.on('error', error => {
                res.raise('error', error);
            });
            obs_q_item.on('complete', data => {
                c++;
                process();
            });
            res.pause = () => {
                obs_q_item.pause();
            }
            res.resume = () => {
                if (obs_q_item.resume) obs_q_item.resume();
            }
        } else {
            // raise an all complete?
            res.raise('complete');
        }
    }
    process();
    return res;
}


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
// obs_to_cb


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

class NextLevelDB_Server extends Evented_Class {
    constructor(spec) {
        super();
        this.db_path = spec.db_path;
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

    get table_ids_with_indexes() {
        let res = [];
        each(this.model.tables, table => {
            if (table.indexes.length > 0) {
                res.push(table.id);
            }

        });
        return res;
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

    ll_get_records_in_range(arr_buf_range, limit = -1, callback) {


        // Optional decoding here would be useful.

        // When checking records against indexes, it will be useful to have those records decoded.
        //  For this, as well as other reasons, record decoding will be moved further down the stack.
        //   The client will be able to get the server to decode records on the server, process them, and send encoded results to the client.

        limit = limit || -1;




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





        } else if (sig === '[a,n]') {
            // Return an observable, which will also, function much like a promise.
            //  No paging option has been specified here.

            // Some kind of Paging_Observer would be quite useful.
            //  Gets given results one at a time (maybe with observer / events), and then it buffers them up into a fixed size.
            //  Would be a useful thing to apply to a function call or an existing Observer to turn into a Paged_Observer.

            // For the moment though, return an Observer with no paging.



        } else {
            throw 'NYI stop';
        }


        let res = new Evented_Class();

        let read_stream = this.db.createReadStream({
                'gte': arr_buf_range[0],
                'lte': arr_buf_range[1],
                'limit': limit
            }).on('data', function (record) {
                //console.log('key', key);
                //console.log('key.toString()', key.toString());
                //res_records.push([record.key, record.value]);
                //console.log('record', record);
                res.raise('next', [record.key, record.value]);


                // Should return every single record as this type of kv array.


            })
            .on('error', function (err) {
                console.log('Oh my!', err)
                //callback(err);

                // but could have just destroyed the stream. ?? Not sure there will be an error if none is given.
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


        // Pausing and resuming causing callbacks to occurr too many times?
        //  Just not sure right now.


        res.pause = () => {
            if (!read_stream.isPaused()) {
                read_stream.pause();
                return res.resume;
            }
        }
        res.resume = () => {
            if (read_stream.isPaused()) {
                read_stream.resume();
            }

        }
        res.stop = () => {
            read_stream.destroy();
            res.raise('complete');
        }

        res.on('complete', () => {
            res.pause = res.resume = res.stop = null;
        })

        return res;


    }

    // The keys are each a buffer.

    ll_delete_records_by_keys(arr_keys, callback) {
        let ops = [];
        // encode these keys...

        // If the keys are not already encoded.


        //let encoded_keys = encoding.encode_keys(arr_keys);

        each(arr_keys, key => {
            ops.push({
                type: 'del',
                key: key
            });
        });
        this.db.batch(ops, (err) => {
            if (err) {
                callback(err);
            } else {
                this.raise('db_action', {
                    'type': 'arr_batch_delete',
                    'value': arr_keys
                });

                callback(null, true);
            }
        })
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

    // Could be possible to have decoding option here too.
    //  Decoding option could be further down the stack and passed through.



    // May be a good usage of js iterators or generators.
    //

    ll_get_table_records(table_id, opt_cb) {
        let a = arguments,
            sig = get_a_sig(a);

        let decode = false;
        if (sig === '[n,b]') {
            decode = a[1];
            opt_cb = null;
        }

        let kp = table_id * 2 + 2;
        let obs = this.ll_get_records_with_kp(xas2(kp).buffer);
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
    }




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

    delete(record, callback) {
        // B_Record
        if (record instanceof B_Record) {
            this.delete_by_key(record.kvp_bufs[0], callback);
        } else {
            throw 'NYI';
        }
    }

    delete_by_key(buf_key, callback) {
        this.db.del(buf_key, callback);
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
        throw 'NYI';
        let table_pk = arr_record[0][0];

        let table_id = (table_pk - 2) / 2;




    }

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



    // A variety of ll functions will have a lot more complexity involving observable results, flexible calling, polymorphism, calling of optimised inner functions.



    // Then get records by prefix limit....


    // Maybe we would want to specify limit = 0 meaning it's -1 in level terms.
    //  We just won't use limit of actually 0 records... just don't do the query.



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

        let execute_q_obs = (q_obs) => {
            let res = new Evented_Class();
            let c = 0;
            let process = () => {
                if (c < q_obs.length) {
                    let q_item = q_obs[c];
                    let obs_q_item = q_item[1].apply(q_item[0], q_item[2]);

                    obs_q_item.on('next', data => {

                        /*
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': data
                        }
                        */
                        //console.log('e', e);
                        res.raise('next', data);
                    });
                    obs_q_item.on('error', error => {
                        /*
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': error
                        }
                        */
                        res.raise('error', error);
                    });
                    obs_q_item.on('complete', data => {

                        /*
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': data
                        }
                        */
                        //console.log('pre raise item complete');
                        // Only want complete to be raised when all are complete.
                        //res.raise('complete');
                        c++;
                        process();
                    });
                    // Pausing and resuming causing multiples to pop up?
                    //  
                    res.pause = () => {
                        obs_q_item.pause();
                    }
                    res.resume = () => {
                        if (obs_q_item.resume) obs_q_item.resume();
                    }
                } else {
                    // raise an all complete?
                    res.raise('complete');
                }
            }
            process();
            // functions in res to pause, unpause, stop
            return res;
        }

        let obs_all = execute_q_obs(q_obs);
        /*
        obs_all.on('next', data => {
            console.log('obs_all data', data);
        });
        obs_all.on('complete', () => {
            console.log('obs_all complete');
        });
        */
        return obs_all;
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



    get_first_and_last_keys_beginning(key_beginning, remove_pks = false, decode = false, callback) {
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

        let buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        let buf_255 = Buffer.alloc(1);
        buf_255.writeUInt8(255, 0);
        //console.log('kp', kp);
        //console.log('buf_key_beginning', buf_key_beginning);
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



    // key_beginning_to_range = kp_to_range

    get_first_key_beginning(buf_beginning, callback) {
        let range = kp_to_range(buf_beginning);
        this.get_first_key_in_range(range, callback);
    }

    get_last_key_beginning(buf_beginning, callback) {
        let range = kp_to_range(buf_beginning);
        this.get_last_key_in_range(range, callback);
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

            console.log('a.length', a.length);
            console.trace();

            throw 'get_first_key_in_range NYI';
        }

        //console.log('get_first_key_in_range [remove_kp, decode]', remove_kp, decode);

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
                            //console.log('remove_kp', remove_kp);
                            //console.log('!* res', res);

                            let decoded_res = Model_Database.decode_key(res);
                            //console.log('decoded_res', decoded_res);

                            let rkpr = Binary_Encoding.remove_kp(res);
                            //console.log('rkpr', rkpr);
                            callback(null, rkpr);
                        } else {
                            callback(null, res);
                        }
                    }
                } else {
                    callback(null, undefined);
                }
            });
    }

    // Should make decoding faslse by default in various places.

    get_last_key_in_range(arr_range, remove_kp, decode, callback) {
        //console.log('buf_l', buf_l);
        //console.log('buf_u', buf_u);

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


    get_records_in_range(buf_l, buf_u, decoding = false, remove_kp = true, limit = -1, callback) {

        // may want to call this using a single arr to hold the ranges.



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

        console.log('get_records_in_ranges', arr_ranges);

        // multiple observables, carry them out in sequence.
        //  unlikely that making parallel calls here will give much of an advantage, could mess up ordering.


        // get records in range, called as a sequence of observables.
        // obs_arrayified_call(this, this.get_records_in_range, arr_ranges);
        //  just pass through all of the results.

        let res = obs_arrayified_call(this, this.ll_get_records_in_range, arr_ranges);
        res.response_type = res.response_type || 'records';
        return res;
    }





    // An options object may be the right way
    //  Could make a decoding 
    get_keys_in_range(buf_l, buf_u, decoding = false, remove_kp = true, limit = -1, callback) {

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




    // a limit property would be cool too.
    //  but then would need to work on param parsing more.

    // Decoding while removing the key prefix could be useful... but we don't have it right now
    get_table_records(table, decode = false, remove_kp = false, callback) {

        //console.log('get_table_records');
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


    get_table_keys(table, decode = false, remove_kp = false, callback) {

        let inner = () => {
            //console.log('this.model.table_id(table)', this.model.table_id(table));


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
    }

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

    // Another arg, as result pairs

    // in p2p? get_table_key_subdivisions_sync_ranges
    //  gets the ranges on the server after the ranges on the client.

    // But it's decode false by default on the server
    //  Client api should have same api as here.

    // Parameter order
    //  remove_kp always before decode?
    //  seems right because some calling in some places will never decode.

    // Worth setting up the subdivisions without decoding.
    //  Maybe worth having a .decode function returned as part of the observable.


    // Will (maybe) move away from building decoding into these functions.
    //  Better to return the data in a standard encoded format and have convenient ways to decode it.

    // Decoding here should fully decode.
    //  In other places, there may be 2 levels of decoding:
    //  Decoding the message / page structure
    //   Decoding encoded data enclosed in the recently decoded envelope.

    // Removing the decoding parameter may make sense.
    //  It makes the code more complex.
    //  It may be easier to make an observable wrapper that carries out the decoding.
    //   Could return a function to decode the data.


    // Because of the different layers of decoding, and the ambiguity in specifing what gets decoded and where, as well as wishing for the same decoding interface to also be available from
    // client-server calls.

    // Not sure that removing the KPs is that useful or appropriate.
    //  Setting them as options on the results object may be a good way.
    //  Using an observable wrapper.
    //   Not sure that will be the most performant, but it makes for quite clear code.


    // May be worth keeping this lower level without kp removal or decoding.
    //  That's about how the results get processed, and we can process the results later on.
    //  


    // not having the callback option herre could help.

    // 

    get_table_key_subdivisions(table_id, remove_kp = true, decode = true, callback) {

        // 17/04/2018 - Processing the keys using encoded data. Much less processing using JS objects with encoding and decoding, we carry out operations using encoded data where possible and reasonably practical.

        let a = arguments,
            sig = get_a_sig(a);

        //console.log('get_table_key_subdivisions sig', sig);

        if (sig === '[n,b,b,f]') {

        } else if (sig === '[n,b,b]') {

        } else if (sig === '[n,f]') {
            callback = a[1];
            decode = true;
            remove_kp = true;
        } else if (sig === '[n]') {} else {
            console.trace();
            throw 'get_table_key_subdivisions unexpected signature ' + sig;
        }

        let as_result_pairs = true;

        // This should be extended for server-side use so that it does minimal decoding, and returns the encoded results.
        //  It will be of 'array' response type. That somewhat determines how the messages are sent back to the client.

        // Getting this working without / with minimal decoding of records will be useful.



        let res = new Evented_Class();
        // Tell the result if it's encoded or not.


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

        //console.log('table_id', table_id);
        //console.log('!!this.model.map_tables_by_id[table_id]', !!this.model.map_tables_by_id[table_id]);

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


                // Keeping the keys encoded here...
                //  That way we join together the encoded keys and key parts.



                // Get the table keys, then join the encoded key beginning and the encoded table keys.
                //  Doing operations in encoded mode should produce some performance improvements, though it's not clear quite how much.

                let obs_tks = this.get_table_keys(pk_fks[0].fk_to_table, false, true);

                // Need to avoid finishing this too soon.
                //  Can't call complete until all the results are in.
                let count_in_progress = 0;
                let obs_complete = false;
                //console.log('decode', decode);
                // encode the table kp as a buffer.
                //  model_table.buf_kp
                // 

                // Does this have a race condition where one lookup could get ahead of another?
                //  Will need to compare different reading of these (depends on how though.)


                // Does seem simplest to get rid of decoding here.
                //  Label the result object with how it was encoded, perhaps.

                obs_tks.on('next', key => {
                    //console.log('obs_tks key', key);
                    let search_key = Buffer.concat([xas2(table_id * 2 + 2).buffer, Binary_Encoding.xas2_sequence_to_array_buffer(key)]);
                    //console.log('search_key', search_key);
                    count_in_progress++;

                    // and decode option to this function...

                    // Don't remove the KPs here.
                    //  Could do so later on.

                    this.get_first_and_last_keys_beginning(search_key, false, false, (err, keys) => {
                        if (err) {
                            //console.trace(err);
                            // raise complete after error?
                            count_in_progress--;
                            // I think calls should always stop after the err
                            res.raise('error', err);
                        } else {
                            //console.log('keys', keys);
                            //console.log('count_in_progress', count_in_progress);
                            count_in_progress--;
                            //console.log('as_result_pairs', as_result_pairs);

                            if (as_result_pairs) {
                                //throw 'NYI'

                                // get versions of pair with search_key.length characters removed from beginning.
                                //  Binary_Encoding.remove_bytes_from_start_of_buffers


                                // kp has already been removed from results, meaning don't remove the search key's beginning.


                                let key_parts = Binary_Encoding.remove_bytes_from_start_of_buffers(keys, search_key.length);


                                // then remove the key prefix from the search key?



                                // Then add bytes to them identifying them as buffers.

                                // then key parts with buffer encoding.
                                //  encode_buffers_as_buffers



                                //console.log('key_parts', key_parts);
                                // And have the encoded buffers encoded as buffers.
                                //  They should be registered that they are buffers.

                                // 


                                //let buf_key_parts = Binary_Encoding.array_join_encoded_buffers(Binary_Encoding.array_join_encoded_buffers[key_parts]);

                                // Joins buffers that are already encoded
                                //  

                                let buf_key_parts = Binary_Encoding.encode_buffers_as_array_buffer(key_parts);

                                //console.log('buf_key_parts', buf_key_parts);
                                //throw 'stop';



                                // then just the search key by itself.
                                //  Need that and then buf_key_parts
                                //  Join them as an array

                                // Need to encode the search key.
                                //  Encode it as a buffer.
                                //   Currently it's unencoded.

                                //console.log('1) search_key', search_key);
                                //console.log('search_key.length', search_key.length);

                                // Encoding the data server-side so it can be read by current client-side processing.
                                //  encode_arr_to_buffer_items (but not an array in that buffer)


                                // Encode to marked_buffer
                                //  It will treat the data as an encoded buffer.
                                let enc_search_key = Binary_Encoding.encode_to_buffer([search_key]);
                                //let enc_search_key = Binary_Encoding.encode_to_buffer([search_key]);
                                //console.log('enc_search_key', enc_search_key);

                                //let buf_res = Buffer.concat([enc_search_key, buf_key_parts]);
                                let buf_res = Binary_Encoding.array_join_encoded_buffers([enc_search_key, buf_key_parts]);

                                //let buf_res = Binary_Encoding.array_join_encoded_buffers([Binary_Encoding.array_join_encoded_buffers([Binary_Encoding.encode_to_buffer(search_key)]), buf_key_parts]);

                                //let buf_res = Binary_Encoding.array_join_encoded_buffers([search_key, ]);

                                //console.log('server buf_res', buf_res);
                                //console.log('server buf_res.length', buf_res.length);

                                //console.log('');
                                res.raise('next', buf_res);
                                //console.log('* count_in_progress', count_in_progress);
                                if (obs_complete && count_in_progress === 0) {
                                    //console.log('all get_first_and_last_keys_beginning complete');
                                    res.raise('complete');
                                }
                            } else {
                                res.raise('next', keys);
                                if (obs_complete && count_in_progress === 0) {
                                    //console.log('all get_first_and_last_keys_beginning complete');
                                    res.raise('complete');
                                }
                            }

                        }
                    });
                });


                /*
                if (decode) {
                    obs_tks.on('next', key => {

                        //console.log('obs_tks key', key);

                        // need to encode this key into an array if it's more than one item...?
                        //  Binary_Encoding.count_encoded_items
                        //   A PK may have multiple fields in it.
                        //    If another field refers to that PK as though it's a single field, it gets wrapped in an array.

                        let key_fields_count = Binary_Encoding.count_encoded_items(key);
                        //console.log('key_fields_count', key_fields_count);

                        let search_key;
                        if (key_fields_count > 1) {
                            // wrap the search key as an array
                            search_key = Buffer.concat([model_table.buf_kp, Binary_Encoding.encode_buffer_as_array_buffer(key)]);
                        } else {
                            throw 'NYI'

                            search_key = Buffer.concat([model_table.buf_kp, (key)]);
                        }

                        // 
                        / *

                        if (key.length > 1) {
                            search_key = [model_table.id * 2 + 2].concat([key]);
                        } else {
                            search_key = [model_table.id * 2 + 2].concat(key);
                        }
                        * /

                        //console.log('model_table.buf_kp', model_table.buf_kp);

                        count_in_progress++;
                        console.log('search_key', search_key);

                        //

                        // Does not decode this.
                        this.get_first_and_last_keys_beginning(search_key, remove_kp, (err, keys) => {
                            if (err) {
                                count_in_progress--;
                                console.trace(err);
                                res.raise('error', err);
                            } else {
                                count_in_progress--;

                                console.log('keys', keys);
                                console.log('as_result_pairs', as_result_pairs);

                                if (as_result_pairs) {

                                    // OK, need to handle this in the case where results have been decoded.
                                    //console.log('keys', keys);
                                    let arr_res = [search_key, keys];

                                    console.log('arr_res', arr_res);

                                    res.raise('next', arr_res);


                                    if (obs_complete && count_in_progress === 0) {
                                        res.raise('complete');
                                    }
                                    //let decoded_keys = Binary_Encoding.decode(keys);
                                    //console.log('decoded_keys', decoded_keys);
                                    //throw 'NYI'
                                } else {

                                    console.log('8) keys', keys);
                                    res.raise('next', keys);
                                    if (obs_complete && count_in_progress === 0) {
                                        res.raise('complete');
                                    }
                                }
                                // [search_key, [low_extent, high_extent]]
                                //  could remove the search key from the results.

                                // prefix and bounds results.

                            }
                        })
                    })
                } else {

                    // need to encode those within an array possibly.
                    //  Seems like more simple Binary_Encoding functions will help.
                    //   Then need to encode these xas2 numbers, as a buffer, into an array that contains them.  

                    //throw 'NYI'
                }
                */

                obs_tks.on('complete', () => {
                    //console.log('obs_tks complete');
                    //throw 'stop';
                    //res.raise('complete');
                    obs_complete = true;
                })



            }
        }

        res.decode_envelope = () => {
            let d_res = new Evented_Class();
            res.on('next', data => d_res.raise('next', Binary_Encoding.decode_buffer(data)[0]));
            res.on('error', err => d_res.raise('error', err));
            res.on('complete', () => d_res.raise('complete'));
            return d_res;
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

    obs_get_records(key_list) {

        // would be nice to do for of with key list

        //console.log('obs_get_records key_list', key_list);


        // But keys that are not there?

        let res = new Evented_Class();

        let inner = async () => {
            //console.log('key_list.length', key_list.length);
            for (let key of key_list) {
                //console.log('key_list key', key);
                try {
                    let record = await this.get(key);
                    //console.log('** record', record);

                    if (record) {
                        res.raise('next', record);
                    } else {
                        //res.raise('error', key);
                    }

                    //
                    //res.raise('next', record);
                } catch (err) {
                    // not found ???

                    //console.log('err', err);

                    res.raise('error', key);
                }

            }
            res.raise('complete');
        }
        inner();
        return res;

    }

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

        let res = new Evented_Class();

        let kl = new Key_List(records);

        // We would want to be able to yield keys as we want them too?

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






    open_new_log_file_writer(name, callback) {
        // open in the db path

        console.log('this.db_path', this.db_path);

        // then a new 'logs' directory within that DB.

        // Then the log name with a datetimestamp

        // result will be the wrtiable stream.

        // Definitely want to get onto this using the fixed db. It's been running a while longer.
        //  Will see if we can get some working price histories.
        //  Graph them
        // See about distributing that data to other nodes. Sync between these servers.
        //  

        // Timestamp for right now.
        let sutc = new Date().toUTCString().split(':').join('_');

        // ensure the directory path.
        // Need to ensure the directory exists

        // fs2.ensure_directory_exists


        // first : is ok.

        let log_file_path = path.join(this.db_path, 'logs', sutc + '-' + name + '.log');

        let dirname = path.dirname(log_file_path);
        console.log('log_file_path', log_file_path);

        console.log('dirname', dirname);

        fs2.ensure_directory_exists(dirname, (err, res_exists) => {
            if (err) {
                callback(err);
            } else {

                console.log('have created log dir', dirname);

                let stream = fs.createWriteStream(log_file_path);

                callback(null, stream);

            }
        });


    }









    ensure_index_records(arr_index_records, callback) {
        // Put them in a buffer backed record list

        console.log('ensure_index_records');



        let obs_rnf = this.obs_records_not_found(arr_index_records);

        obs_rnf.on('next', key_not_found => {
            console.log('key_not_found', key_not_found);


            // could pause records not found until the record has been put.
            console.log('key_not_found', key_not_found);
            obs_rnf.pause();

            // construct the record.




        })
        obs_rnf.on('complete', () => {
            console.log('complete');
            callback(null, true);
        })




        //go through each record, checking if they are there.

        // observe_keys_not_found?

        // want it so that we can get which of the keys are found, which are not found.

        // an observable that one by one checks if records are found would be nice.
        //  or even just checking for records not found and raising them




        //let rl = 




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






    // Looks more like it's a job for the model.
    //  Deprecate?
    arr_fields_to_arr_field_ids(table_id, arr_fields, callback) {
        // May well be able to do this by consulting the model.
        //console.log('arr_fields', arr_fields);
        let res = [];
        each(arr_fields, field => {
            let t_field = tof(field);
            //console.log('t_field', t_field);
            if (t_field === 'number') {
                res.push(field);
            } else if (t_field === 'string') {
                let field_id = this.model.map_tables_by_id[table_id].map_fields[field].id;
                //console.log('field_id', field_id);
                res.push(field_id);
            }
        })
        return res;
    }

    // Setting removal of kp to false here may help.
    //  It could be done in a later processing stage. May be less efficient that way.






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
            //console.log('selected_from_key', selected_from_key);


            //console.log('num_key_fields', num_key_fields);

            // buffer_starting_index

            // number of fields in the key.

            //let buffer_starting_index = 

            let selected_from_value = Binary_Encoding.buffer_select_from_buffer(buf_value, arr_i_fields, 1, 1, num_key_fields);
            //console.log('selected_from_value', selected_from_value);

        }
        // go through that key range.
    }


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

    select_from_table(table, arr_fields, decode = true, callback) {

        let a = arguments,
            sig = get_a_sig(a);
        //console.log('sig', sig);

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
            //console.log('arr_fields', arr_fields);
            // need to ensure we have the fields as ids.
            let arr_field_ids = this.arr_fields_to_arr_field_ids(table_id, arr_fields);
            //console.log('arr_field_ids', arr_field_ids);
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


                            let buf_0 = Buffer.alloc(1);
                            buf_0.writeUInt8(0, 0);
                            let buf_255 = Buffer.alloc(1);
                            buf_255.writeUInt8(255, 0);

                            let range_1 = [Buffer.concat([xas2(0).buffer, buf_0]), Buffer.concat([xas2(0).buffer, buf_255])];
                            let range_2 = [Buffer.concat([xas2(2).buffer, buf_0]), Buffer.concat([xas2(2).buffer, buf_255])];


                            console.log('range_1', range_1);


                            let obs_get_records_in_ranges = ls.get_records_in_ranges([range_1, range_2]);


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


module.exports = NextLevelDB_Server;