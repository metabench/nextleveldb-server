const lang = require('jsgui3');
const tof = lang.tof;
const each = lang.each;
const is_array = lang.is_array;
const arrayify = lang.arrayify;
const get_a_sig = lang.get_a_sig;
const Fns = lang.Fns;
//const clone = jsgui.clone;



const Evented_Class = lang.Evented_Class;

const NextLevelDB_Server = require('./nextleveldb-server');
const NextLevelDB_Client = require('nextleveldb-client');
const fs2 = lang.fs2;


const os = require('os');
const path = require('path');
// Best to read the data out of the config before initialising.
//  Will use local config for these p2p servers.
//  Each server will have basic modes it operates under.
//   Telling it where to get its data from.

// Very much want a machine that amalgamates data from multiple clients.

// Get data since...
//  Would help if all records had their dt_put timestamp
//   dt it was put into that db, dt it was first put into any NextLevelDB


// 29/03/2018 - Want to get syncing from remote servers done today.
//  Most likely would need a few minutes to sync.
//  Reading through records by timestamp would help here too.

// Are dealing with a large amount of records so don't expect operations to be immediate.

// Condensed record set files (like a blockchain) would help too.

// Need it so that the full data set can be made available quickly.

// For the moment, work on getting the full db copy sync to work.
//  Want it to connect to a remote server, and copy over all the tables.




// To start with - comparisons of the core models.
//  Tables have the same fields and IDs.
//  Then comparison of structural records (those which are used as FKs by other records)
//   Then if all goes well, copy over every record in given tables.

// Could compare the core model rows.
//  Think this is one of the last stages before there is a well running data infrastructure.
//   Handling disconnection and reconnection will be useful for this.
//   Some kind of tracking of what has already been downloaded into the DB.
//    Checking / checksums / hashed to show that we have the full span of data for some of the datasets.

// In the very near term, need to get this downloading all of the data properly to the local net / workstation machines.
//  Then need to have it able to amalgamate data into a db that's running remotely.
//   May also be worth indexing snapshot records by their timestamps.
//    That would mean creating some kind of a bucket (maybe virtual) that can hold multiple keys.
//     Keeping them all in one record seems simpler.

// The distributed side of the advanced functionality seems most important now
//  1) To get usage on the LAN, with full data set
//  2) To get permanance. Some computers may go down sometime, want to have it running in a distributed and reliable way.
//  3) Further down the line, for performance regarding sharding.

// Rapid usage on the LAN will be very important for workstation and development purposes.
//  Backup onto local HD
//  Verification of those backups
//   (could use checksums / blockchain)

// 





















class NextLevelDB_P2P_Server extends NextLevelDB_Server {
    constructor(spec) {
        super(spec);

        // use some servers as full sources.


        if (spec.sync) {
            this.source_dbs = spec.sync.source;
        }


        this.peers_info = spec.peers;



        // then when we start it, we then copy all data from that other db.
        //  Connect to the source DB, then attempt a full_copy_from_remote

        this.clients = [];
        this.map_client_indexes = {};


    }

    get_client_by_db_name(source_db_name) {
        console.log('this.map_client_indexes', this.map_client_indexes);
        console.log('this.clients', this.clients);
        console.log('source_db_name', source_db_name);
        return this.clients[this.map_client_indexes[source_db_name]];
    }


    // Could compare the models with specific tables loaded too.


    // The core models
    get_local_and_remote_models(remote_db_name, callback) {
        let client = this.get_client_by_db_name(remote_db_name);
        console.log('client', client);

        // Maybe a copy of the local model?

        client.load_core((err, remote_model) => {
            if (err) {
                callback(err);
            } else {
                this.load_model((err, local_model) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, [local_model, remote_model]);
                    }
                })

            }
        })
    }

    diff_local_and_remote_models(remote_db_name, callback) {
        this.get_local_and_remote_models(remote_db_name, (err, models) => {
            if (err) {
                callback(err);
            } else {
                let [local, remote] = models;
                let res = local.diff(remote);
                callback(null, res);
            }
        })
    }


    /*
    compare_remote_table_to_local(remote_db_name, table_name, callback) {
        // Would be more complicated to do an observable streaming comparison.
        let client = this.get_client_by_db_name(remote_db_name);
        console.log('client', client);

        // compare the table definition
        // compare the table records.

        // Could use a version of the remote model and a version of the local model.
        //  get_local_and_remote_models would be a good basis to start comparisons.
        //   loading models is one of those platform features now, we can use it to carry out various tasks.






    }
    */


    // This syncing looks like it will be somewhat complex, as there are different possibilities as to what can be directly copied over and how.



    copy_from_source_db(name, callback) {
        console.log('copy_from_source_db', name);

        console.log('this.map_client_indexes[name]', this.map_client_indexes[name]);

        let client = this.clients[this.map_client_indexes[name]];
        //console.log('client', client);

        let local_model = this.model;

        console.log('pre load core');

        client.load_core((err, remote_model) => {
            if (err) {
                callback(err);
            } else {

                //throw 'sto';
                //console.log('local_model', local_model);
                //console.log('remote_model', remote_model);

                let diff = local_model.diff(remote_model);
                //console.log('diff', JSON.stringify(diff, null, 2));

            }

        })



        // Compare the cores
        // Compare tables


        // A whole bunch of validation checks.
        //  Comparing one db with another.

        // get the core rows of the remote db.







    }

    copy_from_source_dbs(callback) {
        let fns = Fns();
        each(this.source_dbs, source_db => {
            console.log('source_db', source_db);
            fns.push([this, this.copy_from_source_db, [source_db]]);
        })
        fns.go(callback);
    }

    connect_all_clients(callback) {
        let fns = Fns();
        each(this.clients, client => {
            fns.push([client, client.start, []]);
        });
        fns.go(callback);

    }

    connect_to_source_dbs(callback) {
        each(this.source_dbs, source_db => {

            console.log('source_db', source_db);

            //let client = new NextLevelDB_Client();

            console.log('this.peers_info, ', JSON.stringify(this.peers_info));

            let source_db_connection_info = this.peers_info[source_db];

            console.log('source_db_connection_info', source_db_connection_info);

            // then connect to all of the DB clients. Create them, then start them all together.

            let client = new NextLevelDB_Client(source_db_connection_info);

            let idx = this.clients.length;
            this.clients.push(client);
            this.map_client_indexes[source_db] = idx;



        })

        this.connect_all_clients((err, res) => {
            if (err) {
                callback(err);
            } else {

                callback(null, res);

                //this.copy_from_source_dbs(callback);
            }
        });
    }

    start(callback) {
        super.start((err, res) => {
            if (err) {
                callback(err);
            } else {
                console.log('cb super NextLevelDB_P2P_Serverstart');

                this.connect_to_source_dbs((err, res) => {
                    if (err) {
                        callback(err);
                    } else {
                        console.log('connected to source DBs');
                        callback(null, true);
                    }
                })

                // connect to the source DBs
            }

        })
    }

    // When it starts up, it will sync from other database(s).
    //  Will look at the sync_from peers.

    // When for every peer, it establishes a connection.

    // When it first connects, it will attempt to sync all records.
    //  Want it to do so with the current db if possible.
    //  Keeping the same structure tables / key values looks important.




    // More complex sharding:
    //  Will be able to know which server to send any request over to when the data is sharded.
    //  When receiving data, will split it up to the relevant machines.

    // Does not seem possible / easy at all to keep grouped records together.

    // Want flow control likely improved so that when we receive keys, we can then check for them in the db, and send off for them if we don't have them.

    // Also, get_table_rows_hash would be useful for checking some tables are the same.
    //  or table_model_hash
    //  or db_core_hash

    // table_records_hash

    // the table_model_hash would be nice if it included index rows too.
    //  Should quickly tell if syncing the core is possible.
    //   Direct sync
    //   Otherwise would need to do some value translation / get in denormalised forms. Could be slower.
    //   Want good progress updates.
    //    Would need to sync upon start.

    // Soon, want to work on a sharded db, have it running on 8 servers.
    // Could try 4 server shard processes on 1 node. Does not increase storage that way, would increase throughput, and be a way to test it.

    // Separate machines would be better for the moment.
    //  Would definitely be nice to go wide for storage and speed.
    //  Could make it so that any server then sends on storage operations to the relevant servers.
    //   It would use a formula to work out from the binary key which of the shards it goes to.

    // System could seamlessly change between sharding modes.
    //  Different nodes could have different sharding rules.
    //   Would know which machine to send any data to.




















}


if (require.main === module) {

    // Want to be able to get a full path from the command line

    const option_definitions = [{
        name: 'path',
        alias: 'p',
        type: String
    }];


    const commandLineArgs = require('command-line-args');

    const options = commandLineArgs(option_definitions);

    var config = require('my-config').init({
        path: path.resolve('../../config/config.json') //,
        //env : process.env['NODE_ENV']
        //env : process.env
    });

    console.log('options', options);

    //throw 'stop';

    var user_dir = os.homedir();
    console.log('OS User Directory:', user_dir);
    //var docs_dir =
    var path_dbs = user_dir + '/NextLevelDB/dbs';

    let access_token = config.nextleveldb_access.root[0];
    console.log('access_token', access_token);

    // Select all the listed dbs, then choose the selected source DBs.

    //let clients_info = [];

    let clients_info = {};

    console.log('config.source_dbs', config.source_dbs);
    //throw 'stop';

    console.log('config.nextleveldb_connections', config.nextleveldb_connections);

    each(config.source_dbs, (name) => {
        //console.log('config_source_db', config_source_db);

        let db_client_info = config.nextleveldb_connections[name];
        db_client_info.access_token = access_token;
        //console.log('db_client_info', db_client_info);
        //clients_info.push(db_client_info);
        clients_info[name] = db_client_info;
    });

    console.log('clients_info', clients_info);

    //throw 'stop';

    // then make them into client connection params






    //throw 'stop';

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

                    // Access token for itself, then to access clients.

                    var ls = new NextLevelDB_P2P_Server({
                        'db_path': db_path,
                        'port': port,
                        'access_token': access_token,
                        'peers': clients_info,
                        'sync': {
                            'source': config.source_dbs
                        }
                    });



                    // There could be a web admin interface too.

                    ls.start((err, res_started) => {
                        if (err) {
                            console.trace();
                            throw err;
                        } else {
                            console.log('NextLevelDB_P2P_Server Started');


                            /*

                            ls.compare_remote_table_to_local('data4', 'bittrex currencies', (err, res) => {
                                if (err) {
                                    throw err;
                                } else {
                                    console.log('res', res);
                                }
                            })

                            */

                            // get_local_and_remote_models

                            ls.diff_local_and_remote_models('data5', (err, res) => {
                                if (err) {
                                    throw err;
                                } else {
                                    console.log('res', res);

                                    console.log('diff', JSON.stringify(res, null, 2));


                                }
                            })

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