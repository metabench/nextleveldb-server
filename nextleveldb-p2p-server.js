const lang = require('jsgui3');
const tof = lang.tof;
const each = lang.each;
const is_array = lang.is_array;
const arrayify = lang.arrayify;
const get_a_sig = lang.get_a_sig;
const Fns = lang.Fns;
//const clone = jsgui.clone;

const pr_obs_all_complete = lang.pr_obs_all_complete;

const Evented_Class = lang.Evented_Class;

const NextLevelDB_Server = require('./nextleveldb-safer-server');
const NextLevelDB_Client = require('nextleveldb-client');
const Model = require('nextleveldb-model');
const Model_Database = Model.Database;
const fs2 = lang.fs2;


const Binary_Encoding = require('binary-encoding');
const database_encoding = Model.encoding;

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
//  Later on - improving the immediancy of it with feedback

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


// Do a bit more to get it reading the field data from the live servers.




// Carry out the same operation on all peers, then get all the results back.
//  Try with callback function.
//  Would get all records in some table.

// Same operation on multiple clients.
//  Could have a multi_client component.
//  Would get separate responses from each client.
//  May want to check that the responses are the same.

// For the moment, want to receive the data into the local workstation / xeon server.
//  Maintain a cache of local data
//  See how big it is.
//  Check for outlying data
//  Use the Xeon server to build up a data cache. It could go in colocation sometime.

//  Downloading partial data sets from the server will help with verification.
//   May also wish to download other types of data, such as candlestick data.
//    Then incoming data could be checked against the candlestick data for verification.

// Definitely want ongoing data gathering. Need to do read validation on incoming records somehow from the servers which have had problems.
//  Selecting some info from records would make for slightly faster retrieval too.

// Do more work on the local syncing.
//  Find out how up-to-date any of the data is, either locally or remotely.

// Get first and last records within key ranges.
//  Get the syncing to local working fine, then get sets of data from local
//  Could just look within the past 12 hours for analysis, once the streaming data system is working fine.

// Want it to be able to verify it has done the streaming relatively quickly.
//  A 'streaming operations' table could help.


// Individually downloading price histories based on their market id, then timestamp.
// Individually downloading table subsections based on their foreign key's table's ids, then timestamp.
//  Want to write this general case.
//  Get key ranges for table subsections based on foreign key and timestamp.
//   get_fk_timestamp_key_ranges
//    from timestamp, to timestamp
//    use max and min timestamp vals.
//  Could do it individually by the market. That would go in assets_client, could use some expanded underlying functionality.

// https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=BTC-WAVES&tickInterval=thirtyMin&_=1499127220008
//  gets data going back over 1 month, daily.

// Want to store these records
//  Look at the data itself, put it into the db.



// 12/04/2018
//  Have made some more supporting functionality for getting the sync done.
//  Will make a generalised case that allows syncing of the bittrex assets
//   will check tables it depends on
//    will sync them
//   will make key space splits
//    will use those key space splits to determine what ranges to sync from the server.
//  Syncing in a user-friendly way will take some more work.
//  Will make it so that syncing and sync checking is very fast.
//  A computer on the LAN staying in sync will help too.
//   Syncing and sync checking will be vey fast in a veriety of cases.
//  Next to do: syncing of structural changes
//   Could have an ordered and timestamped structural changes log.
//    May well exclude autoincrementor changes? Or could only write the autoincrementor changes once something else gets written, so it's not updated each time?
//     Or include it and know there could be many 'structural' changes?
//    More worth like its worth logging add-table etc.
//     Then would need to work to get copies of the table using the right autoincrement values.
//      With some comparisons will have exclusions for autoinc values.
//   Will log major, repeatable, reversable changes. Generally / never will log a row being added. Will log row structure changing.
//    Will leave incrementor changes because there could be very many.
//   Want to be able to upload new structure and have it pushed to the clients immediately or on connection if they are not connected.

// So prior to syncing, a fairly complex function to make gets multiple key prefixes (all possible ones) and then fo each of them gets the first and last keys
//  get_all_possible_fk_key_values
//   then with all those values it gets the first and last keys in the range
//   it does this on both the local and remote, compares the results.
// This will enable rapid syncing of a fairly simple type of timeline data.
//  Will do more automated syncing, bugs fixed, more functionality etc until we are reliably getting historic and immediate data efficiently and performing analysis on it.








































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
        //console.log('this.map_client_indexes', this.map_client_indexes);
        //console.log('this.clients', this.clients);
        //console.log('source_db_name', source_db_name);
        return this.clients[this.map_client_indexes[source_db_name]];
    }


    // Could compare the models with specific tables loaded too.


    // The core models
    get_local_and_remote_models(remote_db_name, callback) {
        let client = this.get_client_by_db_name(remote_db_name);
        //console.log('!!client', !!client);

        if (client) {
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
        } else {
            callback(new Error('No client found for database ' + remote_db_name));
        }

        // Maybe a copy of the local model?


    }

    get_remote_buf_core(remote_db_name, callback) {
        console.log('remote_db_name', remote_db_name);
        let client = this.get_client_by_db_name(remote_db_name);
        client.load_buf_core(callback);
    }

    get_table_key_subdivision_sync_ranges(db_name, table_name) {

        // an observable makes most sense.
        // Would be nice to use an inner observable
        //  or 2 - get back the subdivisions one at a time.

        // get both the local and remote subdivisions
        // Same table id local and remote?

        let client = this.clients[this.map_client_indexes[db_name]];
        let local_table_id = this.model.table_id(table_name);
        let remote_table_id = client.model.table_id(table_name);

        let table_id;

        let res = new Evented_Class();
        // res.encoding_type
        // res.encoding
        //  res.decode ???

        // .go could return a promise or observable.


        if (local_table_id === remote_table_id) {

            // Observe the subdivisions from both tables
            table_id = local_table_id;


        } else {
            throw 'Table ID mismatch. Make sure both the local and remote tables have the same ID.'
        }

        let obs_local = this.get_table_key_subdivisions(table_id, false, true).decode_envelope();

        let obs_remote = client.get_table_key_subdivisions(table_id);
        let map_local_subdivisions = {};
        let map_remote_subdivisions = {};

        let pr_all = pr_obs_all_complete([obs_local, obs_remote]);

        // But with decoding as false on local it behaves differently.
        //  Unify the behaviours and APIs of both the client and server versions.


        obs_local.on('next', data => {

            map_local_subdivisions[data[0].toString('hex')] = data[1];

            if (map_remote_subdivisions[data[0].toString('hex')]) {
                res.raise('next', [data[0],
                [data[1], map_remote_subdivisions[data[0].toString('hex')]]
                ])
            }
        })
        obs_remote.on('next', data => {

            map_remote_subdivisions[data[0].toString('hex')] = data[1];
            if (map_local_subdivisions[data[0].toString('hex')]) {
                //console.log('found match');
                res.raise('next', [data[0],
                [map_local_subdivisions[data[0].toString('hex')], data[1]]
                ])
            }
        });
        obs_remote.on('error', err => {
            console.log('err', err);
            throw 'stop';
        });

        obs_remote.on('complete', () => {
            console.log('remote is complete');
        });
        obs_local.on('complete', () => {
            console.log('local is complete');
        });

        pr_all.then(() => {

            console.log('pr all complete')

            res.raise('complete');
        })
        // and want the results of each of them.
        return res;
    }



    diff_local_and_remote_models(remote_db_name, callback) {

        let a = arguments,
            sig = get_a_sig(a);


        if (sig === '[s,f]') {

        }
        if (sig === '[f]') {
            callback = a[0];
            remote_db_name = null;
        }


        // diff all of them...

        if (remote_db_name !== null) {
            this.get_local_and_remote_models(remote_db_name, (err, models) => {
                if (err) {
                    callback(err);
                } else {
                    let [local, remote] = models;
                    let res = local.diff(remote);
                    callback(null, res);
                }
            });
        } else {
            // Do it on all of them
            throw 'NYI';
        }



        // Don't get given a remote DB name, then it's all of them.


    }


    // Will be used to check the bittrex currencies and markets are the same before syncing data.
    //  If they are not the same, we could use some specific error recovery.


    diff_local_and_remote_table(remote_db_name, table_name, callback) {

        // get the local table records, get the remote table records.
        let local_table_records, remote_table_records;


        let proceed = () => {
            //console.log('proceed');
            //console.log('!!local_table_records', !!local_table_records);
            //console.log('!!remote_table_records', !!remote_table_records);

            if (local_table_records && remote_table_records) {
                //console.log('local_table_records.length', local_table_records.length);
                //console.log('remote_table_records.length', remote_table_records.length);


                let diff = Model_Database.diff_model_rows(local_table_records, remote_table_records);
                //console.log('diff', diff);
                //throw 'stop';

                callback(null, diff);
            }


        }


        // decode and remove the key prefixes

        //console.log('local this.get_table_records');

        this.get_table_records(table_name, true, true, (err, _local_table_records) => {

            // Should have kps removed.

            if (err) {
                callback(err);
            } else {
                local_table_records = _local_table_records;
                //console.log('p1');
                proceed();
            }
        });

        let client = this.get_client_by_db_name(remote_db_name);

        // option to remove kps.

        client.get_table_records(table_name, true, true, (err, _remote_table_records) => {
            if (err) {
                callback(err);
            } else {
                remote_table_records = _remote_table_records;

                //console.log('remote_table_records', remote_table_records);
                //throw 'stop';
                //console.log('p2');
                proceed();
            }
        });


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



    // copy_key_ranges_to_local(db_name, key_ranges)

    // get_records_in_ranges

    copy_key_ranges_to_local(db_name, key_ranges) {
        let res = new Evented_Class();
        // could have an observable that gives feedback on the number of records put.

        let client = this.clients[this.map_client_indexes[db_name]];



        /*

        // Could count them first for better progress report.
        let obs_get = client.ll_get_records_in_ranges(key_ranges);
        // Don't want these to be unpaged.
        //  The ll client version will keep them paged.

        obs_get.on('next', data => {

            console.log('obs_get data.length', data.length);

        });
        */



    }



    // 

    copy_key_range_to_local(db_name, buf_l, buf_u) {

        let res = new Evented_Class();
        // could have an observable that gives feedback on the number of records put.

        let client = this.clients[this.map_client_indexes[db_name]];

        let obs_count = client.ll_count_keys_in_range(buf_l, buf_u);

        console.log('copy_key_range_to_local buf_l, buf_u', buf_l, buf_u);

        obs_count.on('next', data => {
            console.log('obs_count data', data);

            // Then raise a result, saying it's counting.

            let o = {
                'type': 'count'
            }
            res.raise('next', o);

        });

        obs_count.on('complete', count => {
            console.log('obs_count complete count', count);



            if (count > 0) {
                let obs_get = client.ll_get_records_in_range(buf_l, buf_u);

                // Total put in this key range
                let total_put = 0,
                    prop_put, pct_put;


                // How many bytes downloaded

                obs_get.on('next', data => {

                    console.log('obs_get data.length', data.length);

                    // Buffer put this data into the DB.



                    this.batch_put(data, (err, put_count) => {

                        // Want to say how many records were put in the result.
                        //  Not sure we want other data?
                        //  res.count

                        if (err) {
                            res.raise('error', err);
                        } else {
                            // the batch put result could say the number of records.
                            //  That way we can work out the proportion complete.
                            //console.log('put_count', put_count);
                            total_put = total_put + put_count;
                            prop_put = total_put / count;
                            pct_put = (prop_put * 100).toFixed(1);

                            //console.log('pct_put ' + pct_put + '%');


                            let o = {
                                'type': 'put',
                                'pct': pct_put,
                                'total_records_put': total_put
                            }
                            res.raise('next', o);





                            /*


                            let obj_res = {
                                'db_name': db_name,
                                'table_name': table_name,
                                'prop_complete': prop_put,
                                'pct_complete': pct_put
                            }
                            //console.log('have put record batch.');
                            res.raise('next', obj_res);

                            */

                            // 
                        }
                    });





                })

                obs_get.on('complete', () => {
                    console.log('data has been put into the db');

                    res.raise('complete');
                })

            } else {
                console.log('count is 0');
                res.raise('complete');
            }

            // then do the actual key range copy.
            //

            //  Want all of the records grouped together for faster put.







            //res.raise('complete');
        });

        // Could count the keys there.


        // Count first, then sync over.

        // 





        return res;

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

                        // Automatic syncing on start looks useful
                        //  So long as it's reliable and performant.

                        // Download all data from remote. 

                        callback(null, true);
                    }
                });
                // connect to the source DBs
            }
        })
    }



    // This isn't working (syncing by table).
    //  Would probably need further tests to check everything is working OK when syncing.

    //  I think that a duplicate_db table sync system would be best.
    //  It simply gets the core from the other DB, then it syncs over the table records
    //   Probably best to recreate the index records on the receiving side.


    // Downloads the whole core from elsewhere, then replaces the local core with that.
    //  This is to be considered an overwrite_copy_sync.
    //   Should possibly check for existing tables that conflict with the key space.
    //   Could assume we are doing this on new DBs though.
    //    Restarting that DB becomes more complex.
    //     With the same structure we would see only a few differences in the core records, if any.

    // Could possibly copy over a larger key range too, including the whole of the structure tables.



    // copy_key_range_to_local

    // A multi-call observable wrapper would be useful.
    //  Calls a number of different observables,
    //   callbacks could still be useful for retaining closure context.



    // May be good to get sharding sorted out before long.
    //  Make some functionality that moves it closer to sharded capability
    //  Ability to store key ranges on other machines.

    // Also want to be able to compress larger amounts of data further. Could be useful for transmitting to the client.


    // Should probably do some more work on full syncing.
    //  These gigabytes are filling up.
    //  A data sharding / distribution system would definitely help.
    //  Allocation of key ranges to specific DBs seems like the right way.
    //   Then sparse distribution which would mix the data amongst different servers?
    // Could make a specialised client that will treat the group as it it's one, exposing the standard API.
    //  The client would have different connections open.

    // RocksDB also looks like it would be worth a good look, it would use less disk space.






    // Want to use an improved system in the background for this, handling message encoding / decoding / wrapping.
    //  There will be quite a number of different callbacks
    //  Parallelism options, which it would be nice to answer in a general way.
    //   Options about how the data is returned.
    //    Observables encoders / decoders as necessary


    // Probably best to progress through them with no parallelism. Just get the data back in certainly the right order, reasonably quickly.


    // A result joining client will later be useful for requests to multiple peers at the same time.



    // This will enable much more rapid syncing, without requiring so many messages and the associated latency.
    //  A server-side record paging sender / observer would probably be of use.


    // Also, want to do more in terms of keeping a data structure up-to-date.
    //  Could make something with 1s resolution.



    // sync_remote_puts

    //  Can sync the remote puts by table
    //   In some cases, may need to re-index records when that syncing takes place.
    //   Will subscribe to the DB, and put these into the local db.

    // Want it so that we then load a data structure from the local DB with historic pricing info for one item.

    // 19/04/2018
    //  Will get some analysis on a single timeseries done today, possibly including updates.

    // Want updates / events to be generated on the client-side for pricing events (such as RSI moving above or below a threshold)



    // Still faster syncing looks important too.
    //  Want to download all of them in one operation from the server.
    //  Have the server produce results that are from the retrieval request, covering different data ranges.
    //   Syncing all at once, in some cases will mean the sync takes just a second or so.


















    //  count_key_ranges



    // A db get_key_ranges function will be useful.

















    unsafe_sync_core(db_name, callback) {
        // Does not do it by model, does it lower level by records.

        this.get_remote_buf_core(db_name, (err, remote_buf_core) => {
            if (err) {
                callback(err);
            } else {
                this.batch_put(remote_buf_core, (err, res_put) => {
                    if (err) {
                        res.raise('error', err);
                    } else {
                        console.log('have put record batch.');

                        // load the model on the local client?
                        //  And use that model to index the incoming records.
                        //   Sync the records from the tables.












                        callback(null, res_put);
                    }
                });
            }
        })

    }










    sync_db_table_structure(db_name, table_name, remote_model) {
        // This is an observable too.

        // Could do a get table structure records from the server.
        //  Model invalidation on the server?
        //   So when a record within the core changes, the model becomes invalid, and needs to be reloaded before it can used.
        //    get_valid_model(callback) function.
        //    could use load_model for the moment, then replace it with the more optimised version.

        console.log('sync_db_table_structure table_name', table_name);

        // get the table structure binary records.
        let buf_structure = remote_model.map_tables[table_name].buf_structure;
        console.log('buf_structure', buf_structure);

        let rbs = database_encoding.buffer_to_buffer_pairs(buf_structure);
        console.log('rbs', rbs);

        //throw 'stop';

        let decoded_rbs = database_encoding.decode_model_rows(rbs);
        console.log('decoded_rbs', decoded_rbs);




        // then do the (low level) put operation upon these encoded model rows.

        let res = new Evented_Class();


        this.batch_put(buf_structure, (err, res_put) => {
            if (err) {
                res.raise('error', err);
            } else {


                //throw 'stop';
                res.raise('complete');
            }
        });
        return res;
    }




    // It looks like syncing won't be too difficult in this case.

    // Getting a cockroachdb up and running would be very useful too.
    //  Would be interesting to sync to and from that.




    // getting the syncing data 
    //  maybe we really want it to be partially decoded, as [b, [b, b]]
    //  fully decoded: [arr_values, [arr_values, arr_vulues]]


    // Decoding level
    //  'none'
    //  'standard'
    //  'full'

    // Decoding in terms of decoding the message (or page)
    //  Decoding in terms of decoding the data in the message.

    // Some functions may want a buffer to represent a data item, but that data item could compose of other buffers / data items.





    sync_db_table_data(db_name, table_name) {


        // An inner observable function would make sense here.
        //  This can be sped up using a better key ranges sync single operation.
        //   The server will provide all of the data in all key ranges through one command.




        let res = new Evented_Class();
        let client = this.clients[this.map_client_indexes[db_name]];

        this.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                res.raise('error', err);
            } else {



                // 09/04/2018 - Want to introduce a system to sync only the latest records, and leave older records alone.
                //  Will apply only to tables that have a timestamp field in the key.

                // look at the model to see if any of the pk fields are timestamps
                //  by name or type.
                // Then if so (maybe) we can get all combinations of what can occur before the timestamp field.
                //  better just to do one variable right now.

                // Call them timestamped id keys.

                // Timestamped id could mean one thing at one time. The id says what it is, timestamp when it is.
                //  Timestamped fk id - that's more like it.
                //  So because its fk we can retrieve all of the ids for the key
                //   Then we use these ids, along with timestamp ranges to generate a whole bunch of key range values.
                //   Can use get first, last key in ranges.
                //    Can get counts.



                // Could have different syncing systems to handle large or small amounts of rows?

                // Could avoid decoding the records too.
                //  What about the index records?
                //  Looks like they would be handled separately in ll downloads.
                //  Before syncing, could carry out index verification on the target DB.

                // Before getting the table records, we need to sync the table structure




                // let's get the count for the records.

                //  [time] counting records... n so far


                // Could use different table syncing based on ranges?
                //  Having a private local DB to track syncing operations would be very useful.
                //  It would recognise which sync operations it has already done, and resume syncing in that case.
                //   Would also be good for table_structure_changes or db_structure_changes or structure_changes
                //    what got changed, storing info that makes it reversable.
                //     It's previous state, then the current state
                //     Or every operation recorded with enough detail to make it reversable.

                // Just need to get this data syncing soon.
                //  Want history as well as the very latest data being streamed.

                // Want to try connecting to, and syncing from all servers.
                //  Should be fine so long as the core structure is the same.

                // Need to pay close attention to handling markets that have been deleted.
                //  A 'deleted' field may be of use here.
                //  May need to do some higher level syncing when there is a table mismatch.
                //   Should be OK for markets, but less OK for currencies.
                //    As the PKs are sequential and must match.

                // Also, testing the import of time-range data through cross-referencing will be useful.

                // Import the data from the servers, run the db on the network, then re-sync from the client.
                //  For the moment, full retrieval on each of them seems OK.
                //   Tracking sync ops would be very useful indeed, would be quite a lot of work, requiring a separate local db.
                //   Re-writing already existing records makes the DB grow in size I think. Maybe not very much.


                // Table characteristics
                //  Is table relatively small?
                //   Indication that it's not a dyncamically added data collection.

                // Is structural
                //  Used for normalisation of other tables.

                // If the table has got fks pointing to it, then get the checksum of the table to compare.
                //  table_records_checksum
                //   table_records_hash
                //   Not so sure about directly hashing all the records. A hash chain could be used to verify they are the same.

                // table_records_hash
                //  Will hash the binary of all of the table records.

                // multi-peer sync will be the next stage
                //  will begin the syncing process with all of them at the same time.

                // Want it so that it can be started reasonably quickly, where it gets the latest data, but is not sure to get the full data from all servers.

                // table.split_key_ranges
                //  all_referenced_key_possibilities
                //   could calculate if it's a reasonable amount to start with.
                //   for where there is just one key reference...
                //   one primary key with a foreign key component and a timestamp.



                // Handling of tables by characteristics:
                //  Smaller table: get the hash of all records
                //  Larger table: go into sub-ranges
                //   do this if it has got a key that refers to another table, then has timestamps.
                //   timestamped reference to an id on another table
                //    there is no very concise way to put it.








                // Will 


                let model_table = this.model.map_tables[table_name];


                // lets specify a limit.

                //let obs_count = client.count_table_records(table_name, 50000);
                //let obs_count = client.count_table_records_up_to(table_name, 50000);


                client.count_table_records_up_to(table_name, 50000, (err, count) => {
                    if (err) {
                        res.raise('error', err);
                    } else {

                        //console.log('count', count);
                        let sync_all_records = () => {
                            // Could be nice to have the number of table records here.
                            //  Number of records could be returned by the server, but it's not.
                            //  Could be a bit tricky to change this.
                            //  Could more easily count the records provided in the put statement.

                            let obs_table_records = client.get_table_records(table_name, false);

                            // Not decoded....



                            // obs_table_records.pause(), obs_table_records.resume();
                            //  could fit that into the client and server with some lower level instructions.

                            // How far through the count of records is a good stat


                            let total_put = 0;
                            let prop_put, pct_put;

                            obs_table_records.on('next', data => {

                                // A page of records, all encoded as binary.


                                //console.log('obs_table_records data', data);
                                //console.log('obs_table_records data.length', data.length);


                                if (data.length > 1) {

                                    // Batch put table records.
                                    //  Will insert the table kp itself.



                                    // this.batch_put_table_records

                                    // try decoding the data.

                                    //console.log('data', data);


                                    //var row_buffers = Binary_Encoding.get_row_buffers(data);
                                    //let decoded = database_encoding.decode_model_rows(row_buffers);

                                    //console.log('decoded', decoded);
                                    //throw 'stop';





                                    this.batch_put(data, (err, put_count) => {

                                        // Want to say how many records were put in the result.
                                        //  Not sure we want other data?
                                        //  res.count

                                        if (err) {
                                            res.raise('error', err);
                                        } else {
                                            // the batch put result could say the number of records.
                                            //  That way we can work out the proportion complete.
                                            //console.log('put_count', put_count);
                                            total_put = total_put + put_count;
                                            prop_put = total_put / count;
                                            pct_put = (prop_put * 100).toFixed(1);

                                            //console.log('pct_put ' + pct_put + '%');

                                            let obj_res = {
                                                'db_name': db_name,
                                                'table_name': table_name,
                                                'prop_complete': prop_put,
                                                'pct_complete': pct_put
                                            }
                                            //console.log('have put record batch.');
                                            res.raise('next', obj_res);
                                        }
                                    });
                                }
                                // Want to put this data into the db.
                                // 
                            })
                            obs_table_records.on('complete', data => {
                                console.log('obs_table_records complete', data);

                                res.raise('complete');

                            });
                        }

                        // Could also have a sync_records_since(timestamp);
                        // All sorts of ways of doing this, but they require care.
                        //  

                        if (count > 32000) {
                            // check the model to see if the PKs refer to another table and then have / are a timestamp

                            let pk = model_table.pk;
                            //console.log('pk', pk);

                            if (pk.fields.length === 2) {
                                if (pk.fields[0].fk_to_table && pk.fields[1].name === 'timestamp') {
                                    console.log('FK ref then timestamp in PK. Able to split into ranges.');

                                    // Would also be nice to have a local_and_remote function wrapper that would do the same function on the local server and the connected client.

                                    // Once we have the local and remote subdivisions, we can compare them to see what ranges of data are missing on each of them.
                                    //  Then can get counts from the server about how many records are within each of those ranges.
                                    //   This is the kind of thing where streaming data will help with larger results sets.

                                    // This will make syncing of a variety of timestamped records that are added to sequentially much quicker.
                                    //  It also makes me think that automatically wrapping all put rows with a timestamp would be useful for syncing in general.
                                    //   Or another index.
                                    //    It will be fast to retrieve all rows added within a set time.
                                    //     [timestamp, row_key]
                                    //      that way we could quickly read through all row keys added or changed at a timestamp
                                    //       could record the old values with changes.
                                    //        It depends on if we want an immutable record or not, and how much data we are prepared to use for it.
                                    //   Timestamping puts and updates, and storing those timestamps would be very useful to sync rows which have changed since a given timestamp.
                                    //    Co-ordination of time zones becomes an issue accross machines.
                                    //   Timestamps should be in UTC universal format.
                                    //    It would be another local system table.
                                    //     It's not the data of the row itself, but the time that the record was added / updates
                                    //    Probably best to keep within the same DB.
                                    //     Brings up the question of having a strictly local part of that DB.
                                    //      However, creating a virtual DB inside a local space seems cool. Then shifting to an actual one would be easier.

                                    // Virtual DBs within table space would also help provide DBs to system users.
                                    //  Not doing it right now. Will lead to huge counts. Need better performance.

                                    // Will have the syncing going quite fast soon.
                                    //  Then will work on syncing between onlin servers.
                                    //   Want it so that when they start up, they get the data from the others.

                                    // Key-based full syncing will help too.
                                    //  Digest-based range comparison
                                    //  Count based range comparison

                                    // Getting the records in a range quicker over faster network?

                                    // Will want to start a new server (data9), have it sync from data7 and data8.
                                    //  Then start data10, syncing from 7, 8, 9
                                    //  Leave these on for a while.
                                    //   Be sure they handle disconnections

                                    // See about restoring data from previous dbs.
                                    // Use the live, new data.
                                    //  Get it into a data structure that quickly has the historic data, then updates with new data as it comes in.

                                    // 10s of MB of financial data should be useful for some things.

                                    //   Want something that has a good few 100MB or few GB of data loaded into a nice index in RAM, with some OO functionality to query it.
                                    //   Will be able to run event generators and listeners to monitor events that take place live in the data.
                                    //   Will be able to run scans to find previous / historic events.
                                    // this.get_local_and_remote_table_key_subdivisions

                                    //sync_by_subdivisions
                                    // a callback is unambiguous that it gets them all at once.

                                    // Then for every table key subdivision (range) we do comparisons
                                    //  Not sure exactly what we are looking for.

                                    // Probably best to do a 'get all records since',
                                    //  while also using the remote computer's upper level subdivision.

                                    // Ideally want the subdivisions split up like:
                                    //  key beginning, 2 values

                                    // Comparing them can see if there is the right overlap.
                                    //  Really most interested in when the latest of each is.

                                    // Get the values from the clients, within their subdivisions, between local-last and client-last.


                                    // get local and remote table key subdivisions
                                    //  do they come back in the same order?

                                    // Get the local subdivisions.
                                    //  Then, from the server get all records within those subdivisions (before or) after the data we have locally.

                                    // Translates to server get all within given id, with timestamp after x
                                    //  From subdivision beginning to the end.

                                    // Seems like we may need to do more work on requested and given return values.
                                    //  Or quickly parsing / selecting values we want out of the binary using more Binary_Encoding functionality.

                                    // Get both local and remote subdivisions.
                                    //  Nicest if they match with each other. Is a bit of a race condition that could get in the way of that...?


                                    // So for the local subdivisions, get every value in that subdivision after the 



                                    // Could have a subdivisions since end of time options.
                                    //  
                                    //  Better just to use local subdivisions right now?
                                    //   would need to match them with the remote ones.

                                    // So, local time until last possible time.
                                    //  Or ensure the order of subdivision retrieval?
                                    //   Could be tricky / require additional buffer.


                                    // this.get_local_and_remote_table_key_subdivisions(remote_db_name, table_id, (err, subdivisions_set))
                                    // this.local_and_remote('get_table_key_subdivisions', )

                                    // get_table_key_subdivision_sync_ranges()
                                    //  get the local ones.
                                    //   from these subdivisions
                                    //  get the remote ones
                                    //   up to these.

                                    // Then use these ranges to sync these key ranges.
                                    //  Then keep a server running that syncs from the online servers.
                                    //   This will then be programmed to return data in efficiennt typed array formats.
                                    //   Then we'll be able to look for short term changes and trade opportunities using them.

                                    // And a get subdivisions that will then split the results back to [key_beginning, [the_rest_1, the_rest_2]]
                                    //  Call this paired results.

                                    let obs_syncable_subdivisions = this.get_table_key_subdivision_sync_ranges(db_name, table_name);




                                    // May be good to sequence these calls, to do them one at a time.


                                    let arr_key_ranges = [];



                                    obs_syncable_subdivisions.on('next', data => {
                                        console.log('next data', data);

                                        let buf_beginning = data[0];
                                        let arr_local_key_parts = data[1][0];
                                        let arr_remote_key_parts = data[1][1];

                                        let last_local_key_part = arr_local_key_parts[1];
                                        let last_remote_key_part = arr_remote_key_parts[1];

                                        let buf_l = Buffer.concat([buf_beginning, last_local_key_part]);
                                        let buf_r = Buffer.concat([buf_beginning, last_remote_key_part]);

                                        console.log('buf_l', buf_l);
                                        console.log('buf_r', buf_r);

                                        //throw 'stop';

                                        // then copy the ranges over.

                                        console.log('subdivisions complete');

                                        arr_key_ranges.push([buf_l, buf_r]);


                                        /*
                                        let obs_copy = this.copy_key_range_to_local(db_name, buf_l, buf_r);
                                        obs_copy.on('next', data => {
                                            console.log('obs_copy data', data);
                                        });
                                        obs_copy.on('complete', () => {

                                        });
                                        */



                                        // sync_key_ranges


                                    });
                                    obs_syncable_subdivisions.on('complete', () => {
                                        console.log('arr_key_ranges', arr_key_ranges);
                                        // Get the key ranges counts.
                                        // sequentially call observable function calls.
                                        // fns.observe

                                        // copy_key_ranges_to_local



                                        // Server-side processing of key ranges will be of use.




                                        // count_records_in_ranges

                                        // Could get the overall count, then get the ranges.

                                        // Also, subscribing to the DB puts would better enable syncing.
                                        //  Want to get the latest records while downloading the older ones.

                                        // use a DB function to get them all




                                        // Use observable sequencing.
                                        //  or callbacks... but want the count as it's ongoing too.
                                        //   However many records so far. Could look nice in a GUI, showing progress.

                                        // just do them one at a time here.

                                        // A server-side multi key range send to local would help.
                                        //  Give it multiple ranges of keys, get it to download all of them at once.

                                        // GET_KEY_RANGES
                                        // LL_GET_RECORDS_BY_KEY_RANGES

                                        // Getting the count for all of them to the client would be best.
                                        //  Doing 8 or so in parallel would be cool.   

                                        let obs = this.copy_key_ranges_to_local(db_name, arr_key_ranges);
                                        obs.on('next', data => {
                                            console.log('copy_key_ranges_to_local data', data);
                                        });
                                        obs.on('complete', () => {
                                            console.log('copy_key_ranges_to_local obs complete');

                                        });


                                        let individually = () => {
                                            let processing_limit = 4;

                                            let c = 0,
                                                l = arr_key_ranges.length;

                                            let currently_processing = 0;
                                            let done = () => {

                                            }
                                            let process = () => {
                                                currently_processing++;
                                                // May be nice to get 2 or more of these running at once.
                                                if (c < l) {
                                                    let obs = this.copy_key_range_to_local(db_name, arr_key_ranges[c][0], arr_key_ranges[c][1]);
                                                    obs.on('next', data => {
                                                        console.log('process data', data);
                                                    });
                                                    obs.on('complete', () => {
                                                        currently_processing--;
                                                        console.log('obs complete');
                                                        if (currently_processing < processing_limit) {
                                                            process();
                                                        }
                                                    });
                                                    c++;
                                                } else {
                                                    done();
                                                }
                                            }
                                            process();
                                        }

                                        //throw 'stop';
                                    })

                                    /*

                                    console.log('pre get_table_key_subdivisions');
                                    this.get_table_key_subdivisions(model_table.id, (err, subdivisions) => {
                                        if (err) {
                                            res.raise('error', err);
                                        } else {
                                            console.log('subdivisions', JSON.stringify(subdivisions, null, 2));
                                            console.log('subdivisions', (subdivisions));
                                            // Probably want to keep the KPs when dealing with subdivisions.

                                            each(subdivisions, item => console.log('subdivisions item', item));
                                            console.log('subdivisions.length ' + subdivisions.length);


                                            // Get the remote table subdivisions.
                                            //  The part that is the same would be nice to have grouped together.


                                            client.get_table_key_subdivisions(model_table.id, (err, client_subdivisions) => {
                                                if (err) {
                                                    res.raise('error', err);
                                                } else {

                                                    console.log('client_subdivisions', client_subdivisions);
                                                    // These will be encoded as binary rather than keys.






                                                }
                                            });








                                        }

                                    })
                                    */



                                }
                            }

                            //throw 'stop';


                        } else {

                            // do a table digest comparison.

                            // want to be able to do functions comparing and syncing local and a variety of remote machines.
                            //  Could get the ranges to start with, then sync based on that. In the future, resuming would check progress of ongoing operations.




                            sync_all_records();

                        }




                        // Then if it's a large count, we could break down the values to retrieve.
                        //  A table record retrieval sync could be broken down into sub-sections that only get data we don't already have in range.
                        //  That becomes more difficult when there are overlapping records.
                        //   Want it soon so the servers share data with each other?
                        //    May well have sub 1s resolution now.

                        // In this case, want to do the full sync.
                        //  It may be possible to get big blocks of data back quicker, especially if the blocks have already been prepared.
                        //   Want to do more with just the main current architecture, but having a local system db will prove useful.
                        //    It will be especially useful for recording progress of syncing operations.








                    }
                });

                // So it's not returning the final result in the last page?
                //  With timed counts it definitely should do.


                /*

                obs_count.on('next', data => {
                    console.log('count data', data);
                })
                obs_count.on('complete', count => {
                    console.log('complete count', count);


                    






                });
                */






                // Decoding them this way.

                //let obs_table_records = client.get_table_records(table_name, true);





                // paged download of the table rows

                // just sync by getting all of the records for the moment.


                // Syncing by keys...


            }
        })

        return res;
    }



    // For the moment, will just copy over the table data.
    //  Think we can do things in a simpler way with some ll operations.

    // May need to change some table ids if necessary.
    //  make_local_compatable_with
    //  make_local_table_compatable_with
    //   that could then do some rearrangement if necessary.



    sync_db_non_core_tables_data(db_name) {
        let res = new Evented_Class();
        // Assuming the models match.

        // get the non-core table names from the model

        //let non_core_table_names = this.model.

        console.log('non_core_table_names', this.model.non_core_table_names);


        // Go through them in order, syncing the records.
        //  Bandwidth could be saved by checking with a checksum / hash.

        // but anyway, for each of these, in sequence, we will sync that table

        //let fns = Fns();

        // Do this using a callback?
        //  Get fns working with observable?

        // Sequence the observables?

        // fns but with an observable?
        //  Don't think that fns will work that way, doubt it can fit the API.

        // Observable sequencing seems best here.



        //each(this.model.non_core_table_names, table_name => fns.push([this, sync_db_table, [db_name, table_name]]));
        //fns.go((err, res_all) => {
        //    if (err) {
        //        res.raise('error', err);
        //    }
        //});

        let q_obs = [];

        // But this won't execute the observables in sequence.

        // Queue up the observables fn calls.
        each(this.model.non_core_table_names, table_name => {

            q_obs.push([this, this.sync_db_table_data, [db_name, table_name]]);
        });



        let execute_q_obs = (q_obs) => {
            let res = new Evented_Class();

            let c = 0;
            let process = () => {

                if (c < q_obs.length) {
                    let q_item = q_obs[c];

                    let obs_q_item = q_item[1].apply(q_item[0], q_item[2]);

                    obs_q_item.on('next', data => {
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': data
                        }
                        //console.log('e', e);
                        res.raise('next', e);
                    });
                    obs_q_item.on('error', error => {
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': error
                        }
                        res.raise('error', e);
                    });
                    obs_q_item.on('complete', data => {
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': data
                        }
                        console.log('pre raise item complete');
                        res.raise('item_complete', e);
                        c++;
                        process();
                    });

                } else {
                    // raise an all complete?
                    res.raise('complete');
                }

            }
            process();

            return res;
        }


        let obs_all = execute_q_obs(q_obs);

        obs_all.on('next', data => {

        });


        //each(this.model.non_core_table_names, table_name => {
        //    arr_obs.push(this.sync_db_table(db_name, table_name));
        //});







        return obs_all;

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
    let access_token = config.nextleveldb_access.root[0];
    console.log('access_token', access_token);

    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420,
        'access_token': access_token
    }

    console.log('options', options);

    //throw 'stop';

    var user_dir = os.homedir();
    console.log('OS User Directory:', user_dir);
    //var docs_dir =
    var path_dbs = user_dir + '/NextLevelDB/dbs';
    // Select all the listed dbs, then choose the selected source DBs.

    //let clients_info = [];
    let clients_info = {};

    console.log('config.source_dbs', config.source_dbs);
    //throw 'stop';
    console.log('config.nextleveldb_connections', config.nextleveldb_connections);
    // Using local as a source for some testing.

    each(config.source_dbs, (name) => {
        //console.log('config_source_db', config_source_db);
        let db_client_info = config.nextleveldb_connections[name];
        db_client_info.access_token = access_token;
        //console.log('db_client_info', db_client_info);
        //clients_info.push(db_client_info);
        clients_info[name] = db_client_info;
    });

    // Want it to connect to local clients
    console.log('config.source_dbs', config.source_dbs);
    console.log('clients_info', clients_info);
    // will tell it to connect to itself as a peer for testing.

    let test_against_localhost = () => {
        config.source_dbs = ['localhost'];
        clients_info = {
            'localhost': local_info
        }
    }

    //test_against_localhost();




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

                    let remote_server_name = 'data8';
                    //let remote_server_name = 'localhost';


                    ls.start((err, res_started) => {
                        if (err) {
                            console.trace();
                            throw err;
                        } else {
                            console.log('NextLevelDB_P2P_Server Started');







                            // Get the diff to handle mal-formed rows?
                            //  Or log such rows, and drop them.

                            ls.diff_local_and_remote_models(remote_server_name, (err, diff) => {
                                if (err) {
                                    throw err;
                                } else {
                                    if (diff.count === 0) {

                                        // Should be able to do the sync so far...
                                        console.log('no need to sync core');
                                        let model = diff.orig;




                                        ls.diff_local_and_remote_table(remote_server_name, 'bittrex currencies', (err, res_diff_currencies) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                //console.log('res_diff_currencies', JSON.stringify(res_diff_currencies));
                                                console.log('res_diff_currencies.added', JSON.stringify(res_diff_currencies.added));
                                                console.log('res_diff_currencies.deleted', JSON.stringify(res_diff_currencies.deleted));



                                                setTimeout(() => {
                                                    ls.diff_local_and_remote_table(remote_server_name, 'bittrex markets', (err, res_diff_markets) => {
                                                        if (err) {
                                                            throw err;
                                                        } else {
                                                            //console.log('res_diff_markets', res_diff_markets);

                                                            console.log('res_diff_markets.added', JSON.stringify(res_diff_markets.added));
                                                            console.log('res_diff_markets.deleted', JSON.stringify(res_diff_markets.deleted));

                                                            // Probably market never existed on the server anyway.
                                                            //  At least existing markets are the same

                                                            if (res_diff_markets.added.length === 0 && res_diff_markets.changed.length === 0) {

                                                                let obs = ls.sync_db_non_core_tables_data(remote_server_name);
                                                                obs.on('next', data => {
                                                                    //console.log('sync_db_non_core_tables_data obs data', data);
                                                                    let pct_complete = data.data.pct_complete;
                                                                    let table_name = data.data.table_name;
                                                                    console.log(table_name + ' ' + pct_complete + '% complete');
                                                                });
                                                                obs.on('complete', () => {
                                                                    console.log('sync_db_non_core_tables_data obs complete');
                                                                })
                                                            }

                                                            // So locally we have a larger set of records, I think.
                                                            //  Really do need to keep the deleted currency or market records locally.




                                                            // Found Bittrex has deleted a market.
                                                            //  May be worth attaching a 'deleted' annotation to it?
                                                            //   Deleted by external source, keeping track of it means we know it's been deleted and existed there to begin with.
                                                            //    Don't want to lost the price data either.

                                                            // So if any have been deleted either locally or remotely, we need to act accordingly.
                                                            //  A lower level 'record deleted' tag?
                                                            //   Don't really want to sync over the deletions.




                                                            //throw 'stop';


                                                            // sync_db_non_core_tables_data

                                                            // This info may not be quite so important once we have better syncing.
                                                            //  Table that logs all db operations carried out, separate to the main db could be quite useful.
                                                            //   There would be an id for each operation, and syncing clients could download all operations since x.
                                                            //    







                                                        }
                                                    })
                                                }, 0);

                                            }
                                        })






                                    } else {


                                        // No, not sure we want this.
                                        //  Don't just replace the core, that is haphazard.

                                        // May well start some new DB instances, possibly only instance 8 is working OK.
                                        //  Would like very much to retrieve data from existing DBs.
                                        //  May need to make the safer version of it fix various bugs on startup, such as incrementor values being out of sync with existing record ids.



                                        //throw 'stop';

                                        // Copies over the core

                                        /*
                                        ls.unsafe_sync_core(remote_server_name, (err, res) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('unsafe sync res', res);
                                            }
                                        })

                                        */

                                        console.log('diff', diff);

                                        console.log('diff.count', diff.count);
                                        console.log('diff.changed', diff.changed[0]);

                                        // If there is a difference, it could be a bug in one db.
                                    }

                                    // 


                                }
                            })


                        }
                    });
                }
            });
        }
    });
} else {
    //console.log('required as a module');
}