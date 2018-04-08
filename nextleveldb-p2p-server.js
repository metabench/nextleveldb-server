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

    get_remote_buf_core(remote_db_name, callback) {
        let client = this.get_client_by_db_name(remote_db_name);
        client.load_buf_core(callback);
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

    // 

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









    _start_db_sync(db_name) {



        let res = new Evented_Class();

        this.get_local_and_remote_models(db_name, (err, models) => {
            if (err) {
                throw err;
            } else {

                let [local, remote] = models;
                let diff = local.diff(remote);

                //console.log('diff', diff);

                //console.log('diff', JSON.stringify(diff, null, 2));

                console.log('diff.count', diff.count);

                // Could use these differences in the model to determine which tables will need to be synced.
                //  Syncing here will always be about requesting data.


                if (diff.count === 0) {
                    // Should be able to do the sync so far...
                    res.raise('next', 'Verified database models match');

                    // Then download all of the keys....
                    //  Could do this on a very low level.
                    //   Even the incrementors would be the same at this stage
                    //    Meaning the syncing would have to take place amongst the non-structural tables.
                    //     Such as snapshot records.

                    // find every table that is not core / system, and does not have any autoincrementing PKs?
                    // When syncing tables, will need to sync the tables ahead of them

                    let obs_sync_non_core_tables = this.sync_db_non_core_tables(db_name);
                } else {
                    // This looks like it will be the way to spin up a db instance and have it copy data from another instance automatically
                    //  and relatively quickly.

                    // Will not be that huge an algorithm.


                    // Need to find out for every table what its outward fk links are


                    each(diff.changed, change => {
                        //console.log('change', change);

                        let [before, after] = change;

                        let int_kp = before[0][0];
                        //console.log('int_kp', int_kp);

                        if (int_kp === 0) {
                            // Its an incrementor

                            let name = before[0][2];
                            console.log('incrementor ' + name + ' changed from ' + before[1] + ' to ' + after[1]);


                            if (name === 'incrementor') {
                                let vdiff = after[1] - before[1];
                                console.log(vdiff + ' new incrementors');
                            }

                            if (name === 'table') {
                                let vdiff = after[1] - before[1];
                                console.log(vdiff + ' new tables');
                            }



                        }

                        if (int_kp === 2) {
                            // Tables table record
                            //  Need to reflect / process a change to the table record.
                            //   Will need to handle the structure of tables changing, or their number of keys...
                            //   Currently working on creating new tables in the sync.

                            // Would be nice to have a function to download a table, and all tables it relies on.
                            //  Would be quite a useful function on the server-side that would sync a table.

                            // The normalised nature of the db makes syncing more difficult right now.
                            //  Need to approach it in stages.

                            // Very soon want it so that it downloads all data smoothly and relatively quickly.

                            //  Should probably keep the DB structures syncronised accross dbs.
                            //   ie the same IDs for anything which gets referred to accross the cluster.

                        }
                    });


                    let syncable_tables = [];
                    let ctu = true;

                    each(diff.added, item => {
                        console.log('item', item);
                        let int_kp = item[0][0];
                        if (int_kp === 2) {
                            let table_id = item[0][1];
                            let table_name = item[1][0];
                            console.log('Added table: ' + table_name + ' at id ' + table_id);
                            // Check there is not already a table at that ID.
                            //  
                            if (this.model.tables[table_id]) {
                                res.raise('error', new Error('Attempting to sync table ' + table_name + ' to id ' + table_id + ' but table ' + this.model.tables[table_id].name + ' is already there.'));
                                ctu = false;
                                // Stop running this
                            } else {
                                syncable_tables.push(table_name);
                            }
                        }
                    })

                    if (ctu) {
                        let map_tables_fk_refs = remote.map_tables_fk_refs;
                        console.log('map_tables_fk_refs', map_tables_fk_refs);
                        console.log('syncable_tables', syncable_tables);


                        // then sync the tables when ready.
                        //  Observe something to see when it's ready.

                        let ready_notifier = new Evented_Class();

                        let level_0_ref_tables = [];
                        let map_l0 = {};
                        each(syncable_tables, syncable_table => {
                            if (!map_tables_fk_refs[syncable_table]) {
                                level_0_ref_tables.push(syncable_table);
                                map_l0[syncable_table] = true;
                            }
                        });

                        console.log('level_0_ref_tables', level_0_ref_tables);

                        let q_table_sync = [];
                        let pending = [];

                        let sync_when_ready = function (table_name) {
                            if (map_l0[table_name]) {
                                q_table_sync.push(table_name);
                            } else {
                                // a pending table list.
                                pending.push(table_name);
                            }
                        }



                        each(syncable_tables, syncable_table => sync_when_ready(syncable_table));

                        console.log('q_table_sync', q_table_sync);





                        let process = () => {
                            // shift the first item from q_table_sync

                            let item = q_table_sync.shift();
                            console.log('process item', item);


                            if (item) {
                                let obs_sync = this.sync_db_table(db_name, item, remote);
                                obs_sync.on('complete', () => {
                                    console.log('obs_sync complete');




                                    process();
                                })
                                obs_sync.on('error', (err) => {
                                    console.log('obs_sync error');
                                    res.raise('error', err);



                                    //process();
                                })
                            } else {
                                process_complete();
                            }


                            // sync that table

                            // could do sync table records.
                            //  would need to update relevant incrementor and table field records too.
                            //   Even keeping the table field ids the same between syncs would be useful.
                            //   Means that a db that has got a different structure already (extra tables / fields) can't join that cluster.
                            //    The joining db could change itself so that the needed key positions are free.
                            //     Maybe that would even mean a process of pausing where it updates its own structure.
                            //      Would have to notify clients of this.













                        }

                        let process_complete = () => {
                            console.log('process_complete', process_complete);
                        }

                        process();


                        // start syncing these.

                        // 

                    }

                    // Then for each of the syncable tables, we sync it when it's ready to be synced. Precursor / structural / platform tables must have already been synced.

















                    // Looks like we will need to sync various structural tables.

                    // // Need to work out the syncing path.
                    //  Sequence of tables to sync.

                    // Sync structural tables which have got no fk references.

                    // Keep track of when tables are ready to sync.

                    // For each table, will will know which 





                }
            }
        });
        return res;
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

        // 

        return res;






    }


    // It looks like syncing won't be too difficult in this case.

    // Getting a cockroachdb up and running would be very useful too.
    //  Would be interesting to sync to and from that.





    sync_db_table_data(db_name, table_name) {

        let res = new Evented_Class();
        let client = this.clients[this.map_client_indexes[db_name]];

        this.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                res.raise('error', err);
            } else {

                // Could have different syncing systems to handle large or small amounts of rows?

                // Could avoid decoding the records too.
                //  What about the index records?
                //  Looks like they would be handled separately in ll downloads.
                //  Before syncing, could carry out index verification on the target DB.

                // Before getting the table records, we need to sync the table structure






                // Decoding them this way.

                //let obs_table_records = client.get_table_records(table_name, true);
                let obs_table_records = client.get_table_records(table_name, false);

                // Not decoded....



                // obs_table_records.pause(), obs_table_records.resume();
                //  could fit that into the client and server with some lower level instructions.

                obs_table_records.on('next', data => {
                    //console.log('obs_table_records data', data);
                    console.log('obs_table_records data.length', data.length);


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





                        this.batch_put(data, (err, res_put) => {
                            if (err) {
                                res.raise('error', err);
                            } else {
                                console.log('have put record batch.');
                                res.raise('next', 'Have put record batch.');
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


    __sync_db_table(db_name, table_name, remote_model) {
        let res = new Evented_Class();


        // Maybe don't give an update on all of the syncing. Could do it after pages loaded
        console.log('sync_db_table');

        console.log('db_name', db_name);
        console.log('table_name', table_name);


        let client = this.clients[this.map_client_indexes[db_name]];
        // 


        // Do any other tables use this table as an FK?
        //  Would be nice if the model held maps of what links to a table.
        //   Would be: whenever a table is added to / created within the Model, it checks to see which table(s) it refers to. Then adds that to inward_fk_refs array on that table.
        //    for the moment, an inward_fk_refs scan would be OK.
        //  If it has any inward fk refs, we don't want to update any values.
        //   Want to verify that the values are the same.
        //    Will raise an error if we find any differing values in such a table.
        //    Will copy over new values.

        let model_table = remote_model.map_tables[table_name];


        let do_table_data_sync = () => {
            this.get_table_id_by_name(table_name, (err, table_id) => {
                if (err) {
                    res.raise('error', err);
                } else {

                    // Could have different syncing systems to handle large or small amounts of rows?

                    // Could avoid decoding the records too.
                    //  What about the index records?
                    //  Looks like they would be handled separately in ll downloads.
                    //  Before syncing, could carry out index verification on the target DB.

                    // Before getting the table records, we need to sync the table structure





                    // Doing this without decoding / with minimal decoding would be fastest.
                    //  Could maybe send at 3* the speed.
                    //  Could also explore worker threads for decoding?



                    let obs_table_records = client.get_table_records(table_name, true);

                    // obs_table_records.pause(), obs_table_records.resume();
                    //  could fit that into the client and server with some lower level instructions.




                    obs_table_records.on('next', data => {
                        //console.log('obs_table_records data', data);
                        console.log('obs_table_records data.length', data.length);

                        if (data.length > 1) {

                            // Batch put table records.
                            //  Will insert the table kp itself.





                            this.batch_put_table_records(table_name, data, (err, res_put) => {
                                if (err) {
                                    res.raise('error', err);
                                } else {
                                    console.log('have put record batch.');
                                }
                            });



                        }




                        // Want to put this data into the db.

                        // 





                    })
                    obs_table_records.on('complete', data => {
                        console.log('obs_table_records complete', data);

                        res.raise('complete');

                    })









                    // paged download of the table rows

                    // just sync by getting all of the records for the moment.


                    // Syncing by keys...


                }
            })
        }

        if (model_table.inward_fk_refs.length === 0) {

            // Still need to sync the structure.

            let obs_sync = this.sync_db_table_structure(db_name, table_name, remote_model);


            obs_sync.on('complete', () => {
                do_table_data_sync();
            })

            //


            // Also want to avoid indexed tables for the moment.
            //  Better to verify the index on the remote table before copying it.
            //  However the safer db system will have its own index verification.










        } else {
            // Becomes trickier.
            //  Syncing will be about compare / verify / add.
            //   Fine to add new data (however not expecting to because of model comparison showing the incrementors are in the same state)
            console.log('has inward fk refs');

            // That's OK if the table with the FKs does not exist here yet.

            if (this.model.map_tables[table_name]) {

                // Should be fine.

                do_table_data_sync();

                //throw 'table already exists';
            } else {

                let obs_sync = this.sync_db_table_structure(db_name, table_name, remote_model);


                //throw 'stop';
                //do_table_data_sync();

            }


            // Get the data, and compare with what exists.
            // Checking the records individually could take a while.



            /*
            process.nextTick(() => {
                res.raise('complete');
            })
            */



        }


        // Check to see if the table exists in the local model.
        if (this.model.map_tables[table_name]) {


            console.log('model_table.inward_fk_refs', model_table.inward_fk_refs);





        } else {

        }


        // Probably need to load the remote model version.



        //console.log('model_table', model_table);

        //let inward_fk_refs = model_table.inward_fk_refs;

















        // get the table id, then download all of the records.

        // Table ids should match in both DBs, because the core models have been compared and found to be the same.

        return res;

    }

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
                        res.raise('next', e);
                    });
                    obs_q_item.on('error', error => {
                        let e = {
                            n: c,
                            params: q_item[2],
                            'data': data
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







        return res;

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

                            let obs_sync = ls.start_db_sync('data5');

                            obs_sync.on('next', message => {
                                console.log('message', message);
                            });
                            obs_sync.on('error', err => {
                                console.log('err', err);
                                throw err;
                            });
                            */





                            ls.diff_local_and_remote_models('data5', (err, diff) => {
                                if (err) {
                                    throw err;
                                } else {
                                    console.log('diff', diff);

                                    //console.log('diff', JSON.stringify(diff, null, 2));


                                    if (diff.count === 0) {
                                        // Should be able to do the sync so far...
                                        console.log('no need to sync core');

                                        // Copy over the table data.
                                        //  Sync all table data

                                        // sync all tables data
                                        //  sync all non-core tables.

                                        let model = diff.orig;


                                        // Could record syncing in a sync_progress table
                                        //  Harder to do now that we are not changing the core
                                        //  Expanding system tables will be useful in the future.

                                        // May make a change_table_id procedure, it would take quite a while.
                                        //  Best to do this on start I think, but if it changes the index then writes will be OK.
                                        //  Best to freeze the table ID until it is free.

                                        // sync non-core data

                                        //let model = Model_Database.load_buf_core(remote_buf_core);
                                        // then we go over the non-core tables
                                        //  sync the table data for each of them from the server.
                                        //   sync index data directly?
                                        //   probably better to re-index records.
                                        //    (maybe slower and more complex)


                                        //let fns = Fns();


                                        // Could either do the low level syncing with the table data (and maybe index data), or with just the table data, and recreate the indexes.
                                        //  Second option definitely seems better, as we avoid orphan indexes


                                        /*

                                        let fns = Fns();

                                        //ls.sync_download_table_records(model.non_core_tables)

                                        each(model.non_core_tables, table => {
                                            // sync that table.
                                            console.log('table.name', table.name);

                                            fns.push([this, this.sync_download_table_records, [table.name]]);


                                            // Then for all of them, we sync the table data.
                                            //  May try using rocksdb before all that long to see if the performance improves.




                                        });
                                        fns.go((err, res_all) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('all table syncing done');
                                            }
                                        })
                                        */

                                        let obs_sync = ls.sync_db_non_core_tables_data('data5');
                                        obs_sync.on('next', data => {
                                            console.log('* data');
                                        })








                                    } else {

                                        // Copies over the core
                                        ls.unsafe_sync_core('data5', (err, res) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('unsafe sync res', res);
                                            }
                                        })
                                    }

                                    // 


                                }
                            })



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