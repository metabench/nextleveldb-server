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


const Model = require('nextleveldb-model');

const BB_Record = Model.BB_Record;

const Index_Record_Key = Model.Index_Record_Key;
const database_encoding = Model.encoding;
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





// This could also enable some safety checking instructions from the client.
//  To begin with though, it will mainly do the safety checks on restart.

// This could have functionality to check when there are data outliers, or points that vary massively from the record before, with it likely to be contamination from another data series.


// Test data series for outliers.
//  That seems like a long-running process that could be done over the binary API.

// The safer version will be made and deployed to servers which have been running for a while, down to server 1.
//  Checking and fixing the incrementors.

// Could do some more tests from the client to detect discrepencies.
//  Download table subset, and detect value changes between items.
//   The distribution of value % changes between items would be useful too.


// Server could also send incrementor updates to all clients.
//  Need some way of avoiding multiple clients from overwriting each other on incrementors
//   Could do this with write protected records.
//   Could also do this by getting the incrementation done on the server side.

// We'll get this data mismatch / contamination problem sorted out.
//  Let's look at the tables of currencies and markets on the various servers.
// Data1 has been running over 1 month, with 0 server restarts and 15 collector restarts.

// Want to make sure the other machines have got their data properly too.
//  Data validation / verification before data import.


// Get one server (local for the moment) to get updates from one of the remote servers.


// Generating graphs would be a good next stage on the server.
//  Rendering data as time-series data.

// Find repeated rows where values should be unique



// Could check for repeated or orphan rows.
//  Seems very likely that some of the data, specifically data2 and data3, have been corrupted.
//  Not sure about data1. It looks like it is worthwhile to put a new server instance onto it soon.

// Maybe be sure to get data copying / syncing / amalgamation going from 2 servers to a local server soon.
//  Worth leaving server1 just in case it's coping OK.
//  Possibly some values have been written very wrong, and we may need to cross-reference to see what the correct values are going back.

// Data4 and data5 are now going.
//  We could copy their data to the local machine, and / or a server running on the Xeon.
//  Seems important to be able to reconnect to a dropped connection, with a connection being dropped for a few seconds / minutes.



// Downloading / copying to local seems most important.
//  Checking table integrity (same structure) before copying is the right way to do it for now.

// Check that it has got the same core db.


// The p2p db will do that as it connects.

// For the moment, will get it to confirm that it's got the same table for a named table.
//  For snapshot data sync, referenced tables must be synced too.

// copy_remote_table_to_local


// compare_remote_table_to_local







































class NextLevelDB_Safer_Server extends NextLevelDB_Server {
    constructor(spec) {
        super(spec);
        // use some servers as full sources.
    }

    check_autoincrementing_table_pk(table_name, callback) {

        console.log('check_autoincrementing_table_pk table_name', table_name);
        this.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                //console.log('table_id', table_id);
                //throw 'stop';

                this.get_last_key_in_table(table_id, (err, last_key) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('last_key', last_key);
                        if (typeof last_key === 'undefined') {
                            // Incrementor should be 0.
                            callback(null, true);

                        } else {
                            // Then look up the incrementor value in the model.
                            let table = this.model.tables[table_id];
                            let inc_value = table.pk_incrementor.value;
                            console.log('inc_value', inc_value);
                            console.log('last_key', last_key);

                            if (inc_value !== last_key[1] + 1) {
                                // need to update it in the DB

                                table.pk_incrementor.value = last_key[1] + 1;
                                let buf_inc = table.pk_incrementor.get_record_bin();

                                //console.log('buf_inc', buf_inc);

                                this.db.put(buf_inc[0], buf_inc[1], (err, res_put) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        console.log('FIXED - Updated incrementor ' + table.pk_incrementor.name + ' from ' + inc_value + ' to ' + table.pk_incrementor.value);
                                        callback(null, true);
                                    }
                                });

                            } else {
                                callback(null, true);
                            }
                        }
                    }
                })
            }
        })
    }

    // check_record_to_index_validity

    // Iterates through every record in the DB.
    //  creates the record object using the Model.
    //  checks that the index record keys are there.
    //   if they are not, puts them in place.
    //   want it to log / send to its observer (after more coding) that it has found a record that has not been indexed properly.
    //    then it will index that record.

    // Looks like before all that long it will be possible to draw a line under the safety features.
    //  Then hopefully we can get the data connected objects back.
    //  Will be nice to get a live ethereum price object.
    //   One web server could give access to them as shared resources for different components to use.
    //   Then client-side components would use client-side resources.


    // This historic and live data object could run on a phone or tablet within a web page.
    //  View objects would then display the live data.
    //   Initial rendering still (most likely) server-side.


    // check_table_records first

    // would go through all records checking for malformed records.


    check_record_to_index_validity(callback) {

        // Should have done check on records first.
        //  Invalid records deleted makes sense.

        console.log('check_record_to_index_validity');

        let model = this.model;
        // go through all table records.

        // do we have invalid table records here?

        // this.get_all_table_records_where_tables_are_indexed // get_all_table_records_where_tables_are_indexed get_all_table_records

        let obs_all_table_records = this.get_all_table_records_where_tables_are_indexed();

        obs_all_table_records.on('next', data => {
            console.log('obs_all_table_records data', data);
            obs_all_table_records.pause();
            // see about constructing the index from these

            //  A rapid server-side way of assembling indexes would be nice.
            //   Looking up fields by number from the record data
            //    Reading the key / knowing how many items in the key
            //     Reading from the records, getting the individual encoded buffers.
            //     Then comparing these to what is in the records.
            //      Decoding will be easier for the moment.
            // Using an OO record would definitely help, could use it for index lookups
            let kv = new BB_Record(data);
            // bpair = buffer pair
            // kvp = key value pair
            console.log('kv.bpair', kv.bpair);
            console.log('kv', kv);
            // then from the record we should be able to get the kp, and therefore the table_id
            console.log('kv.kp', kv.kp);
            console.log('kv.table_id', kv.table_id);
            let table = model.map_tables_by_id[kv.table_id];
            console.log('table.name', table.name);
            // then create the index records for that record.
            //  


            // could we use a Record_List instead?

            let bbris = table.get_record_bb_index_records(kv);

            // array of such records, not a buffered record-list
            console.log('bbris', bbris);

            each(bbris, bbri => {
                console.log('bbri', bbri);
                console.log('Object.keys(bbri)', Object.keys(bbri));

                console.log('bbri.key.decoded', bbri.key.decoded);
            })











            // table_id

            // Then want to get the index records from it.






            setTimeout(() => {
                obs_all_table_records.resume();
            }, 0);

        })
        //console.trace();
        //throw 'stop';
    }







    check_index_to_record_validity(callback) {
        // Looks like there need to be 2 functions called.




        // Seems like a significant extra function to write, in order to check that all the indexes are OK.
        //  It's possible that there are some records which do not have their index records in place properly.
        //  In that case, we need to create and put the relevant index records.


        // Looking from the indexes to the values, checking it matches.
        // Looking from all the values, checking that the records are indexes.







        let obs_all_indexes = this.get_all_index_records();

        // Can we pause and unpause this observable?
        //  Unpause it whenever we work out a result and want the next.

        obs_all_indexes.on('next', data => {
            //console.log('');
            //console.log('data', data);

            // The data could be full records, not just the index keys.
            //  Seems fine so far.


            // Some indexes are malformed...
            //  When doing the safety check, need to be able to spot malformed index records...?
            //  If we can't decode the index, we should delete it.






            let irk = new Index_Record_Key(data);

            // irk.validate();
            //  will check the spacing, following an alg like decode, but without actual decoding.
            //  in future, improve validation functions.
            //  for the moment, validate will try catch attempt decoding.

            // Seems we have dealt with malformed table records as well as index records.

            if (irk.validate()) {


                let decoded = irk.decoded;


                //console.log('irk.decoded', irk.decoded);

                // be able to get other data from the Index_Record_Key
                //  index pk, table pk, table id
                //  index id
                //  index fields
                //   

                //console.log('irk.table_id', irk.table_id);
                //console.log('irk.index_id', irk.index_id);

                //console.log('irk.fields', irk.fields);

                // numker of pk fields?



                // fields

                // Then want to be able to get specific fields, by id, from the index key.

                // Each field gets encoded with its type.
                //  Binary_Encoding will have some selection and skipping capabilities.

                // We will also need to connect index fields with value fields.

                // then to check the index against the records.

                // for that table, should be able to refer to the model to look at what the indexes should be.
                //  and what the fields should be.

                let table = this.model.map_tables_by_id[irk.table_id];
                //console.log('table.field_names', table.field_names);

                let field_names = table.field_names;
                let num_pk_fields = table.pk.fields.length;

                //console.log('num_pk_fields', num_pk_fields);



                // and look into the table indexes.

                // generator functions would be better for program flow here.
                //  not sure exactly how they would work though.
                //  could have a generator that buffers results from a pausable observable.
                //  could try reading 1 row per second even.


                // could sequence getting the records / doing the index lookup.


                //console.log('table.indexes.length', table.indexes.length);

                // oh.. some tables have 2 indexes to check.
                //  It's OK. They both come back at the same time.
                //  No big problem.



                each(table.indexes, (index, idx_index) => {

                    if (irk.index_id === idx_index) {


                        let def = index.to_arr_record_def();



                        //console.log('index.id', index.id);
                        //console.log('def', def);

                        //console.log('index.key_fields.length', index.key_fields.length);


                        //console.log('index.value_fields.length', index.value_fields.length);

                        // kv_field_ids
                        //console.log('index.kv_field_ids', index.kv_field_ids);


                        // slice out the key fields in the index kv_field_ids
                        let kv_key_part = index.kv_field_ids[0];
                        //console.log('kv_key_part', kv_key_part);

                        // after part 2, is the real key part
                        let real_key_part = kv_key_part.slice(2);
                        //console.log('real_key_part', real_key_part);
                        let key_field_name_0 = field_names[real_key_part[0]];
                        //console.log('key_field_name_0', key_field_name_0);




                        // key field ids contains a bit more than just that, and other parts of the system rely on it.
                        let key_field_ids_with_table_id_and_index_id = index.kv_field_ids[0];


                        // [ [ 6, 0, 3 ], [ 0, 1 ] ]
                        //  table id 6, index id 0, indexed field 3, points to key (with field ids) [0, 1]

                        // just a single field in the index.
                        //  maybe should look for ways to get more field IDs.


                        let indexed_field_id = index.kv_field_ids[0][2];
                        //console.log('indexed_field_id', indexed_field_id);
                        let indexed_field_name = field_names[indexed_field_id];

                        //console.log('indexed_field_name', indexed_field_name);


                        let indexed_ref_key = index.kv_field_ids[1];
                        //console.log('indexed_ref_key', indexed_ref_key);
                        // Would likely refer to the primary key.

                        // Then we can get the pk value / values out of the index indexed_ref_key
                        let found_key;
                        if (indexed_ref_key.length === 1) {
                            let single_pk_field_id = indexed_ref_key[0];
                            //console.log('single_pk_field_id', single_pk_field_id);

                            // Then find the position of that field within the index.

                            // Would be nice to have functionality to get a field by its normal id, but from within an index.
                            // 

                            // Get the last out of irk.decoded

                            found_key = decoded[decoded.length - 1];
                            //console.log('found_key', found_key);




                        } else {

                            // Multiple fields in a primary key.
                            //console.log('indexed_ref_key.length', indexed_ref_key.length);

                            found_key = decoded.slice(decoded.length - indexed_ref_key.length);
                            //console.log('found_key', found_key);


                            //throw 'NYI';
                        }






                        // then we need to refer to one or more fields.


                        // Pause and resume is working fine here.

                        // pause even to return the resume function?

                        obs_all_indexes.pause();

                        // Before its resumed, do the lookup of the record referred to in the index.
                        //  Then could even generate a new index record from the looked up one.
                        //   Then compare against the index.
                        //  Should be able to detect mismatches, it indexes that refer to the wrong record.

                        // look up the record by key

                        //console.log('pre this.get_table_record_by_key found_key', found_key);
                        this.get_table_record_by_key(irk.table_id, found_key, (err, found_record) => {
                            if (err) {
                                callback(err);
                            } else {

                                //console.log('\n*found_record', found_record);

                                // 


                                if (found_record) {
                                    // Decode the record, check it against 
                                    let decoded = database_encoding.decode_model_row(found_record);
                                    //console.log('decoded', decoded);

                                    decoded[0].shift();
                                    //console.log('2) decoded', decoded);
                                    let r = table.new_record(decoded);
                                    //console.log('r', r);

                                    let record_index_records = r.get_arr_index_records();
                                    //console.log('record_index_records', record_index_records);
                                    //console.log('idx_index', idx_index);
                                    let rsri = record_index_records[idx_index];

                                    //console.log('rsri', rsri);
                                    //console.log('irk.decoded', irk.decoded);

                                    let matches = rsri + '' == irk.decoded + '';
                                    //console.log('matches', matches);
                                    // Then if it does not match, delete the index by key.

                                    if (matches) {
                                        setTimeout(() => {
                                            //console.log('\n\n\n');
                                            obs_all_indexes.resume();
                                        }, 0);
                                    } else {


                                        console.log('index key not valid, record has different data, deleting index', irk.decoded);

                                        // Should probably put this aside, or even delete it here.
                                        //  Doing this sequentially, record by record, helps to delete it here.

                                        //console.log('pre delete');

                                        this.delete_by_key(irk.buffer, (err, res_delete) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                //console.log('delete complete res_delete', res_delete);
                                                setTimeout(() => {
                                                    // If the index record does not match the record's generated index, delete it.
                                                    //console.log('\n\n\n');
                                                    obs_all_indexes.resume();
                                                }, 0);
                                            }
                                        })
                                    }



                                    // check the found record against what the index expects.

                                    // could even create a new Record object, using the Model.

                                    //table.





                                } else {
                                    console.log('record not found, deleting index', irk.decoded);
                                    //console.log('----------------');
                                    //console.log('\n\n\n');

                                    // then delete the index record.

                                    // can delete by key.

                                    //throw 'stop';
                                    this.delete_by_key(irk.buffer, (err, res_delete) => {
                                        if (err) {
                                            throw err;
                                        } else {
                                            //console.log('delete complete res_delete', res_delete);
                                            setTimeout(() => {
                                                // If the index record does not match the record's generated index, delete it.
                                                //console.log('\n\n\n');
                                                obs_all_indexes.resume();
                                            }, 0);
                                        }
                                    });







                                    // Will need to delete the index record.

                                }

                            }



                        })


                    } else {



                    }
                    // 

                    // meaning we look up the value, using the key, then we check field 3 of that value to see that it matches the indexed value.

                    // could maybe have convenience function in table?

                    // need to be able to do an index lookup - should not be so difficult.
                })
            } else {
                console.log('index key not valid, deleting index', irk.buffer);

                // Should probably put this aside, or even delete it here.
                //  Doing this sequentially, record by record, helps to delete it here.
                //console.log('pre delete');

                this.delete_by_key(irk.buffer, (err, res_delete) => {
                    if (err) {
                        throw err;
                    } else {
                        //console.log('delete complete res_delete', res_delete);

                        setTimeout(() => {

                            // 

                            obs_all_indexes.resume();
                        }, 0);
                    }
                })
                //throw 'stop';
            }
            //throw 'stop';
        });
        obs_all_indexes.on('complete', () => {
            console.log('obs_all_indexes complete');
            callback(null, true);
        })
    }

    safety_check_indexes(callback) {


        // check_index_to_record_validity
        // check_record_to_index_validity

        this.check_index_to_record_validity((err, res) => {
            if (err) {
                callback(err);
            } else {
                this.check_record_to_index_validity((err, res) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, true);
                    }
                })
            }
        })
    }


    safety_check_autoincrementing_pk_tables(callback) {
        let autoincrementing_pk_tables = [];
        each(this.model.tables, table => {
            if (table.pk_incrementor) {
                autoincrementing_pk_tables.push(table);
            }
            //if (table.)
        })

        console.log('autoincrementing_pk_tables.length', autoincrementing_pk_tables.length);
        let fns = Fns();
        each(autoincrementing_pk_tables, table => {
            fns.push([this, this.check_autoincrementing_table_pk, [table.name]])
        });
        fns.go((err, res_all) => {
            if (err) {
                callback(err);
            } else {
                callback(null, res_all);
            }
        });
    }

    // this.log seems like the best way to record the invalid table records.
    //  A log to disk function looks like the best way.

    // this.log_to_disk(data)
    //  daily log?
    //   opens a new log file when the server starts?
    //  A new logfile for each operation that logs it would be cool.

    // this.open_file_log_write(operation_name)
    //  callback or promise.




    //  puts in a timestamp too
    //   May log binary records, if so, will do so in hex.
    //   Will need to store encoded data, but should store the data itself in a very easy to understand way.






    get_all_invalid_table_records() {

        // Could be done through observable_filter, when it exists.

        //let res_problem_records = [];
        let obs_all_table_records = this.get_all_table_records();


        let res = new Evented_Class();

        // get_all_encoding_error_table_records



        // load them all to bbrecord objects, and then call its validate_format function
        //  Have a whole load or records been written wrong?
        //   Types not given within the encoding?


        obs_all_table_records.on('next', data => {
            //console.log('data', data);
            let bbr = new BB_Record(data);
            //console.log('bbr.validate_encoding()', bbr.validate_encoding());
            //console.log('bbr.decoded', bbr.decoded);


            // but how does the validation fail?

            if (!bbr.validate_encoding()) {
                //console.log('problem record', bbr);
                res.raise('next', bbr);
            }
        });

        obs_all_table_records.on('complete', () => {
            //callback(null, res_problem_records);
            res.raise('complete');
        });

        obs_all_table_records.on('error', err => res.raise('error', err));

        res.pause = obs_all_table_records.pause;
        res.resume = obs_all_table_records.resume;
        res.stop = obs_all_table_records.stop;

        return res;
    }

    // log_all_invalid_table_records

    log_all_invalid_table_records() {
        let obs = this.get_all_invalid_table_records();
        obs.on('next', data => {
            //console.log('get_all_invalid_table_records data', data);
            console.log('get_all_invalid_table_records data.kvp_bufs', data.kvp_bufs);

        });
    }


    //  save it in the db path 

    // A specific separate DB may make sense, or a system part of the DB that stores the invalid records.

    // Put the invalid records fully into their own encoded buffers, then the can be stored within valid records.
    //  Could these records refer to invlid index keys elsewhere?



    // It definitely seems worth to log these to another file / sub-db.
    //  Making a decent logging system, or error containment zone makes sense.
    //  Keeping it in the same DB makes difficulties with much larger system or other parts.
    //   Making another leveldb dir for the errored rows makes sense.
    //   That brings up a general sub-dbs question. Maybe they would be useful for some things, like completed blocks of records (that would have very slow random access times, but are compressed and many records in one file)
    //   Also, sub-dbs would be possible within the key prefix, same db
    //   These would be separate to any data in the cluster, not shared.


    //  Would just be one table.
    //   Could have a database with just one table.
    //   Adding /moving all of the malformed records to another db for later reading would definitely help.
    //   Single separate databases would be simpler themselves, less to go wrong in them.
    // .private_storage('malformed records').put()
    // Does make the architecture more complex.
    // For the moment, just putting removed invalid records into a text log will be fine.
    //  Will also help to log records that get moved / removed.
    //   Could analyse invalid records, because some invalid records will point to other ones. Perhaps the key can't be normally decoded, but it still does point to the data in a record with an invalid key.

    // definitely worth logging all invalid records to file, leaving it at that right now.











    // But we may be able to identify invalid keys, and then be able to return them at a later point.
    //  Even move them to a 'recovery' part of the DB, system table even.

    // May need to think about separate local DBs.
    //  Or separate parts within that one DB.

    // A 'recovered records' system table would bq quite useful.
    //  However, don't have more space for system tables right now.

    // Getting closer to successful row remapping now though.
    //  when we make some kinds of changes, eg deleting invalid records / index records, they could give us a clue about where corrupt records have gone.

    // A quarantine / containment zone for invalid records would be useful.


    // Secondary / subdbs would definitely be useful.
    // Subdbs would help with client session management
    //  Syncing to servers
    //  Knowing where ranges of synced records have been stored.

    // Defining syncing / placement of records in the network by range makes a lot of sense.
    //  Separate DB parts may need more work to test.
    //  Would also be worth reserving some more space for system tables.

    // Logging to a file of invalid records would be OK.
    //  Could have a Logfile system.
    //  Could even be records, with timestamps.

    // A containment field for invalid records would make sense.
    //  Deleting them for the moment would be OK? Or just would not later recover them from backup files.

    // Containing them would make sense as then we can identify which of them match indexes or records that were changed.
    //  But that does add more complexity to the project right now.
    //   Just deleting them for the moment would work, can later look into them.

    // A decoded log of deleted records could be useful.


    // maybe fix them too.
    check_records_validity(callback) {


        let obs_log = this.log_all_invalid_table_records();


        // What about loads of problem records?

        // Deleting all invalid records would make sense.
        //  Could later identify gaps in the valid data.

        // Could have a system to download / sync only the valid records.
        //  Could validate on server-side.

        // Starting data9 and data10 would be useful too.

        // Then data11 and data12 would be the new version running on more exchanges.

        // Deleting invalid records would make sense.

        // This function would better be an observable that returns the invalid table records

        // this.get_all_table_records_with_encoding_errors

        //  then can delete these by key.


        // Continuing to do analysis just with Bittrex records will be fine for the moment.
        //  Want to get and explore complete data sets.









        //let res_problem_records = [];

    }

    safety_check_table_records(callback) {


        this.check_records_validity(callback);
    }

    safety_check(fix_errors = true, callback) {


        let fns = Fns();

        fns.push([this, this.safety_check_table_records, []]);


        //fns.push([this, this.safety_check_autoincrementing_pk_tables, []]);

        // safety check records.
        //  find the malformed records, delete them.
        //   could log this in a file.

        // safety_check_table_records
        //  checks the records are valid. Any records which are not valid get reported.


        // get_all_invalid_table_records





        //fns.push([this, this.safety_check_indexes, []]);
        fns.go((err, res_all) => {
            if (err) {
                callback(err);
            } else {
                console.log('fns all cb');
                callback(null, res_all);
            }
        })



        // Check all tables which have got autoincrementing keys



        // Then with these tables, find the last key in those tables.

        // May as well fix all these (right now)

        /*

        

        */




        // for each record, check that it is indexed properly.
        //  Need to retrieve the record itself by key.





        // 


        // Sorting out the index records right now seems like the most important task.
        //  Erroneous index records can make it look like data is there when it is not, and mean that the wrong data gets loaded.

        // lower level all_index_records generator seems like a good way of going about things.
        //  Could be another day's work to get this in operation successfully.

        // Then worth seeing about saving full trading data.
        //  Could possibly send the trades few at a time.
        //  Would be nice to have the server doing real-time trade event processing.

        // Clustering on a lower-level would be better in the future.
        //  Could have a few operations in a client that are the low level operations, and they work on the cluster.
        //  Then the higher level operations use these lower level ones.

        // Basically, need to soon draw a line under coding the database itself, and make good use of the data that it provides.
        //  However, weeks more coding on this would result in a fairly well-rounded database application.

        // Having a server be able to look at a record list, and then redirect records to the appropriate server would be powerful.
        //  Client could do that too.
        //   That looks like functionality should be in the model to split up key / record lists according to which server they should go to.
        //    Index records would always go alongside the records they refer to.
        //    Hashes / checksums / digests would be easier because the records / data items themselves always have the buffer backing them.

        // May even be a few more months of work, but we can get reliability by writing to separate data collector instances and amalgamating the data from them much sooner than that.
        //  Is not actually an infinite number of problems to solve. A few more things until we have the data being collected and amalgamated.

        // Then should not be too hard to generate a bunch of indicators / trading signals.



        // 30/04/2018
        //  Fixing the indexes is the main problem I now face.
        //   While there are other issues, such as malformed rows, the indexes prevent accurate lookups.
        //   Indexes can be regenerated too.
        //   Iterate / generate iterate all indexes.

        // Now / soon should be possble to download data serieses.

        // Generators / iterators def look useful in doing what's needed.

        // Seems like a few more days of hammering away at this problem will get it solved.
        //  Would not take long to write code to download a (full) data series, and put it into a 1s resolution high performance typed array structure.
        //  Maybe just the last prices in the typed array.
        //   Then could make moving average typed arrays to go alongside it.

        // Though not efficient, could repeat this moving typed array generation / analysis to see if / when there are crossing points.
        //  Moving average crossover being one effective measure.

        // Signals going back a few weeks seem most appropriate to look into.

        // Just really need to get the data serieses in.
        //  Correct some indexes / structures where possible (7, 8)

        // Save previous data.
        //  Want to get nice views of the data showing ranges in (small) graphs.

        // Very soon, want to use this to power trading.
        //  Setting moving stop losses and such. Could catch it falling 0.1% even.

        // Sort out the indxing, then the malformed rows, then get the data.

        // To do soon:
        // Lower resolution candlestick data (any exchange)
        // Snapshot data (any exchange)
        // Full market data (any exchange)

        // Daily block backups
        //  Checksummed, could make a simple blockchain.
        //  For the moment, need to press on with the DB. Have come so far already, have plenty of valid data to fish out.

        // Index consistency checking.
        //  Could maybe be done by reading specific items out of records.

        // record.








































        // A table record generator / iterator would be quite useful.
        //  Especially when we may need to carry out async functions with the records.

        // Soon want to be able to get the working / valid data from it.
        //  May try some kinds of sync developed to get and test data sets.
        //   Have lots of records in the system now, and definitly want to be able to get them back.

        // Applying a full fix on data8 makes sense, data7 too.
        //  Could see about the other ones too.
        //  Data1 seems out of action for some reason.

        // Maybe we don't have data going quite so far back.
        //  May need to write off some of the data gathered, but still need to get the data from data8.



        // Reliably saving into CockroachDB does seem like a good course of action from now.
        //  Maybe it could complement NextLevelDB.



        // Maybe get data from data8 and data7, and then work to ensure and monitor consistency of ongoing data.
        //  Can't spend too long trying to salvage some old data, need to write it off as its restoration seems too complex, and it's likely incomplete data anyway.
        //   Unless the different servers have their data all tracing back far enough.
        // Best to download the old data, see about local read-through type restore.
        //  May need to apply logic to each record.

        // For the moment, best plan is to get the cluster up and running on a series of machines, say data2 to data5.
        //  Have them all downloading and adding data. See about amalgamating data from all of them.
        //  

        // However, still does not allow for proper clustering / sharding / accessing them all as one.
        //  That would require quite a lot more code.

        // For the moment, getting the data from remote machines onto local makes the most sense.
        //  Then should be able to read through the data, giving the last price every second, quite quickly.
        //   Could get the key just at (lte) than the given timestamp, do that for a series of times.
        //    Would not be very fast to query, but will be OK.

        // Checking and fixing invalid index records on startup would be nice.

        // Some work with generators / iterators could help process index records while doing async operations to validate them.



        // find_orphan_index_records
        //  does not point to a record, or points to the wrong record.

        // find_records_missing_indexes

























        // Check the indexes on all tables
        //  (all tables that have index records)

        // 




        // then for each of them, do check_autoincrementing_table_pk
        //  Then make sure we have got the data gathering running OK, getting the data to local.
        //   Gather from more sources too.

        // Currency data, other exchanges, stock exchanges
        //  Make data viewing UI.





        // Then check that the highest pk value is the incrementor value - 1.
        //  If it's not, then fixing the error will be to update the incrementor value.


        // Then for each of these, check the 



        // An efficient get_last_record_in_table will be useful.
        //  Will read backwards within a range, only one record.

        // Worth getting this current and improved version running on data4 and data5.
        //  It may be a while before wrinkles are fully ironed out so that the client-side or workstation db can quickly download all its data from the server.
        //  Past errors could have led to some data corruption. It's probably worth testing for this data corruption on streams of data as they come in.

        // Raises questions about the quality of data that is now stored in some of the DBs.
        //  Could have data integrity errors when data refers to the wrong currencies.

        // Would be possible to visually inspect data, or to detect volatility outliers and notice them as corrupted data streams.
        //  Much of the data is likely to be good though, could have corrupted lower index value currencies.


        // Getting a fully working implementation up, with backups, and then attempting to restore the data from data1, data2, data3.

        // Want to have the data storage program running and capable for a long time.
        //  Data outlier detection would be useful.
        //  Number of records in a table, or subdivision by key within that table, that have their value field that differs from the previous (that was a few s ago) by more than 5 or n %.
        //   Could be useful for detecting corrupted key space.
        //   Or when importing data, check for such outliers.

        // It may be, though, that the data is fairly corrupted now, and it would be a somewhat slower process to import it while cleaning it.
        //  Will want to show a variety of different graphs, and the anomolies would appear there.
















    }

    // check, fix, maintain (check and fix)



    // core check
    //  malformed records in the core
    //  incrementors wrong
    //  

    // comprehensive check
    //  check to see all records are encoded properly
    //  check for any index records that don't refer to data
    //  check for data records that should be indexed (fully with all indexes) but are not
    //   create / suggest the index records.
    //  When a problem is found, that part could make a suggested fix.



    // Data recovery will attempt to download various data serieses.
    //  Apply some analysis to see that it's corect enough.
    //   Put that data in place in a working DB.

    // Should look at doing some data copy operations on a higher level.
    //  Not assuming various IDs are consistent between databases.

    // Want to get the syncing working tightly, but need to come up with some ways to fix it should there be a problem.
    //  Need to get the core operations and API very reliable.

    // Spotting the missing currency codes will be useful to do elsewhere, this is more base level db reliability and recovery features.








    start(callback) {
        super.start((err, res) => {
            if (err) {
                callback(err);
            } else {
                console.log('cb super NextLevelDB_Safer_Server start');

                let fix_errors = true;


                // Could have diagnose, fix, diagnose_fix

                //  May also make client-side remote diagnosis and fixing.
                //   Server-side on start-up seems a bit safer and easier too.



                this.safety_check(fix_errors, callback);

                //callback(null, true);

                // connect to the source DBs
            }

        })
    }





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

                    var ls = new NextLevelDB_Safer_Server({
                        'db_path': db_path,
                        'port': port,
                        'access_token': access_token
                    });



                    // There could be a web admin interface too.

                    ls.start((err, res_started) => {
                        if (err) {
                            console.trace();
                            throw err;
                        } else {
                            console.log('NextLevelDB_Safer_Server Started');


                        }
                    });
                }
            });
        }
    });
} else {
    //console.log('required as a module');
}

module.exports = NextLevelDB_Safer_Server;