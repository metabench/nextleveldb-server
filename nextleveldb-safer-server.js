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

        //console.log('table_name', table_name);
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
                            //console.log('inc_value', inc_value);
                            //console.log('last_key', last_key);

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

    safety_check(fix_errors = false, callback) {
        // Check all tables which have got autoincrementing keys

        let autoincrementing_pk_tables = [];
        each(this.model.tables, table => {
            if (table.pk_incrementor) {
                autoincrementing_pk_tables.push(table);
            }
            //if (table.)
        })

        console.log('autoincrementing_pk_tables.length', autoincrementing_pk_tables.length);

        // Then with these tables, find the last key in those tables.


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
        })




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