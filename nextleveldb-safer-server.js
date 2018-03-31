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





















class NextLevelDB_Safer_Server extends NextLevelDB_Server {
    constructor(spec) {
        super(spec);

        // use some servers as full sources.





    }

    safety_check(fix_errors = false) {
        // Check all tables which have got autoincrementing keys

        let autoincrementing_pk_tables = [];
        each(this.model.tables, table => {
            if (table.pk_incrementor) {
                autoincrementing_pk_tables.push(table);
            }
            //if (table.)
        })

        console.log('autoincrementing_pk_tables.length', autoincrementing_pk_tables.length);
        // Then check that the highest pk value is the incrementor value - 1.
        //  If it's not, then fixing the error will be to update the incrementor value.



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