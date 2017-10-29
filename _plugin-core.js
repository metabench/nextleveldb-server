/**
 * Created by james on 21/12/2016.
 */

// Plugins are good when the plugins get put in a directory.
//  It's a different distribution system to npm.

// Encodings could be part of the plugin system too.

// Could make a web cache part of the core, or as another module.
//  Want this web cache to record, and be able to replay, the results from various URLs.
//  Could store URLs as one key, or break them up further.
//   There would likely be other path parameters within various URLs.

// Treat full URLs as a single key? May make searching by URL harder.
//  Still, indexes get more complicated.

// URLs can be better tied to an online source.
//  Could have a way of defining online data sources, but that is less of a core item.

// nextlevedb-online-data-source makes sense.
//  Helps to provide attribution to things.
//  Helps to maintain bookmarks within the system. Tasks for retrieval of data could refer to them.

// Could have a whole load of watcher and collector objects running, in 1 process or as multiple.










var jsgui = require('jsgui3');
var xas2 = require('xas2');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
var Fns = jsgui.Fns;

var global_prefix_incrementors;
var global_prefix_table_table;
var global_prefix_table_table_indexes;
var global_prefix_native_types_table;
var global_prefix_native_types_table_indexes;

// incrementor id incrementor will always be 0

var incrementor_id_global_prefix;
var incrementor_id_table;
var incrementor_id_native_types;


var incrementor_table_table_row;

var global_prefix_poloniex_markets_table;
var global_prefix_poloniex_markets_table_indexes;

var core_encodings = require('./encodings-core');

var Binary_Encoding = require('binary-encoding');

var table_table_record_encoder = new Binary_Encoding.Record(core_encodings.table);

// Keeping encodings within the database seems like a decent medium-term goal.
//  A problem with making continuous upgrades is with breaking existing database implementations.
//   Worth being careful about having these databases able to export data to a simple, standard format, such as compressed CSV files, within a ZIP.

// A GUI to browse the data would be very nice - both web GUI and electron app.






// A binary data plugin that can be cancelled.
//  Would be worth making a system to cancel existing requests.

// In the client, it would be fn_operation(err, res, fn_cancel);
//  function cancel has a callback to show the cancellation has been done.

const int_table_table_key_prefix = 1;

var merge_plugin = function(nextleveldb_server, db, running_means_per_second) {
    // Can refer to standard API objects in nextleveldb-server
    //var index_lookup = nextleveldb_server.index_lookup;

    //console.log('db', db);
    //console.trace();
    //throw 'stop';
    var output_transform_to_hex = function(item) {
        if (Array.isArray(item)) {
            var res = [];
            each(item, (v) => {
                res.push(output_transform_to_hex(v));
            });
            return res;
        } else if (item instanceof Buffer) {
            return item.toString('hex');
        } else {
            return item;
        }
    };
    // get_all_keys = function(cancel, callback)

    var get_all_keys = function(callback) {

        // This has got not means to cancel it.
        //  An iterator would be able to check if it is worth continuing.
        //   Though it could get more complicated, in depth, with some kind of an array necessary to keep track of which operations have been cancelled or not.
        //    May be possible through the connection and the request id.

        // This looks like it would be done with a LevelDown interator, if we are to allow cancellation.

        //  The cancellation comes after the callback?
        //   Maybe promises that involve cancellation would be a useful abstraction.

        // Being able to cancel the function, from its call, seems useful.
        //  Specifying cancel after the callback makes most sense because cancel is more optional / less used.
        //  Makes it trickier to put them into fns, but that should be OK.

        var res_keys = [];

        db.createKeyStream({}).on('data', function(key) {
            //console.log('key', key);
            //console.log('key.toString()', key.toString());
            res_keys.push(encodeURI(key));
        })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                callback(null, res_keys);
            })
    };

    var get_all_records = function (callback) {

        // This has got not means to cancel it.
        //  An iterator would be able to check if it is worth continuing.
        //   Though it could get more complicated, in depth, with some kind of an array necessary to keep track of which operations have been cancelled or not.
        //    May be possible through the connection and the request id.

        // This looks like it would be done with a LevelDown interator, if we are to allow cancellation.

        //  The cancellation comes after the callback?
        //   Maybe promises that involve cancellation would be a useful abstraction.

        // Being able to cancel the function, from its call, seems useful.
        //  Specifying cancel after the callback makes most sense because cancel is more optional / less used.
        //  Makes it trickier to put them into fns, but that should be OK.

        var res_records = [];

        db.createReadStream({}).on('data', function (record) {
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
    };

    var count_records = function (callback) {
        var count = 0;
        db.createKeyStream({}).on('data', function (key) {
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

    var get_data_keys_ranging = function(lower, upper, callback) {
        var res_records = [];

        db.createReadStream({
            'gte': encodeURI(lower),
            'lte':  encodeURI(upper),
        }).on('data', function (data) {
            //console.log(data.key, '=', data.value)
            //console.log('')
            res_records.push([decodeURI(data.key), data.value]);

        })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream closed')

                //console.log('res_records', res_records);

                callback(null, res_records);

            })
    };

    var get_keys_ranging = function(lower, upper, callback) {
        var res_records = [];

        db.createKeyStream({
            'gte': encodeURI(lower),
            'lte':  encodeURI(upper),
        }).on('data', function (key) {
            //console.log(data.key, '=', data.value)
            //console.log('')
            res_records.push(key);
        })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream closed')

                //console.log('res_records', res_records);

                callback(null, res_records);

            })
    };

    var get_keys_beginning = function(key_prefix, callback) {
        get_keys_ranging(key_prefix + ' ', key_prefix + '~', callback);
    };

    // Could push 1000 records at a time in the streaming JSON.

    var get_data_multi_callbacks = function(num_per_callback, cb_data, cb_end) {
        var res_records = [];
        var c_records = 0;
        var stream = db.createReadStream();
        var pause = function() {
            stream.pause();
        };
        var resume = function() {
            stream.resume();
        };
        stream.on('data', function (data) {
            //console.log(data.key, '=', data.value)
            //console.log('')
            res_records.push([decodeURI(data.key), data.value]);
            c_records++;
            if (c_records === num_per_callback) {
                cb_data(res_records, pause, resume);
                c_records = 0;
                res_records = [];
            }
        })
            .on('error', function (err) {
                //console.log('Oh my!', err)
                callback(err);
            })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {

                if (c_records > 0) {
                    cb_data(res_records, pause, resume);
                }
                //console.log('Stream closed')
                //console.log('res_records', res_records);
                //callback(null, res_records);
                cb_end();
            })
    };

    var get_data_keys_beginning = function(key_prefix, callback) {
        get_data_keys_ranging(key_prefix + ' ', key_prefix + '~', callback);
    };




    // Get new prefix key
    //  Keeps a running tally of the number
    //  Uses the buffer from xas2


    // Different incrementor keys?
    //  Have a means of calling incrementors with different numeric keys...
    //  If the incrementor does not exist, its assumed to be 0.


    // Some incrementor ids, the basic ones, will be consts / magic numbers.
    //  Others will be held within the right table.


    // Incrementor incrementor = incrementor 0?
    // Table incrementor = incrementor 1?

    // Create the incrementor incrementor. Whenever there is a new incrementor, we need a reference number for it.

    // Table of tables, table of incrementors?
    //  Having and using a few system tables to start with will be useful.
    //  Will make functionality that tests they are set up right.


    // Keys get more difficult when there are individual table prefixes
    //  The table table key would need to know the key prefix for the table table.

    var _inc_key = function(int_incrementor_id) {
        var x_0 = xas2(0);
        var x_inc = xas2(int_incrementor_id);
        return Buffer.concat([x_0.buffer, x_inc.buffer]);
    };

    // Get the table table reference to a table, from the table's int id
    var _table_table_key = function(int_table_id) {
        // Can use lower level things, ie constants rather than knowing the table key prefix from a lookup.
        //  Could always assume the table table key prefix is 1

        var x_0 = xas2(int_table_table_key_prefix);                              // Table table key
        var x_table = xas2(int_table_id);
        return Buffer.concat([x_0.buffer, x_table.buffer]);
    };

    // Index key uses the table key prefix.

    var _index_key = function(int_table_key_prefix, int_index_id, buf_item_value) {
        // Not so sure about storing all table indexes within one key prefix.
        //  Seems OK because there are fewer of them than records.
        //   Its more important to optimize for a small key space.

        // Will store each table with its own key prefix.
        //  All table indexes have key prefix of table records + 1

        //var x_0 = xas2(2);                              // Initial key prefix for table indexes
        // Don't have an initial table index key prefix.
        //  Use table key prefix + 1

        //console.log('int_table_key_prefix', int_table_key_prefix);
        //console.trace();

        var x_index_key_prefix = xas2(int_table_key_prefix + 1);
        var x_index_id = xas2(int_index_id);
        //console.log('arguments', arguments);

        //console.log('x_index_key_prefix.buffer', x_index_key_prefix.buffer);
        //console.log('x_index_id.buffer', x_index_id.buffer);
        //console.log('buf_item_value', buf_item_value);


        return Buffer.concat([x_index_key_prefix.buffer, x_index_id.buffer, buf_item_value]);
    };



    var create_incrementor = function(callback) {
        // Current possible optimization: keep a hash table of the incrementor numbers, but put them in the DB...
        //  Would need to load the cache of them to start with though, makes things more likely / possible to get out of sync.
        //  Consider for future optimization.
        // A particular key space for incrementors?
        //  Within space 0?
        // 0 (for incrementors) then incrementor id

        increment_incrementor_incrementor((err, int_incrementor_id) => {
            if (err) {
                callback(err);
            } else {
                var b_inc_key = _inc_key(int_incrementor_id);
                // And set the incrementor value to 0
                var x_inc_value = xas2(0);
                db.put(b_inc_key, x_inc_value, (err, res_set) => {
                    if (err) {
                        throw err;
                    } else {

                        // Incrementor name index?

                        callback(null, int_incrementor_id);
                        // return a buffer with the key?
                        //  seems better (theoretically) than returning an integer.
                        //   but using integers in JavaScript for the moment as they are fine for expected needs.
                    }
                });
            }
        });

    };

    var create_incrementor_incrementor = function(callback) {
        // This may need to work on a lower level, and set the value to 1
        //  Because it needs to note the existance of itself.



        var b_inc_key = _inc_key(0);
        var x_inc_value = xas2(1);
        db.put(b_inc_key, x_inc_value, (err, res_set) => {
            if (err) {
                throw err;
            } else {
                // Also worth storing its name, indexing it, adding record to incrementor table (when that table exists)
                callback(null, 0);
            }
        });
        //create_incrementor(0, callback);
    };
    // Possibly we can't create all of the incrementors before we need them to create records about themselves.
    //  To start with, some records will be created on a lower level to provide the platform.
    var create_global_prefix_incrementor = function(callback) {
        // Use the incrementor incrementor for this.
        //incrementor_id_global_prefix()
        /*
         increment_incrementor_incrementor((err, inc) => {
         if (err) {
         callback(err);
         } else {
         incrementor_id_global_prefix = inc;
         create_incrementor(1, callback);

         }
         })
         */
        create_incrementor((err, inc) => {
            if (err) {
                callback(err);
            } else {
                incrementor_id_global_prefix = inc;
                callback(null, inc);
            }
        });
        // Could use the incrementor incrementor to get these incrementor numbers.
    };

    // Have an index of incrementors by name?
    //  Or create the initial incrementors in a set order, an remember what they are.
    //  OOP with async / await (likely with Bluebird) would make much more concise programming.





    var create_table_incrementor = function(callback) {
        create_incrementor((err, inc) => {
            if (err) {
                callback(err);
            } else {
                incrementor_id_table = inc;
                callback(null, inc);
            }
        });
        //create_incrementor(callback);
    };
    var create_native_types_incrementor = function(callback) {
        create_incrementor((err, inc) => {
            if (err) {
                callback(err);
            } else {
                incrementor_id_native_types = inc;
                callback(null, inc);
            }
        });
        //create_incrementor(callback);
    };
    var increment = function(int_incrementor_id, callback) {
        var b_inc_key = _inc_key(int_incrementor_id);
        // Get its current value.
        // get value, increment it, set it.
        //  The value will be encoded in the xas2 format.
        db.get(b_inc_key, (err, incrementor_value) => {
            if (err) {
                callback(err);
            } else {
                var xv = xas2(incrementor_value);
                // then get a number from it...
                var num = xv.number;
                //var res = xv.number + 1;
                var xv2 = xas2(num + 1);
                db.put(b_inc_key, xv2, (err, res_set) => {
                    if (err) {
                        throw err;
                    } else {
                        callback(null, num);
                    }
                });
            }
        })
    };
    var increment_incrementor_incrementor = function(callback) {
        increment(0, callback);
    };
    var increment_global_prefix_incrementor = function(callback) {
        increment(1, callback);
    };

    var increment_table_incrementor = function(callback) {

        increment(2, callback);
    };
    var increment_native_types_incrementor = function(callback) {
        increment(3, callback);
    };

    //var put_table_id_in_table_id_by_name_index =
    // put_item_in_table_index_id_by_name
    // table_name_index - better name:
    //  idx_id_by_name
    //  add_idx_record_table_table_idx_id_by_name

    // Defining the indexes:
    //  Could possibly be done with a table?
    //   but indexing that table?

    // get table name by id
    // get table key prefix by id

    var get_table_name_by_id = function (int_table_id, callback) {

        //console.log('get_table_name_by_id int_table_id', int_table_id);
        var idx_row_name_by_id = 1;
        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_name_by_id).buffer);
        db.get(index_key, (err, b_val) => {
            if (err) {
                callback(err);
            } else {
                // then need to read the value
                var res = b_val.toString();
                callback(null, res);
            }
        });
    };

    var get_table_key_prefix_by_name = function (table_name, callback) {
        //console.log('get_table_key_prefix_by_name');
        //console.log('');
        get_table_id_by_name(table_name, function(err, table_id) {
            if (err) {
                callback(err);
            } else {
                //console.log('get_table_key_prefix_by_name');
                //console.log('table_name', table_name);
                //console.log('table_id', table_id);
                //throw 'stop';

                //console.log('pre get_table_key_prefix_by_id');
                get_table_key_prefix_by_id(table_id, callback);
            }
        });
    };


    // Defining the indexes of the table table would help.
    //  When the DB starts, an OO Table Table could be set up, with its definitions?
    //   Or continue to use lower level setup of table table.

    // Setting up a table table with OO code would definitely help.


    // Guessing the Incrementors could actually be stored in a Table.

    var get_table_key_prefix_by_id = function (int_table_id, callback) {
        //console.log('get_table_key_prefix_by_id int_table_id', int_table_id);
        //console.log('');


        // 4th row in the key prefix table?
        //  Need to look into how the key prefixes get stored in the DB.
        //  
        var idx_row_name_by_id = 4;


        //var b_name = Buffer.from(name);
        //console.log('int_table_id', int_table_id);
        //console.log('idx_row_name_by_id', idx_row_name_by_id);

        // It's stored within the table table itself?

        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_name_by_id).buffer);
        console.log('index_key', index_key);
        //throw 'stop';

        // This looks wrong.



        db.get(index_key, (err, b_val) => {
            if (err) {



                callback(err);
            } else {


                //console.log('b_val', b_val);
                // then need to read the value
                var res = xas2(b_val).number;
                //console.log('***** res', res);
                callback(null, res);
            }
        });
    };

    var get_table_incrementor_id_by_id = function(int_table_id, callback) {
        var idx_row_name_by_id = 2;
        //var b_name = Buffer.from(name);
        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_name_by_id).buffer);
        db.get(index_key, (err, b_val) => {
            if (err) {
                callback(err);
            } else {
                // then need to read the value
                var res = xas2(b_val).number;
                //console.log('***** res', res);
                callback(null, res);
            }
        });
    };

    var get_table_id_by_name = function (str_table_name, callback) {
        //console.log('get_table_id_by_name\n');
        //console.log('global_prefix_table_table', global_prefix_table_table);
        //console.log('global_prefix_table_table_indexes', global_prefix_table_table_indexes);

        //console.log('get_table_id_by_name str_table_name', str_table_name);

        //throw 'stop';
        var index_key = _index_key(global_prefix_table_table, 0, Buffer.from(str_table_name));
        //console.log('index_key', index_key);
        db.get(index_key, (err, b_table_id) => {
            if (err) {
                // And if the table does not exist?
                //console.log('err', err);
                //console.log('typeof err', typeof err);
                var str_err = err + '';
                if (str_err.indexOf('Key not found') > -1) {
                    callback(null, -1);
                } else {
                    callback(err);
                };
                //console.log('get_table_id_by_name error getting name');
            } else {

                


                var x_table_id = xas2(b_table_id);
                //console.log('x_table_id.number', x_table_id.number);
                callback(null, x_table_id.number);
            }
        });
    };

    // add_idx_record_table_table_idx_key_prefix_by_id

    //  the key prefixes are part of the table table.

    /*

     var get_i_server_time = function(callback) {
     var t = Date.now();
     callback(null, t);
     }

     */


    //add_idx_record_table_table_idx_key_prefix_by_id

    // The index itself is a bit complex for the table table, without a definition of it having been made clear.

    var add_idx_record_table_table_idx_key_prefix_by_id = function(int_table_id, int_key_prefix, callback) {
        //var table_key = xas2(int_table_id);

        var idx_row_name_by_id = 4;
        // An index to look up the row name by its id, within the table table.
        //  This is used for fast lookup of what the tables are.


        //var b_name = Buffer.from(name);

        //
        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_name_by_id).buffer);
        //console.log('table_key', table_key);
        //console.log('index_key', index_key.toString());

        db.put(index_key, xas2(int_key_prefix).buffer, (err) => {
            if (err) {
                callback(err);
            } else {

                callback(null, true);

            }
        });
    };
    // index of table names by id

    var add_idx_record_table_table_idx_name_by_id = function (int_table_id, name, callback) {

        var idx_row_name_by_id = 1;
        var b_name = Buffer.from(name);
        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_name_by_id).buffer);

        //console.log('add_idx_record_table_table_idx_name_by_id\n');
        //console.log('index_key', index_key);
        //console.log('b_name', b_name.toString());

        db.put(index_key, b_name, (err) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
            }
        });
    };

    // add_idx_record_table_idx_row_incrementor_by_id

    // ???Index to get from the table id and the row incrementor to the table row incrementor?
    //  Use does not seem clear right now.

    // The encodings and decodings system with the keys and records seems like enough to work on for the moment.
    //  Also having a correct table of tables.

    var add_idx_record_table_table_idx_row_incrementor_by_id = function(int_table_id, int_table_row_incrementor, callback) {

        var table_key = xas2(int_table_id);
        var idx_row_incrementor_by_id = 2;
        var index_key = _index_key(global_prefix_table_table, xas2(int_table_id).buffer, xas2(idx_row_incrementor_by_id).buffer);

        //console.log('table_key', table_key);
        //console.log('index_key', index_key);

        //throw 'stop';
        db.put(index_key, xas2(int_table_row_incrementor).buffer, (err) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
            }
        });
    };

    var add_idx_record_table_table_idx_id_by_name = function(int_table_id, str_table_name, callback) {
        // Need to put this into the index for the table names table
        //console.log('add_idx_record_table_table_idx_id_by_name', int_table_id, str_table_name);
        // Link it back to the table by the table key/id
        // but what about a proper table key?
        //  The table key should just be xas2(int_table_id)???
        // Or leave for moment...
        //  This goes in the table name index, so the table table.
        //  however, the table key is the int representation of the table.
        //var table_key = _table_table_key(int_table_id);
        var table_key = xas2(int_table_id);
        // but we need to put it into table 0, the table table
        // no, we need to use the table table global key prefix.
        //var int_idx_table_table = 0;
        var name_index_id = 0;
        // Index the index accorging to the table table key prefix id.
        var index_key = _index_key(global_prefix_table_table, name_index_id, Buffer.from(str_table_name));
        //console.log('table_key', table_key);
        //console.log('index_key', index_key.toString());
        db.put(index_key, table_key, (err) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
            }
        });
    };
    // Will have a more generalised version of this that puts an index record with string and int in place

    var put_native_type_name_in_native_type_name_index = function(int_native_type_id, str_native_type_name, callback) {
        //var int_idx_native_types_table = 1;  // the native types table
        var table_key = xas2(int_native_type_id);
        var name_index_id = 0;
        // The name index will still be index 0
        //console.log('global_prefix_native_types_table', global_prefix_native_types_table);
        var index_key = _index_key(global_prefix_native_types_table, name_index_id, Buffer.from(str_native_type_name));
        //console.log('table_key', table_key);
        //console.log('index_key', index_key);
        db.put(index_key, table_key, (err) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
            }
        });
    };

    var create_table_table = function(callback) {
        //console.log('create_table_table');

        // table index 0

        // has got name and value indexes.
        //  a value index would speed up retrieval of table names from if significantly, worth doing
        //

        // Use the incrementor, to get a new id

        //  And it's the table incrementor
        //

        // Use the initial/global prefix incrementor to get the table key prefix
        //  Though it seems neater to have all tables within a single global prefix, it makes for longer keys (not sure about long run that much).
        //  Keeping keys as short as possible does seem like a good way of doing things.
        // Most of the data will be table records anyway, good to make the most use of the key space.
        //  Though maybe it would help to keep all of the indexes to a table in one place too.

        // For the moment, the initial table prefix system seems the best.
        //  Seems like one of the things that could be made an option.



        // Global Incrementor.

        // Each table could have 2 global prefix values
        // first for the records
        // second for the indexes
        //  then another part/prefix identifies the index number

        // Native types table
        //  indexes
        // Table table
        //  indexes


        // The table table, like other tables, will need a row incrementor.


        increment_global_prefix_incrementor((err, res_gp) => {
            if (err) {
                callback(err);
            } else {
                global_prefix_table_table = res_gp;
                //console.log('global_prefix_table_table', global_prefix_table_table);
                //throw 'stop';

                // global_prefix_table_table_indexes

                increment_global_prefix_incrementor((err, res_gp) => {
                    if (err) {
                        callback(err);
                    } else {
                        global_prefix_table_table_indexes = res_gp;
                        // create table row incrementor for this table.

                        increment_table_incrementor((err, int_table_table_id) => {
                            if (err) {
                                callback(err);
                            } else {
                                //console.log('int_table_table_id', int_table_table_id);
                                //console.log('');


                                // Worth creating a record for itself in the table table.


                                // The table metadata / description record...
                                //  holds the id and the name too.

                                // Should create the table record.
                                //  Will need to create a table key


                                // put table name in table name index.

                                //var table_key = _table_key(int_table_table_id);


                                //console.log('table_key', table_key);

                                // then put the record in saying its the table itself.
                                // put the ID in in hex?
                                // and say it's indexed by name as well?
                                //  define the types here?
                                // Can treat the table table record a bit special as its a platform for other operations.
                                //  Programatically set up the index.
                                //   Automatic indexes may require a table of indexes.

                                // create the two table index keys...

                                //var table_name = 'tables';



                                // make the index key for the name index.
                                //  so we get the table id when we look it up by name

                                //var index_1_value_name = 'name';
                                // _index_key

                                //var index_key = _index_key(int_table_table_id, 0, Buffer.from(table_name));


                                //console.log('index_key', index_key);
                                //var index_value = table_key;



                                // The table with name tables refers to the item we are making here
                                // Seems like it would do the job of much of what we have here.

                                //  put table name in table name index?
                                // put_table_name_index(int_table_table_id, table_name, callback)




                                // As well as the table name index, we can have a table global prefix index.
                                //  Will be indexed by id.

                                // For the moment, indexing the table names by ID makes sense.
                                // Indexing the table global prefix values makes a lot of sense too.


                                //  Function to get a table global prefix value.
                                //   The value for the tables table could be hardcoded.
                                //   Then we can refer to the tables table, or an index of it, to get the global prefix value.

                                // The tables table will be indexed by name (to get id),
                                //  want to index the table global prefixes too.

                                // get thr value from the global incrementors?

                                // 0 incrementors
                                // 1 then the spaces for the tables?
                                //  just use a function of the table id?
                                //  1 + (table_id * 2)?


                                // put_table_global_prefix_in_table_global_prefix_index(int_table_table_id
                                //console.log('pre add_idx_record_table_table_idx_id_by_name');

                                // id by name

                                // put_table_name_in_table_name_by_id_index

                                add_idx_record_table_table_idx_id_by_name(int_table_table_id, 'tables', (err) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        //console.log('post add_idx_record_table_table_idx_id_by_name');

                                        create_incrementor((err, inc) => {
                                            if (err) {
                                                throw err;
                                            } else {

                                                incrementor_table_table_row = inc;

                                                // Store the table table row incrementor id?
                                                // Other incrementor ids will be stored in the table's data so it can be retrieved.

                                                // add_idx_record_table_table_idx_row_incrementor_by_id

                                                add_idx_record_table_table_idx_row_incrementor_by_id(int_table_table_id, incrementor_table_table_row, (err, res_add_incrementor_by_id_idx_record) => {
                                                    if (err) {
                                                        callback(err);
                                                    } else {

                                                        // Getting from the ids to the global prefixes for tables?

                                                        add_idx_record_table_table_idx_name_by_id(int_table_table_id, 'tables', function (err, res_add_name_by_id_idx_record) {
                                                            if (err) {
                                                                callback(err);
                                                            } else {

                                                                // Add a record in the tables table, for the tables table.
                                                                //  Add it on the lower level right now.

                                                                // We can put it in directly.
                                                                //  Key: table table prefix, table table row incrementor value



                                                                //var table_table_record = Bina
                                                                var table_table_record = table_table_record_encoder.encode(global_prefix_table_table, [int_table_table_id, 'tables']);
                                                                //console.log('table_table_record', table_table_record);
                                                                //console.log('int_table_table_id', int_table_table_id);

                                                                //throw 'stop';

                                                                db.put(table_table_record[0], table_table_record[1], (err) => {
                                                                    if (err) {
                                                                        callback(err);
                                                                    } else {
                                                                        //callback(null, true);

                                                                        add_idx_record_table_table_idx_key_prefix_by_id(int_table_table_id, global_prefix_table_table, (err, res_add_kp) => {
                                                                            if (err) {
                                                                                callback(err);
                                                                            } else {
                                                                                //callback(null, [int_table_id, global_prefix_table]);
                                                                                callback(null, [int_table_table_id, global_prefix_table_table_indexes]);
                                                                            }
                                                                        })
                                                                    }
                                                                });

                                                                // global_prefix_table_table

                                                                // Add the key prefix index record.


                                                                //callback(null, int_table_table_id);
                                                            }
                                                        });

                                                    }
                                                });

                                                /*
                                                
                                                */

                                            }
                                        });
                                        // Other indexes:

                                        //  Index name by id
                                        //  Index key prefix by id

                                        // put_table_key_prefix_in_table_key_prefix

                                    }
                                });
                            }
                        });
                    }
                });

            }
        });
    };

    var incremented_buffer_copy = function(buffer) {

        // Find the last byte that is not 255.

        var pos = buffer.length - 1;

        var found = false;
        var i8;

        while(!found) {
            //found =
            i8 = buffer.readInt8(pos);
            found = i8 < 255;
            pos--;
        }
        if (found) {
            pos++;

            //console.log('pos', pos);

            var res = Buffer.from(buffer);
            res.writeInt8(i8 + 1, pos);
            return res;
        } else {
            throw 'Buffer value too large - cannot increment';
        }


    }

    var incremented_at_byte_pos_buffer_copy = function(byte_pos, buffer) {
        var i8 = buffer.readInt8(byte_pos);
        if (i8 === 255) {
            throw 'not (yet) supported'
        } else {
            var res = Buffer.from(buffer);
            res.writeInt8(i8 + 1, byte_pos);
            return res;
        }
    }

    var get_table_names_and_int_ids = function(callback) {

        // currently ordered by name, which is the way they are sorted by that index.

        // This could be abstracted as a full data range retrieval.
        //  Also setting of where to retrieve it from, how the keys are set.

        console.log('\n\nget_table_names_and_int_ids\n');

        // the prefix for the tables by name index
        //  table name index

        //  Tables space will be space 1 I think.
        // It's the table indexes space though.

        var x_0 = xas2(global_prefix_table_table_indexes);



        //var int_table_table_id = 0;
        //var x_0 = xas2(int_table_table_id);
        //var x_table_id = xas2(int_table_table_id);

        var x_index_id = xas2(0);

        // Is the db by default being sorted in lexographic, not binary order causing the problem here?




        // 02, 01
        // 02, 02

        //console.log('x_0', x_0);
        //console.log('x_index_id', x_index_id);

        var b_key_beginning = Buffer.concat([x_0.buffer, x_index_id.buffer]);

        // then iterate over those records in range

        // also need to be able to increment a buffer by 1
        //  get an incremented copy

        // It's big endian, so need to increment the first value in the buffer

        var b_key_end = incremented_at_byte_pos_buffer_copy(1, b_key_beginning);

        //var b_key_end = incremented_buffer_copy(b_key_beginning);

        console.log('b_key_beginning', b_key_beginning);
        console.log('b_key_end', b_key_end);

        var res = [];

        throw 'stop';


        db.createReadStream({
            'gte': b_key_beginning,
            'lt': b_key_end
        })
            .on('data', function(data) {
                console.log('data', data);
                var key = data.key;
                var value = data.value;


                // The values are just the int ids.

                // Use table name index classes to get the values.

                // Needs to parse the keys / values based on specified structure.
                //  Looks like we do need to use a decoder that knows the order of the fields.
                //  That's the tricky thing about using integer with varying sizes.
                //  Need to parse the values from a buffer.

                // Or indexed string field decoding?
                //  Need to be able to read xas2 values from a buffer, and advance to the next position.
                //   Could make use of 2 return values - the value read, and the new position.

                //var [record_type_key_prefix, pos] = xas2.read(key, 0);

                //console.log('record_type_key_prefix, pos', record_type_key_prefix, pos);

                // record_type_key_prefix is 2 when reading the table names.
                //  because 2 is the first / very primary key for all indexes

                var [table_index_key_prefix, pos2] = xas2.read(key, 0);


                //console.log('int_table_id', int_table_id);
                //console.log('pos2', pos2);

                var [int_table_index_id, pos3] = xas2.read(key, pos2);



                //console.log('int_table_index_id', int_table_index_id);

                // then read to the end of the buffer

                var name = key.toString('utf-8', pos3);
                //console.log('name:', name);


                var [int_id] = xas2.read(value, 0);
                //var [number_one, pos4] = xas2.read(value, 0); // Number 1 refers to the table of tables
                //var [int_id, pos5] = xas2.read(value, pos4);

                // int_id

                //console.log('int_id:', int_id);

                res.push([name, int_id]);

            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {

                //

                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')

                // callback with the last page

                callback(null, res);
            })


    };

    var get_native_type_names_and_int_ids = function(callback) {

        // currently ordered by name, which is the way they are sorted by that index.

        // This could be abstracted as a full data range retrieval.
        //  Also setting of where to retrieve it from, how the keys are set.


        //console.log('\n\get_native_type_names_and_int_ids');

        // the prefix for the tables by name index
        //  table name index

        // It's the table indexes space though.

        //console.log('global_prefix_native_types_table_indexes', global_prefix_native_types_table_indexes);


        var x_0 = xas2(global_prefix_native_types_table_indexes);  // signifies table indexes space

        //var int_table_table_id = 0;
        //var x_0 = xas2(int_table_table_id);
        //var x_table_id = xas2(int_table_table_id);

        // This x_index_id looks like it refers to the table id of native_type_names (1)

        var x_index_id = xas2(0);

        // Is the db by default being sorted in lexographic, not binary order causing the problem here?




        // 02, 01
        // 02, 02

        var b_key_beginning = Buffer.concat([x_0.buffer, x_index_id.buffer]);

        // then iterate over those records in range

        // also need to be able to increment a buffer by 1
        //  get an incremented copy

        // It's big endian, so need to increment the first value in the buffer

        var b_key_end = incremented_at_byte_pos_buffer_copy(1, b_key_beginning);

        //var b_key_end = incremented_buffer_copy(b_key_beginning);

        //console.log('b_key_beginning', b_key_beginning);
        //console.log('b_key_end', b_key_end);

        var res = [];

        //throw 'stop';

        db.createReadStream({
            'gte': b_key_beginning,
            'lt': b_key_end
        })
            .on('data', function(data) {
                //console.log('data', data);
                var key = data.key;
                var value = data.value;


                // The values are just the int ids.

                // Use table name index classes to get the values.

                // Needs to parse the keys / values based on specified structure.
                //  Looks like we do need to use a decoder that knows the order of the fields.
                //  That's the tricky thing about using integer with varying sizes.
                //  Need to parse the values from a buffer.

                // Or indexed string field decoding?
                //  Need to be able to read xas2 values from a buffer, and advance to the next position.
                //   Could make use of 2 return values - the value read, and the new position.

                //var [record_type_key_prefix, pos] = xas2.read(key, 0);

                //console.log('record_type_key_prefix, pos', record_type_key_prefix, pos);

                // record_type_key_prefix is 2 when reading the table names.
                //  because 2 is the first / very primary key for all indexes

                var [table_indexes_global_prefix, pos2] = xas2.read(key, 0);


                //console.log('int_table_id', int_table_id);
                //console.log('pos2', pos2);

                var [int_table_index_id, pos3] = xas2.read(key, pos2);



                //console.log('int_table_index_id', int_table_index_id);

                // then read to the end of the buffer

                var name = key.toString('utf-8', pos3);
                //console.log('name:', name);


                var [int_id] = xas2.read(value, 0);
                //var [number_one, pos4] = xas2.read(value, 0); // Number 1 refers to the table of tables
                //var [int_id, pos5] = xas2.read(value, pos4);

                // int_id

                //console.log('int_id:', int_id);

                res.push([name, int_id]);

            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {

                //

                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')

                // callback with the last page

                callback(null, res);
            })


    };

    var _ll_get_all_keys_and_values = function(callback) {

        // currently ordered by name, which is the way they are sorted by that index.

        // This could be abstracted as a full data range retrieval.
        //  Also setting of where to retrieve it from, how the keys are set.


        var res = [];

        db.createReadStream({

        })
            .on('data', function(data) {
                //console.log('data', data);
                var key = data.key;
                var value = data.value;
                res.push([key, value]);

            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {

                //

                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')

                // callback with the last page

                callback(null, res);
            })
    };


    // Create the table with its indexes as well?
    //  Much of the issue is just defining the key space for the table and its indexes.
    //  Noting things down so that these can be returned in the future.

    // Once we have created tables and got the keyspace for them, we can try populating them with data.
    //  Possibly lots of data.
    //  Would be nice to compress datasets down to small sizes, and then host them permanently / long term on file hosting systems like Google cloud file storage
    //




    var create_table = function(table_name, callback) {
        // Each table should have an incrementor, for its row number (for adding new rows).

        // Should create a new incrementor for that table.




        increment_table_incrementor(function(err, int_table_id) {

            // Increment the global incrementor twice.

            // table global prefix, table indexes global prefix

            increment_global_prefix_incrementor((err, gp) => {
                if (err) {
                    callback(err);
                } else {
                    var global_prefix_table = gp;

                    increment_global_prefix_incrementor((err, gp) => {
                        if (err) {
                            callback(err);
                        } else {
                            var global_prefix_table_indexes = gp;


                            // add the record to the tables table

                            //  create the table value object item

                            //  add the table id to the index of table names

                            //  add the table name to the index of table ids?
                            //   would help with faster lookup of the name field.

                            // put the table name in the table name index.

                            //console.log('1) table_name', table_name);



                            add_idx_record_table_table_idx_id_by_name(int_table_id, table_name, (err) => {
                                if (err) {
                                    callback(err);
                                } else {

                                    create_incrementor((err, inc) => {
                                        if (err) {
                                            throw err;
                                        } else {

                                            //incrementor_table_table_row = inc;

                                            // Store reference to the incrementor.

                                            // Create the table record in the table table.
                                            //  


                                            // Store the table table row incrementor id?
                                            // Other incrementor ids will be stored in the table's data so it can be retrieved.

                                            // May be worth having an incrementors table early on, so we can look up what incrementors are used.

                                            // This info could be needed for various fast lookups.



                                            add_idx_record_table_table_idx_row_incrementor_by_id(int_table_id, inc, (err, res_add_idx_record) => {
                                                if (err) {
                                                    callback(err);
                                                } else {

                                                    add_idx_record_table_table_idx_name_by_id(int_table_id, table_name, function(err, res_add_name_by_id_idx_record) {
                                                        if (err) {
                                                            callback(err);
                                                        } else {

                                                            // //

                                                            // Add the key prefix index record.

                                                            add_idx_record_table_table_idx_key_prefix_by_id(int_table_id, global_prefix_table_indexes, (err, res_add_kp) => {
                                                                if (err) {
                                                                    callback(err);
                                                                } else {

                                                                    // Create the record in the table table.


                                                                    var table_table_record = table_table_record_encoder.encode(global_prefix_table_table, [int_table_id, table_name]);
                                                                    console.log('table_table_record', table_table_record);
                                                                    console.log('int_table_id', int_table_id);

                                                                    //throw 'stop';

                                                                    db.put(table_table_record[0], table_table_record[1], (err) => {
                                                                        if (err) {
                                                                            callback(err);
                                                                        } else {
                                                                            //callback(null, true);
                                                                            callback(null, [int_table_id, global_prefix_table]);

                                                                        }
                                                                    });





                                                                    
                                                                }
                                                            });

                                                        }
                                                    });



                                                    //callback(null, int_table_table_id);
                                                }
                                            });

                                            //callback(null, int_table_table_id);
                                        }
                                    })
                                }
                            });
                        }
                    })


                }
            })
        })
    };

    var create_tables = arrayify(create_table);

    var create_fields_table = function(callback) {
        create_table('fields', callback);
    };

    var ensure_table = function(table_name, callback) {
        console.log('ensure_table', table_name);
        get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                console.log('table_id', table_id);
                if (table_id === -1) {
                    create_table(table_name, callback);
                } else {
                    // gets its table key prefix

                    get_table_key_prefix_by_id(table_id, (err, int_prefix) => {
                        if (err) {
                            callback(err);
                        } else {
                            console.log('int_prefix', int_prefix);
                            callback(null, [table_id, int_prefix]);
                        }
                    })

                }
            }
        })
    };

    // Arrayify working with callback functions?
    var ensure_tables = arrayify(ensure_table);

    var create_native_types_table = function(callback) {
        create_table('native_types', (err, res_create_table) => {
            if (err) {
                throw callback(err);
            } else {
                var ntt_id = res_create_table[0];

                global_prefix_native_types_table = res_create_table[1];
                global_prefix_native_types_table_indexes = global_prefix_native_types_table + 1;
                callback(null, true);

            }
        });
    };

    var put_native_types_record = function(name, callback) {
        increment_native_types_incrementor((err, int_new_native_type_id) => {
            if (err) {
                callback(err);
            } else {
                //console.log('int_new_native_type_id', int_new_native_type_id);

                // Then put together the record itself.
                //  The native types table should have an index, by name
                //  Possibly need to add to the index on a lower level here

                //  For the moment, the native type record itself is less important than being able to get native type ids by name
                //   (and get the names by id)

                //  Need to implement some native type record get and set functionality on a lower level.
                //   Some parts of the native types functionality is likely to be used by the more general purpose get and set functions.

                // _table_record_key

                // TODO: Add the native type record itself.

                put_native_type_name_in_native_type_name_index(int_new_native_type_id, name, (err) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, int_new_native_type_id);
                    }
                });

            }
        })
    };


    // Initial creation of incrementors seems OK on a low level...

    // But making an OO Incrementor object could make sense.


    var create_system_incrementors = function(callback) {
        create_incrementor_incrementor(function(err, b_inc_inc_key) {
            if (err) {
                callback(err);
            } else {

                // Global prefix incrementor.
                //  Will use these global prefixes while assigning key spaces (to tables and their indexes).

                create_global_prefix_incrementor(function(err, res_global_prefix_incrementor) {
                    if (err) {
                        callback(err);
                    } else {

                        // Use the global prefix incrementor to get the incrementors global prefix.

                        increment_global_prefix_incrementor((err, res_increment_global_prefix_incrementor) => {
                            if (err) {
                                callback(err);
                            } else {
                                global_prefix_incrementors = res_increment_global_prefix_incrementor;
                                create_table_incrementor((err, b_native_types_inc_key) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        create_native_types_incrementor((err, b_native_types_inc_key) => {
                                            if (err) {
                                                callback(err);
                                            } else {
                                                callback(null, true);
                                            }
                                        })

                                        //callback(null, true);
                                    }
                                });

                            }
                        });
                        //console.log('b_inc_inc_key', b_inc_inc_key);
                        //console.log('b_table_inc_key', b_table_inc_key);
                    }
                });

                // create the table incrementor...
                //console.log('x_inc_inc', x_inc_inc);
            }
        });
    };

    var create_system_tables = function(callback) {
        create_table_table((err, res_create_table_table) => {
            if (err) {
                callback(err);
            } else {

                get_all_records((err, all_records) => {
                    if (err) {

                    } else {
                        console.log('all_records', all_records);

                        //throw 'stop';

                        each(all_records, (record) => {
                            console.log(record[0], record[1].toString());
                        });

                        create_native_types_table((err, res_create_native_types_table) => {
                            if (err) {
                                callback(err);
                            } else {

                                //console.log('res_create_native_types_table', res_create_native_types_table);

                                // Then create the various records for the table table, fields table, and native_types table

                                // put_record
                                //  not indexed yet

                                // Because fields refer to native types and tables

                                create_fields_table((err, res_create_fields_table) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        callback(null, true);

                                    }
                                })

                            }
                        });

                        //throw 'stop';













                    }
                });
                //

                //console.log('res_create_table_table', res_create_table_table);

                
                // Much of the power comes from effective use of the key space.

                // A fields table would be useful.
                //  Want to be able to save the fields records for various tables.
                //  This will be useful when putting together keys, encoded records
                // Could be better than referring to encodings.
                //  Would help to keep the data, metadata and operations all in one place.

                // create_fields_table
                //  then can add the table table's fields to the database.
            }
        })
    };

    var create_native_types_records = function(callback) {
        //console.log('pre put_native_types_record xas2');
        put_native_types_record('xas2', (err, res_put_xas2_native_types_record) => {
            if (err) {
                callback(err);
            } else {
                // Other native types records...
                //console.log('res_put_xas2_native_types_record', res_put_xas2_native_types_record);
                put_native_types_record('date', (err, res_put_xas2_native_types_record) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('res_put_xas2_native_types_record', res_put_xas2_native_types_record);
                        // Other native types records...
                        put_native_types_record('string', (err, res_put_xas2_native_types_record) => {
                            if (err) {
                                callback(err);
                            } else {
                                //console.log('res_put_xas2_native_types_record', res_put_xas2_native_types_record);
                                // Other native types records...
                                put_native_types_record('float32le', (err, res_put_xas2_native_types_record) => {
                                    if (err) {
                                        callback(err);
                                    } else {


                                        // Now we have the native types records, we can put in place record definitions (fields) that use them




                                        // Other native types records...


                                        //console.log('res_put_xas2_native_types_record', res_put_xas2_native_types_record);


                                        callback(null, true);

                                    }
                                });
                            }
                        });
                    }
                })
            }
        })
    };

    // Won't be part of the core.
    //  Could be within it own node package.

    // A table is just a simple thing right now, somewhere within the key space.
    //  Key encoding, record encoding, key is put within the table key space.
    //   That is a way to ensure the lower level functionality works throughout, and supports more advanced table functionality.
    //    More info can be held within the database system about the key and record encodings.
    



    var console_log_system = function(callback) {
        get_table_names_and_int_ids(function(err, res_tables) {
            if (err) {
                callback(err);
            } else {

                console.log('res_tables', res_tables);
                throw 'stop';

                // _ll_get_all_keys_and_values

                get_native_type_names_and_int_ids(function(err, res_native_types) {
                    if (err) {
                        callback(err);
                    } else {
                        // _ll_get_all_keys_and_values
                        console.log('res_native_types', res_native_types);
                        //callback(null, true);
                        _ll_get_all_keys_and_values(function(err, all_keys_and_values) {
                            if (err) {
                                callback(err);
                            } else {
                                console.log('all_keys_and_values', all_keys_and_values);
                                callback(null, true);
                            }
                        });
                    }
                });
            }
        });
    };


    var initial_db_setup = function(callback) {
        //console.log('initial_db_setup');
        initial_system_db_setup(function(err, res_system_db_setup) {
            if (err) {
                callback(err);
            } else {
                // do the custom setup

                /*
                custom_db_setup(function(err, res_custom_db_setup) {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('res_custom_db_setup', res_custom_db_setup);
                        console_log_system(callback);
                    }
                });
                */
                //console_log_system(callback);
                callback(null, res_system_db_setup);
            }
        });
    };

    var starting_db_setup = function(callback) {
        global_prefix_table_table = 1;
        global_prefix_table_table_indexes = 2;

        // And some other global prefixes which are known about?



        callback(null, true);
    }


    var initial_system_db_setup = function(callback) {
        //console.log('initial_system_db_setup');


        //global_prefix_table_table = 1;

        //throw '** stop';

        //db = that.db = replace_db_put(levelup(that.db_path, options), that);

        // Could have an Incrementor object as well.



        create_system_incrementors((err, res_create_system_incrementors) => {
            if (err) {
                callback(err);
            } else {

                //console.log('cb create_system_incrementors');

                create_system_tables((err, res_create_system_tables) => {
                    if (err) {
                        callback(err);
                    } else {

                        

                        create_native_types_records((err, res_create_native_types_records) => {
                            if (err) {
                                callback(err);
                            } else {

                                callback(null, true);
                            }
                        });

                        //console.log('res_create_system_tables', res_create_system_tables);


                        
                    };
                });

                


                //console.log('res_create_system_incrementors', res_create_system_incrementors);
                // incrementors global space = 0


                // Then will get new global incrementor space for each table.

                
            }
        });
        // create_incrementor
        //  create the incrementor incrementor       inc 0

        // create the table incrementor              inc 1
    };


    // This function will / may be rendered obselete by records working out their own indexes.



    var put_id_by_string_index_record_tkp = function (int_table_key_prefix, table_index_id, str_value, record_id, callback) {

        //console.log('');
        //console.log('put_id_by_string_index_record_tkp (int_table_key_prefix, table_index_id, str_value, record_id)');
        //console.log(int_table_key_prefix, table_index_id, str_value, record_id);

        var index_key = _index_key(int_table_key_prefix, table_index_id, Buffer.from(str_value));
        //console.log('index_key', index_key);
        var b_val = xas2(record_id).buffer;
        //console.log('b_val', b_val);
        db.put(index_key, b_val, (err) => {
            if (err) {
                callback(err);
            } else {
                //console.log('pre cb');

                //throw 'stop';

                callback(null, true);
            }
        });
    };



    // This seems to be the main way of putting data into the database.

    var encode_new_id_put_record = function (str_table_name, encoder, record, callback) {
        //console.log('encode_new_id_put_record');
        get_table_id_by_name(str_table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                get_table_key_prefix_by_id(table_id, (err, table_key_prefix) => {
                    if (err) {
                        callback(err);
                    } else {
                        get_table_incrementor_id_by_id(table_id, (err, incrementor_id) => {
                            if (err) {
                                callback(err);
                            } else {
                                // increment the relevant incrementor to get the new id value
                                increment(incrementor_id, (err, new_id_value) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        //console.log('incrementor_id', incrementor_id);
                                        //console.log('poloniex_markets_table_key_prefix', poloniex_markets_table_key_prefix);
                                        record.unshift(new_id_value);
                                        var arr_record = encoder.encode(table_key_prefix, record);
                                        //console.log('arr_record', arr_record);
                                        //console.log('');
                                        //callback(null, false);

                                        db.put(arr_record[0], arr_record[1], (err, res_put) => {
                                            if (err) {
                                                callback(err);
                                            } else {
                                                callback(null, [new_id_value, table_key_prefix]);
                                                // Not indexing this record here (for the moment).

                                            }
                                        })

                                    }
                                })

                            }
                        })
                    }
                })
            }
        });
    };

    var count_within_int_prefix_key = (int_prefix_key, callback) => {

        var b_key_gte = xas2(int_prefix_key).buffer;
        //console.log('b_key_gte', b_key_gte);

        var b_key_lt = xas2(int_prefix_key + 1).buffer;

        var count = 0;

        db.createKeyStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function(key) {
                //console.log('key', key);
                count++;
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                callback(null, count);
            })


    };

    // This gets the records in an encoded format.
    //  Would be good to have a function that reads, and iterates through the records.
    //  That would not be called directly, most likely, but would go through a page maker.

    // Storing larger data pages on the server would make sense.
    //  Though it would be more difficult with insertions and deletions of records.
    //  Compacting/storing values from days ago to within a data page would mean that large amounts of data could be read back quickly.
    //   Could still keep the indexed records for more rapid queries. However, the way to go seems to be loading larger amounts of data at a time.


    // A streaming get records that decodes the records would be nice.

    // streaming_get_transform_decoded_records_within_int_prefix_key



    var streaming_paged_get_transform_decoded_records_within_int_prefix_key = (int_prefix_key, encoding, fn_transform, callback) => {
        //console.log('streaming_get_decoded_records_within_int_prefix_key int_prefix_key', int_prefix_key);
        var x_prefix = xas2(int_prefix_key);


        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;
        //console.log('b_key_lt', b_key_lt);

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.
        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = new Array(obj_buffer_size);
        var page = 0;
        var prefix_key_length = x_prefix.length;

        db.createReadStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function(data) {
                //console.log('key', key);
                //var key_without_prefix_key = data.key.slice(prefix_key_length);
                // fn_transform

                //console.log('[key_without_prefix_key, data.value]', [key_without_prefix_key, data.value]);
                // Problem occurrs if we have a row in the table that can't be decoded.

                var decoded = encoding.decode([data.key, data.value]);
                var transformed = fn_transform(decoded);

                //console.log('1) decoded', decoded);
                //console.log('1) transformed', transformed);

                arr_res[count] = [transformed];
                // Counts in different scopes.
                count++;

                //c_out++;
                if (count === obj_buffer_size) {
                    // send the current buffer
                    // need to put it in an event
                    // a page / chunk / block
                    // can say what page it is
                    //

                    // Data may be enough to say we have page encoding.
                    //  Currently getting about 60K records output per second at maximum.
                    running_means_per_second.add(2, count);



                    var e = {
                        'type': 'data',
                        'page': page++,
                        'data': arr_res,
                        '__page_encoding': true
                    };



                    callback(null, e);
                    //console.log('e', e);
                    //throw 'stop';
                    count = 0;
                }
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                var e;
                if (count === 0 && page === 0) {
                    e = {
                        empty: true
                    }
                } else {
                    var partial_arr_res = arr_res.slice(0, count - 1);
                    running_means_per_second.add(2, count);
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'last': true,
                        'data': partial_arr_res
                    };
                    callback(null, e);
                }
            });
    };


    var streaming_paged_get_decoded_records_within_int_prefix_key = (int_prefix_key, encoding, callback) => {
        //console.log('streaming_paged_get_decoded_records_within_int_prefix_key int_prefix_key', int_prefix_key);
        var x_prefix = xas2(int_prefix_key);
        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;
        //console.log('b_key_lt', b_key_lt);

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.
        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = new Array(obj_buffer_size);
        var page = 0;
        var prefix_key_length = x_prefix.length;
        db.createReadStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function(data) {
                //console.log('key', key);
                //var key_without_prefix_key = data.key.slice(prefix_key_length);
                var decoded = encoding.decode([data.key, data.value]);
                arr_res[count] = [decoded];
                // Counts in different scopes.
                count++;
                //c_out++;
                if (count === obj_buffer_size) {
                    // send the current buffer
                    // need to put it in an event
                    // a page / chunk / block
                    // can say what page it is
                    //
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'data': arr_res
                    };
                    callback(null, e);
                    //console.log('e', e);
                    //throw 'stop';
                    count = 0;
                }
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                var e;
                if (count === 0 && page === 0) {
                    e = {
                        empty: true
                    }
                } else {
                    var partial_arr_res = arr_res.slice(0, count - 1);
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'last': true,
                        'data': partial_arr_res
                    };
                    callback(null, e);
                }
            });
    };

    var decoding_iterator_by_int_prefix_key = (int_prefix_key, encoding, callback_data, callback_end) => {
        //console.log('streaming_get_decoded_records_within_int_prefix_key int_prefix_key', int_prefix_key);
        var x_prefix = xas2(int_prefix_key);
        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;



        //console.log('b_key_lt', b_key_lt);

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.
        //var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        //var obj_buffer_size = 1024;
        //var arr_res = new Array(obj_buffer_size);
        //var page = 0;
        var prefix_key_length = x_prefix.length;
        db.createReadStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function(data) {
                //console.log('key', key);
                //var key_without_prefix_key = data.key.slice(prefix_key_length);

                //var arr_record = [key_without_prefix_key, data.value];
                //console.log('arr_record', arr_record);


                // And raise an error callback on specific items?
                //  Makes sense when there could be an error with some items but not others.

                var decoded = encoding.decode([data.key, data.value]);
                running_means_per_second.increment(2);

                //console.log('decoded', decoded);

                //arr_res[count] = [decoded];
                // Counts in different scopes.
                //count++;
                //c_out++;

                // The output is just the decoded record.

                callback_data(null, decoded);
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                callback_end(null, true);


            });

    };

    /*

     var decoding_iterator_by_partial_key = (b_partial_key, encoding, callback_data, callback_end) => {
     // Would need to increment the buffer slightly.
     var b_key_lt = incremented_buffer_copy(b_partial_key);

     console.log('b_partial_key', b_partial_key);
     console.log('b_key_lt', b_key_lt);

     // need to look at the prefix key.

     // that means reading an xas2 from the b_partial_key

     };
     */

    var decoding_iterator_record_by_key_range = (obj_range, encoding, callback_data, callback_end) => {
        //var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        //var obj_buffer_size = 1024;
        //var arr_res = new Array(obj_buffer_size);
        //var page = 0;
        // Would need to read the prefix from each record?
        //  Need to read it from the first?
        //var read = xas2.read()
        //var prefix_key_length = x_prefix.length;

        db.createReadStream(obj_range)
            .on('data', function(data) {
                //console.log('key', key);
                // Would need to read the key value and get past the prefix
                //var [t, prefix_key_length] = xas2.read(data.key);
                var arr_b_record = [data.key, data.value];

                // Something to decode as one single array?
                var arr_decoded = encoding.decode(arr_b_record);
                //console.log('arr_decoded', arr_decoded);
                var arr_res = [].concat(arr_decoded[0], arr_decoded[1]);
                //console.log('arr_res', arr_res);

                running_means_per_second.increment(2);
                callback_data(null, arr_res);
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                //var e;
                //if (count === 0 && page === 0) {
                //e = {
                //	empty: true
                //}
                //} else {
                callback_end(null, 'end');
                //}
            });
    };

    var decoding_iterator_by_key_range = (obj_range, encoding, callback_data, callback_end) => {
        //var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        //var obj_buffer_size = 1024;
        //var arr_res = new Array(obj_buffer_size);
        //var page = 0;
        // Would need to read the prefix from each record?
        //  Need to read it from the first?
        //var read = xas2.read()
        //var prefix_key_length = x_prefix.length;

        db.createReadStream(obj_range)
            .on('data', function(data) {
                //console.log('key', key);
                // Would need to read the key value and get past the prefix
                //var [t, prefix_key_length] = xas2.read(data.key);
                var arr_b_record = [data.key, data.value];
                var arr_decoded = encoding.decode(arr_b_record);

                running_means_per_second.increment(2);
                callback_data(null, arr_decoded);
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                //var e;
                //if (count === 0 && page === 0) {
                //e = {
                //	empty: true
                //}
                //} else {
                callback_end(null, 'end');
                //}
            });
    };

    var get_keys_within_int_prefix_key = (int_prefix_key, callback) => {

        console.log('get_keys_within_int_prefix_key', int_prefix_key);
        // will need to use a key reader iterator, through a range

        // Range starting at the timeline value initial key space


        //var timeseries_values_prefix_key = 1;


        //console.log('count_all_timeseries_values');
        //console.log('timeline_values_prefix_key', int_prefix_key);

        var x_prefix = xas2(int_prefix_key);


        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;
        //console.log('b_key_lt', b_key_lt);

        //throw 'stop';

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.

        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        //var obj_buffer_size = 1024;
        var arr_res = [];
        var page = 0;
        var prefix_key_length = x_prefix.length;

        db.createKeyStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function (key) {
                //console.log('key', key);
                arr_res.push(key);

                // Seems like the only time when removing the prefix key is appropriate?
                //  Though keeping it consistent so it works with record decoding means keeping the prefix key


                //var key_without_prefix_key = data.key.slice(prefix_key_length);

            }).on('error', function (err) {
                //console.log('Oh my!', err)
            })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                callback(null, arr_res);
            });
    };


    // Some of this would be much faster within C++
    var get_records_within_int_prefix_key = (int_prefix_key, callback) => {

        console.log('get_records_within_int_prefix_key', int_prefix_key);
        // will need to use a key reader iterator, through a range

        // Range starting at the timeline value initial key space


        //var timeseries_values_prefix_key = 1;


        //console.log('count_all_timeseries_values');
        //console.log('timeline_values_prefix_key', int_prefix_key);

        var x_prefix = xas2(int_prefix_key);


        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;
        //console.log('b_key_lt', b_key_lt);

        //throw 'stop';

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.

        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = [];
        var page = 0;
        var prefix_key_length = x_prefix.length;

        db.createReadStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function (data) {
                //console.log('key', key);
                arr_res.push([data.key, data.value]);

                // Seems like the only time when removing the prefix key is appropriate?
                //  Though keeping it consistent so it works with record decoding means keeping the prefix key


                //var key_without_prefix_key = data.key.slice(prefix_key_length);
                
            }).on('error', function (err) {
                //console.log('Oh my!', err)
            })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                callback(null, arr_res);
            });
    };


    // Some of this would be much faster within C++
    var streaming_paged_get_records_within_int_prefix_key = (int_prefix_key, callback) => {

        //console.log('streaming_get_within_int_prefix_key', int_prefix_key);
        // will need to use a key reader iterator, through a range

        // Range starting at the timeline value initial key space


        //var timeseries_values_prefix_key = 1;


        //console.log('count_all_timeseries_values');
        //console.log('timeline_values_prefix_key', int_prefix_key);

        var x_prefix = xas2(int_prefix_key);


        var b_key_gte = x_prefix.buffer;
        //console.log('b_key_gte', b_key_gte);
        var b_key_lt = xas2(int_prefix_key + 1).buffer;
        //console.log('b_key_lt', b_key_lt);

        // this is a function that could stream the result with multiple callbacks.
        //  it could identify the envelope as saying what event type it is.

        // ['data', ...]
        //  respond with both end and close events too.

        //  Can buffer up the data events though.

        //var buffer_size = 1024;

        //var arr_res_buffer = new Buffer(buffer_size);
        //var buffer_pos = 0;

        // Not even streaming all the keys back.
        //  No need for buffering.

        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = new Array(obj_buffer_size);
        var page = 0;
        var prefix_key_length = x_prefix.length;

        db.createReadStream({
            'gte': b_key_gte,
            'lt': b_key_lt
        })
            .on('data', function(data) {
                //console.log('key', key);

                // Seems like the only time when removing the prefix key is appropriate?
                //  Though keeping it consistent so it works with record decoding means keeping the prefix key


                //var key_without_prefix_key = data.key.slice(prefix_key_length);

                arr_res[count] = [data.key, data.value];


                // Counts in different scopes.
                count++;
                //c_out++;

                if (count === obj_buffer_size) {
                    // send the current buffer
                    // need to put it in an event
                    // a page / chunk / block
                    // can say what page it is
                    //
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'data': arr_res
                    }
                    callback(null, e);
                    //console.log('e', e);
                    //throw 'stop';
                    count = 0;
                }
            }).on('error', function (err) {
            //console.log('Oh my!', err)
        })
            .on('close', function () {
                //
                //console.log('Stream closed')
            })
            .on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                var e;
                if (count === 0 && page === 0) {
                    e = {
                        empty: true
                    }
                } else {
                    var partial_arr_res = arr_res.slice(0, count - 1);
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'last': true,
                        'data': partial_arr_res
                    }
                    callback(null, e);
                }
            });
    };


    var streaming_paged_get_all_keys = (callback) => {
        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = new Array(obj_buffer_size);
        var page = 0;

        //var prefix_key_length = x_prefix.length;

        db.createKeyStream({
        })
            .on('data', function (key) {
                //console.log('key', key);

                //var key_without_prefix_key = data.key.slice(prefix_key_length);

                arr_res[count] = key;


                // Counts in different scopes.
                count++;
                //c_out++;

                if (count === obj_buffer_size) {
                    // send the current buffer
                    // need to put it in an event
                    // a page / chunk / block
                    // can say what page it is
                    //
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'data': arr_res,
                        '__page_encoding': true
                    }
                    callback(null, e);
                    //console.log('e', e);
                    //throw 'stop';
                    count = 0;
                }
            }).on('error', function (err) {
                //console.log('Oh my!', err)
            }).on('close', function () {
                //
                //console.log('Stream closed')
            }).on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                var e;
                if (count === 0 && page === 0) {
                    e = {
                        empty: true
                    }
                } else {
                    var partial_arr_res = arr_res.slice(0, count - 1);
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'last': true,
                        'data': partial_arr_res,
                        '__page_encoding': true
                    }
                    callback(null, e);
                }
            });
    };

    // May be better to have the streamin here, and a paging function too.


    var streaming_paged_get_all_records = (callback) => {
        var count = 0;
        // And could use more specific functions to read specific data, or be given a function as a param.
        var obj_buffer_size = 1024;
        var arr_res = new Array(obj_buffer_size);
        var page = 0;

        //var prefix_key_length = x_prefix.length;

        db.createReadStream({
        })
            .on('data', function(data) {
                //console.log('key', key);

                //var key_without_prefix_key = data.key.slice(prefix_key_length);

                arr_res[count] = [data.key, data.value];


                // Counts in different scopes.
                count++;
                //c_out++;

                if (count === obj_buffer_size) {
                    // send the current buffer
                    // need to put it in an event
                    // a page / chunk / block
                    // can say what page it is
                    //
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'data': arr_res,
                        '__page_encoding': true
                    }
                    callback(null, e);
                    //console.log('e', e);
                    //throw 'stop';
                    count = 0;
                }
            }).on('error', function (err) {
            //console.log('Oh my!', err)
            }).on('close', function () {
                //
                //console.log('Stream closed')
            }).on('end', function () {
                //console.log('Stream ended')
                // callback with the last page
                var e;
                if (count === 0 && page === 0) {
                    e = {
                        empty: true
                    }
                } else {
                    var partial_arr_res = arr_res.slice(0, count - 1);
                    var e = {
                        'type': 'data',
                        'page': page++,
                        'last': true,
                        'data': partial_arr_res,
                        '__page_encoding': true
                    }
                    callback(null, e);
                }
            });
    };
    // streaming_get_transform_decoded_table_records

    var streaming_paged_get_transform_decoded_table_records = function(str_table_name, encoding, fn_transform, callback) {
        //console.log('str_table_name', str_table_name);
        //console.trace();
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('pre streaming_get_transform_decoded_records_within_int_prefix_key int_table_key_prefix', int_table_key_prefix);
                streaming_paged_get_transform_decoded_records_within_int_prefix_key(int_table_key_prefix, encoding, fn_transform, callback);
            }
        });
    };

    // decoding_iterator_by_table_name
    var decoding_iterator_by_table_name = function(str_table_name, encoding, callback_data, callback_end) {
        //console.log('str_table_name', str_table_name);
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('decoding_iterator_by_table_name int_table_key_prefix', int_table_key_prefix);
                decoding_iterator_by_int_prefix_key(int_table_key_prefix, encoding, callback_data, callback_end);
                //streaming_get_decoded_records_within_int_prefix_key(int_table_key_prefix, encoding, callback);
            }
        });
    };

    var streaming_paged_get_decoded_table_records = function(str_table_name, encoding, callback) {
        //console.log('str_table_name', str_table_name);
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('streaming_get_decoded_table_records int_table_key_prefix', int_table_key_prefix);
                streaming_paged_get_decoded_records_within_int_prefix_key(int_table_key_prefix, encoding, callback);
            };
        });
    };

    // get_table_names

    var get_table_names = function (callback) {



        get_table_key_prefix_by_name('tables', (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('* int_table_key_prefix', int_table_key_prefix);
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('streaming_get_table_records int_table_key_prefix', int_table_key_prefix);
                var res = [];
                get_records_within_int_prefix_key(int_table_key_prefix, (err, table_records) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('table_records', table_records);
                        each(table_records, (table_record) => {
                            //console.log('table_record', table_record);
                            //console.log('table_record[1].toString()', table_record[1].toString());
                            res.push(table_record[1].toString());
                        });
                    }
                    callback(null, res);

                });
            };
        });
    };

    var get_table_records = function (str_table_name, callback) {
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('streaming_get_table_records int_table_key_prefix', int_table_key_prefix);
                get_records_within_int_prefix_key(int_table_key_prefix, callback);
            };
        });
    }

    var get_table_index_records = function (str_table_name, callback) {
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('streaming_get_table_records int_table_key_prefix', int_table_key_prefix);
                get_records_within_int_prefix_key(int_table_key_prefix + 1, callback);
            };
        });
    }

    var streaming_paged_get_table_records = function(str_table_name, callback) {
        get_table_key_prefix_by_name(str_table_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                //console.log('streaming_get_table_records str_table_name', str_table_name);
                //console.log('streaming_get_table_records int_table_key_prefix', int_table_key_prefix);
                streaming_paged_get_records_within_int_prefix_key(int_table_key_prefix, callback);
            };
        });
    };

    var count_table = function(str_name, callback) {
        get_table_key_prefix_by_name(str_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {
                count_within_int_prefix_key(int_table_key_prefix, callback);
            }
        });
    };

    var index_lookup = function(int_index_prefix, int_index_id, b_index_value, callback) {
        // With a string value?
        //  xas2 should have a different default to loading string from hex?

        var b_index = Buffer.concat([xas2(int_index_prefix).buffer, xas2(int_index_id).buffer, b_index_value]);
        //console.log('b_index', b_index);
        db.get(b_index, (err, b_res) => {
            if (err) {
                //console.log(
                //console.log(typeof err);
                //console.log(err);
                //console.log('err.message ' + err.message);
                if (err.message.indexOf('Key not found in database') > -1) {
                    callback(null, -1);
                } else {
                    callback(err);
                };
                //throw err;
            } else {
                //console.log('index_lookup b_res', b_res);
                var res = xas2(b_res).number;
                //console.log('res', res);
                callback(null, res);
            }
        });
    };


    var get_table_index_keys = function (table_name, callback) {
        get_table_key_prefix_by_name(str_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {

                var int_index_key_prefix = int_table_key_prefix + 1;
                get_keys_within_int_prefix_key(int_index_key_prefix, callback);

                //count_within_int_prefix_key(int_poloniex_trades_table_key_prefix, callback);
            }
        });

    }

    var get_table_index_records = function (table_name, callback) {
        get_table_key_prefix_by_name(str_name, (err, int_table_key_prefix) => {
            if (err) {
                callback(err);
            } else {

                var int_index_key_prefix = int_table_key_prefix + 1;
                get_records_within_int_prefix_key(int_index_key_prefix, callback);

                //count_within_int_prefix_key(int_poloniex_trades_table_key_prefix, callback);
            }
        });
    }


    // --------------------------------------------------------------------------


    var get_i_datetime = function(callback) {
        // Should get the utc datetime.
        //  Not sure exactly how to get that in JavaScript.
        //   The difference between UTC and other dates could put the times out of sync.

        // It may be necessary to start a new database, with more care taken to the times
        //  So the trades we have in the DB could be stored under Canadian time, with server in Canada?

        callback(null, Date.now());
    }

    // Could do with more functions to either set up indexes, or to put data in place as well as to index the data.

    // put_with_idx_spec
    //  that way, when we put data into the database, we can specify how to index that specific data.
    //  specifying indexing alongside encoding may make sense.

    // Also want to read through table, decoding records.

    // Before long, will again have plenty of data going into the database.
    //  Will have tools to monitor it through client and web interfaces.






    var fns_ws = {
        // Function to ensure multiple keys
        //  Will use simultaneous calling of other functions.

        // get_timeseries_hex_keys
        // Just use arrayify?

        //'get_i_server_time': get_i_server_time,
        '_index_key': _index_key,

        'initial_db_setup': initial_db_setup,
        'starting_db_setup': starting_db_setup,
        'console_log_system': console_log_system,
        'output_transform_to_hex': output_transform_to_hex,

        'count_records': count_records,
        'get_all_keys': get_all_keys,
        'get_all_records': get_all_records,

        'get_keys_beginning': get_keys_beginning,
        'get_keys_ranging': get_keys_ranging,

        'get_i_datetime': get_i_datetime,
        'index_lookup': index_lookup,
        'ensure_table': ensure_table,
        'ensure_tables': ensure_tables,
        'count_table': count_table,
        'encode_new_id_put_record': encode_new_id_put_record,

        'get_table_key_prefix_by_id': get_table_key_prefix_by_id,
        'get_table_name_by_id': get_table_name_by_id,
        'get_table_id_by_name': get_table_id_by_name,
        'get_table_key_prefix_by_name': get_table_key_prefix_by_name,

        // get_table_names
        // get_table_key_prefixes

        'get_table_names': get_table_names,

        // Get table table records...



        // get records within table
        'get_table_records': get_table_records,
        'get_table_index_records': get_table_index_records,
        'get_keys_within_int_prefix_key': get_keys_within_int_prefix_key,
        'get_records_within_int_prefix_key': get_records_within_int_prefix_key,

        'streaming_paged_get_all_keys': streaming_paged_get_all_keys,
        // Get the table table records

        'streaming_paged_get_table_records': streaming_paged_get_table_records,
        'streaming_paged_get_all_records': streaming_paged_get_all_records,
        'streaming_paged_get_records_within_int_prefix_key': streaming_paged_get_records_within_int_prefix_key,

        'streaming_paged_get_decoded_table_records': streaming_paged_get_decoded_table_records,
        'streaming_paged_get_transform_decoded_table_records': streaming_paged_get_transform_decoded_table_records,
        'decoding_iterator_by_table_name': decoding_iterator_by_table_name,
        'decoding_iterator_by_key_range': decoding_iterator_by_key_range,
        'decoding_iterator_record_by_key_range': decoding_iterator_record_by_key_range,
        'put_id_by_string_index_record_tkp': put_id_by_string_index_record_tkp,

        //'subscribe_to_puts': subscribe_to_puts
    };

    Object.assign(nextleveldb_server.fns_ws, fns_ws);

    /*
    nextleveldb_server.get_categorised_timeseries_record_by_category_id_by_time_lte = get_categorised_timeseries_record_by_category_id_by_time_lte;
    nextleveldb_server.get_categorised_timeseries_record_value_by_category_id_by_time_lte = get_categorised_timeseries_record_value_by_category_id_by_time_lte;
    nextleveldb_server.get_categorised_timeseries_record_time_by_category_by_time_lte = get_categorised_timeseries_record_time_by_category_by_time_lte;
    nextleveldb_server.get_categorised_timeseries_record_time_by_table_name_category_by_time_lte = get_categorised_timeseries_record_time_by_table_name_category_by_time_lte;
    nextleveldb_server.streaming_get_timeseries_categorised_records_between_i_times = streaming_get_timeseries_categorised_records_between_i_times;
    nextleveldb_server.streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms;
    nextleveldb_server.streaming_get_timeseries_categorised_records_value_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_value_between_i_times_at_boundary_interval_n_ms;
    nextleveldb_server.streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms;
    */



    //nextleveldb_server.streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms;

};
module.exports = merge_plugin;