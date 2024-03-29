const jsgui = require('lang-mini');
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

//const fs2 = jsgui.fs2;
const handle_ws_binary = require('./handle-ws-binary');
//var Binary_Encoding = require('binary-encoding');
const Binary_Encoding = require('binary-encoding');
const Binary_Encoding_Record = Binary_Encoding.Record;

const recursive_readdir = require('recursive-readdir');
const Running_Means_Per_Second = require('./running-means-per-second');

const levelup = require('levelup');

const Model = require('nextleveldb-model');

const fnl = require('fnl');
const fnlfs = require('fnlfs');
const observable = fnl.observable;

const B_Record_List = Model.Record_List;
const B_Record = Model.BB_Record;
const B_Key = Model.BB_Key;
const Key_List = Model.Key_List;

const Model_Database = Model.Database;
const encoding = Model.encoding;
const Index_Record_Key = Model.Index_Record_Key;

const CORE_MIN_PREFIX = 0;
const CORE_MAX_PREFIX = 9;

// looks like this can go in fnl
//  but it maybe changed and generalised and optimised in some ways.
//  may have a call function

// promise => observable


// observables list
//  This may be tough to generalise, but could try.

// A queue of observables, exewcuted sequentially.

// seq_obs
// seq


const execute_q_obs = fnl.seq;
const NextlevelDB_Core_Server = require('./nextleveldb-core-server');

const obs_to_cb = (obs, callback) => {
    let arr_all = [];
    obs.on('next', data => arr_all.push(data));
    obs.on('error', err => callback(err));
    obs.on('complete', () => callback(null, arr_all));
}

const obs_or_cb = (obs, callback) => {
    if (callback) {
        obs_to_cb(obs, callback);
    } else {
        return obs;
    }
}

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


let obs_arrayified_call = (caller, fn, arr_params) => {
    // Just execure one at a time.
    let res = new Evented_Class();

    //throw 'stop';

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


// bufs_lu = (buf_kp)

// Inherit from core-server
//  Will then use mixin functionality




class NextLevelDB_Server extends NextlevelDB_Core_Server {
    constructor(spec) {
        super(spec);
        //this.db_path = spec.db_path;
        //this.port = spec.port;
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

    // get this from the model.
    //  the model is isomorphic.



    get table_ids_with_indexes() {
        return this.model.table_ids_with_indexes;
    }

    // start is in core

    // Ensure maybe won't be core.
    //  Makes use of has and put. Has logic implemented on top of core.


    //ensure_record, ensure_table in the core now.

    // active_table object
    //  would have various functions given to it / loaded here.


    // put function
    //  and then would check that any records actually refer to this one?
    //  used to encode records

    // .records
    //  an async iterator?

    //
    // for (record of active_table.records) ...
    //  looks like that would take some more buffered paging.
    //  so it pages to the active_table, and keeps a buffer populated with results.
    //   different ways of doing this I'm sure.


    // get table records by keys
    //  not giving the full key.
    //   need to be able to give an undefined key prefix.
    //   sending keys to the server - if they are binary, they lack a kp. Or it encodes it to an array, they lack KPs.
    //   The command_message could decode them OK as buffers.


    // A variety of ll functions will have a lot more complexity involving observable results, flexible calling, polymorphism, calling of optimised inner functions.
    // Then get records by prefix limit....

    // Maybe we would want to specify limit = 0 meaning it's -1 in level terms.
    //  We just won't use limit of actually 0 records... just don't do the query.

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

    // advanced get?

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
        } else if (sig === '[n]') { } else {
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
        });

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


    // could use an observable creator function.
    //  the handler gets called with 3 functions available for it to call.


    // can just be get_records or get_records_by_keys
    //  observable is becoming more standard.





    // into safety / checking
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
        // could try using an iterator.
        // for (let key_not_found of this.obs_records_not_found(arr_index_records))
        //  maybe that would need a blocking queue
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
    }

    // increment_incrementor (incrementor_id)


    // Could ensure multiple tables with one command from the client.
    //  Would need to encode the table definitions on the client, and send them to the server.
    //  The server having its own copy of the model makes it more efficient.





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

    // selection handlers
    //  done after the records are found.

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


    // get a record, with all of the referring records.

    // // get record by key with the associated records

    // will also want to batch various requests like this.

    // get the associated fk records from a record.
    //  we have the record key.
    //  need to then find records which refer to it.

    // refer to the record's model table, then refer to the db model.
    //  find fks that point to it.
    //  then with double associative records would need to find the other record that they point to.



    // want parameter parsing to be much shorter.
    //  don't have decode, callback is optional.
    //  only 2 or 3 params.


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
                //
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

    fnlfs.ensure_directory_exists(user_dir + '/NextLevelDB', (err, exists) => {
        if (err) {
            throw err
        } else {
            fnlfs.ensure_directory_exists(path_dbs, (err, exists) => {
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

                            let test_observe_table_records = () => {
                                let o_all = ls.get_all_table_records();
                                o_all.next(data => {
                                    console.log('data', data);
                                    //throw 'stop';
                                });

                                // OK, so delaying works.
                                //  It's more like a direct wrapper to the data source, an observable data source, 

                                setTimeout(() => {
                                    o_all.delay(5000);
                                }, 3000);
                            }

                            let test_get_malformed_index_records = () => {
                                console.log('test_get_malformed_index_records');
                                // but that's in safety.
                                //  it's a much easier function now though.

                                //let obs_iir = ls.get_in
                            }
                            //test_get_malformed_index_records();

                            let test_get_all_index_records = () => {

                                obs_ir = ls.get_all_index_records();
                                obs_ir.on('next', data => {
                                    console.log('get_all_index_records data', data);
                                });
                            }
                            //test_get_all_index_records();

                            // view tables and fields
                            let show_tables = () => {
                                console.log('ls.model.description\n', ls.model.description);
                            }
                            show_tables();

                            // looks good so far.
                            // let's go through every record.


                            // //observe(ls.get_all_records, )

                            //ls.get_all_records.next(data => ...).catch(error => ...).complete(() => ...)
                            //ls.get_all_records.data(data => ...).error(error => ...).done(() => ...)


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

                            // let decode = true;

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

                            /*

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

                            */


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