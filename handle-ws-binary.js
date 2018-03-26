/**
 * Created by james on 27/12/2016.
 * 
 * Could possibly load some other modules, like time series handling and encoding.
 * Probably best to write moe code here for the moment.
 
 Will extend the server, using the model. That means that a variety of extensions made possible with the models will be available on the server.
Having the model client-side enables queries to be encoded effectively.
Making select - where type queries would be effective, in that OO classes can be used to construct lower level key searches.


Could also do with some lower level monitoring functions
How many records in each table
How many index records

Getting paging working soon will be useful.
Want to send a number of individual messages back to the client.
 
A higher level binary handling system would be of 


Handling more complex and thorough server queries would make sense, eg to get a load of info about tables all at once, including record counts.

The core of the DB could do with a nice bit of improvement, allowing for functionality that would have taken multiple calls to be done at once.



// Number of records in each table being returned alongside the table names.


// Getting the table names
//  Though that means reading it from the server-side model.

// Should generally set the table field types.

 
 */




var jsgui = require('lang-mini');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
let def = jsgui.is_defined;
var Fns = jsgui.Fns;

var x = xas2 = require('xas2');

// While these will be some work, they will enable a variety of data to be quickly moved in and out of the DB.

var Binary_Encoding = require('binary-encoding');
var Model = require('nextleveldb-model');
var Model_Database = Model.Model_Database;

// Could have paged versions of these instructions too?
//  Building paging into the relevant instructions would make sense, so that we don't have too many instructions, with different versions.


const NO_PAGING = 0;
const PAGING_RECORD_COUNT = 1;
// Followed by p number
const PAGING_BYTE_COUNT = 2;
const PAGING_TIMER = 3;

// Followed by p number


// A streaming ll get all records would be of use too.
//  Espcially when starting with 0 records and wanting to clone a remote DB.

// Keeping track of the other DBs that the DB is connected to would be a very useful thing.
//  A peers table
//   Could show a source db

// Wipe and replace with a remote DB
//  That's one instruction

// To start with, a streaming and paging get_all_records will be useful.


// A blockchain-like system would be very useful for ensuring all of the records get copied over.
//  Could keep hashes of received data pages.
//  The data would be sent in predictable pages, if they are already on the client, they don't need to be copied.

// Data blocks.
//  This would be a way to preserve records that have been stored in the DB. Generally will be used on historic records.
//   A data block will have a load of keys in the block which will be available on request. Also the keys and values are available through lookup.
//    Could have the hash of the keys and values, as well as the keys hash.
//    Each record could have a hash.
//    
// Then there could be a collection of data blocks.
//  Would not care so much about the order of blocks?
//   Though could arange them as a blockchain.

// Blockchain copy would have some setup time, as it goes through the records, but once it is running, we get better feedback on the progress of the copy.
//  It's a nice way to verify the copying of data.


// Could create a blockchain out of a series of ordered db records, and use that to transfer the data to another db in a way where the data intrgrity can be checked in such a way that we know
//  the records synced correctly match the records from the original DB.

// DB properties will be useful for this.








// All low level functions, operating on the core db.
// Higher level functions could do things like put a record, where the db system automatically creates the index records too.

// A way to LL put a single record?


// Will do some significant expansion of the lower level capabilities.


// A wider variety of basic functions, and continuing the improved streaming of existing functionality.

// There will be more functionality in the standard server codebase, upon which an API could make it available to clients, which would make that API available locally.
//  Want structure and index verification available server-side.
//  Still stuck on problems to do with adding, ensuring and looking up Bittrex currencies :(
//  Need to be totally sure that these records are added and indexed properly.

// Will do more on server-side testing and copying of databases.
//  Want it so that a server can copy over records from another server, with ongoing progress indications.


// Before DB records are copied over, it could verify that it's the same table structure.
//  I expect the copying of records will work quickly, but I want to see and measure how many MB/s it goes at, and how many records/s get transferred.





/*

LL_GET_TABLE_ID_BY_NAME
LL_GET_TABLE_KP_BY_NAME
LL_GET_TABLE_IKP_BY_NAME

These will have versions in the nextleveldb-server codebase, and here the remote calling API will be provided.

table_index_value_lookup
TABLE_INDEX_VALUE_LOOKUP


*/





// 05/03/2018 - Will have much more functionality moved into this lower level part, because this part handles the communications, and that is becoming more complicated / advanced.

// Ensuring tables
//  Getting the index records from a named table








const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_ALL_RECORDS = 3;



const LL_GET_KEYS_IN_RANGE = 4;
const LL_GET_RECORDS_IN_RANGE = 5;




const LL_COUNT_KEYS_IN_RANGE = 6;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 7;
const LL_GET_RECORD = 8;

// Limited to a certain number of items
const LL_COUNT_KEYS_IN_RANGE_UP_TO = 9;
const LL_GET_RECORDS_IN_RANGE_UP_TO = 10;


// Or have further handlers in other files?





// Has paging option.
//  Number of records per page
//  Number of bytes per page

// Needs to delete the database from disk and replace it.
//  Would mean replacing a calling object I think.
//  Or call a method in it.



const LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH = 11;
//const LL_PUT_RECORD_INDEX_IT = 12;


// Could make 12 polymorphic so it checks if it is given an array?
const INSERT_TABLE_RECORD = 12;
const INSERT_RECORDS = 13;


// Definitely want it taking in plenty of records per second, and saving them to the DB.
//  The DB would then send the data on to any subscribers.
//  The DB would also enable rapid download of historical data in a convenient format / various convenient formats
//  The DB would enable data items that contain historical data but also have live data updates, from the db, with low latency.



// For the moment, need to make absolutely certain that the bittrex currencies and markets have been set up correctly.
//  Then would need to ensure that the trades / order books / market snapshots data tables have been set up correctly.
//  Then need to collect and save the data.
//   Do this with multiple nodes.
//   Set up a node / nodes that collects data from these existing nodes.
//    Run backups from that node.
//     Verify those backups, and copy them to other drives.
//     Copy those backups to cloud storage too.
//      Work on restoring a new database from the cloud backups.



// Think we need get the data automatically indexed on the server.

// Could have a lower level db function to index a table.



//  Worth using the model for this.




const ENSURE_TABLE = 20;
const ENSURE_TABLES = 21;
const TABLE_EXISTS = 22;
const TABLE_ID_BY_NAME = 23;
// RENAME_TABLE
const GET_TABLE_FIELDS_INFO = 24;









// const LL_GET_SYSTEM_MODEL = 10

// The system model is tables 0 to 8 I think.

// Load model from data on the server
//  That would then enable data to be indexed as its put into the database.

// Could still use the relatively complex data set to do with bittrex.
//  Quite a lot of records here.


// LL_PUT_RECORD_INDEX_IT

// LL_PUT_RECORD_NO_OVERWRITE
//  Needs to check against the unique keys



// There are advantages to keeping a smaller LL api.
//  Perhaps I could make a level 2 of the API?

// Or just stick to the essentials to do with indexing records?
//  Remember - want this to be able to take plenty of data from multiple exchanges quickly.

// Some operations would be simpler without requiring client-side use of a db model.
//  Updating the database using indexed records and preventing overwrites where necessary will help.






// Some higher level lower level functions?
//  Put records using table indexes?







const LL_SUBSCRIBE_ALL = 60;
const LL_SUBSCRIBE_KEY_PREFIX_PUTS = 61;
const LL_UNSUBSCRIBE_SUBSCRIPTION = 62;


// Misc operations


// Then the subscription messages send back data that's been put into the database / commands that have been done on the db.

const LL_WIPE = 100;
const LL_WIPE_REPLACE = 101;



// Subscribe
//  Subscribe by key prefix
//   Would mean extra processing in batch puts.
//    Would mean scanning them to see which subscribers they should be sent to.

const SUB_CONNECTED = 0;
const SUB_RES_TYPE_BATCH_PUT = 1;

var map_subscription_event_types = {
    'connected': SUB_CONNECTED,
    'batch_put': SUB_RES_TYPE_BATCH_PUT

}



// Then could have client app that downloads all of the data / the recent data, then subscribes to updates.
//  Also worth making use of local leveldb server.
//  A local client would be really useful for this. Could keep a local db synced with a remote one.
//   Use the local db for startup, so that it does not need to download all the data from a server.

// Short term algorythms or decisions...
//  Need to make use of data from subscriptions.
// Subscriptions to the latest data seems like one of the most important features.

// subscribe (buf kp)
//  returns a subscription id (unique per client)
//  would work a bit like paging, in that it keeps returning subscription messages to the client.
//   Each subscription message is numbered.

// unsubscribe (client's subscription id).


// Want to load a relatively large dataset to the client, have it use underlying typed arrays.
//  For the moment, may deal with megabytes of data.









// Optional parameters could help...







const BOOL_FALSE = 6;
const BOOL_TRUE = 7;

const NULL = 8;


var client_subscriptions = {};


// Can include a NO_PAGING flag on many responses.
//  Could do it on all responses, but it would make them take a tiny bit more space.
//  The same receiver function could pick them up though.

// Message Type indicators.
//  All messages will have the message type indicator, though it could be optional.
//   For the moment, the client will expect it.







const BINARY_PAGING_NONE = 0;
const BINARY_PAGING_FLOW = 1;
const BINARY_PAGING_LAST = 2;

const RECORD_PAGING_NONE = 3;
const RECORD_PAGING_FLOW = 4;
const RECORD_PAGING_LAST = 5;
const RECORD_UNDEFINED = 6;

// A whole message type for undefined record?

const KEY_PAGING_NONE = 7;
const KEY_PAGING_FLOW = 8;
const KEY_PAGING_LAST = 9;

// Simplest error message.
//  Could have a number, then could have encoded text.
//  
const ERROR_MESSAGE = 10;


// Message Types (structure involving paging)
const buf_binary_paging_none = xas2(BINARY_PAGING_NONE).buffer;
const buf_binary_paging_flow = xas2(BINARY_PAGING_FLOW).buffer;
const buf_binary_paging_last = xas2(BINARY_PAGING_LAST).buffer;

const buf_record_paging_none = xas2(RECORD_PAGING_NONE).buffer;
const buf_record_paging_flow = xas2(RECORD_PAGING_FLOW).buffer;
const buf_record_paging_last = xas2(RECORD_PAGING_LAST).buffer;


const buf_key_paging_none = xas2(KEY_PAGING_NONE).buffer;
const buf_key_paging_flow = xas2(KEY_PAGING_FLOW).buffer;
const buf_key_paging_last = xas2(KEY_PAGING_LAST).buffer;

const return_message_type = true;


// When returning a lot of keys at once (or maybe just one key?), will use key paging options.
//  They are really about specific encoding for transmitting sets of keys.


// Need to upgrade and test the client and server side code for transmitting sets of keys, records, and other binary data.





var handle_ws_binary = function (connection, nextleveldb_server, message_binary) {

    // Need error handling.
    //  Need some way of indicating error in the response.
    //  1 byte seems too much to indicate this as a flag, but it could be OK. Not sure of a simple way to do anything different...

    // Except reserve 0 as an id for errors. Would make the normal responses shorter.
    //  Don't accept 0 as a message ID.
    //   Could return an error saying 0 is reserved for errors.

    // Which connection is it coming from?
    //  Assign a connection id.

    var pos, buf_res;
    //bytes_in_this_second = bytes_in_this_second + message.utf8Data.length;

    // We may have access to a working model of the system db.
    //  That could help with some more complex queries.
    // For the moment, want it to handle very simple queries.

    // In the near future, this will store plenty of info.
    //  Will maybe be very high performance with all the buffer usage.
    var db = nextleveldb_server.db;
    var message_id, i_query_type, pos = 0;
    [message_id, pos] = x.read(message_binary, pos);
    [i_query_type, pos] = x.read(message_binary, pos);

    var buf_the_rest = Buffer.alloc(message_binary.length - pos);
    message_binary.copy(buf_the_rest, 0, pos);
    var buf_msg_id = xas2(message_id).buffer;


    // This would be a point to read the auth token, if there is one.
    //  Auth will first be done on connection, so an access token would not need to be provided with each websocket request
    //   - Except that would be an extra security option.


    if (i_query_type === LL_WIPE) {
        console.log('LL_WIPE');
        nextleveldb_server.ll_wipe((err, db) => {
            if (err) {
                throw err;
            } else {
                // Something for a buffer just saying true?

                if (return_message_type) {
                    // Though paging is an option, this is not a paged or pagable response.
                    buf_res = Buffer.concat([xas2(message_id).buffer, buf_binary_paging_none, xas2(BOOL_TRUE).buffer]);
                } else {
                    buf_res = Buffer.concat([xas2(message_id).buffer, xas2(BOOL_TRUE).buffer]);
                }
                //console.log('buf_res', buf_res);
                connection.sendBytes(buf_res);

            }
        });
    }

    // wipe replace
    //  does ll wipe, then puts records.
    //   verify the records first?

    // Not transactional

    if (i_query_type === LL_WIPE_REPLACE) {
        console.log('LL_WIPE_REPLACE');
        nextleveldb_server.ll_wipe((err, db) => {
            if (err) {
                throw err;
            } else {
                // Something for a buffer just saying true?

                // Possible to page? Not this one now.

                nextleveldb_server.batch_put(buf_the_rest, (err, res_batch_put) => {
                    if (err) {
                        throw err;
                    } else {

                        if (return_message_type) {
                            // Though paging is an option, this is not a paged or pagable response.
                            buf_res = Buffer.concat([xas2(message_id).buffer, buf_binary_paging_none, xas2(BOOL_TRUE).buffer]);
                        } else {
                            buf_res = Buffer.concat([xas2(message_id).buffer, xas2(BOOL_TRUE).buffer]);
                        }



                        //console.log('buf_res', buf_res);
                        connection.sendBytes(buf_res);
                    }
                });
            }
        });
    }

    // Paging could be further enabled in various functions by looking at the paging options.
    //  Could become standard on more functions.

    if (i_query_type === LL_COUNT_RECORDS) {
        // Count all records
        console.log('LL_COUNT_RECORDS');

        // Count could return ongoing counts when the paging option is selected.
        //  A paging type that returns timed ongoing responses would be nice, meaning every second or whenever, a 'page' message goes to the client, updating the status of the ongoing operation.

        // This can use paging options.

        //console.log('buf_the_rest.length', buf_the_rest.length);


        if (buf_the_rest.length === 0) {
            nextleveldb_server.ll_count((err, count) => {
                if (err) {
                    throw err;
                } else {
                    //console.log('count', count);
                    //console.log('return_message_type', return_message_type);

                    // This could be done with paging.
                    //  Could return a record with each page.
                    //  Could be different to a normal page if necessary.

                    if (return_message_type) {
                        // Though paging is an option, this is not a paged or pagable response.
                        buf_res = Buffer.concat([xas2(message_id).buffer, buf_binary_paging_none, xas2(count).buffer]);
                    } else {
                        buf_res = Buffer.concat([xas2(message_id).buffer, xas2(count).buffer]);
                    }

                    //console.log('buf_res', buf_res);
                    connection.sendBytes(buf_res);
                }
            });
        } else {
            // Look at the paging buffer.
            //console.log('buf_the_rest', buf_the_rest);

            let pos = 0,
                page_number = 0,
                paging_option, delay;
            [paging_option, pos] = x.read(buf_the_rest, pos);

            //console.log('paging_option', paging_option);

            if (paging_option === PAGING_TIMER) {

                // Will keep track of the count as it goes on.
                //  A function to do the count with timed callbacks?
                //  Maybe observables would be better than them anyway.

                // Call the server count function with callbacks (or maybe an observable.)

                console.log('LL_COUNT_RECORDS, timed paging');


                [delay, pos] = x.read(buf_the_rest, pos);
                console.log('delay', delay);

                // then use the observable interface to ll_count.

                let obs_count = nextleveldb_server.ll_count(delay);
                console.log('obs_count', obs_count);

                let n_page = 0;

                obs_count.on('next', (e => {
                    let count = e.count;

                    //buf_res = Buffer.concat([xas2(message_id).buffer, buf_binary_paging_last, xas2(count).buffer]);
                    //connection.sendBytes(buf_res);

                    console.log('count', count);
                }))
                obs_count.on('complete', (count => {
                    console.log('complete count', count);

                    // Last page / complete page.

                    buf_res = Buffer.concat([xas2(message_id).buffer, buf_binary_paging_last, xas2(page_number++).buffer, xas2(count).buffer]);
                    connection.sendBytes(buf_res);

                    // Then output this to the client.




                }));

            }



            //throw 'NYI';
        }




    }

    // Lower level put records, where it puts it directly into the db

    // Want another put records where it also creates the relevant index values.

    if (i_query_type === LL_PUT_RECORDS) {
        // Need to parse the rest of the message as an array of records.
        //  This is where the model would be somewhat useful.
        //  Other queries, that make use of indexing, or look up names or other values from foreign keys, would benefit further from having a working Model instance.

        // batch_put
        console.log('LL_PUT_RECORDS');

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        //throw 'stop';

        // Not sure if server batch_put needs more upgrades.



        if (buf_the_rest.length > 0) {
            nextleveldb_server.batch_put(buf_the_rest, (err, res_batch_put) => {
                if (err) {
                    throw err;
                } else {
                    if (return_message_type) {
                        // Though paging is an option, this is not a paged or pagable response.
                        buf_res = buf_res = Buffer.concat([buf_msg_id, buf_binary_paging_none, xas2(BOOL_TRUE).buffer]);
                    } else {
                        buf_res = buf_res = Buffer.concat([buf_msg_id, xas2(BOOL_TRUE).buffer]);
                    }
                    //console.log('buf_res', buf_res);
                    connection.sendBytes(buf_res);
                }
            })
        } else {
            connection.sendBytes(buf_res);
        }


    }




    if (i_query_type === LL_COUNT_KEYS_IN_RANGE) {
        console.log('LL_COUNT_KEYS_IN_RANGE');

        // Could return the ongoing count when using paging.

        // get the rest of the buffer.
        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // then get these separate buffers.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        // then make a new buffer, having read paging
        //console.log('pos', pos);

        //var buf_2 = Buffer.from(buf_the_rest, pos);
        var buf_2 = Buffer.alloc(buf_the_rest.length - pos);
        buf_the_rest.copy(buf_2, 0, pos);

        var s_buf = Binary_Encoding.split_length_item_encoded_buffer(buf_2);

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_2', buf_2);

        //console.log('s_buf', s_buf);

        // then the count range query with those

        if (paging_option === NO_PAGING) {

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_binary_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }
            var count = 0;
            //var res = [];
            db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function (key) {
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);
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
                    //callback(null, res);
                    //console.log('*** count', count);

                    arr_res.push(xas2(count).buffer);

                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                });
        }
    }


    // 



    // LL_COUNT_KEYS_IN_RANGE_UP_TO

    if (i_query_type === LL_COUNT_KEYS_IN_RANGE_UP_TO) {
        console.log('LL_COUNT_KEYS_IN_RANGE_UP_TO');

        // get the rest of the buffer.
        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // then get these separate buffers.


        // Fully decoding the input buffer could make sense.



        var paging_option, page_size, count_limit;

        pos = 0;



        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        [count_limit, pos] = x.read(buf_the_rest, pos);

        //console.log('count_limit', count_limit);

        // then make a new buffer, having read paging
        //console.log('pos', pos);

        //var buf_2 = Buffer.from(buf_the_rest, pos);
        var buf_2 = Buffer.alloc(buf_the_rest.length - pos);
        buf_the_rest.copy(buf_2, 0, pos);

        var s_buf = Binary_Encoding.split_length_item_encoded_buffer(buf_2);

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_2', buf_2);

        //console.log('s_buf', s_buf);



        // then the count range query with those

        if (paging_option === NO_PAGING) {

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_binary_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }
            var count = 0;

            //let reached_limit = false;


            //var res = [];
            let stream = db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function (key) {
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);



                    count++;

                    if (count > count_limit) {
                        stream.destroy();
                    }
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')

                    //arr_res.push(xas2(count).buffer);
                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);


                    arr_res.push(xas2(count).buffer);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);

                })
                .on('end', function () {
                    //callback(null, res);
                    //console.log('*** count', count);

                });
        }
    }



    // call the createKeyStream but be able to cancel it.
    //  Possibly more advanced iterators could be used.





    // LL_GET_FIRST_LAST_KEYS_IN_RANGE

    if (i_query_type === LL_GET_FIRST_LAST_KEYS_IN_RANGE) {
        console.log('LL_GET_FIRST_LAST_KEYS_IN_RANGE');

        // get the rest of the buffer.
        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // then get these separate buffers.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        // then make a new buffer, having read paging
        //console.log('pos', pos);

        //var buf_2 = Buffer.from(buf_the_rest, pos);
        var buf_2 = Buffer.alloc(buf_the_rest.length - pos);
        buf_the_rest.copy(buf_2, 0, pos);

        var s_buf = Binary_Encoding.split_length_item_encoded_buffer(buf_2);

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_2', buf_2);

        //console.log('s_buf', s_buf);

        // then the count range query with those

        if (paging_option === NO_PAGING) {
            //var arr_res = [buf_msg_id];

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_binary_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }
            //var count = 0;

            var first;
            var last;

            //var res = [];
            db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function (key) {
                    if (!first) first = key;
                    last = key;
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);
                    //count++;
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    //console.log('*** count', count);

                    // encode 2 values basically.
                    var encoded_res = Binary_Encoding.join_buffer_pair([first, last]);
                    console.log('encoded_res', encoded_res);

                    arr_res.push(encoded_res);

                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                });
        }
    }

    // LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE

    //  Think this will be used to show there are at least 2 records there?
    // Count between keys actually?




    if (i_query_type === LL_GET_ALL_KEYS) {
        console.log('LL_GET_ALL_KEYS');
        // Need to parse the rest of the message as an array of records.
        //  This is where the model would be somewhat useful.
        //  Other queries, that make use of indexing, or look up names or other values from foreign keys, would benefit further from having a working Model instance.
        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }
        // Could feed through a paging function that batches the results.
        // batch_put

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // Need to encode each key separately.
        //  Say how long the key is in bytes, then write that key buffer.
        //  Some kind of buffer vector while building?
        //   How to compose the whole thing in memory reasonably efficiently?
        //   Put the buffers in a vector...

        //console.log('paging_option', paging_option);
        if (paging_option === NO_PAGING) {
            //var arr_res = [buf_msg_id];

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_key_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }

            //var res = [];
            db.createKeyStream({}).on('data', function (key) {
                    arr_res.push(x(key.length).buffer);
                    arr_res.push(key);
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }


        if (paging_option === PAGING_RECORD_COUNT) {
            page_records_max = page_size;
            let arr_page = new Array(page_records_max);

            let c = 0,
                page_number = 0,
                arr_res, buf_combined;
            db.createKeyStream({}).on('data', (key) => {
                    arr_page[c++] = Buffer.concat([xas2(key.length).buffer, key]);
                    if (c === page_records_max) {
                        connection.sendBytes(Buffer.concat([buf_msg_id, buf_key_paging_flow, xas2(page_number++).buffer].concat(arr_page)));
                        c = 0;
                        // Could empty that array, would that be faster than GC?
                        arr_page = new Array(page_records_max);
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
                    //arr_res = ;
                    //buf_res = ;
                    connection.sendBytes(Buffer.concat([buf_msg_id, buf_key_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c))));
                })
        }




    }

    // Could try this without managing our own paging
    //  However, the pages could be given hash values
    //   And made into a bit of a blockchain

    // Copying / streaming a DB over the network should be relatively fast.

    if (i_query_type === LL_GET_ALL_RECORDS) {
        console.log('LL_GET_ALL_RECORDS');
        // Need to parse the rest of the message as an array of records.
        //  This is where the model would be somewhat useful.
        //  Other queries, that make use of indexing, or look up names or other values from foreign keys, would benefit further from having a working Model instance.
        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        console.log('paging_option', paging_option);
        console.log('page_size', page_size);
        // Could feed through a paging function that batches the results.
        // batch_put

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // Need to encode each key separately.
        //  Say how long the key is in bytes, then write that key buffer.
        //  Some kind of buffer vector while building?
        //   How to compose the whole thing in memory reasonably efficiently?
        //   Put the buffers in a vector...

        let page_records_max, page_bytes_max;

        //console.log('paging_option', paging_option);

        // Option to include a 'not paged' value when we are not using paging.
        //  Currently not expecting one.

        if (paging_option === NO_PAGING) {
            //var arr_res = [buf_msg_id];

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_record_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }
            //var res = [];
            db.createReadStream({}).on('data', function (data) {
                    let buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    //console.log('buf_combined', buf_combined);
                    arr_res.push(buf_combined);
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)

                    // Should have a way of sending failure messages back to the client.
                    //  Having a return message type seems essential for error handling.
                    //   Though could have some binary sequence which would only be for errors, eg [!ERROR!], and check for that in the messages when not using return_message_type



                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }


        // Can we assume that the paging is selected and expected with this?
        //  Saying if its a paging flow or paging last message. Paging blockchain will be an interesting answer too. Will create a blockchain data structure that is not necessarily distributed, but it will set it up for
        //  distribution. For the moment will treat a blockchain as a (distributed) data structure that won't carry and ensure a limited supply of money-like values with POW or anything. Will be more like a chain of validation
        //  and will not cover what is meant in the full meanin of 'blockchain', at least to start with. The system will generate blockchains from the database, then with the data as a blockchain it will be able to be distributed
        //  with the downloading of conveniently small blocks, as well as with the validation to ensure that all blocks have been copied to the other computers.


        if (paging_option === PAGING_RECORD_COUNT) {
            //[page_records_max, pos] = x.read(buf_the_rest, pos);
            //console.log('page_records_max', page_records_max);

            page_records_max = page_size;

            // array of result items up to that size.
            //  then create the buffer to send back for that page.

            // Need to look at the client to see how it deals with paged responses.
            //  Will need some kind of encoding to say that it's a page, what the page number is, and then the data in the page.

            // This will be useful to get one db to download the contents of another db.
            //var arr_res = [buf_msg_id];

            // Then have the page number in each response.
            //  What to indicate it's the last page?
            //  Could have a boolean value to say if it's the last page after the page number (another 1 byte).

            // Then in each page's result we include: paging type (signal including the last page), page number.
            //  Then it is that page's binary data that gets through.

            // When handling the last page on the client, it will remove the event handler.

            // For the moment, need to get it able to pump out its (snapshot) records quickly.
            //  Paging will be useful for showing the ongoing progress of a data download.

            // Could be preceeded by a count.

            // Blocks of keys will also be useful for counting records.
            //  We could quickly count how many records have been recorded into blocks.
            //  Blocks could also carry info such as the recorded record size.
            //  It will definitely make it easier in the long run to download and store the data, connecting to possibly multiple DBs, and confirming that all of the completed blocks from those other DBs have
            //  been correctly copied into a specific db.

            // 1MB block size seems OK here (to start with). Seems like a convenient size right now.

            let arr_page = new Array(page_records_max);

            let c = 0,
                page_number = 0,
                arr_res, buf_combined;
            db.createReadStream({}).on('data', (data) => {
                    buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    //console.log('buf_combined', buf_combined);
                    // include it into the paged records.
                    arr_page[c++] = buf_combined;

                    if (c === page_records_max) {
                        // Send back a page
                        //  Would need to encode the page, try using binary encoding to put a bunch of records together.
                        // encoding the records together.
                        //  can concat the separate records together as with non-paging.
                        //console.log('page_number', page_number);

                        //arr_res = [buf_msg_id, buf_record_paging_flow, xas2(page_number++).buffer].concat(arr_page);
                        //buf_res = Buffer.concat(arr_res);
                        //connection.sendBytes(buf_res);

                        connection.sendBytes(Buffer.concat([buf_msg_id, buf_record_paging_flow, xas2(page_number++).buffer].concat(arr_page)));

                        //console.log('buf_res.length', buf_res.length);
                        //let buf_encoded_page = Binary_Encoding.flexi_encode()
                        c = 0;
                        // Could empty that array, would that be faster than GC?
                        arr_page = new Array(page_records_max);
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
                    arr_res = [buf_msg_id, buf_record_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c));
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);

                    // Could change it so it checks to see if there is another page afterwards before sending a page.
                    //  For the moment, the last page having no data should be fine.
                    //  Won't always be like that.

                    // The last page with no data?????


                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);
                })
        }
    }




    var read_l_buffer = (buffer, pos) => {
        var l, pos2;
        [l, pos2] = x.read(buf_the_rest, pos);
        var pos3 = pos2 + l;
        var buf_res = Buffer.alloc(l);
        buffer.copy(buf_res, 0, pos2, pos3);
        return [buf_res, pos3];
    }

    // LL_GET_KEYS_IN_RANGE

    if (i_query_type === LL_GET_KEYS_IN_RANGE) {
        console.log('LL_GET_KEYS_IN_RANGE');

        // when there are 0 records?
        // Maybe not returning data OK.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        //if (paging_option > 0) {

        //}

        // Could feed through a paging function that batches the results.
        // batch_put

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // read two buffers from the query... greater than and less than.

        // Need to encode each key separately.
        //  Say how long the key is in bytes, then write that key buffer.
        //  Some kind of buffer vector while building?
        //   How to compose the whole thing in memory reasonably efficiently?
        //   Put the buffers in a vector...

        console.log('paging_option', paging_option);

        var b_l, b_u;

        if (paging_option === NO_PAGING) {
            // read a couple more buffers.

            // want to read a buffer with the length first.


            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);

            //console.log('b_l', b_l);
            //console.log('b_u', b_u);

            //throw 'stop';

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_key_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }

            //var res = [];
            db.createKeyStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function (key) {
                    // will be both the key and the value
                    // will need to combine them as buffers.

                    //var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    // key is a buffer.

                    //console.log('buf_combined', buf_combined);

                    // A keys paging return type?
                    //  Could just use Binary_Encoding.

                    // Possibly going through ws-binary and changing to Binary_Encoding where possible would be best.

                    arr_res.push(xas2(key.length).buffer);
                    arr_res.push(key);
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);

                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }

        if (paging_option === PAGING_RECORD_COUNT) {
            // Read the page size.
            [page_size, pos] = x.read(buf_the_rest, pos);
            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);
            let c = 0;
            let arr_page = new Array(page_size);
            let page_number = 0;

            db.createKeyStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function (key) {

                    //arr_page.push(xas2(key.length).buffer);
                    //arr_page.push(key);

                    arr_page[c++] = (Buffer.concat([xas2(key.length).buffer, key]));


                    if (c === page_size) {
                        connection.sendBytes(Buffer.concat([buf_msg_id, buf_key_paging_flow, xas2(page_number++).buffer].concat(arr_page)));
                        c = 0;
                        // Could empty that array, would that be faster than GC?
                        arr_page = new Array(page_size);
                    }

                    //Binary_Encoding.join_buffer_pair([data.key, data.value])
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);
                })
                .on('error', function (err) {
                    //console.log('Oh my!', err) 

                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);
                    //connection.sendBytes(Buffer.concat([buf_msg_id, buf_key_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c))));


                    arr_res = [buf_msg_id, buf_record_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c));
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }
    }


    // Could move some of the paging handling / processing out of this level, and into the nextleveldb-server.
    //  Having observables in there would be useful.
    //   Also would be nice to have a lower surface area of DB calls, so the the DB API can be upgraded (though that would make it slower if there is another function call in between)
    //    Could do with a slightly more abstracted interface involving observables for managing fairly large throughputs of data in managable and efficient chunks.

    // Seems like we will have fairly large / comprehensive changes to the core of the database operations, but not to the record structure.









    if (i_query_type === LL_GET_RECORDS_IN_RANGE) {
        console.log('LL_GET_RECORDS_IN_RANGE');

        // when there are 0 records?
        // Maybe not returning data OK.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        //console.log('paging_option', paging_option);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        //let c = 0,
        //    page_number = 0;

        //console.log('page_size', page_size);
        // Could feed through a paging function that batches the results.
        // batch_put

        //console.log('buf_the_rest', buf_the_rest);
        //console.lo g('buf_the_rest.length', buf_the_rest.length);

        // read two buffers from the query... greater than and less than.

        // Need to encode each key separately.
        //  Say how long the key is in bytes, then write that key buffer.
        //  Some kind of buffer vector while building?
        //   How to compose the whole thing in memory reasonably efficiently?
        //   Put the buffers in a vector...

        //console.log('paging_option', paging_option);

        // Paging here would be useful.
        //  Test it too.

        var b_l, b_u;

        if (paging_option === NO_PAGING) {
            // read a couple more buffers.

            // want to read a buffer with the length first.


            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);

            //console.log('b_l', b_l);
            //console.log('b_u', b_u);

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_record_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }

            //var res = [];
            db.createReadStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function (data) {

                    //arr_page[c++] = (Buffer.concat([xas2(key.length).buffer, key]));



                    // will be both the key and the value
                    // will need to combine them as buffers.
                    var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    arr_page[c++] = buf_combined;
                    //console.log('buf_combined', buf_combined);
                    //arr_res.push(buf_combined);
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);

                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    //console.log('arr_res', arr_res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        } else if (paging_option === PAGING_RECORD_COUNT) {

            //[page_size, pos] = x.read(buf_the_rest, pos);

            //console.log('page_size', page_size);

            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);
            let c = 0;
            let arr_page = new Array(page_size);
            let page_number = 0;

            var arr_res = [buf_msg_id, buf_record_paging_flow];
            // Send back flow pages when the data is being got.

            // And need to put the results into pages.
            db.createReadStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function (data) {

                    // will be both the key and the value
                    // will need to combine them as buffers.
                    var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    //console.log('buf_combined', buf_combined);
                    //arr_res.push(buf_combined);
                    arr_page[c++] = buf_combined;

                    if (c === page_size) {

                        console.log('sending page', page_number);

                        connection.sendBytes(Buffer.concat([buf_msg_id, buf_record_paging_flow, xas2(page_number++).buffer].concat(arr_page)));
                        c = 0;
                        // Could empty that array, would that be faster than GC?
                        arr_page = new Array(page_size);
                    }
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);

                })
                .on('error', function (err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function () {
                    //console.log('Stream closed')
                })
                .on('end', function () {
                    //callback(null, res);
                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);

                    arr_res = [buf_msg_id, buf_record_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c));
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })

        } else {
            throw 'Unexpected paging option'
        }
    }

    if (i_query_type === LL_GET_RECORD) {
        // Decode / get a single key
        console.log('LL_GET_RECORD', LL_GET_RECORD);
        let buf_key = buf_the_rest;

        db.get(buf_key, (err, buf_value) => {
            if (err) {
                //callback(err);


                // Key not found...
                //  Maybe just return an 'undefined'.


                // An undefined record / or a record not found response.

                // 

                if (return_message_type) {
                    // Though paging is an option, this is not a paged or pagable response.
                    let msg_response = [buf_msg_id, xas2(RECORD_UNDEFINED).buffer];
                    console.log('buf_msg_id', buf_msg_id);
                    console.log('msg_response', msg_response);
                    connection.sendBytes(Buffer.concat(msg_response));
                    console.log('1) sent RECORD_UNDEFINED message (it\'s own message type!) to the client.');
                } else {
                    throw 'NYI';
                }


                //console.trace();
                //console.log('err', err);
                //console.log('Object.keys(err)', Object.keys(err));
                //console.log('buf_key', buf_key);

                // error return.



            } else {
                console.log('buf_value', buf_value);
                // But does get also get the key as well?

                if (return_message_type) {
                    // Though paging is an option, this is not a paged or pagable response.
                    buf_res = Buffer.concat([buf_msg_id, buf_record_paging_none, Binary_Encoding.join_buffer_pair([buf_key, buf_value])]);
                } else {
                    buf_res = Buffer.concat([buf_msg_id, Binary_Encoding.join_buffer_pair([buf_key, buf_value])]);
                }

                connection.sendBytes(buf_res);

            }
        })

    }


    // LL_GET_RECORDS_IN_RANGE_UP_TO

    // Could just use a limit property.


    if (i_query_type === LL_GET_RECORDS_IN_RANGE_UP_TO) {
        console.log('LL_GET_RECORDS_IN_RANGE_UP_TO');

        // when there are 0 records?
        // Maybe not returning data OK.

        var paging_option, page_size, count_limit;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        [count_limit, pos] = x.read(buf_the_rest, pos);

        console.log('count_limit', count_limit);

        // Could feed through a paging function that batches the results.
        // batch_put

        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // read two buffers from the query... greater than and less than.

        // Need to encode each key separately.
        //  Say how long the key is in bytes, then write that key buffer.
        //  Some kind of buffer vector while building?
        //   How to compose the whole thing in memory reasonably efficiently?
        //   Put the buffers in a vector...

        //console.log('paging_option', paging_option);

        var b_l, b_u;

        if (paging_option === NO_PAGING) {
            // read a couple more buffers.

            // want to read a buffer with the length first.


            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);

            //console.log('b_l', b_l);
            //console.log('b_u', b_u);

            //throw 'stop';

            if (return_message_type) {
                // Though paging is an option, this is not a paged or pagable response.
                var arr_res = [buf_msg_id, buf_record_paging_none];
            } else {
                var arr_res = [buf_msg_id];
            }
            let count = 0;

            //var res = [];
            let stream = db.createReadStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', data => {
                    // will be both the key and the value
                    // will need to combine them as buffers.
                    var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    //console.log('buf_combined', buf_combined);
                    arr_res.push(buf_combined);
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);
                    count++;
                    if (count > count_limit) {
                        stream.destroy();
                    }

                })
                .on('error', err => {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', () => {
                    console.log('Stream closed')
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
                .on('end', () => {
                    //callback(null, res);
                })
        }

        if (paging_option === PAGING_RECORD_COUNT) {
            throw 'NYI'

        }


    }


    // TODO: Make subscriptions return the response type number.
    //  Says paging info, whether it is to be decoded using record decode, or by using Binary_Encoding standard decode.





    if (i_query_type === LL_SUBSCRIBE_ALL) {
        console.log('LL_SUBSCRIBE_ALL');

        // Subscriptions don't use paging
        //  Have their own subscription response handler.

        // Send back a message saying the subscription has been set up, give the subscription id.
        var unsubscribe_all = nextleveldb_server.ll_subscribe_all((subscription_event) => {
            // sub_msg_id:
            /*
            if (err) {
                console.trace();
                throw err;
            } else {
                console.log('ws binary subscription_event', subscription_event);
                //
            }
            */
            // remove the target from the subscription event?
            //delete subscription_event.target;
            //console.log('ws binary subscription_event', subscription_event);
            // then encode the message

            var i_response_type = map_subscription_event_types[subscription_event.type];
            //console.log('i_response_type', i_response_type);
            var msg_response = [buf_msg_id, xas2(subscription_event.sub_msg_id).buffer, xas2(i_response_type).buffer];
            //console.log('msg_response', msg_response);


            if (subscription_event.type === 'batch_put') {
                msg_response.push(subscription_event.buffer);
            }
            buf_res = Buffer.concat(msg_response);
            //console.log('buf_res', buf_res);
            connection.sendBytes(buf_res);
        });

        // then store the unsubscribe function.
        // 

        client_subscriptions[connection.id] = client_subscriptions[connection.id] || {};
        client_subscriptions[connection.id][message_id] = {
            'unsubscribe': unsubscribe_all
        }
    }

    // LL_SUBSCRIBE_KEY_PREFIX_PUTS

    if (i_query_type === LL_SUBSCRIBE_KEY_PREFIX_PUTS) {
        console.log('LL_SUBSCRIBE_KEY_PREFIX_PUTS');

        var unsubscribe = nextleveldb_server.ll_subscribe_key_prefix_puts(buf_the_rest, (subscription_event) => {
            //console.log('LL_SUBSCRIBE_KEY_PREFIX_PUTS subscription_event', subscription_event);
            var i_response_type = map_subscription_event_types[subscription_event.type];
            //console.log('i_response_type', i_response_type);
            var msg_response = [buf_msg_id, xas2(subscription_event.sub_msg_id).buffer, xas2(i_response_type).buffer];

            if (subscription_event.type === 'batch_put') {
                msg_response.push(subscription_event.buffer);
            }
            // both buffer puts and individual record puts
            // Also handle buffering / debouncing of recodrd puts.

            //console.log('msg_response', msg_response);

            buf_res = Buffer.concat(msg_response);
            //console.log('buf_res', buf_res);
            //console.log('no response sent (still developing)');
            connection.sendBytes(buf_res);

        });

        // then store the unsubscribe function.
        // 

        client_subscriptions[connection.id] = client_subscriptions[connection.id] || {};
        client_subscriptions[connection.id][message_id] = {
            'unsubscribe': unsubscribe_all
        }

    }

    if (i_query_type === LL_UNSUBSCRIBE_SUBSCRIPTION) {
        console.log('LL_UNSUBSCRIBE_SUBSCRIPTION');
        //console.log('connection.id', connection.id);

        //console.log('buf_the_rest', buf_the_rest);

        //var pos = 0, subscription_id;
        //[subscription_id, pos] = x.read(buf_the_rest, pos);

        var subscription_id = message_id;
        //console.log('subscription_id', subscription_id);

        var fn_unsubscribe = client_subscriptions[connection.id][subscription_id].unsubscribe;
        //console.log('fn_unsubscribe', typeof fn_unsubscribe);
        var sub_msg_id = fn_unsubscribe();

        // Send a subscription event.

        // We don't have a sub message id for unsubscribe.

        // 

        var msg_response = [buf_msg_id, xas2(sub_msg_id).buffer, xas2(BOOL_TRUE).buffer];
        buf_res = Buffer.concat(msg_response);
        connection.sendBytes(buf_res);




    }


    if (i_query_type === LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH) {
        // Need to search for the table records in the database.

        // Need to decode the record.
        //  Find out what table it is.

        // Consult the model's record and index definition for that table
        //  Determine what the index values would be for that record
        //  Check the DB to see if it has that record / count the number of matching index records.

        // Would need to be a full index match.
        //  At least one of the indexes having a full match.

        // Do this in order to see which bittrex currencies or markets match the given index values of records we have just downloaded

        // Just doing it a single record at a time for the moment.


        console.log('LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH');

        // buf_the_rest
        //  this gets decoded into the record.


    }

    if (i_query_type === ENSURE_TABLE) {
        console.log('ENSURE_TABLE');

        let def = Binary_Encoding.decode_buffer(buf_the_rest);
        console.log('def', def);

        // A callback usage of ensure_table should be OK.
        //  Though logging / checking data could be returned when using an observable.

        // overwrite / modify / error if exists already.



        nextleveldb_server.ensure_table(def, (err, res_ensure) => {
            // If there is an error, need to use error encoding.
            //  I think that encoding error statuses in the return messages is important.
            //   Don't use http codes over websockets.
            //   Should send status codes back in the websocket messages.

            // send a message back encapsulating the result.

            //  should be true or an error.
            //   need to be able to send back error messages.

            if (err) {
                throw 'NYI'
            } else {

                // Maybe something saying the table already exists, or was created.
                //  Not keen on overwriting existing table structure.
                //   Could have an error for if it already exists.
                //   Though in some conditions could change fields, including going through records doing that.




                var msg_response = [buf_msg_id, xas2(BOOL_TRUE).buffer];
                buf_res = Buffer.concat(msg_response);
                connection.sendBytes(buf_res);
            }





        });





        // Won't have a paged response

        throw 'stop';

    }

    if (i_query_type === ENSURE_TABLES) {
        console.log('ENSURE_TABLES');

        let def = Binary_Encoding.decode_buffer(buf_the_rest)[0];
        // For some reason, decoding of the buffer puts it all into a new array.

        console.log('def', def);


        // When we use an observable but have no paging as an option...


        // A callback usage of ensure_table should be OK.
        //  Though logging / checking data could be returned when using an observable.

        // overwrite / modify / error if exists already.
        // This may be better with an observer, where it sends back multiple reply messages.

        // An observable for all the tables would be better.

        // Get an observable for it, and use a wrapper to output it to the response.

        nextleveldb_server.ensure_tables(def, (err, res_ensure) => {
            // If there is an error, need to use error encoding.
            //  I think that encoding error statuses in the return messages is important.
            //   Don't use http codes over websockets.
            //   Should send status codes back in the websocket messages.

            // send a message back encapsulating the result.

            //  should be true or an error.
            //   need to be able to send back error messages.

            // Could look to see what paging is requsted.


            if (err) {
                console.trace();
                throw 'NYI'
            } else {

                // Maybe something saying the table already exists, or was created.
                //  Not keen on overwriting existing table structure.
                //   Could have an error for if it already exists.
                //   Though in some conditions could change fields, including going through records doing that.

                // 

                // Something in the return to say there is no paging in the message too?
                console.log('ENSURE_TABLES res_ensure', res_ensure);
                var msg_response = [buf_msg_id, buf_binary_paging_none, xas2(BOOL_TRUE).buffer];
                buf_res = Buffer.concat(msg_response);
                connection.sendBytes(buf_res);
            }





        });





        // Won't have a paged response

        //throw 'stop';

    }

    if (i_query_type === INSERT_TABLE_RECORD) {
        console.log('INSERT_TABLE_RECORD');

        // Won't have a paged response

        // Need to decode the record (array or other object)

        //buf_the_rest = Buffer.alloc(message_binary.length - pos);
        //message_binary.copy(buf_the_rest, 0, pos);

        var table_id;


        /*
        pos = 0;
        [table_id, pos] = x.read(buf_the_rest, pos);

        // then make a new buffer, having read paging
        //console.log('pos', pos);

        //var buf_2 = Buffer.from(buf_the_rest, pos);
        var buf_2 = Buffer.alloc(buf_the_rest.length - pos);
        buf_the_rest.copy(buf_2, 0, pos);
        */


        // We should be able to tell from the model what fields there are.
        // Want the count of primary keys for that table.
        //console.log('table_id', table_id);

        var buf_res = Buffer.concat([buf_msg_id]);

        if (buf_the_rest.length > 0) {
            // Decode a Record according to the Model?
            // Or the record would just be encoded as the binary flexi encoding?

            var [table_id, record] = Binary_Encoding.decode_buffer(buf_the_rest);

            //var decoded = Model_Database.decode_model_row(buf_the_rest);
            //console.log('decoded', decoded);

            //let [table_id, record] = decoded;

            // So, this way we are able to decode the record.
            console.log('[table_id, record]', [table_id, record]);

            let model_table = nextleveldb_server.model.tables[table_id];
            console.log('model_table', model_table);

            // then create a record on that table.

            // Seems like the incrementors have not been set up on the server side model.
            //  When loading the model, need to make sure that the server side incrementor gets set up properly.


            let model_record = model_table.new_record(record);
            //  That model record could have funtionality to encode itself as a buffer.

            console.log('model_record', model_record);

            // Then do ll put on the model record encoded.

            // Need encoding to the DB system to be part of the model record.
            //  Could also get the index records, so to do a batch put.

            //let encoded_record = model_record.to_b

            // I think this does the job on the server of making index records OK.

            let arr_record_and_index_buffers = model_record.to_arr_buffer_with_indexes();
            console.log('arr_record_and_index_buffers', arr_record_and_index_buffers);

            // Then can ll put records.
            //  

            // Something that arranges the batches.

            nextleveldb_server.arr_batch_put(arr_record_and_index_buffers, (err, res_put) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_put', res_put);

                    // Could do this so we return the record's new ID.

                    // Though possibly return thw whole primary key?
                    //  Different case for when it's an array / multiple items.

                    if (model_record.arr_data[0].length === 1) {

                        if (return_message_type) {
                            // Though paging is an option, this is not a paged or pagable response.
                            var msg_response = [buf_msg_id, buf_binary_paging_none, xas2(model_record.arr_data[0][0]).buffer];
                        } else {
                            var msg_response = [buf_msg_id, xas2(model_record.arr_data[0][0]).buffer];
                        }

                        buf_res = Buffer.concat(msg_response);
                        connection.sendBytes(buf_res);
                    } else {
                        throw 'NYI';
                    }

                }
            });

        } else {
            connection.sendBytes(buf_res);
        }
    }

    if (i_query_type === TABLE_EXISTS) {
        // Decode the table name

        // Will send back a binary response message.
        console.log('TABLE_EXISTS');

        let table_name = Binary_Encoding.decode_buffer(buf_the_rest)[0];
        console.log('table_name', table_name);

        nextleveldb_server.table_exists(table_name, (err, exists) => {
            if (err) {
                throw 'NYI';
            } else {

                let msg_response = [buf_msg_id, buf_binary_paging_none, xas2(exists).buffer];
                connection.sendBytes(Buffer.concat(msg_response));

            }
        })

        //throw 'stop';

    }

    // TABLE_ID_BY_NAME

    if (i_query_type === TABLE_ID_BY_NAME) {
        // Decode the table name

        // Will send back a binary response message.
        console.log('TABLE_ID_BY_NAME');

        // Always need to fish it out of an array, because there are 0 or more items encoded into the buffer.


        let table_name = Binary_Encoding.decode_buffer(buf_the_rest)[0];
        //console.log('table_name', table_name);

        nextleveldb_server.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                // connection send error, could provide the string error message, possibly an int / xas2 error code.
                // ERR_TABLE_NOT_FOUND

                // paging flow error, so that it knows the error 

                console.log('err', err);
                throw err;
            } else {
                //console.log('table_id', table_id);

                if (def(table_id)) {
                    // It seems the client needs to use Binary_Encoding's decode_bufer, which is a bit less concise.
                    let buf_encoded = Binary_Encoding.flexi_encode_item(table_id);

                    //console.log('buf_encoded', buf_encoded);


                    let msg_response = [buf_msg_id, buf_binary_paging_none, buf_encoded];
                    connection.sendBytes(Buffer.concat(msg_response));

                } else {
                    // return an error message, because the table does not exist.

                    // Table not found.



                    let msg_response = [buf_msg_id, xas2(ERROR_MESSAGE).buffer];
                    connection.sendBytes(Buffer.concat(msg_response));
                    //console.log('1) sent simple error message to the client');
                    console.trace();

                    // Table not found.

                    // 

                    // Just the simplest error returned.


                }





            }
        })




        // Then call the server function.
        //  Maybe xas2 could handle negative integers too, this could return -1 if the table is not found.
        //  However, for the moment, from this part we send back an error message when appropriate.





        //throw 'stop';

    }

    if (i_query_type === GET_TABLE_FIELDS_INFO) {
        let table_id;
        console.log('GET_TABLE_FIELDS_INFO: ' + GET_TABLE_FIELDS_INFO);

        // Read the data out, just as an xas2.
        //  

        console.log('buf_the_rest', buf_the_rest);

        //let i = Binary_Encoding.decode_buffer(buf_the_rest);

        [table_id, pos] = x.read(buf_the_rest, 0);
        console.log('table_id', table_id);
        //let table_id = i[0];

        nextleveldb_server.get_table_fields_info(table_id, (err, table_fields_info) => {
            if (err) {
                throw err;
            } else {
                console.log('table_fields_info', table_fields_info);
                // encode it

                let buf_encoded = Binary_Encoding.flexi_encode_item(table_fields_info);
                console.log('buf_encoded', buf_encoded);

                // Send the response back to the client.
                let msg_response = [buf_msg_id, buf_binary_paging_none, buf_encoded];
                connection.sendBytes(Buffer.concat(msg_response));



            }
        })






        //console.log('table_id', table_id);


        // Get the table name or id.
        //  If we have the table name, look up the id.




    }

};

module.exports = handle_ws_binary;