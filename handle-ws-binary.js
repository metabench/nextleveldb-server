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
// Followed by p number



// All low level functions, operating on the core db.
// Higher level functions could do things like put a record, where the db system automatically creates the index records too.

// A way to LL put a single record?

const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_KEYS_IN_RANGE = 3;
const LL_GET_RECORDS_IN_RANGE = 4;

const LL_COUNT_KEYS_IN_RANGE = 5;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 6;
const LL_GET_RECORD = 7;

// Limited to a certain number of items
const LL_COUNT_KEYS_IN_RANGE_UP_TO = 8;
const LL_GET_RECORDS_IN_RANGE_UP_TO = 9;


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




const LL_WIPE = 20;
const LL_WIPE_REPLACE = 21;


const LL_SUBSCRIBE_ALL = 60;
const LL_SUBSCRIBE_KEY_PREFIX_PUTS = 61;
const LL_UNSUBSCRIBE_SUBSCRIPTION = 62;

// Then the subscription messages send back data that's been put into the database / commands that have been done on the db.





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




var handle_ws_binary = function(connection, nextleveldb_server, message_binary) {

    // Need error handling.
    //  Need some way of indicating error in the response.
    //  1 byte seems too much to indicate this as a flag, but it could be OK. Not sure of a simple way to do anything different...

    // Except reserve 0 as an id for errors. Would make the normal responses shorter.
    //  Don't accept 0 as a message ID.
    //   Could return an error saying 0 is reserved for errors.

    // Which connection is it coming from?
    //  Assign a connection id.





    var pos, buf_res;
    //console.log('Received message_binary: ' + message_binary);
    //console.log('Received message_binary length: ' + message_binary.length);
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

    //console.log('message_id, i_query_type', message_id, i_query_type);
    //console.log('connection.id', connection.id);

    // Need to define a bunch more queries.

    // Such as putting a batch of records.

    var buf_the_rest = Buffer.alloc(message_binary.length - pos);
    message_binary.copy(buf_the_rest, 0, pos);
    var buf_msg_id = xas2(message_id).buffer;

    // Function to read the paging option?



    if (i_query_type === LL_WIPE) {
        console.log('LL_WIPE');
        nextleveldb_server.ll_wipe((err, db) => {
            if (err) {
                throw err;
            } else {
                // Something for a buffer just saying true?

                buf_res = Buffer.concat([xas2(message_id).buffer, xas2(BOOL_TRUE).buffer]);
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

                nextleveldb_server.batch_put(buf_the_rest, (err, res_batch_put) => {
                    if (err) {
                        throw err;
                    } else {
                        buf_res = Buffer.concat([xas2(message_id).buffer, xas2(BOOL_TRUE).buffer]);
                        //console.log('buf_res', buf_res);
                        connection.sendBytes(buf_res);
                    }
                });
            }
        });
    }


    if (i_query_type === LL_COUNT_RECORDS) {
        // Count all records
        console.log('LL_COUNT_RECORDS');

        nextleveldb_server.ll_count((err, count) => {
            if (err) {
                throw err;
            } else {
                console.log('count', count);
                buf_res = Buffer.concat([xas2(message_id).buffer, xas2(count).buffer]);
                //console.log('buf_res', buf_res);
                connection.sendBytes(buf_res);
            }
        });
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
        var buf_res = Buffer.concat([buf_msg_id]);

        if (buf_the_rest.length > 0) {
            nextleveldb_server.batch_put(buf_the_rest, (err, res_batch_put) => {
                if (err) {
                    throw err;
                } else {

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
            var arr_res = [buf_msg_id];
            var count = 0;


            //var res = [];
            db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function(key) {
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);
                    count++;
                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')
                })
                .on('end', function() {
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
            var arr_res = [buf_msg_id];
            var count = 0;

            //let reached_limit = false;


            //var res = [];
            let stream = db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function(key) {
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);



                    count++;

                    if (count > count_limit) {
                        stream.destroy();
                    }
                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')

                    //arr_res.push(xas2(count).buffer);
                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);


                    arr_res.push(xas2(count).buffer);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);

                })
                .on('end', function() {
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
            var arr_res = [buf_msg_id];
            //var count = 0;

            var first;
            var last;

            //var res = [];
            db.createKeyStream({
                    'gt': s_buf[0],
                    'lt': s_buf[1]
                }).on('data', function(key) {
                    if (!first) first = key;
                    last = key;
                    //arr_res.push(x(key.length).buffer);
                    //console.log('key', key);
                    //arr_res.push(key);
                    //count++;
                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')
                })
                .on('end', function() {
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
            var arr_res = [buf_msg_id];

            //var res = [];
            db.createKeyStream({}).on('data', function(key) {
                    arr_res.push(x(key.length).buffer);
                    arr_res.push(key);
                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')
                })
                .on('end', function() {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
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
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

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


            var arr_res = [buf_msg_id];

            //var res = [];
            db.createKeyStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function(key) {
                    // will be both the key and the value
                    // will need to combine them as buffers.

                    //var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    // key is a buffer.



                    //console.log('buf_combined', buf_combined);
                    arr_res.push(xas2(key.length).buffer);
                    arr_res.push(key);
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);

                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')
                })
                .on('end', function() {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }
    }



    if (i_query_type === LL_GET_RECORDS_IN_RANGE) {
        console.log('LL_GET_RECORDS_IN_RANGE');

        // when there are 0 records?
        // Maybe not returning data OK.

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


            var arr_res = [buf_msg_id];

            //var res = [];
            db.createReadStream({
                    'gt': b_l,
                    'lt': b_u
                }).on('data', function(data) {
                    // will be both the key and the value
                    // will need to combine them as buffers.
                    var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                    //console.log('buf_combined', buf_combined);
                    arr_res.push(buf_combined);
                    //arr_res.push(x(key.length).buffer);
                    //arr_res.push(key);

                })
                .on('error', function(err) {
                    //console.log('Oh my!', err)
                    callback(err);
                })
                .on('close', function() {
                    //console.log('Stream closed')
                })
                .on('end', function() {
                    //callback(null, res);
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                })
        }
    }

    if (i_query_type === LL_GET_RECORD) {
        // Decode / get a single key
        console.log('LL_GET_RECORD', LL_GET_RECORD);

        //let [buf_key, pos] = read_l_buffer(buf_the_rest, pos);

        // buf_the_rest is the key
        let buf_key = buf_the_rest;

        db.get(buf_key, (err, buf_value) => {
            if (err) {
                callback(err);
            } else {
                console.log('buf_value', buf_value);
                // But does get also get the key as well?

                buf_res = Buffer.concat([buf_msg_id, Binary_Encoding.join_buffer_pair([buf_key, buf_value])]);
                connection.sendBytes(buf_res);

            }
        })


    }


    // LL_GET_RECORDS_IN_RANGE_UP_TO

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


            var arr_res = [buf_msg_id];
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
    }


    if (i_query_type === LL_SUBSCRIBE_ALL) {
        console.log('LL_SUBSCRIBE_ALL');

        // Send back a message saying the subscription has been set up, give the subscription id.

        /*
        db.on('db_action', (obj_db_action) => {
            var type = obj_db_action.type;

            // Then if it's a batch put
        })
        */


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


    /*
    if (i_query_type === LL_SUBSCRIBE_KEY_PREFIX) {
        console.log('LL_SUBSCRIBE_KEY_PREFIX');

        var unsubscribe_key_prefix = nextleveldb_server.ll_subscribe_key_prefix((subscription_event) => {

            // sub_msg_id:

            // remove the target from the subscription event?

            //delete subscription_event.target;

            //console.log('ws binary subscription_event', subscription_event);

            // then encode the message

            var i_response_type = map_subscription_event_types[subscription_event.type];
            //console.log('i_response_type', i_response_type);

            var msg_response = [buf_msg_id, xas2(subscription_event.sub_msg_id).buffer, xas2(i_response_type).buffer];
            //console.log('msg_response', msg_response);

            // would need to 

            if (subscription_event.type === 'batch_put') {
                msg_response.push(subscription_event.buffer);
            }

            buf_res = Buffer.concat(msg_response);
            //console.log('buf_res', buf_res);
            connection.sendBytes(buf_res);



            
        });
    }
    */

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

    if (i_query_type === INSERT_TABLE_RECORD) {
        console.log('INSERT_TABLE_RECORD');

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

        console.log('table_id', table_id);


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
                        var msg_response = [buf_msg_id, xas2(model_record.arr_data[0][0]).buffer];
                        buf_res = Buffer.concat(msg_response);
                        connection.sendBytes(buf_res);
                    } else {
                        throw 'NYI';
                    }




                }
            })





            //throw 'stop';

            //var decoded = Binary_Encoding.dec

            /*
            nextleveldb_server.batch_put(buf_the_rest, (err, res_batch_put) => {
                if (err) {
                    throw err;
                } else {
                    
                    //console.log('buf_res', buf_res);
                    connection.sendBytes(buf_res);
                }
            })
            */


        } else {
            connection.sendBytes(buf_res);
        }


    }



};

module.exports = handle_ws_binary;