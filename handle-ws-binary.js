/**
 * Created by james on 27/12/2016.
 * 
 * Could possibly load some other modules, like time series handling and encoding.
 * Probably best to write moe code here for the moment.
 */



var jsgui = require('jsgui3');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
var Fns = jsgui.Fns;

var x = xas2 = require('xas2');

// While these will be some work, they will enable a variety of data to be quickly moved in and out of the DB.

var Binary_Encoding = require('binary-encoding');

// Could have paged versions of these instructions too?
//  Building paging into the relevant instructions would make sense, so that we don't have too many instructions, with different versions.


const NO_PAGING = 0;
const PAGING_RECORD_COUNT = 1;
// Followed by p number
const PAGING_BYTE_COUNT = 2;
// Followed by p number



// All low level functions, operating on the core db.
// Higher level functions could do things like put a record, where the db system automatically creates the index records too.


const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_KEYS_IN_RANGE = 3;
const LL_GET_RECORDS_IN_RANGE = 4;

const LL_COUNT_KEYS_IN_RANGE = 5;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 6;
const LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE = 7;
// Has paging option.
//  Number of records per page
//  Number of bytes per page

// Needs to delete the database from disk and replace it.
//  Would mean replacing a calling object I think.
//  Or call a method in it.

// const LL_GET_SYSTEM_MODEL = 10

// The system model is tables 0 to 8 I think.


const LL_WIPE = 20;
const LL_WIPE_REPLACE = 21;

// Optional parameters could help...



const BOOL_FALSE = 6;
const BOOL_TRUE = 7;

const NULL = 8;



var handle_ws_binary = function (connection, nextleveldb_server, message_binary) {

    // Need error handling.
    //  Need some way of indicating error in the response.
    //  1 byte seems too much to indicate this as a flag, but it could be OK. Not sure of a simple way to do anything different...

    // Except reserve 0 as an id for errors. Would make the normal responses shorter.
    //  Don't accept 0 as a message ID.
    //   Could return an error saying 0 is reserved for errors.



    var pos, buf_res;
    //console.log('Received message_binary: ' + message_binary);
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

    if (i_query_type === LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE) {
        console.log('LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE');

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
                    // encode 2 values basically.
                    var encoded_res = Binary_Encoding.join_buffer_pair([first, last]);
                    //console.log('encoded_res', encoded_res);
                    arr_res.push(encoded_res);

                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                });
        }
    }



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

        console.log('buf_the_rest', buf_the_rest);
        console.log('buf_the_rest.length', buf_the_rest.length);

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

            console.log('b_l', b_l);
            console.log('b_u', b_u);

            //throw 'stop';


            var arr_res = [buf_msg_id];

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

        console.log('buf_the_rest', buf_the_rest);
        console.log('buf_the_rest.length', buf_the_rest.length);

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

            console.log('b_l', b_l);
            console.log('b_u', b_u);

            //throw 'stop';


            var arr_res = [buf_msg_id];

            //var res = [];
            db.createReadStream({
                'gt': b_l,
                'lt': b_u
            }).on('data', function (data) {
                // will be both the key and the value
                // will need to combine them as buffers.
                var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                //console.log('buf_combined', buf_combined);
                arr_res.push(buf_combined);
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
    }





};

module.exports = handle_ws_binary;
