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

// Soon expand this to neatly sync from other DBs.
//  Will take a while, but progress indicators and maybe validation will help.


// 12/05/2018
//  This will have to change somewhat to use the changed server / core server functions.
//   They will have the decode and remove_kp options removed.
//   If KPs are to be removed, they need to be removed between the DB layer and sending them.
//   Could have an observable processor / map
//   Can get better and more concise code here.


 
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

const Command_Response_Message = Model.Command_Response_Message;

const Paging = Model.Paging;
const Model_Database = Model.Model_Database;
const Command_Message = Model.Command_Message;

// Could have paged versions of these instructions too?
//  Building paging into the relevant instructions would make sense, so that we don't have too many instructions, with different versions.


// Data types

const XAS2 = 0;
const DOUBLEBE = 1;
const DATE = 2;
const STRING = 4;
const BOOL_FALSE = 6;
const BOOL_TRUE = 7;
const NULL = 8;
const BUFFER = 9;
const ARRAY = 10;
const OBJECT = 11;
const COMPRESSED_BUFFER = 12;


// Paging options


const NO_PAGING = 0;


// Maybe combine to PAGING_ITEM_COUNT
const PAGING_RECORD_COUNT = 1;
const PAGING_COUNT = 1;
const PAGING_KEY_COUNT = 2; // Deprecated


// Followed by p number
const PAGING_BYTE_COUNT = 3;
const PAGING_TIMED = 4;

// Not very keen on including compression option in with paging.
//  That would be whether to use compression or not.
//  Then if compression is used, we read at least another xas2 number to see what compression type is being used.
//   Think we want a fast and fairly high compression default / mid compression and high speed.
//  Or have an option for extended options following.
//   The extended options would have bytes for compression type / params, and limit.

// Extended options for compression, and limit, would be really useful, and backwards compat.

// Seems somewhat tricky at this stage to extend the calling options.

// Extending the return options too.

// More capabilities to track records per second, and also the paging count records in range would help.

// Key samples, eg every 1000th key would help.
//  Then we could get relatively long lists of sampled keys, and use them to download the data while being able to give a progress report on how far to completion.
//  Sampling keys every 64000 (or n) records could help to tell how far through it's gone.










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



// Get this syncing, by copying table records, checking table records match values too
//  Can read records of whole tables too, check them. Start with currencies and markets.

// Want to get this working for other exchanges too soon.

// See about an observable key stream soon. That would be useful in order to see which records we would need to get from the DB.







const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_ALL_RECORDS = 3;



const LL_GET_KEYS_IN_RANGE = 4;
const LL_GET_RECORDS_IN_RANGE = 5;


const LL_GET_RECORDS_IN_RANGES = 50;


// Call it the LL version for GET_RECORDS_IN_RANGES?

// Maybe get rid of 'LL' here when calling the fns. They will generally / all call the ll version, and then there could be some processing for the results.

// Maybe use the same result processors on the client side as on the server side.
//  This is why some kinds of observable processing should be in Model, or another mutual dependency between client and server.
//   Client could get back the info in the same ll format, and then process it.

// Want the level above ll to be coded in a way that makes more code reuse on both the client and server for decoding / modifying these observable data results.











const LL_COUNT_KEYS_IN_RANGE = 6;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 7;

const LL_GET_FIRST_LAST_KEYS_BEGINNING = 36; // Maybe don't implement yet.
const LL_GET_FIRST_KEY_BEGINNING = 37;
const LL_GET_LAST_KEY_BEGINNING = 38;



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



// Change to 'Put'?
// Could make 12 polymorphic so it checks if it is given an array?
const INSERT_TABLE_RECORD = 12;
const INSERT_RECORDS = 13;


const ENSURE_RECORD = 14;
const ENSURE_TABLE_RECORD = 15;



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

const DELETE_RECORDS_BY_KEYS = 18;


const ENSURE_TABLE = 20;
const ENSURE_TABLES = 21;
const TABLE_EXISTS = 22;
const TABLE_ID_BY_NAME = 23;
// RENAME_TABLE
const GET_TABLE_FIELDS_INFO = 24;
const GET_TABLE_KEY_SUBDIVISIONS = 25;




// Could have SELECT_FROM_KEYS_IN_RANGE
//  More versitile than table, and reads through those keys, selecting out fields from them.

const SELECT_FROM_RECORDS_IN_RANGE = 40;
const SELECT_FROM_TABLE = 41;
// So we can download the trade numbers without the volumes, can more usefully get back the data we request rather than having to filter it on the client.


// Select from being important to easily get th ids alongside name or another value.


//const SELECT_FROM_TABLE = 40;
// Then read a selection_params object, we decode a binary encoded object. Don't have the fields encoded ad-hoc, use they system we have.
// Choose which fields.
//  Could do field-skip decoding?
//   Probably not with the structure as it is.
//    Likely need to decode the whole DB record.
//    A function that reads through the encoded data in a more optimised way would work well.
//     Not decode_buffer, but decode_select_from_buffer

// Fields returned should be encoded as rows?
//  Or it's just an array?
//   Just returning an array looks a bit more sensible.
//   An array for each row. It's not keys and values separated.










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

const LL_SEND_MESSAGE_RECEIPT = 120;
// also want stop, pause and resume messages.
//  Though the server side will pause and later stop if the client is too slow / itself stops, want the client to be able to control this process too.
//   That would mean a slow client could itself do pauses to prevent disconnection.

const LL_MESSAGE_STOP = 121;
const LL_MESSAGE_PAUSE = 122;
const LL_MESSAGE_RESUME = 123;









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


let map_received_page = {};


var handle_ws_binary = function (connection, nextleveldb_server, message_binary) {


    // It could become much simpler, like this.
    //  Putting message parsing in the Message object will save on code here.
    //  Command rather than message? Command_Message
    //   There would be other messages that could interact with a Command_Message, such as stop, resume, pause, acknowledge receipt.

    // The server could also parse and return messages.
    //  Not sure they should be in the same format.
    //   Command_Response_Message

    // Writing advanced separate code for these would help to greatly shorten the code here, and to make it more reliable.




    /*

    let message = new Message(message_binary);
    let obs = command_runner(message);
    send(obs, message.options);

    */


    // Would be nicest to fully parse the message_binary with one call.
    //  Parse it into an object, will itself have a command_name property.
    //   call that on the DB.
    // Even for compatability, this parser could have specific parsings / encodings for specific message command names.
    //  Keep the encoding protocol specific and more efficient for many cases.
    //   Could incorporate some more options here.

    // Modelling the message means having functionality that will work anywhere and is not specific to client or server, it serialises and deserialises messages, and can interact with ways to set / specify a command.
    //  Will have quite a lot of functionality within that part. Will help to make the code for encoding and decoding messages concise and will move code out of both the ll and normal client, as well as from the server.
    //  Keeping the exact same API in various cases while allowing for expanded functionality will help this to work as a client when using the old servers.
    //   However, it is likely worth deploying this on yet another server.
    //   This would be a major code upgrade. Maybe getting on with syncing without this Model Command object would make sense.










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
    let nldb = nextleveldb_server;


    var message_id, i_query_type, pos = 0;
    [message_id, pos] = x.read(message_binary, pos);
    [i_query_type, pos] = x.read(message_binary, pos);

    //console.log('message_id', message_id);
    //console.log('i_query_type', i_query_type);


    var buf_the_rest = Buffer.alloc(message_binary.length - pos);
    message_binary.copy(buf_the_rest, 0, pos);
    var buf_msg_id = xas2(message_id).buffer;


    // This would be a point to read the auth token, if there is one.
    //  Auth will first be done on connection, so an access token would not need to be provided with each websocket request
    //   - Except that would be an extra security option.

    // May make a different version of this that uses an actual paging object which gets read from the buffer.
    //  Would also have some decoding options.
    //   Could be a place to remove KPs from encoded data. Keep the KPs in there in the server calls. Don't further complicate the code there.

    // May experiment with Observable_Processor
    // Obs_IO


    let parse_paging_options_message = (buf_the_rest) => {
        // use Paging.read
        let pos = 0,
            paging;
        [paging, pos] = Paging.read(buf_the_rest);

        //console.log('pos', pos);
        //console.log('buf_the_rest', buf_the_rest);


        //let args = Binary_Encoding.decode_buffer(buf_the_rest, 0, pos);
        return paging;

    }

    let cb_send = (err, res) => {
        // Encode as binary

        // Using Command_Response_Message will be best.

        // And something to say we are sending back a single item?


        //let message_type_id = BINARY_PAGING_NONE;
        //console.log('cb_send message_id', message_id);

        //console.log('*** res', res);

        // The result could already be a buffer.
        //  It won't always be.

        //console.log('Binary_Encoding.encode_to_buffer(res)', Binary_Encoding.encode_to_buffer([res]));


        //let message = new Command_Response_Message(message_id, BINARY_PAGING_NONE, Binary_Encoding.encode_to_buffer(res));
        let message = new Command_Response_Message(message_id, BINARY_PAGING_NONE, Binary_Encoding.encode_to_buffer([res]));
        //console.log('message', message);
        connection.sendBytes(message.buffer);

        //let encoded = Binary_Encoding.encode_to_buffer(res);

        // the message id
        //  then the response type?



    }



    // Will have a maybe nicer 'send' function.
    //  (communication_options, obs_call)
    //  Maybe not very different, will handle kp removal id that's an option.
    //   It's part of a system that handles message and page size parsing behind the scenes.
    //    It will make for simpler code that encompases needed functionality in a middleware kind of way.

    let page_binary_stream = (paging_option, page_size, obs_call) => {

        //console.log('page binary stream paging_option', paging_option);

        if (paging_option === PAGING_RECORD_COUNT) {


            let c = 0;
            let arr_page = new Array(page_size);
            let page_number = 0;

            obs_call.on('next', data => {
                //console.log('obs_call next data', data);

                // Want to keep the internal data, but say it is an array of data.
                //  Encode it as if it's an array.
                //  That means give it the array type, and then its inner data length.

                let buf_arr_page_item = Buffer.concat([xas2(ARRAY).buffer, xas2(data.length).buffer, data]);




                //arr_page.push(data);

                // want to say that data is an array.
                //  Check the data is being provided OK.


                arr_page[c++] = buf_arr_page_item;

                if (c === page_size) {

                    // send the page.
                    //  Encode all the arrays to binary.
                    //   Send to the client.


                    // Not sure about having to put the buffer in an array here.


                    // Want simple syntax here... need to be careful about when we have a page of data, and when we have a single item.
                    //console.log('arr_page', arr_page);


                    let buf_page_data = Binary_Encoding.encode_to_buffer([arr_page]);
                    //console.log('flow buf_page_data.length', buf_page_data.length);

                    // message id, binary flow response, page number, the encoded arr_page

                    let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_flow, xas2(page_number).buffer, buf_page_data]);

                    connection.sendBytes(buf_message);


                    c = 0;
                }


            })
            obs_call.on('complete', () => {

                // Send the incomplete page as flow last

                //console.log('obs_call complete');

                if (c > 0) {

                    //console.log('arr_page', arr_page);

                    // A different method of encoding?
                    //let buf_page_data = Binary_Encoding.encode_to_buffer([arr_page.slice(0, c)]);

                    let buf_page_data = Buffer.concat(arr_page.slice(0, c));

                    // Length encoded individual results.
                    //  Or each item just encoded as an array. Don't want these buffers to be encoded as buffers.
                    //   Need to pay close attention to how the result set gets built here.
                    //    If necessary, do a bit of a protocol overhaul if it removes ambiguity in the results.
                    //    Would be nicer to make paging handling code simpler and more concise.
                    //     Could move to the instructions being managed by the Model in an OO way.
                    //     Would be able to use a variety of instruction classes, with their params and some flexibility. Then would encode params to binary.
                    //      

                    // Encoding for results sets...
                    //  Don't say the results contain buffers, we need to encode an array of buffers 




                    console.log('last buf_page_data.length', buf_page_data.length);

                    // Not so sure about getting back the 0th item?

                    let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_last, xas2(page_number).buffer, buf_page_data]);
                    connection.sendBytes(buf_message);
                }


            });
        } else {
            throw 'page_binary_stream: Unsupported paging_option ' + paging_option;
        }

    }


    let send = (communication_options, obs_call) => {

        // Comm options may well hold quite a bit.
        //  KP stripping could happen here.



        //console.log('send communication_options', communication_options);
        let paging_option = communication_options.paging_type;

        let limit = communication_options.limit || -1;
        //console.log('limit', limit);

        //console.log('paging_option', paging_option);

        if (paging_option === PAGING_COUNT) {
            let c = 0;
            let arr_page = new Array(page_size);
            let page_number = 0;

            let response_type = obs_call.response_type;
            if (!response_type) {
                console.trace();
                console.log('send communication_options', communication_options);
                throw 'response_type needs to be set in the observable returned previously. Acceptable values are "keys", "records", "array"';
            }
            console.log('response_type', response_type);

            // Need to do this differently depending on response type, it seems.
            //  Since they are records, need to encode and send them as records.




            if (response_type === 'array') {
                obs_call.on('next', data => {
                    console.log('binary send has data ' + send);

                    let buf_arr_page_item = Buffer.concat([xas2(ARRAY).buffer, xas2(data.length).buffer, data]);
                    arr_page[c++] = buf_arr_page_item;
                    if (c === page_size) {
                        let buf_page_data = Binary_Encoding.encode_to_buffer([arr_page]);
                        let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_flow, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                        c = 0;
                    }
                })
                obs_call.on('complete', () => {
                    //console.log('obs_call complete');
                    if (c > 0) {
                        let buf_page_data = Buffer.concat(arr_page.slice(0, c));
                        //console.log('last buf_page_data.length', buf_page_data.length);
                        let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_last, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                    }
                });
            } else if (response_type === 'binary') {
                // Will not need to do much to encode the data to a buffer.
                //  Should not require extra encoding?
                //  Or it's that some results will 

                // Not handling the error paths right now.

                obs_call.on('next', data => {
                    let buf_data;
                    // The data could be as an array.
                    //  If so, need to join them up
                    let t_data = tof(data);
                    //console.log('t_data', t_data);
                    if (t_data === 'array') {
                        throw '8 stop';
                        buf_data = Binary_Encoding.array_join_encoded_buffers([Binary_Encoding.array_join_encoded_buffers(data)]);
                    } else {
                        // It would already be an array buffer, I think
                        //console.log('** data', data);

                        buf_data = data;

                        //console.log('pre try stop');
                        //throw 'stop';

                        //buf_data = Binary_Encoding.encode_buffer_as_array_buffer(data);
                    }
                    arr_page[c++] = buf_data;
                    if (c === page_size) {
                        //let buf_page_data = Binary_Encoding.array_join_encoded_buffers(arr_page);

                        // join encoded buffers, but don't specifically say it's encoded as an array
                        //  it's encoded as an array by default if it's not contained within anything else.

                        //console.log('arr_page', arr_page);
                        //throw 'stop';


                        // encode_buffers_as_buffers
                        let buf_page_data = Binary_Encoding.encode_buffers_as_buffers(arr_page);



                        //let buf_page_data = Buffer.concat(arr_page);


                        let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_last, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                        c = 0;
                    }
                })
                obs_call.on('complete', () => {
                    if (c > 0) {
                        let sliced_page = arr_page.slice(0, c);

                        //console.log('sliced_page', sliced_page);
                        //throw 'stop';



                        // And they wind up encoded as buffers so they can be stored in the envelope.
                        let buf_page_data = Binary_Encoding.encode_to_buffer((sliced_page));
                        //let buf_page_data = Binary_Encoding.encode_to_marked_buffer((sliced_page));

                        // Multiple buffers, all to get encoded into one buffer

                        //console.log('buf_page_data', buf_page_data);

                        let buf_message = Buffer.concat([buf_msg_id, buf_binary_paging_last, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                    }
                });
            } else if (response_type === 'records') {
                // Needs to be decoded differently, only difference in the response here is giving the paging type (and this will maybe be changed soon).

                // 
                obs_call.on('next', data => {
                    console.log('records send has data ', data);

                    // Binary_Encoding.join_buffer_pair([data.key, data.value]);


                    // 

                    // This encodes each record within an array. Less efficient down the wire. Maybe better becuase it's more standard.
                    //let buf_arr_page_item = Buffer.concat([xas2(ARRAY).buffer, xas2(data.length).buffer, Binary_Encoding.join_buffer_pair(data)]);


                    // var buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);

                    let buf_arr_page_item = Binary_Encoding.join_buffer_pair(data);
                    // No, don't encode them as an array.
                    //  Just as it is when sending records elsewhere.

                    arr_page[c++] = buf_arr_page_item;
                    if (c === page_size) {
                        let buf_page_data = Binary_Encoding.encode_to_buffer([arr_page]);
                        let buf_message = Buffer.concat([buf_msg_id, buf_record_paging_flow, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                        c = 0;
                    }
                })
                obs_call.on('complete', () => {
                    //console.log('obs_call complete');
                    if (c > 0) {
                        let buf_page_data = Buffer.concat(arr_page.slice(0, c));
                        //console.log('last buf_page_data.length', buf_page_data.length);
                        let buf_message = Buffer.concat([buf_msg_id, buf_record_paging_last, xas2(page_number).buffer, buf_page_data]);
                        connection.sendBytes(buf_message);
                    }


                });

            } else {
                throw 'response_type ' + response_type + ' unsupported';
            }
        } else {
            throw 'NYI';
        }
    }



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

        // May contain paging option.




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

            if (paging_option === PAGING_TIMED) {

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

        // batch_put is quite low level.
        //  Splits up the buffer itself.

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


    // Putting limit into the communication protocol and making it standard all the way through will be useful.
    //  A calling options object may make sense. Could pass through the comms options and use the relevant attributes.




    if (i_query_type === LL_COUNT_KEYS_IN_RANGE) {
        console.log('LL_COUNT_KEYS_IN_RANGE');


        // Doing full message parsing would give a nice advantage here.
        //  Don't want to have to change so much complex code.
        //  Will have simpler code, will also get the limit value.








        // This could be improved using automatic option parsing, and sending back of observable results.
        //  Would use an nldb count function that returns a stoppable (and pausable?) observable.
        //   Will be able to pause streams from the client by sending commands to do so, referencing the original command id.
        //    Command ID may be better than Message ID.




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

        //console.log('paging_option', paging_option);

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
        } else if (paging_option === PAGING_TIMED) {
            // A timed pager.
            console.log('paging_option === PAGING_TIMED');
            let paging_delay = page_size;
            //console.log('paging_delay', paging_delay);
            var arr_res = [buf_msg_id, buf_binary_paging_flow];

            var count = 0,
                page_number = 0;
            //var res = [];
            let interval;

            let key_stream = db.createKeyStream({
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
                    console.log('*** count', count);
                    clearInterval(interval);

                    //arr_res.push(xas2(count).buffer);

                    //buf_res = Buffer.concat(arr_res);
                    //connection.sendBytes(buf_res);

                    // Repeat the last one?
                    //  Best not to right now.

                    buf_res = Buffer.concat([buf_msg_id, buf_binary_paging_last, xas2(page_number).buffer, Binary_Encoding.flexi_encode_item(count)]);
                    connection.sendBytes(buf_res);
                });

            interval = setInterval(() => {
                // Operation stopped if 20 pages pending reception confirmation from the client.

                console.log('interval count', count);

                let latest_received_page = map_received_page[message_id];
                //console.log('latest_received_page', latest_received_page);

                let delay = 0,
                    pages_diff = 0;
                if (typeof latest_received_page !== 'undefined') {
                    pages_diff = page_number - latest_received_page;

                }
                //console.log('pages_diff', pages_diff);

                if (pages_diff > 20) {
                    key_stream.destroy();
                    clearInterval(interval);
                }


                //buf_res = Buffer.concat([buf_msg_id, buf_binary_paging_flow, xas2(count).buffer]);
                buf_res = Buffer.concat([buf_msg_id, buf_binary_paging_flow, xas2(page_number++).buffer, Binary_Encoding.flexi_encode_item(count)]);
                connection.sendBytes(buf_res);
            }, paging_delay)


        } else {
            throw 'NYI'
        }

        /*

        if (return_message_type) {
            // Though paging is an option, this is not a paged or pagable response.
            var arr_res = [buf_msg_id, buf_binary_paging_flow];
        } else {
            var arr_res = [buf_msg_id];
        }
        */




    }


    // For copying and syncing, will have some key comparison operations.
    //  Stream all keys for a table from the other db, and for missing keys retrieve the records (or just values) and put them into the DB (matching with keys).

    // Should be able to get many keys copied to the DB quickly, though the goal is to have a server running locally, as well as workstation, with syncing taking place from the montreal cloud.
    // Will do more with getting the data from one server to another, and reliably setting it up so it takes place automatically.


    // Connect to another DB, and observe the keys on a table. Compare basis tables first. Could check refs to do this comparison.
    //  Compare all of the values in one table - observe all of the keys in the source table, and compare with the keys in this table.

    // Further key streaming and comparison operations would be very useful.
    //  streaming low level get keys looks like the next thing to work on as an Observable.







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



    if (i_query_type === LL_GET_FIRST_KEY_BEGINNING) {
        //console.log('LL_GET_FIRST_KEY_BEGINNING');
        //let command_message = new Command_Message(message_binary);
        //let communication_options = command_message.paging;
        //let msg_params = new Command_Message(message_binary).inner;
        //let [buf_key_begining] = new Command_Message(message_binary).inner;
        nldb.get_first_key_beginning(new Command_Message(message_binary).inner[0], cb_send);
        // then get the buffer key beginning param...
    }

    // Test this on the local client.

    if (i_query_type === LL_GET_LAST_KEY_BEGINNING) {

        console.log('LL_GET_LAST_KEY_BEGINNING');


        /*
        let command_message = new Command_Message(message_binary);
        console.log('command_message', command_message);

        // Messages will always parse the communications options.

        //let communication_options = command_message.paging;
        //console.log('communication_options', communication_options);
        let msg_params = command_message.inner;
        // Need to decode that inner value

        console.log('msg_params', msg_params);
        let [buf_key_begining] = msg_params;
        // that's fine, the buffer that it begins with is decoded as a buffer.



        //LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH

        nldb.get_last_key_beginning(buf_key_begining, cb_send);
        */

        nldb.get_last_key_beginning(new Command_Message(message_binary).inner[0], cb_send);


        // cb_send

        // this.cb_send(what the normal cb would be)



        //send_cb(nldb.get_last_key_beginning)



        // then get the buffer key beginning param...



    }



    // LL_GET_FIRST_LAST_KEYS_IN_RANGE

    if (i_query_type === LL_GET_FIRST_LAST_KEYS_IN_RANGE) {
        console.log('LL_GET_FIRST_LAST_KEYS_IN_RANGE');

        // get the rest of the buffer.
        //console.log('buf_the_rest', buf_the_rest);
        //console.log('buf_the_rest.length', buf_the_rest.length);

        // Not so sure about leaving out the paging option on this instruction.
        //  Client could set some optimizations.
        //   Makes it more complex, but solving different parts will improve performance.

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

            let fns = Fns();
            fns.push([this, this.get_first_key_in_range, [s_buf]]);
            fns.push([this, this.get_last_key_in_range, [s_buf]]);
            fns.go(2, (err, res_all) => {
                if (err) {
                    throw err;
                } else {
                    console.log('res_all', res_all);
                    throw 'stop';
                }

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

        //console.log('paging_option', paging_option);
        //console.log('page_size', page_size);
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

            // This needs backpressure handling too.
            //  Well connected servers can send data out faster than clients can receive.
            //   This is one reason compression will speed up the client experience if done right.





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

    // Needs paging improvement including backpressure.


    if (i_query_type === LL_GET_KEYS_IN_RANGE) {
        console.log('LL_GET_KEYS_IN_RANGE');

        // when there are 0 records?
        // Maybe not returning data OK.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        //if (paging_option > 0) {


        // let [paging, pos] = Paging.read(buf_the_rest, pos);

        // Paging.read would be easier.

        // Then when having read the paging option, we have easy access to 'limit' and 'reverse' variables.
        //  Reverse and limit 1 will enable the key before a key to be obtained easily, as well as the last key in a range / table.
        //  That will help with checking incrementors have been set correctly.
        //   Will have a simple db repair that sets all pk incrementors to the last key + 1.
        //   Checking and repairing incrementors on start seems like a good way to do this.

        // Worth getting another deployment or two up and running?
        //  Getting the safety / fixed version running will be very useful.

        // Could have lower level, and server function:
        //  get_last_key_in_table
        //   That would mean we can quickly get on with the incrementor db repair on start.
        //    That would fix data4 and data5, hopefully before its too late.



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

        if (paging_option === PAGING_KEY_COUNT) {
            // Read the page size.
            [page_size, pos] = x.read(buf_the_rest, pos);
            [b_l, pos] = read_l_buffer(buf_the_rest, pos);
            [b_u, pos] = read_l_buffer(buf_the_rest, pos);
            let c = 0;
            let arr_page = new Array(page_size);
            let page_number = 0;

            let read_stream = db.createKeyStream({
                'gt': b_l,
                'lt': b_u
            }).on('data', function (key) {

                //arr_page.push(xas2(key.length).buffer);
                //arr_page.push(key);


                // This needs to slow down sending if a receipt backlog builds up.


                arr_page[c++] = (Buffer.concat([xas2(key.length).buffer, key]));

                // This should also have flow control.

                // Having flow control here will enable many keys to be read, and as they get read, if they are not in the db, to get requested.
                //  One way copy sync.
                //  copy_table_records_from

                // copy_table_from
                // copy_table_structure_from

                // (_from)

                // Have a system where a DB server can be told of other clients to connect to.
                //  Then it can do connected operations, such as copy_remote_table

                // A copy_remote_table system would be great to be able to call from the client.
                //  Gets progress updates, as client is not copying it itself, but instructing the server to copy it.

                // Once copy_remote_table works nicely, including on the client, a lot can be done on the xeon.
                //  it will be useful for syncing because it won't copy records it does not need to copy.

                // Would get more complex if it needs to translate any records.
                //  It could download denormalised records.
                //  Denormalizaton and renormalization would then be extra features.

                // Will get the remote DBs copying / syncing to local.
                //  Having a local server will help.
                //  Need to get the data closer to data analysis form.

                // Being able to export the data to disk, on the server will be very useful.
                //  Will be nicest to keep data within the network and the app for the moment though.

                // Want to get all 3 dbs streaming to a local machine soon.
                //  Then get them streaming to the Xeon
                //  Then streaming from xeon to workstation.

                // Will have commands to get a server to subscribe to table updates from another
                //  But may have to validate the table structure is the same.
                //  Would likely need to update incrementors too as they change.


                // Quite a lot more to do to get full syncing.

                // The streaming looks fairly powerful so far, and we would before long have the capability to shard data puts, and the streaming from each would
                //  be a bit tricky to coordinate, but possible. Could get some to slow down or pause if they get ahead. Then merge results and send them to a client.
                //   Or the client could stream results from multiple servers.





                if (c === page_size) {

                    let latest_received_page = map_received_page[message_id];
                    //console.log('map_received_page', map_received_page);
                    let delay = 0,
                        pages_diff = 0;


                    if (typeof latest_received_page !== 'undefined') {
                        pages_diff = page_number - latest_received_page;
                        if (pages_diff > 2) {
                            delay = 250;
                        }
                        if (pages_diff > 4) {
                            delay = 500;
                        }
                        if (pages_diff > 6) {
                            delay = 1000;
                        }
                        if (pages_diff > 8) {
                            delay = 2000;
                        }



                    }

                    if (pages_diff > 20) {
                        key_stream.destroy();
                        clearInterval(interval);
                    } else {
                        if (delay > 0) {
                            //console.log('pages_diff', pages_diff);
                            read_stream.pause();
                            setTimeout(() => {
                                read_stream.resume();
                            }, delay);
                        }
                    }

                    connection.sendBytes(Buffer.concat([buf_msg_id, buf_key_paging_flow, xas2(page_number++).buffer].concat(arr_page)));
                    //console.log('sent page ' + (page_number - 1));
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


                    arr_res = [buf_msg_id, buf_key_paging_last, xas2(page_number).buffer].concat(arr_page.slice(0, c));
                    buf_res = Buffer.concat(arr_res);
                    connection.sendBytes(buf_res);
                    console.log('sent end');
                })
        }
    }


    // Could move some of the paging handling / processing out of this level, and into the nextleveldb-server.
    //  Having observables in there would be useful.
    //   Also would be nice to have a lower surface area of DB calls, so the the DB API can be upgraded (though that would make it slower if there is another function call in between)
    //    Could do with a slightly more abstracted interface involving observables for managing fairly large throughputs of data in managable and efficient chunks.

    // Seems like we will have fairly large / comprehensive changes to the core of the database operations, but not to the record structure.

    if (i_query_type === LL_SEND_MESSAGE_RECEIPT) {
        //console.log('LL_SEND_MESSAGE_RECEIPT');
        pos = 0;
        let message_id, page_number;
        [message_id, pos] = x.read(buf_the_rest, pos);
        [page_number, pos] = x.read(buf_the_rest, pos);

        //console.log('buf_the_rest', buf_the_rest);

        //console.log('LL_SEND_MESSAGE_RECEIPT', message_id, page_number);

        if (!map_received_page[message_id]) {
            map_received_page[message_id] = page_number;
        }

        if (map_received_page[message_id] < page_number) {
            map_received_page[message_id] = page_number;
        }
    }


    // Seems like it's worth redoing this so it calls a get_records_in_range db function
    //  Then output those results accordingly.
    //  That would help with other pieces of functionality?

    // A new version of this could use more abstract functionality to wrap inner db code to do this.
    //  Read the paging / options.
    //  // That specific way of doing things would greatly improve the logical flow, maybe eventually perf

    // Setting a limit on a number of operations through a standard interface.

    // This could be re-done in a much more concise way, using an observable server call.
    //  Want both pause and stop in that observable.

    if (i_query_type === LL_GET_RECORDS_IN_RANGE) {

        // Better if this used the server function, and wrapped it in a message and paging encoder / sender.
        //  Read the message (or Command_Message), then process it.






        console.log('LL_GET_RECORDS_IN_RANGE');


        // Should probably be changed to use the server's get_records_in_range function.

        // when there are 0 records?
        // Maybe not returning data OK.

        var paging_option, page_size;

        pos = 0;
        [paging_option, pos] = x.read(buf_the_rest, pos);

        //console.log('paging_option', paging_option);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

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
                //arr_page[c++] = buf_combined;
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

            let buf_combined;

            // And need to put the results into pages.

            // Can we slow down this stream if it's reading too fast for the client?

            // Can use stream.pause to pause the reading.
            //  Could do this in response to a message?
            //  

            let read_stream = db.createReadStream({
                'gt': b_l,
                'lt': b_u
            }).on('data', function (data) {

                // will be both the key and the value
                // will need to combine them as buffers.
                buf_combined = Binary_Encoding.join_buffer_pair([data.key, data.value]);
                //console.log('buf_combined', buf_combined);
                //arr_res.push(buf_combined);
                arr_page[c++] = buf_combined;

                if (c === page_size) {

                    //console.log('sending page', page_number);
                    //console.log('pre read_stream.pause');

                    // Check the current page number to see how far behind it is.

                    let latest_received_page = map_received_page[message_id];
                    //console.log('map_received_page', map_received_page);
                    let delay = 0,
                        pages_diff = 0;
                    if (typeof latest_received_page !== 'undefined') {
                        pages_diff = page_number - latest_received_page;

                        if (pages_diff > 2) {
                            delay = 250;
                        }
                        if (pages_diff > 4) {
                            delay = 500;
                        }
                        if (pages_diff > 6) {
                            delay = 1000;
                        }
                        if (pages_diff > 8) {
                            delay = 2000;
                        }

                        // if it gets too high, then stop the stream?
                        //  ie the client has 

                    }
                    //console.log('pages_diff', pages_diff);

                    read_stream.pause();
                    setTimeout(() => {
                        read_stream.resume();
                    }, delay);

                    // Possibility of applying a compression algorythm to the arr_page?
                    //  Compressing the data could raise the throughput to the client.
                    //   Currently data seems about 5 times the size when over the wire rather than in the DB.

                    // Could have a compressed data format for record paging.
                    //  Maybe use Binary_Encoding's buffer compression?

                    // Or have a different Buffer_Compression module available.
                    //  Don't want streaming compression, as we compress parts of the stream, ie some messages within it.

                    // record paging flow, with compression?
                    //  then read another xas2 number, or maybe read a CompressionInfo object.

                    // For the moment could have most basic compression options, with sensible defaults.

                    //  The client could request compression too.
                    //  Reading compression info from the request would be sensible.




                    connection.sendBytes(Buffer.concat([buf_msg_id, buf_record_paging_flow, xas2(page_number++).buffer].concat(arr_page)));
                    c = 0;
                    // Could empty that array, would that be faster than GC?
                    arr_page = new Array(page_size);


                    // On the client-side, don't want to pause the whole socket.

                    // Try pausing the reading of the stream for 1s.
                    //  Will be able to pause streams when the client-side receive buffer becomes too large.
                    //   Could have a client-side message to say which is the last message received and processed.
                    //    Then if it gets out of sync by more than n (ie 4), it waits until the client has caught up.

                    // Would need client-side acknowledgement of receiving the messages.
                    //  Client-side and server-side pause commands would be useful.

                    // Important to be able to correctly sync large amounts of data, fast, or at least fast enough while also reliably.
                    //  The sender's internet connection may be much faster than the receiver's.

                    // Some small messages in the protocol to say the last message number in a message chain could help.
                    //  Small receive packets would be sent back to the server.
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

    // also will have select from table?

    if (i_query_type === DELETE_RECORDS_BY_KEYS) {
        console.log('DELETE_RECORDS_BY_KEYS');
        let command_message = new Command_Message(message_binary);
        console.log('command_message', command_message);
        let msg_params = command_message.inner;
        console.log('msg_params', msg_params);
        nldb.ll_delete_records_by_keys(msg_params, cb_send);
    }


    if (i_query_type === LL_GET_RECORDS_IN_RANGES) {

        // Better if this used the server function, and wrapped it in a message and paging encoder / sender.
        //  Read the message (or Command_Message), then process it.

        // Message parsing would take place at an earlier stage?
        //  The paging option could be the next.

        // worth reading the whole message buffer.
        //  The paging options could include communication params, but not the params for the command itself.

        // The ranges would be decoded from the rest of message buffer.

        // message.inner

        console.log('LL_GET_RECORDS_IN_RANGES');

        let command_message = new Command_Message(message_binary);

        let communication_options = command_message.paging;
        console.log('communication_options', communication_options);

        let msg_params = command_message.inner;

        console.log('msg_params', msg_params);

        let obs_res = nldb.get_records_in_ranges(msg_params);

        send(communication_options, obs_res);










    }






    // Selecting from records in range may keep the table kp as an option.



    if (i_query_type === SELECT_FROM_TABLE) {

        console.log('SELECT_FROM_TABLE', SELECT_FROM_TABLE);

        // read the instruction.

        // paging option, then the rest is the encoded instruction args

        let paging_option, page_size, pos = 0,
            b_l, b_u;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        //console.log('buf_the_rest', buf_the_rest);

        // Need to be able to decode from a starting pos.

        let args = Binary_Encoding.decode_buffer(buf_the_rest, 0, pos);
        //console.log('args', args);
        let [table_id, fields] = args;

        // then want a nicer way to output a result stream from the server.

        //page_result_stream(connection, message_id, nldb.select_from_table(table_id, fields));


        // Don't want to decode it on the server.
        //  Should remove the kp by default.
        page_binary_stream(paging_option, page_size, nldb.select_from_table(table_id, fields, false));





    }



    // Select from (records) could also be expressed in the query object, but that's a bit more advanced.
    //  Could select from whatever result set.

    if (i_query_type === SELECT_FROM_RECORDS_IN_RANGE) {

        throw 'NYI';

        let paging_option, page_size, pos = 0,
            b_l, b_u;
        [paging_option, pos] = x.read(buf_the_rest, pos);
        if (paging_option > 0) {
            [page_size, pos] = x.read(buf_the_rest, pos);
        }

        // then decode the range info
        [b_l, pos] = read_l_buffer(buf_the_rest, pos);
        [b_u, pos] = read_l_buffer(buf_the_rest, pos);

        // then decode the field selection array
        //  Then decode a buffer.

        // How about decoding a buffer from a starting position?



        let arr_selection_fields = Binary_Encoding.decode_buffer(buf_the_rest, 0, pos);




        if (paging_option === NO_PAGING) {

            // Use the callback version of the function.

            nldb.select_from_records_in_range()




        }
        // Then it will use Binary Paging, not record paging. It's not returning records.

        if (paging_option === PAGING_RECORD_COUNT) {
            // Return BINARY_PAGING_... messages

            // An output page bufferer may help.
            //  The pager has the rosponse stream, and gets given the data, makes its own choices about when to page.


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

    // The limit property, incorportated into the protocol, will make this method obselete.
    //  Would just set a limit property to the db query.



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

        //console.log('count_limit', count_limit);

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
                'lt': b_u,
                'limit': count_limit
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

        //console.log('buf_the_rest', buf_the_rest);

        let def = Binary_Encoding.decode_buffer(buf_the_rest)[0];
        // For some reason, decoding of the buffer puts it all into a new array.

        //console.log('def', def);


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
                //console.log('ENSURE_TABLES res_ensure', res_ensure);
                var msg_response = [buf_msg_id, buf_binary_paging_none, xas2(BOOL_TRUE).buffer];
                buf_res = Buffer.concat(msg_response);
                connection.sendBytes(buf_res);
            }





        });





        // Won't have a paged response

        //throw 'stop';

    }



    // Would be better to use and wrap a ll server function.

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
            //console.log('[table_id, record]', [table_id, record]);

            let model_table = nextleveldb_server.model.tables[table_id];
            //console.log('model_table', model_table);

            // then create a record on that table.

            // Seems like the incrementors have not been set up on the server side model.
            //  When loading the model, need to make sure that the server side incrementor gets set up properly.


            let model_record = model_table.new_record(record);
            //  That model record could have funtionality to encode itself as a buffer.

            //console.log('model_record', model_record);

            // Then do ll put on the model record encoded.

            // Need encoding to the DB system to be part of the model record.
            //  Could also get the index records, so to do a batch put.

            //let encoded_record = model_record.to_b

            // I think this does the job on the server of making index records OK.

            let arr_record_and_index_buffers = model_record.to_arr_buffer_with_indexes();
            //console.log('arr_record_and_index_buffers', arr_record_and_index_buffers);

            // Then can ll put records.
            //  

            // Something that arranges the batches.

            nextleveldb_server.arr_batch_put(arr_record_and_index_buffers, (err, res_put) => {
                if (err) {
                    throw err;
                } else {
                    //console.log('res_put', res_put);

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


    // This is a way around not having unique constraints right now.
    //   Checks if the record exists based on indexed info.
    if (i_query_type === ENSURE_RECORD) {
        console.log('ENSURE_RECORD');

        // Does the record already exist based on index lookups

        // Is the key complete?

        // Use the oo Command_Message class

        let cm = new Command_Message(message_binary);
        //console.log('cm', cm);





    }


    if (i_query_type === ENSURE_TABLE_RECORD) {
        console.log('ENSURE_TABLE_RECORD');

        // Does the record already exist based on index lookups

        // Is the key complete?

        // Use the oo Command_Message class

        let cm = new Command_Message(message_binary);
        //console.log('cm', cm);

        // decode the command message.
        //console.log('cm.id', cm.id);
        //console.log('cm.command_id', cm.command_id);

        // then thes are the params.
        //  any buffers in there will need to be decoded too.

        // worth having this in Command_Message.





        //console.log('cm.inner', cm.inner);

        let [table_id, record] = cm.inner;

        // then we do ensure table record, sending the result.

        (async () => {
            //console.log('pre ensure_table_record');
            let res_ensure_table_record = await nextleveldb_server.ensure_table_record(table_id, record);

            //console.log('res_ensure_table_record', res_ensure_table_record);

            // then send the record back in the Command_Response_Message

            // Give it an observable and it would make multiple pages?
            //  Or a different class for that?

            // 
            //console.log('message_id', message_id);

            let cmr = new Command_Response_Message(message_id, res_ensure_table_record);
            //console.log('cmr', cmr);
            //console.log('cmr.buffer', cmr.buffer);
            //console.log('cmr.value', cmr.value);
            connection.sendBytes(cmr.buffer);



            // then just a command 

            // 
        })();


        // Then these buffers will be some kind of encoded buffer-backed types
        // Their xas2 prefix indicates what type they are.





        // inner

        // decode inner buffer.
        //  more like we want the command args.





    }



    if (i_query_type === TABLE_EXISTS) {
        // Decode the table name

        // Will send back a binary response message.
        console.log('TABLE_EXISTS');

        let table_name = Binary_Encoding.decode_buffer(buf_the_rest)[0];
        //console.log('table_name', table_name);

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
                    //console.trace();

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

        //console.log('buf_the_rest', buf_the_rest);

        //let i = Binary_Encoding.decode_buffer(buf_the_rest);

        [table_id, pos] = x.read(buf_the_rest, 0);
        //console.log('table_id', table_id);


        // Could do a result pass-through.



        //let table_id = i[0];

        nextleveldb_server.get_table_fields_info(table_id, (err, table_fields_info) => {
            if (err) {
                throw err;
            } else {
                //console.log('table_fields_info', table_fields_info);
                // encode it

                let buf_encoded = Binary_Encoding.flexi_encode_item(table_fields_info);
                //console.log('buf_encoded', buf_encoded);

                // Send the response back to the client.
                let msg_response = [buf_msg_id, buf_binary_paging_none, buf_encoded];
                connection.sendBytes(Buffer.concat(msg_response));



            }
        })

    }






    // Test this with server and client.



    if (i_query_type === GET_TABLE_KEY_SUBDIVISIONS) {


        console.log('GET_TABLE_KEY_SUBDIVISIONS', GET_TABLE_KEY_SUBDIVISIONS);

        // Want to decode the key according to a reasonable standard. Little code to use here.

        // Worth getting the paging options.
        //  let [paging_options, args] = parse_paging_options_message(buf_the_rest);

        // Generally we don't want to get the decoded results. 
        //  Will call the versions of functions that produce encoded results with minimal decoding along the way.

        // paging_response(paging_options, nldb.get_table_key_subdivisions(args));
        //  paging_capable_response

        //let args = Binary_Encoding.

        let communication_options = parse_paging_options_message(buf_the_rest);
        console.log('communication_options', communication_options);
        // communication_options could also useful hold a 'limit' optional number parameter.
        //  Then if it reaches the limit, it could call the observer's 'stop' function, it could be given that function.
        //   Would build 'stop' functions into the lower level, have also done 'pause' in some cases with a different mechanism.

        // Stopping a count with this limit would be useful.
        //  Count makes sense when doing full sync.








        // array response type, set by get_table_key_subdivisions

        // get_table_key_subdivisions false false args to

        // Want the kps removed.
        //  Though that could be an option sent from the client.

        // Could have a communications option about removing key prefixes.
        //  The extra, and specific function in Binary_Encoding helps to process these values.

        let o_res = nldb.get_table_key_subdivisions.apply(nldb, communication_options.args.concat([true, false]));
        //console.log('o_res.responseType', o_res.responseType);


        // or just send
        //paging_response(communication_options, o_res);
        send(communication_options, o_res);

    }
};

module.exports = handle_ws_binary;