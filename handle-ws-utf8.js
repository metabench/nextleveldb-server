
var jsgui = require('lang-mini');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
var Fns = jsgui.Fns;

var x = xas2 = require('xas2');

var buffer_match = (buffer_1, buffer_2) => {
    var b_longer, b_shorter;

    if (buffer_1.length > buffer_2.length) {
        b_longer = buffer_1;
        b_shorter = buffer_2;
    } else {
        b_longer = buffer_2;
        b_shorter = buffer_1;
    }

    var l_shorter = b_shorter.length;
    var l_longer = b_longer.length;

    //var i_byte;
    var res = true;


    for (var c = 0; c < l_shorter; c++) {
        if (b_shorter.readInt8(c) !== b_longer.readInt8(c)) {
            res = false;
            break;
        }
    }
    //console.log('b_longer', b_longer);
    //console.log('b_shorter', b_shorter);
    //console.log('res', res);
    return res;
};


var handle_ws_utf8 = function(connection, nextleveldb_server, fns_ws, message) {
    console.log('Received Message: ' + message.utf8data);
    //bytes_in_this_second = bytes_in_this_second + message.utf8Data.length;
    var obj_message = JSON.parse(message.utf8Data);
    var function_params;

    //var initial_db_setup = fns_ws.initial_db_setup;
    //var starting_db_setup = fns_ws.starting_db_setup;
    var output_transform_to_hex = fns_ws.output_transform_to_hex;
    var get_table_key_prefix_by_name = fns_ws.get_table_key_prefix_by_name;

    if (is_array(obj_message)) {
        // Can have a handlers directory.
        //  Want to handle subscriptions.

        // A way of doing lower level calls, including calling for subscriptions.
        // Would be nice to do more work on the way subscriptions get dealt with.


        var client_request_id = obj_message[0];
        var function_name = obj_message[1];
        // console.log('1) function_params.length', function_params.length);
        //  This callback system has worked well so far, even for paged streaming functions.
        //  It does not look cut out to handle subscription services, such as updates that come in being relayed to subscribers.
        // Could check if the function name is 'subscribe', 'on', or 'listen'

        // Could have a Local_RAM_Timed_Values, which can subscribe to a nextlevel database as well as download a range of data from it.
        //  Want high performance data structures in RAM that can be queried rapidly. Likely to have a number of tests going on every second, or
        //   even in response to each piece of data arriving (would this be too slow?).

        if (function_name === 'subscribe') function_name = 'on';

        if (function_name === 'on') {

            // Need to cancel the subscriptions somewhow.
            //  Keeping track of the subscription function handler makes sense.


            // Client is subscribing to an event
            //  Just subscribe to an event name?
            //   Could also offer subscription params.
            //   put event
            //   put event, specific table
            //   put event, within key range
            //    would help clustering, but then exposing a high level subscription to all events would be trickier.
            //     if every node links to every other node in the cluster, they can subscribe to events to get the data that is required through
            //      the subscriptions that they provide. Could be a large amount of traffic if all nodes handle subscriptions and send updates to
            //      all other nodes.
            //     Maybe amount of traffic internal to the network would not be all that high / bad.
            //      The subscription is provided to the client by one of the nodes, and that node needs to subscribe to the other nodes.
            //      This way sounds reasonably efficient / high performance. Would need to see the costs when it comes to cloud services' machines sending
            //       to each other.

            // Designating a smaller number of event relays could work better?

            // May not be all that convenient describing subscriptions as arrays.
            //  Maybe we want to subscribe to updates within a table, but only with some specified key space.

            // Just listening for event names?

            //  Want to be able to listen for table updates as well.
            //  table-update as an event name?
            //   then it provides the table

            //  table-index-update?
            //   it's not really an event.

            // Some method of specifying which object's events we are listening to would be good.

            // Single event name such as put as a string.
            //  could have multiple parameters to 'subscribe'

            var subscription_params = obj_message[2];
            var event_name;
            var t_subscription_params = tof(subscription_params);

            //console.log('t_subscription_params', t_subscription_params);

            //console.log('subscription_params', subscription_params);
            //throw 'stop';

            if (t_subscription_params === 'string') {
                event_name = obj_message[2];
                //console.log('event_name', event_name);

                var subscription_handler = (obj_event) => {
                    //console.log('on ' + event_name, obj_event);
                    // and send the event back to the client?
                    var arr_response = [client_request_id, output_transform_to_hex(obj_event)];
                    //console.log('arr_response', arr_response);

                    connection.sendUTF(JSON.stringify(arr_response));

                };

                connection.subscription_handlers = connection.subscription_handlers || {};
                connection.subscription_handlers[event_name] = subscription_handler;

                nextleveldb_server.on(event_name, subscription_handler);

            }

            if (t_subscription_params === 'array') {

                //console.log('subscription_params.length', subscription_params.length);

                // Checks every put against the subscription.
                //  Going through a tree would be more effective algorithmically if there are enough subscriptions.
                //  At this stage though, not sure how important that is.
                //   May not handle all that many subscriptions.

                if (subscription_params.length === 2) {

                    var table_name = obj_message[2][0];

                    //console.log('obj_message', obj_message);
                    //throw 'stop';
                    event_name = obj_message[2][1];

                    //console.log('table_name', table_name);
                    //console.log('event_name', event_name);

                    // need to set up a listener for all put events here.
                    //  then check to see what table it is

                    // get the table prefix

                    get_table_key_prefix_by_name(table_name, (err, i_prefix) => {
                        if (err) {

                        } else {
                            //console.log('i_prefix', i_prefix);

                            if (event_name === 'put') {
                                // create a new put subscription handler to test if it is the specified table?

                                var subscription_handler = (obj_event) => {
                                    //console.log('subscription_handler on ' + event_name, obj_event);
                                    // and send the event back to the client?
                                    var arr_response = [client_request_id, output_transform_to_hex(obj_event)];
                                    //console.log('arr_response', arr_response);
                                    connection.sendUTF(JSON.stringify(arr_response));
                                };

                                var db_put_handler = (obj_event) => {
                                    var b_key = obj_event[0];
                                    var [i_table, discarded_pos] = xas2.read(b_key, 0);
                                    //console.log('x_table', x_table);

                                    //console.log('x_table.number', x_table.number);

                                    //var i_table =

                                    if (i_table === i_prefix) {
                                        subscription_handler(obj_event);
                                    }
                                };
                                // and another handler for the actual event we are going to raise, should it occurr
                                nextleveldb_server.on(event_name, db_put_handler);
                            }
                        }
                    });
                }

                if (subscription_params.length === 3) {

                    var table_name = obj_message[2][0];

                    //console.log('obj_message', obj_message);

                    var arr_index_space = obj_message[2][1];

                    //throw 'stop';
                    event_name = obj_message[2][2];

                    //console.log('table_name', table_name);
                    //console.log('arr_index_space', arr_index_space);
                    //console.log('event_name', event_name);

                    // we can encode the index value for comparison of buffer parts?
                    //  we can decode the items to see what the values from the indexes are

                    //  we can decode the indexes, according to the decoding?
                    //   however the encoding / decoding has not been specified at this stage.
                    //  Just checking the key values against this extra index value should do the trick.
                    //  Listen for all put events, and check to see if they satisfy this subscription criteria.
                    //   Could otherwise store info on subscriptions in the db or a RAM structure.


                    // How about having the plugins also manage subscriptions?
                    //  They could then better handle decoding.

                    // On this level, allow subscriptions to encoded data, on a lower level.
                    //  Doing range checks with a tree would likely be considerably faster when there are many subscriptions.

                    // For the moment, want it so clients can subscribe on a lower level.

                    // encode the arr_index_space as a buffer

                    //throw 'stop';

                    // need to set up a listener for all put events here.
                    //  then check to see what table it is

                    // get the table prefix

                    get_table_key_prefix_by_name(table_name, (err, i_prefix) => {
                        if (err) {

                        } else {
                            //console.log('i_prefix', i_prefix);
                            var b_prefix = xas2(i_prefix).buffer;
                            var a_b_key_space = [b_prefix];

                            each(arr_index_space, (i_idx) => {
                                a_b_key_space.push(xas2(i_idx).buffer);
                            });

                            //console.log('a_b_key_space', a_b_key_space);

                            var b_key_space = Buffer.concat(a_b_key_space);
                            //console.log('b_key_space', b_key_space);
                            //throw 'stop';
                            //console.log('b_key_space.length', b_key_space.length);

                            if (event_name === 'put') {
                                // create a new put subscription handler to test if it is the specified table?

                                var subscription_handler = (obj_event) => {


                                    //console.log('subscription_handler on ' + event_name, obj_event);
                                    // and send the event back to the client?
                                    var arr_response = [client_request_id, output_transform_to_hex(obj_event)];
                                    //console.log('arr_response', arr_response);
                                    connection.sendUTF(JSON.stringify(arr_response));
                                };

                                var db_put_handler = (obj_event) => {
                                    //console.log('obj_event', obj_event);

                                    var b_key = obj_event[0];

                                    //console.log('b_key', b_key);

                                    // Buffer part comparisons - read Int8s from the various parts, and compare them

                                    // compare long buffer against shorter buffer.

                                    var matches = buffer_match(b_key, b_key_space);
                                    //console.log('matches', matches);

                                    if (matches) {
                                        subscription_handler(obj_event);
                                    }


                                    // need to compare the beginning of the b_key with b_key_space


                                    // then read further ints?
                                    //  as we don't have a decoder


                                    //var [i_table, discarded_pos] = xas2.read(b_key, 0);

                                    // reading the integers out of the buffer?
                                    //  or we could compart the buffer parts, and that would probably be faster.


                                    //console.log('x_table', x_table);

                                    //console.log('x_table.number', x_table.number);

                                    //var i_table =

                                    //if (i_table === i_prefix) {
                                    //
                                    //}
                                };
                                // and another handler for the actual event we are going to raise, should it occurr
                                nextleveldb_server.on(event_name, db_put_handler);
                            }
                        }
                    });
                }

                /*
                 //console.log('event_name', event_name);

                 var subscription_handler = (obj_event) => {
                 //console.log('on ' + event_name, obj_event);
                 // and send the event back to the client?
                 var arr_response = [client_request_id, output_transform_to_hex(obj_event)];
                 //console.log('arr_response', arr_response);




                 connection.sendUTF(JSON.stringify(arr_response));




                 };

                 connection.subscription_handlers = connection.subscription_handlers || {};
                 connection.subscription_handlers[event_name] = subscription_handler;

                 that.on(event_name, subscription_handler);
                 */

            }

            // we may be given a different type of subscribe instruction though
            // as array:

            // Could have different subscription handlers.
            //  May want to check a list of subscriptions available / handled by data-plugin code.

            // Then for specific events?
            //  want a function such as raise_client_event(name, obj_e);
            // Maybe worth adding event handlers here?

            // Subscribing to events from the database, or database nodes sounds like an important task.
            //  Not sure how versitile the method will be, or how widely used. Its possible that subscriptions would be used to implement clustering
            //  features - however many clustering features would be tightly integrated with a key hashing / sharding algorithm.
            //   One feature of a cluster could be that if a result is not found on that node (and it could tell from the key), it searches other nodes
            //    (or whichever other nodes it knows to be most likely to have the data). This is how some clustering functionality could be set up before a full
            //    cluster is made. I expect that making a full cluster would have very good IO/s, but many operations would be limited by the IO speed of a single
            //    node. That seems fine though, as with growth, intensive operations could be shifted to different nodes on the cluster through an external load balancer.
            //  Could also have upgrade in the client where it could connect to one machine in the cluster, then be redirected to another machine.

            // Provision of time-indexed arrays
            // Subscription to new events
            // Updating of time indexed arrays quickly
            // Backtesting to find patterns / signals
            // Getting the database, or other nodes, to recognise selected patterns / signals as they occurr in real time
            //  Performing analysis to see which longer term patterns / signals ???are occurring??? on a given day
            //  A slightly different timeframe and usage here. Want something that looks at today's data / what is current, and makes predictions
            //   for the next few hours. Could look forward for different amounts of time.

            // Finding very short term trend signals seems like a good opportunity in currency trading.
            //  Some datasets don't look all that huge.

            // Would be interesting to train some models with multiple datasets.
            //  Would be interesting if they used some 'fractal time' scale, or where the time units/scales could be treated differently, so it
            //   can learn patterns from one scale and apply them to a different scale.
            //  Though that may be useful as well for exploring how true this 'fractal time' system works. Theory could be that we are within a fractured time
            //   crystal or imperfect time crystal (or try perfect time crystal) and it uses patterns from one place in a different place and scale.
            //   Possibly would be more keen to stick to the scales and measurements that are closest? Probably best to try, then judge based on the results.

            // A subscriptions data structure would be good.
            //  Or even use / derive from Evented_Class. Would listen for the event, then when its raised, carry out its action (send data to the client).
            // Using jsgui event handlers for these subscriptions does seem like one of the best ways of going forwards.
            //  It's fairly tried and tested, though debugging it is tricky and annoying.

            // Could have an 'execute_query' function.
            //  Range of values for keys

            // For the moment, queries will be about getting values from within the ranges of keys.
            // Single key put / get
            // Put multiple 




        } else if (fns_ws[function_name]) {

            // Definitely want some kind of query processing.
            // Don't want new functions for getting each type of data in and out of the system.
            //  Get from between two keys
            //  Query using the indexes.
            //   Query to get all keys within a range
            // Will use binary data for much of it.

            // May be better to use a binary interface for the data transferral.
            //  Would need to decode a binch of keys and values.
            //  Need to be able to encode (multiple) records as binary.
            //   Array encoding within Binary_Encoder would help these binary messages be put together.
            // Could send multiple instructions / queries over in the same request.
            //  Query can contain subqueries?
            //   Or just be able to send multiple queries in one message.

            // Want to encode queries so that they are compact.
            //  There won't be many basic query types.
            //   Put records, get records, get ranges of records.
            //   Need to have some variety here, but not need different access functions for different record types.
            //   Keep the access simple, fast, and general. The Model code can do much of the processing of exactly what the records are / mean.

            // It's worth making a serializable format for these queries and multi-query requests.
            //  It will be streaming data, so the server can process it as it is received.

            // Do we want paged results?
            //  This will become a parameter, rather than require a different function to be used.

            // A Query object in Model could help.
            //  Would have different types of query.


            // var grq = db.create_key_range_query(table_id, l_key, u_key, paged);

            // Query
            // -----
            // 1) What we want
            // 2) How we want it
            //   Paged option
            //    Page size
            //     Number of records, or bytes
            //      Specifying 64KB chunks of data to be transferred could work well.

            // The server could parse these queries, and use the parsed/deserialised queries to work out what to do.
            //  

            function_params = obj_message.slice(2);
            var callback = (err, res) => {
                if (err) {
                    // Need to look at this again.
                    console.trace();
                    throw err;
                } else {
                    // need different callbacks here for the streaming functions.
                    //console.log('');
                    //console.log('function ' + function_name + ' callback.');
                    //console.trace();
                    //console.log('res', res);
                    //console.log('typeof res', typeof res);
                    // if the result is already an array, its got multiple items to return.
                    if (is_array(res)) {
                        //console.log('response is array');
                        var arr_response = [client_request_id];
                        var t_item;

                        each(res, (item) => {
                            // check the item is a buffer?
                            t_item = typeof item;
                            //console.log('t_item', t_item);
                            if (t_item === 'undefined') {
                                arr_response.push(item);
                            } else if (t_item === 'number' || t_item === 'string') {
                                arr_response.push(item);
                            } else {
                                // if it's an array...
                                if (is_array(item)) {
                                    // but the response gets stringified later on anyway.

                                    arr_response.push(item);
                                    // Seems much better now with some outpuit, simpler code.
                                    //  Important change 05/11/2016 which could break client-side functionality though.
                                    //   Although not much on the client has relied on return values so far.

                                    //arr_response.push(JSON.stringify(item));
                                } else {
                                    // if it's a buffer? assuming it is.
                                    arr_response.push(item.toString('hex'));
                                }

                            }
                        });
                        connection.sendUTF(JSON.stringify(arr_response));
                    } else {
                        // does it have a .page (number)
                        // check if it is a buffer?
                        var t_res = tof(res);
                        //console.log('t_res', t_res);
                        var arr_response;
                        if (t_res === 'buffer') {
                            arr_response = [client_request_id, res.toString('hex')];
                        } else if (t_res === 'number') {
                            // No, keep the numbers as numbers in JSON.

                            arr_response = [client_request_id, res];
                        } else {
                            // just an object to be sent back to the client?
                            //  It may not be an object that contains pages.
                            //   Returning objects seems like a useful, normal feature.
                            //   Want it so that paging objects get recognised more specigically.
                            //    there could be a .page in an object. __PAGE_ENCODING = true;
                            // These callbacks being called wrong?
                            //console.trace();
                            //console.log('res', JSON.stringify(res));
                            if (res.type === 'data') {
                                var t_page = typeof res.page;
                                //console.log('t_page', t_page);
                                if (t_page === 'number') {
                                    //console.log('res.page', res.page);
                                    // Not so sure about always doing hex output transform.
                                    //  Or make more of an option about IO data format transforms.
                                    //var hex_data = output_transform_to_hex(res.data);
                                    if (res.last === true) {
                                        //console.log('res.page', res.page);
                                        //console.log('data', data);
                                        arr_response = [client_request_id, res.page, res.data, 'last'];
                                        //console.log('arr_response', arr_response);
                                    } else {
                                        var data = res.data;
                                        //console.log('data', data);
                                        // Seems like we get different sequences of data back on different pages.
                                        arr_response = [client_request_id, res.page, res.data];
                                        //console.log('arr_response', arr_response);
                                        //console.log('hex_data', hex_data);
                                        //throw 'stop';
                                    }
                                } else {
                                    arr_response = [client_request_id, JSON.stringify(res)];
                                }
                            } else {
                                arr_response = [client_request_id, JSON.stringify(res)];
                            }
                        }
                        connection.sendUTF(JSON.stringify(arr_response));
                    }
                }
            };
            // Hard to see why the wrong callbacks are occurring.
            function_params.push(callback);
            //console.log('pre apply ' + function_name);
            fns_ws[function_name].apply(null, function_params);
        }
    } else {

    }
};

module.exports = handle_ws_utf8;