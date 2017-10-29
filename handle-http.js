/**
 * Created by james on 27/12/2016.
 */


var handle_http = function(request, response) {





    var srvUrl = url.parse(`http://${request.url}`);
    //console.log('srvUrl', srvUrl);
    // Make it so the whole path is just part of the key (unless we use a query).
    // /kvs/restful interface to keys and values
    //  Keys and values can have URL encoding that gets ignored.
    // query/key_beginning/[key]all further url encoding gets ignored
    // query/next/[key]
    var path = srvUrl.path;
    var pos1 = path.indexOf('/', 1);
    if (pos1 > 0) {
        var path1 = path.substr(1, pos1 - 1);
        //console.log('path1', path1);
        if (path1 === 'kvs') {
            // A single byte key in hex?
            //  Could make use of the header encodings.
            var key = path.substr(pos1 + 1);
            // read the value from the request body.
            //  binary value?
            //  header that says how the value is encoded?
            var body = request.body;
            //console.log('key', key);
            //console.log('body', body);
            var method = request.method;
            //console.log('method', method);
            var content_length = parseInt(request.headers['content-length'], 10);
            //console.log('content_length', content_length);
            if (request.body) {

            } else {
                if (method === 'GET') {
                    db.get(key, function (err, value) {
                        if (err) {
                            //console.log('typeof err', typeof err);

                            if ((err + '').substr(0, 13) === 'NotFoundError') {
                                //console.log ('key not found');

                                response.writeHead(404, {"Content-Type": "text/plain"});
                                response.write("404 Not Found\n");
                                response.end();

                            } else {
                                return console.log('DB get error', err);
                                throw err;
                            }
                        } else {
                            //console.log('name=' + value);
                            // Write it as binary
                            //console.log('typeof value', typeof value);
                            response.writeHead(200, {"content-type": "application/octet", 'content-length': value.length});
                            response.write(value);
                            response.end();

                        }// likely the key was not found
                        // ta da!
                    });

                } else if (method === 'PUT') {
                    //if (request.method == 'POST') {
                    // A buffer?
                    //console.log('content_length', content_length);
                    //var body = '';
                    var buf_body = Buffer.alloc(content_length);
                    //var buf_body = new Buffer(content_length);
                    var write_pos = 0;

                    request.on('data', function(data) {
                        //body += data;
                        //console.log('data.length', data.length);
                        data.copy(buf_body, write_pos);
                        write_pos = write_pos + data.length;

                        //console.log('data', data);
                        //console.log('data ' + data);
                        //buf_body.write(data + '');
                        // Too much POST data, kill the connection!
                        // 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
                        //if (body.length > 1e6)
                        //	request.connection.destroy();
                    });

                    request.on('end', function() {
                        //var post = qs.parse(body);
                        //console.log('buf_body', buf_body);
                        //console.log('buf_body', buf_body + '');
                        //console.log('buf_body.length', buf_body.length);
                        // Something to confirm it was set correctly?
                        // return a binary 1
                        // application/octet-stream
                        db.put(key, buf_body, function (err, value) {
                            if (err) {
                                //console.log('typeof err', typeof err);
                                if ((err + '').substr(0, 13) === 'NotFoundError') {
                                    //console.log ('key not found');
                                    response.writeHead(404, {"Content-Type": "text/plain"});
                                    response.write("404 Not Found\n");
                                    response.end();
                                } else {
                                    return console.log('Ooops!', err);
                                }
                            } else {
                                //console.log('name=' + value);
                                var buf_res = Buffer.alloc(1);
                                buf_res[0] = 1;
                                response.writeHead(200, {"content-type": "application/octet", 'content-length': '1'});
                                response.write(buf_res);
                                response.end();
                            }// likely the key was not found
                            // ta da!
                        });
                        // use post['blah'], etc.
                    });
                    //
                }
            }
        }

        if (path1 === 'query') {
            // query/[queryname]
            //  payload is in the request body
            // May query for records with keys beginning with something.
            // Just query for the keys?
            // Keys beginning could look for between key+low ascii and key + high ascii
            // Could add a character 0
            // Need to find out what the query name is.
            //query/?keysbeginning=key
            // query/keys?keys_beginning=...
            var query = path.substr(pos1 + 1);
            //console.log('query', query);
            var pos2 = path.indexOf('?', pos1 + 1);
            var query_word = path.substring(pos1 + 1, pos2);
            //console.log('query_word', query_word);
            // Then split it up by &
            var str_query_params = path.substr(pos2 + 1);
            //console.log('str_query_params', str_query_params);
            var arr_query_params = str_query_params.split('&');
            //console.log('arr_query_params', arr_query_params);
            var arr_query_keys_and_values = [];
            arr_query_params.forEach(function(v) {
                arr_query_keys_and_values.push(decodeURI(v).split('='));
            });
            //console.log('arr_query_keys_and_values', arr_query_keys_and_values);
            // requesting an object indexed set of records for a batch of items.
            // also want to express a collection of the keys to retrieve
            //  JSON encoded array in URL
            // query/record_ranges?key_ranges=[[a,b],[c,d],...]
            // All data
            //  keys and values.
            //  Lets stream it, as there may be quite a lot.
            //   Find out about http pipe where sending gets slowed if it does not get recieved fast enough...
            //    or delay iteration effectively, or delay sending.
            //  Could have a download speed header which it looks at. When copying DB could go for a consistant but fast speed eg 50MB/s
            var is_first = true;
            if (query === 'all') {
                // Iterate all of them
                //  A custom iterator?
                // Something to slow down the iteration...
                get_data_multi_callbacks(256, function(arr_data, pause, resume) {
                    //console.log('arr_data.length', arr_data.length);
                    var json_data = JSON.stringify(arr_data);
                    var internal_data = json_data.slice(1, -1);
                    if (is_first) {
                        response.writeHead(200, {"content-type": "application/json"});
                        response.write('[');
                        pause();
                        response.write(internal_data, function() {
                            setTimeout(function() {
                                resume();
                            }, 20);
                            //resume();
                        });
                        is_first = false;
                        // And cork the readable stream until the output data has been flushed??
                    } else {
                        pause();
                        response.write(internal_data, function() {

                            // Slows down the writing, gives chance to catch up.

                            setTimeout(function() {
                                resume();
                            }, 20);

                        });
                    }
                    //throw 'stop'
                }, function() {
                    response.write(']');
                    response.end();
                    // end
                });
            }
            if (query === 'all_keys') {
                get_all_keys(function(err, res_all_keys) {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('res_all_keys.length', res_all_keys);
                        // Could take too long to stringify all keys.
                        var json_all_keys = JSON.stringify(res_all_keys);
                        response.writeHead(200, {"content-type": "application/json", 'content-length': json_all_keys.length});
                        response.write(json_all_keys);
                        response.end();
                    }
                });
            }
            if (query_word === 'values') {
                // Then we need to look for the ranges.
                //throw 'stop';
                if (arr_query_keys_and_values.length === 1) {
                    if (arr_query_keys_and_values[0][0] === 'key_ranges') {
                        var arr_ranges = JSON.parse(arr_query_keys_and_values[0][1]);
                        var num_retreivals = arr_ranges.length;
                        var errs = [];
                        var res = [];
                        arr_ranges.forEach(function (arr_range) {
                            //
                            get_data_keys_ranging(arr_range[0], arr_range[1], function (err, res_range) {
                                if (err) {
                                    errs.push(err);
                                } else {
                                    //console.log('arr_range', arr_range);
                                    //throw 'stop';
                                    //console.log('res_range', res_range);
                                    //res.push([arr_range, res_range]);
                                    res = res.concat(res_range);
                                    num_retreivals--;
                                    if (num_retreivals === 0) {
                                        var json_res = JSON.stringify(res);
                                        //console.log('res', res);
                                        //console.log('json_res', json_res.substr(0, 200));
                                        response.writeHead(200, {
                                            "content-type": "application/json",
                                            'content-length': json_res.length
                                        });
                                        response.write(json_res);
                                        response.end();
                                        //console.log('written response');
                                    }
                                }
                            });
                        });
                    }
                }
            }

            if (query_word === 'record_ranges') {
                if (arr_query_keys_and_values.length === 1) {
                    if (arr_query_keys_and_values[0][0] === 'ranges') {
                        //console.log('arr_query_keys_and_values[0][1]', arr_query_keys_and_values[0][1]);
                        //throw 'stop';
                        var arr_ranges = JSON.parse(arr_query_keys_and_values[0][1]);
                        var num_retreivals = arr_ranges.length;
                        var errs = [];
                        var res = [];

                        arr_ranges.forEach(function(arr_range) {
                            //
                            get_data_keys_ranging(arr_range[0], arr_range[1], function(err, res_range) {
                                if (err) {
                                    errs.push(err);
                                } else {

                                    res.push([arr_range, res_range]);
                                    num_retreivals--;

                                    if (num_retreivals === 0) {
                                        var json_res = JSON.stringify(res);
                                        //console.log('res', res);
                                        //console.log('json_res', json_res.substr(0, 200));
                                        response.writeHead(200, {"content-type": "application/json", 'content-length': json_res.length});
                                        response.write(json_res);
                                        response.end();

                                    }
                                }
                            });
                        });
                    }

                }
                //console.log('arr_query_keys_and_values', arr_query_keys_and_values);
                //throw 'stop';
            }
            if (query_word === 'records') {
                if (arr_query_keys_and_values.length === 1) {
                    if (arr_query_keys_and_values[0][0] === 'keys_beginning') {
                        var key_prefix = arr_query_keys_and_values[0][1];

                        get_data_keys_beginning(key_prefix, function(err, res_records) {
                            var json_records = JSON.stringify(res_records);

                            response.writeHead(200, {"content-type": "application/json", 'content-length': json_records.length});
                            response.write(json_records);
                            response.end();
                        });

                    }
                }
            }
            if (query_word === 'keys') {
                if (arr_query_keys_and_values.length === 1) {
                    if (arr_query_keys_and_values[0][0] === 'keys_beginning') {

                        var key_prefix = arr_query_keys_and_values[0][1];

                        get_keys_beginning(key_prefix, function(err, res_records) {
                            var json_records = JSON.stringify(res_records);

                            response.writeHead(200, {"content-type": "application/json", 'content-length': json_records.length});
                            response.write(json_records);
                            response.end();
                        });
                    }
                }
            }
            if (query_word === 'count') {
                if (arr_query_keys_and_values.length === 1) {
                    if (arr_query_keys_and_values[0][0] === 'keys_beginning') {
                        var key_prefix = arr_query_keys_and_values[0][1];
                        //var res_records = [];
                        var res_count = 0;
                        db.createReadStream({
                            'gte': key_prefix + ' ',
                            'lte':  key_prefix + '~',
                        }).on('data', function (data) {
                            //console.log(data.key, '=', data.value)
                            //res_records.push([decodeURI(data.key), data.value.split('|')]);
                            res_count++;
                        })
                            .on('error', function (err) {
                                console.log('Oh my!', err)
                            })
                            .on('close', function () {
                                console.log('Stream closed')
                            })
                            .on('end', function () {
                                console.log('Stream closed')
                                var json_count = JSON.stringify(res_count);
                                response.writeHead(200, {"content-type": "application/json", 'content-length': json_count.length});
                                response.write(json_count);
                                response.end();
                            });
                    }
                }
            }
        }
    } else {
        response.writeHead(404, {"Content-Type": "text/plain"});
        response.write("404 Not Found\n");
        response.end();
    }


};

module.exports = handle_http;
