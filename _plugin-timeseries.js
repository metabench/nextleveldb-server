
var jsgui = require('jsgui3');
var tof = jsgui.tof;
var each = jsgui.each;
var is_array = jsgui.is_array;
var arrayify = jsgui.arrayify;
var Fns = jsgui.Fns;

var http = require('http');
var url = require('url');

var WebSocket = require('websocket');
var WebSocketServer = WebSocket.server;

var xas2;
//var encodings = require('../../encodings/encodings');
var x = xas2 = require('xas2');

var fs = require('fs');

//var Binary_Encoding = require('binary-encoding');
//var Binary_Encoding_Record = Binary_Encoding.Record;

// Seems like we can't use them all as local variables.
//  Need to load all of the internal and plugin functions into the server object.

//  Does not merge with the db itself. Merges with the functions object, which then merges with the db object.

// arr_io_stats
// obj_io_stats
//  an array seems better in terms of performance.
//  Maybe a typed array would be better still.

// Currently its not showing how many records in per second.
//  Want this to be very unobtrusive, having a typed array (integer) for io stats would be a decent improvement.
//  Possibly an IO_Stats object would be good for further reducing the amount of code in the core.
//   Less code in core for the logging / calculation of the stats.

var merge_plugin = function(nextleveldb_server, db, running_means_per_second) {
	// Can refer to standard API objects in nextleveldb-server

	var index_lookup = nextleveldb_server.index_lookup;
	var ensure_table = nextleveldb_server.ensure_table;
	var get_table_id_by_name = nextleveldb_server.get_table_id_by_name;
	var streaming_get_table_records = nextleveldb_server.streaming_get_table_records;
	var count_table = nextleveldb_server.count_table;
	var get_table_key_prefix_by_id = nextleveldb_server.get_table_key_prefix_by_id;
	var get_table_key_prefix_by_name = nextleveldb_server.get_table_key_prefix_by_name;
	var encode_new_id_put_record = nextleveldb_server.encode_new_id_put_record;
	var put_id_by_string_index_record_tkp = nextleveldb_server.put_id_by_string_index_record_tkp;
	var decoding_iterator_record_by_key_range = nextleveldb_server.decoding_iterator_record_by_key_range;

	var get_categorised_timeseries_record_time_by_category_by_time_lte = function(int_table_prefix, int_category, int_time_lte, record_encoding, callback) {
        var b_key_prefix = xas2(int_table_prefix).buffer;

		var b_key_without_prefix = record_encoding.key_encoding.encode([int_category, int_time_lte]);

		var b_full_key = Buffer.concat([b_key_prefix, b_key_without_prefix]);
		var found_record = false;

		db.createKeyStream({
			'lte': b_full_key,
			'reverse': true,
			'limit': 1
		}).on('data', function (data) {
				//console.log(data.key, '=', data.value)
				// decode the result.

				var decoded_key = record_encoding.key_encoding.decode(data, true); // true to ignore / skip the xas2 encoded table key prefix
				//console.log('decoded_key', decoded_key);
				var res = decoded_key[1];
				// then expand the data?
				found_record = true;
				running_means_per_second.increment(2);
				callback(null, res);
				//callback(null, Buffer.concat([dat]))

			})
			.on('error', function (err) {
				//console.log('Oh my!', err)
			})
			.on('close', function () {
				//console.log('Stream closed')
			})
			.on('end', function () {
				//console.log('Stream ended')
				// Think there is nothing to do because we just get 1 record anyway.
				//  But if there are 0 records?

				if (!found_record) {
					callback(null, null);
				}
			});
	};

	var get_categorised_timeseries_record_time_by_table_name_category_by_time_lte = function(str_table_name, int_category, int_time_lte, record_encoding, callback) {
		get_table_key_prefix_by_name(str_table_name, (err, int_table_prefix) => {
			if (err) {
				callback(err);
			} else {
				get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, int_category, int_time_lte, record_encoding, callback);
			}
		});
	};


	// record value = a value within a record
	var get_categorised_timeseries_record_value_by_category_id_by_time_lte = function(int_table_prefix, int_category_id, int_time_lte, encoding, idx_value, callback) {
		var b_key_prefix = xas2(int_table_prefix).buffer;
		//console.log('pre encode', [int_poloniex_market_id, i_time_lte]);
		// need to include the suffix in the encoding?


		var b_key_without_prefix = encoding.key_encoding.encode([int_category_id, int_time_lte]);

		var b_full_key = Buffer.concat([b_key_prefix, b_key_without_prefix]);
		var found_record = false;

		db.createReadStream({
			'lte': b_full_key,
			'reverse': true,
			'limit': 1
		}).on('data', function (data) {

				// decode the result.

				var decoded = encoding.decode([data.key, data.value]);
				//console.log('decoded', decoded);
				var res = decoded[0].concat(decoded[1]);

				//res[0] = str_market_name;
				//res[1] = new Date(res[1]).toISOString();
				// The ISO string should denote that the time is in UTC.

				running_means_per_second.increment(2);

				// Think ISO

				// then expand the data?
				found_record = true;

				callback(null, res[idx_value]);

				//callback(null, Buffer.concat([dat]))

			})
			.on('error', function (err) {
				//console.log('Oh my!', err)
			})
			.on('close', function () {
				//console.log('Stream closed')
			})
			.on('end', function () {
				//console.log('Stream ended')
				// Think there is nothing to do because we just get 1 record anyway.
				//  But if there are 0 records?

				if (!found_record) {
					callback(null, null);
				}

			});
	};


	var streaming_get_timeseries_categorised_records_between_i_times = function(int_table_prefix, int_category_id, i_time_start, i_time_end, encoding, callback) {

		var b_key_prefix_start = xas2(int_table_prefix).buffer;
		var b_key_without_prefix_start = encoding.key_encoding.encode([int_category_id, i_time_start]);
		var b_full_key_start = Buffer.concat([b_key_prefix_start, b_key_without_prefix_start]);
		var b_key_prefix_end = xas2(int_table_prefix).buffer;
		var b_key_without_prefix_end = encoding.key_encoding.encode([int_category_id, i_time_end]);
		var b_full_key_end = Buffer.concat([b_key_prefix_end, b_key_without_prefix_end]);

		// then do a decoding full record read.
		//  can use a more key feature for doing this.

		decoding_iterator_record_by_key_range({
			'gte': b_full_key_start,
			'lte': b_full_key_end
		}, encoding, (err, res_data) => {
			callback(null, res_data);
		}, (err, res_complete) => {
			// just announce that the stream is over?
			callback(null, 'end');
		});

	};

	// get categorised timeseries records by category between times at boundary interval n_ms

	var streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms = function(int_table_prefix, category_id, i_time_start, i_time_end, ms_interval, record_encoding, callback) {
		// needs to start at the time before i_time_start so we can get the tick from that time.
		//  finds the time for the tick preceeding
		// needs to get the time for just before the start and the end.
		// get_poloniex_ticks_time_before_i_time()
		// What about having the boundary on the second itself?
		//  Does make this trickier.
		//  Could process the input values further to remove the millisecond components.
		// get_market_provider_item_by_market_id_by_i_time_lte
		// get_categorised_timeseries_time_by_i_time_lte
		//  and we give it the table prefix for poloniex

		// int_table_prefix, int_category, int_time_lte, record_encoding, callback

		//console.log('streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms');



		get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_start, record_encoding, (err, i_time_tick_before_start) => {
			if (err) {
				callback(err);
			} else {
				get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_end, record_encoding, (err, i_time_tick_before_end) => {
					if (err) {
						callback(err);
					} else {
						var i_date;
						var c_date = i_time_start;
						var last_tick;
						// date of the tick
						var d_tick;
						//var c_tick;
						// then we do the streaming get decoded records.
						//  don't give it the callback, but process the results to construct / return the results requested.
						//console.log('i_time_tick_before_start', i_time_tick_before_start);
						//console.log('i_time_tick_before_end', i_time_tick_before_end);
						//streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms()
						//  Need the encoding to decode the records.
						//console.log('category_id', category_id);
						// Could be having trouble decoding them...
						streaming_get_timeseries_categorised_records_between_i_times(int_table_prefix, category_id, i_time_tick_before_start, i_time_tick_before_end, record_encoding, (err, res_tick) => {
							if (err) {
								callback(err);
							} else {
								//console.log('');
								//console.log('res_tick', res_tick);
								if (is_array(res_tick)) {
									i_date = res_tick[1];
									//console.log('i_date', i_date);
									if (!d_tick) {
										d_tick = i_date;
										last_tick = res_tick;
									}
									while(c_date < i_date) {
										// advance the c_date
										//console.log('c_date', c_date);
										//console.log('pre cb1');
										callback(null, [c_date, last_tick]);
										// what if it's the same as i_date?
										c_date = c_date + ms_interval;
										// and give the actual times in the results.
										//  Don't modify the records, return them according to the criteria
									}
									if (c_date === i_date) {
										//console.log('pre cb2');
										callback(null, [c_date, res_tick]);
										c_date = c_date + ms_interval;

									}
									last_tick = res_tick;
								} else {
									// if it's the end, need to continue to the present, repeating the last tick
									//console.log('res_tick', res_tick);
									if (res_tick === 'end') {
										//console.log('reached end');
										//console.log('c_date', c_date);
										while(c_date <= i_time_end) {
											// advance the c_date
											//console.log('c_date', c_date);
											//console.log('pre extra cb');
											callback(null, [c_date, last_tick]);
											// what if it's the same as i_date?
											c_date = c_date + ms_interval;
											// and give the actual times in the results.
											//  Don't modify the records, return them according to the criteria
										}
									}
									callback(null, res_tick);
								};
								//if (c_date === i_date) {

								//}

								// a loop top bring the c_date up to (or after?) the i_date

								// decide if we output the tick result.
								//  we probably don't directly do that.
								//  the tick result shows the tick at a given time. We show a tick at a boundary possibly some time after that given time.

								// While the time we have read is more advanced than the time we have on the counter...
								//  advance the time on the counter by n ms, and output a result that's repeated from before.
							}
						});
					}
				});
			};
		});
	};

	var streaming_get_timeseries_categorised_records_value_between_i_times_at_boundary_interval_n_ms = function(int_table_prefix, category_id, i_time_start, i_time_end, ms_interval, idx_value, record_encoding, callback) {
		get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_start, record_encoding, (err, i_time_tick_before_start) => {
			if (err) {
				callback(err);
			} else {
				get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_end, record_encoding, (err, i_time_tick_before_end) => {
					if (err) {
						callback(err);
					} else {
						var i_date;
						var c_date = i_time_start;
						var last_tick;
						// date of the tick
						var d_tick;
						streaming_get_timeseries_categorised_records_between_i_times(int_table_prefix, category_id, i_time_tick_before_start, i_time_tick_before_end, record_encoding, (err, res_tick) => {
							if (err) {
								callback(err);
							} else {
								//console.log('res_tick', res_tick);
								if (is_array(res_tick)) {
									i_date = res_tick[1];
									//console.log('i_date', i_date);
									if (!d_tick) {
										d_tick = i_date;
										last_tick = res_tick;
									}
									// less than or equal is not working properly.
									//  may have the tick on the actual boundary.
									while(c_date < i_date) {
										// advance the c_date
										//console.log('c_date', c_date);
										//console.log('pre cb1');
										callback(null, last_tick[idx_value]);
										// what if it's the same as i_date?
										c_date = c_date + ms_interval;
										// and give the actual times in the results.
										//  Don't modify the records, return them according to the criteria
									}
									if (c_date === i_date) {
										//console.log('pre cb2');
										callback(null, res_tick[idx_value]);
										c_date = c_date + ms_interval;
									}
									// and if it's the date itself, then callback that date.
									last_tick = res_tick;
								} else {
									// if it's the end, need to continue to the present, repeating the last tick
									//console.log('res_tick', res_tick);
									if (res_tick === 'end') {
										//console.log('reached end');
										//console.log('c_date', c_date);
										while(c_date <= i_time_end) {
											// advance the c_date
											//console.log('c_date', c_date);
											//console.log('pre extra cb');
											callback(null, last_tick[idx_value]);
											// what if it's the same as i_date?
											c_date = c_date + ms_interval;
											// and give the actual times in the results.
											//  Don't modify the records, return them according to the criteria
										}
									}
									callback(null, res_tick);
								};
							}
						});
					}
				});
			};
		});
	};

	// get categorised timeseries records by category between times at boundary interval n_ms

	var streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms = function(int_table_prefix, category_id, i_time_start, i_time_end, ms_interval, idx_value, record_encoding, callback) {
		
		get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_start, record_encoding, (err, i_time_tick_before_start) => {
			if (err) {
				callback(err);
			} else {
				get_categorised_timeseries_record_time_by_category_by_time_lte(int_table_prefix, category_id, i_time_end, record_encoding, (err, i_time_tick_before_end) => {
					if (err) {
						callback(err);
					} else {
						var i_date;
						var c_date = i_time_start;
						var last_tick;
						// date of the tick
						var d_tick;

						//var c_tick;
						// then we do the streaming get decoded records.
						//  don't give it the callback, but process the results to construct / return the results requested.
						//console.log('i_time_tick_before_start', i_time_tick_before_start);
						//console.log('i_time_tick_before_end', i_time_tick_before_end);
						//streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms()
						//  Need the encoding to decode the records.
						//console.log('category_id', category_id);
						// Could be having trouble decoding them...
						streaming_get_timeseries_categorised_records_between_i_times(int_table_prefix, category_id, i_time_tick_before_start, i_time_tick_before_end, record_encoding, (err, res_tick) => {
							if (err) {
								callback(err);
							} else {
								//console.log('res_tick', res_tick);
								if (is_array(res_tick)) {
									i_date = res_tick[1];
									//console.log('i_date', i_date);
									if (!d_tick) {
										d_tick = i_date;
										last_tick = res_tick;
									}
									// less than or equal is not working properly.
									//  may have the tick on the actual boundary.
									while(c_date < i_date) {
										// advance the c_date
										//console.log('c_date', c_date);

										//console.log('pre cb1');
										callback(null, [c_date, last_tick[idx_value]]);
										// what if it's the same as i_date?
										c_date = c_date + ms_interval;
										// and give the actual times in the results.
										//  Don't modify the records, return them according to the criteria

									}
									if (c_date === i_date) {
										//console.log('pre cb2');
										callback(null, [c_date, res_tick[idx_value]]);
										c_date = c_date + ms_interval;
									}
									// and if it's the date itself, then callback that date.



									last_tick = res_tick;
								} else {
									// if it's the end, need to continue to the present, repeating the last tick
									//console.log('res_tick', res_tick);
									if (res_tick === 'end') {
										//console.log('reached end');
										//console.log('c_date', c_date);
										while(c_date <= i_time_end) {
											// advance the c_date
											//console.log('c_date', c_date);
											//console.log('pre extra cb');
											callback(null, [c_date, last_tick[idx_value]]);
											// what if it's the same as i_date?
											c_date = c_date + ms_interval;
											// and give the actual times in the results.
											//  Don't modify the records, return them according to the criteria
										}
									}
									callback(null, res_tick);
								};
								//if (c_date === i_date) {

								//}

								// a loop top bring the c_date up to (or after?) the i_date

								// decide if we output the tick result.
								//  we probably don't directly do that.
								//  the tick result shows the tick at a given time. We show a tick at a boundary possibly some time after that given time.

								// While the time we have read is more advanced than the time we have on the counter...
								//  advance the time on the counter by n ms, and output a result that's repeated from before.
							}
						});
					}
				});
			};
		});
	};



	var get_categorised_timeseries_record_by_category_id_by_time_lte = function(int_table_prefix, int_category_id, int_time_lte, encoding, callback) {
		var b_key_prefix = xas2(int_table_prefix).buffer;
		// need to include the suffix in the encoding?

		var b_key_without_prefix = encoding.key_encoding.encode([int_category_id, int_time_lte]);

		var b_full_key = Buffer.concat([b_key_prefix, b_key_without_prefix]);
		var found_record = false;

		db.createReadStream({
			'lte': b_full_key,
			'reverse': true,
			'limit': 1
		}).on('data', function (data) {

				// decode the result.

				var decoded = encoding.decode([data.key, data.value]);
				//console.log('decoded', decoded);
				var res = decoded[0].concat(decoded[1]);

				//res[0] = str_market_name;
				//res[1] = new Date(res[1]).toISOString();

				// The ISO string should denote that the time is in UTC.

				running_means_per_second.increment(2);

				// Think ISO

				// then expand the data?
				found_record = true;
				callback(null, res);

				//callback(null, Buffer.concat([dat]))

			})
			.on('error', function (err) {
				//console.log('Oh my!', err)
			})
			.on('close', function () {
				//console.log('Stream closed')
			})
			.on('end', function () {
				//console.log('Stream ended')
				// Think there is nothing to do because we just get 1 record anyway.
				//  But if there are 0 records?

				if (!found_record) {
					callback(null, null);
				}
			});
    };

    var fns_ws = nextleveldb_server.fns_ws;

    fns_ws.get_categorised_timeseries_record_by_category_id_by_time_lte = get_categorised_timeseries_record_by_category_id_by_time_lte;
    fns_ws.get_categorised_timeseries_record_value_by_category_id_by_time_lte = get_categorised_timeseries_record_value_by_category_id_by_time_lte;
    fns_ws.get_categorised_timeseries_record_time_by_category_by_time_lte = get_categorised_timeseries_record_time_by_category_by_time_lte;
    fns_ws.get_categorised_timeseries_record_time_by_table_name_category_by_time_lte = get_categorised_timeseries_record_time_by_table_name_category_by_time_lte;
    fns_ws.streaming_get_timeseries_categorised_records_between_i_times = streaming_get_timeseries_categorised_records_between_i_times;
    fns_ws.streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_between_i_times_at_boundary_interval_n_ms;
    fns_ws.streaming_get_timeseries_categorised_records_value_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_value_between_i_times_at_boundary_interval_n_ms;
    fns_ws.streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms = streaming_get_timeseries_categorised_records_timed_value_between_i_times_at_boundary_interval_n_ms;

};
module.exports = merge_plugin;