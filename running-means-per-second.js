/**
 * Created by James on 26/10/2016.
 */

var jsgui = require('jsgui3');
var Evented_Class = jsgui.Evented_Class;
var log = require('single-line-log').stdout;


class Running_Means_Per_Second extends Evented_Class {
	'constructor'() {
		super();

		// Could take an array of field names.
		//  Could apply some transformations such as showing KB.
		//  Leave it for the moment.


		// Keep track of the totals throughout the life of the object too?


		// Define / allocate for the varous data points.

		var num_data_points = this.num_data_points = 4;
		var lookback_duration_s = this.lookback_duration_s = 10;

		var working_size = num_data_points * lookback_duration_s;

		this.rotator_position = 0;

		this.operating_s = 0;

		this.ta_working = new Int32Array(working_size);
		//  which way is the table arranged though?

		// x axis is time makes sense.


		this.ta_res = new Float64Array(num_data_points);



	}
	'start'() {
		var that = this;
		this.started = true;
		setInterval(function() {
			that.tick();
		}, 1000);
	}
	'start_single_line_log'() {
		if (!this.started) this.start();
		this.on('tick', (obj_log) => {
			//log('obj_log', obj_log.ta_res);
			log(obj_log.ta_res);
			// don't know the names of the fields.
		});
	}
	'tick'() {
		//console.log('tick');

		// Want counts per second.



		// operate at the current rotator position.
		this.operating_s++;

		// put into the data at current rotator position? Only when there is data to put.
		var ta_working = this.ta_working;

		var num_data_points = this.num_data_points;

		var rotator_position = this.rotator_position;
		var lookback_duration_s = this.lookback_duration_s;

		// do the running calculation.
		//  weighted average being possible?
		//  going back through the values, then back through the start.

		// keep one counter, and an index as well.
		//  if the index overflows, move it to the beginning or end.

		// do the calculations working back from the current pos.

		var ta_totals = new Float64Array(num_data_points);

		// ta weights totals

		if (this.operating_s < 10) {
			// raise an event with a lower averaging period.
			//this.raise('tick', this.ta_res);
			for (i_data_point = 0; i_data_point < num_data_points; i_data_point++) {
				//
				this.ta_res[i_data_point] = ta_working[i_data_point * this.lookback_duration_s + this.rotator_position];

			};
			this.raise('tick', {
				'ta_res': this.ta_res
			});

		} else {
			// read backwards through the items

			// Running average, with a total where we subtract the leaving value, and add the returning one.
			//  Does not allow for weighted averages that way though.

			var i_data_point;
			var c, i = rotator_position, pos;

			//console.log('ta_working', ta_working);

			for (i_data_point = 0; i_data_point < num_data_points; i_data_point++) {
				ta_totals[i_data_point] = 0;

				for (c = 0; c < lookback_duration_s; c++) {
					// calculate the value and put it in the running total



					//
					pos = i_data_point * lookback_duration_s + c;
					ta_totals[i_data_point] = ta_totals[i_data_point] + ta_working[pos];
					//ta_working[pos] = 0;



				}
				this.ta_res[i_data_point] = ta_totals[i_data_point] / lookback_duration_s;

			};

			this.raise('tick', {
				'ta_res': this.ta_res
			});


		}

		this.rotator_position++;
		//console.log('1) this.rotator_position', this.rotator_position);



		if (this.rotator_position === lookback_duration_s) {
			this.rotator_position = 0;

			// wipe the values in the table at that rotation position.



		}
		for (i_data_point = 0; i_data_point < num_data_points; i_data_point++) {
			this.ta_working[i_data_point * this.lookback_duration_s + this.rotator_position] = 0;

		}


		// position = i_data_point * rotator_position;

	}
	'increment'(i_data_point) {
		//console.log('2) this.rotator_position', this.rotator_position);
		this.ta_working[i_data_point * this.lookback_duration_s + this.rotator_position]++;
	}
	'add'(i_data_point, value) {
		this.ta_working[i_data_point * this.lookback_duration_s + this.rotator_position] = this.ta_working[i_data_point * this.lookback_duration_s + this.rotator_position] + value;
	}
}


module.exports = Running_Means_Per_Second;