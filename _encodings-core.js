/**
 * Created by James on 18/10/2016.
 */

// var encoding = require('binary-encoding');
// Have system encodings too...

// Encodings for index keys and values
// Call decode and get an object that holds the various parts.

// Encodings for a table name index?
// Or make simpler, less abstract version, test it, then replace it.


// A bit more like record definitions, goes further than encodings.
//  Defining the primary keys

// table (table table) encodings would maybe be useful.
//  however, the initial system tables and fields are being made on a lower level.

// Storing encodings in the database may be nice.
//  Some encodings may need to be hardcoded though.


var core_encodings = {

	// May be worth having different levels of encodings?

	// Market Provider level.


	// opk for original primary key?
	//  or just say pk so that the system recognises the record has a primary key and does not need to add one.

	// Want a way to define a poloniex trade as referring to

	// Poloniex markets dont really need to have values.
	//  They are just indexed names, used for reference

	// The records are made out of the key and value together.
	//  May use a different type of specification of this?
	//  Keep for the moment, as it splits the keys and the values ready for encoding and decoding.

	// And a data types table?
	//  Just connecting names with IDs for the moment...
	//  Though the records could be made to contain some more useful information in the future.
	//  call it a native_types table?
	//   things like xas2, uint32, float64, string


	// Translating fields to record encodings?
	//  We say which fields are the primary key fields.

	// Possibly it is worth getting encodings from outside of the database,
	//  as it seems like a way to get the system to work quickly.
	//  Solving the association problem is not that severe, as we can use Binary_Encoding for encoding the poloniex records, based on the structure we have defined here.

	// Does not look like there is a consistent encoding for market provider records.
	//  Different market providers use different encodings for different records.

	// When we use a named key,

	'table': {
		'key': [
			['id'] // xas2 default
		],
		'value': [
			// index by name

            // This would take more work
            //  Could have idx(0) or idx(1) notation.
            //['name', indexed] // string default

            ['name'] // string default
			// tables will make use of a global key prefix.

        ],

        // A single index, where the name points to the id.
        //  It definitely seems worth storing the indexing information in the database.
        //   Possibly needs a new table, 'table indexes'.
        //    Each row in that table is an index for a table.
        //  We may still need to make use of some low-level indexing functionality where indexing info is not stored in the DB.
        //   The functions that interact with the 'table index' table (and 'table' table) may need to be lower level, as they would be used as a platform for other functions.


        // Could run an ensure_idx function on the table, with the table indexing information.
        //  There is plenty of space in the DB for the indexes themselves, but it seems as though index definitions need more space, attention, and process integration.



        
        // Maybe no need to add this index right now.

        //'idx': ['name', 'id']

    }

    // Put data, alongside indexing it in a specific way, would be useful.
    //  The indexes allow for fast retrieval.





	

	// Could compress times in milliseconds or seconds too.
	//  Define different data-specific epochs.
	//  Just define a start time for a data series, and we get to compress the times down to times within that range.

	// For the moment, date32 encoding seems like the easiest way to get lots of trades into the database.



    

	// Don't much like the way that Poloniex trades are stored here.
	//  A better way would be to include the date within the key.
	//  Having a trade ID does not make so much sense.

	// May be worth having a system of reading both old and new encodings?
	//  Also, would be worth doing more to get the backups of data.

	// Does get complicated having both the old and new encodings around.

	// May be worth having some overall market provider abstraction for trades.
	//  Then get Poloniex trades working according to that provider abstraction.

	// Or have a bucket of trades at any one time value.

	// At the moment:
	//  Market ID, then the trade ID.

	// Wanted:
	//  Market ID, datetime, trade number within datetime.

	// Would require some iteration over the database.

	// For the moment, dealing with Poloniex ticks with an abstraction makes sense.
	//  IG watchlist ticks as well.

	// Would be nice to have a more variable / flexible way of getting data.
	//  Right now we have all these functions which get called.

	// dt32
	
};


module.exports = core_encodings;


if (require.main === module) {







} else {
	//console.log('required as a module');
}
