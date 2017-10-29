// Active Database

// Seems to have (more) client-side uses.
//  It is not in-process with the DB, but connects to it through the client.
//   Seems more like an actual part of the client.

// When active db loads, it loads all of the tables (and their components), 


// Allows creation of tables

// Allows creation of records
//  Connected records allow updates - but likely don't need this for many records we intend to be persisted without change.

// Allows querying of records.
//  Query within primary key range
//  Query within indexed range, looking up the indexed pk values, and individually looking up those records.


// This would make a convenient interface to then send and receive lots of trading records.
//  Would have another class that carries out specific queries to do with finanical information.
//  Want a JavaScript class that contains a Typed Array / Typed Arrays so that it stores a large amount of financial data in a nicely indexed and also computable way.


// Want to get this up and running with live trading data ASAP.

// 




// Extends the Model, or has a smaller API that uses Model items?

// Having actual Table and Record objects would be useful.
//  May need to use them in a different way / overlying API.

// This type of Active Database could then be extended with Crypto Active Database












var model = require('../nextleveldb-model');
// The model database has been defined, and the connected database persists it / ensures it's already written.

var Database = model.Database;

class Active_Database extends Database {

}

module.exports = Active_Database;