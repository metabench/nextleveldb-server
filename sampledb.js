/*
    Sample DB

Experimental sample DB to test features without using a lot of data.
A fairly small number of records, but will use a variety of DB features.

Want to have a really simple and easy-to-use API.

*/

const NextLevelDB_Server = require('./nextleveldb-server');
const lang = require('lang-mini');
const os = require('os');
const path = require('path');
// db of countries


// Unique indexes on both
let def_table_countries = [['+id'], ['!name', '!code']];

// then have regions

var user_dir = os.homedir();
console.log('OS User Directory:', user_dir);
//var docs_dir =
var path_dbs = path.join(user_dir + '/NextLevelDB/dbs');
console.log('path_dbs', path_dbs);


let server = new NextLevelDB_Server({
    path: path_dbs + '/sample'
});

(async () => {
    // server
    await server.start();

    let count = await server.count();
    console.log('awaited count', count);



    //let countries = await server.ensure_table('countries', def_table_countries);
    // countries.....
    // countries.put('United States of America', 'US')

    let model = server.model;

    console.log('pre add');


    let added = await server.ensure_table('countries', def_table_countries);


    //let active_table_countries = await server.ensure_table('countries', def_table_countries);

    // Active_Table would definitely be useful.
    //  Get it back from the server, and can directly interact with the db.
    //  Makes records with no KPs useful in some cases.



    // and reload the model once the table has been ensured?

    console.log('added', added);
    //throw 'stop';



    // get a reference to that table.

    //let tbl_countries = server.get_active_table('countries');
    //tbl_countries.add(['United States of America', 'US']);

    //let rec_usa = server['countries'].put(['United States of America', 'US']);


    let model_tbl_countries = model.map_tables['countries'];
    console.log('!!model_tbl_countries', !!model_tbl_countries);

    console.log('model_tbl_countries.fields.length', model_tbl_countries.fields.length);

    // ensure record.

    let record = model_tbl_countries.add_record(['United States of America', 'US']);
    console.log('record', record);




    /*
    //for (let record of active_table_countries.get_records()) {
    for (let record of active_table_countries.get_records()) {

    }
    */


    //model_tbl_countries.

    //added = await server.put(record);

    //let record = await server.put_table_record('countries', ['United States of America', 'US']);
    //console.log('record', record);
    // 

    // active_table_countries.select(fields, conditions);
    //  will definitaly be useful for selecting a range of stored crypto value data.



})();


