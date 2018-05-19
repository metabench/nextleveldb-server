/*
    Sample DB

Experimental sample DB to test features without using a lot of data.
A fairly small number of records, but will use a variety of DB features.

Want to have a really simple and easy-to-use API.

*/

const NextLevelDB_Server = require('./nextleveldb-server');
const Active_Database = require('../nextleveldb-active/active-database');

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



//const nextleveldb_active = require('nextleveldb-active');




let server = new NextLevelDB_Server({
    path: path_dbs + '/sample'
});

(async () => {
    // server
    await server.start();

    let added = await server.ensure_table('countries', def_table_countries);


    //let active_table_countries = await server.ensure_table('countries', def_table_countries);

    // Active_Table would definitely be useful.
    //  Get it back from the server, and can directly interact with the db.
    //  Makes records with no KPs useful in some cases.

    // and reload the model once the table has been ensured?

    console.log('added', added);

    let active = new Active_Database(server);

    //console.log('active.tables', active.tables);

    let active_table_countries = active['countries'];
    console.log('Object.keys(active_table_countries)', Object.keys(active_table_countries));

    // maybe it can only return an observable?

    // May need to await this?


    // 
    console.log('countries values active_table_countries.records', (await active_table_countries.records).map(x => x.decoded[1]));

    let us_record = await active_table_countries.put(['United States of America', 'US']);
    console.log('us_record', us_record);


    //because it's a unique field, will only get one record.
    let record_us = await active_table_countries.get_by_field('code', 'us');






    let beginning_functions = async () => {
        let count = await server.count();
        console.log('awaited count', count);



        //let countries = await server.ensure_table('countries', def_table_countries);
        // countries.....
        // countries.put('United States of America', 'US')

        let model = server.model;

        console.log('pre add');



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

        // client-side active functionality will be useful too
        //  active functionality is built out of lower level code, which could work on both client and server.


        //let active_db = new Active_Database(server);




        // let usa_record = active_table_countries.records.add(['United States of America', 'US']);

        console.log('record', record);
    }






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


