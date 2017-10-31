/* jshint node: true, esversion: 6 */

const 
  argv        = require('yargs')                                              // `yargs` is a command line argument parser
                .demand('connection')                                         // pass in the node_redis connection object location with '--connection'
                .argv,                                                        // return it back as a plain object
  connection  = require(argv.connection),                                     // load and parse the JSON file at `argv.connection`
  redis       = require('redis'),                                             // node_redis module
  rediSearch  = require('./index.js'),                                        // rediSearch Abstraction library
  data        = rediSearch(redis,'the-bard',{ clientOptions : connection });  // create an instance of the abstraction module using the index 'the-bard'
                                                                              // since we passed in redis module instead of a client instance, it will create a client instance
                                                                              // using the options specified in the 3rd argument.
data.createIndex([                                                            // create the index using the following fields
    data.fieldDefinition.text('line', true),                                  // field named 'line' that holds text values and will be sortable later on
    data.fieldDefinition.text('play', true, { noStem : true }),               // 'play' field is a text values that won’t be stemmed
    data.fieldDefinition.numeric('speech',true),                              // 'speech' is a numeric field that is sortable
    data.fieldDefinition.text('speaker', false, { noStem : true }),           // 'speaker' is a text field that is not stemmed and not sortable
    data.fieldDefinition.text('entry', false),                                // 'entry' is a text field that stemmed and not sortable
    data.fieldDefinition.geo('location')                                      // 'location' is a geospatial index
  ],
  function(err) {                                                             // Error first callback after the index is created
    if (err) { throw err; }                                                   // Handle the errors
    data.batch()                                                              // Start a 'batch' pipeline
      .rediSearch.add(57956, {                                                // index the object at the ID 57956
        entry     : 'Out, damned spot! out, I say!--One: two: why,',
        line      : '5.1.31',
        play      : 'macbeth',
        speech    : '15',
        speaker   : 'LADY MACBETH',
        location  : '-3.9264,57.5243'
      })
      .rediSearch.getDoc(57956)                                               // Get the document index at 57956
      .rediSearch.search('spot')                                              // Search all fields for the term 'spot'
      .rediSearch.exec(function(err,results) {                                // execute the pipeline
        if (err) { throw err; }                                               // Handle the errors
        console.log(JSON.stringify(results[1],null,2));                       // show the results from the second pipeline item (`getDoc`)
        console.log(JSON.stringify(results[2],null,2));                       // show the results from the third pipeline item (`search`)
        data.dropIndex(function(err) {                                        // drop the index and send any errors to `err`
          if (err) { throw err; }                                             // handle the errors
          data.client.quit();                                                 // `data.client` is direct access to the client created in the `rediSearch` function
        });
      });
  }
);