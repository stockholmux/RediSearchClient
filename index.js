/* jshint node: true, esversion: 6 */
const
  _                   = require('lodash'),
  rediSearchBindings  = require('redis-redisearch'),
  s                   = {
    noOffsets   : 'NOOFFSETS',
    noFields    : 'NOFIELDS',
    noFreqs     : 'NOFREQS',
    noStem      : 'NOSTEM',
    noSave      : 'NOSAVE',
    stopwords   : 'STOPWORDS',
    schema      : 'SCHEMA',
    text        : 'TEXT',
    numeric     : 'NUMERIC',
    geo         : 'GEO',
    weight      : 'WEIGHT',
    sortable    : 'SORTABLE',
    fields      : 'FIELDS',
    language    : 'LANGUAGE',
    payload     : 'PAYLOAD',
    replace     : 'REPLACE',
    limit       : 'LIMIT',

    //node_redis strings
    mulitConstructor
                : 'Multi'
  },
  defaultNumberOfResults 
                    = 10,
  noop              = function() {};

function optionalOptsCbHandler(passedPenultimateArg,passedUltimateArg) {
  var out = {
    opts    : {},
    cb      : noop
  };

  if (typeof passedPenultimateArg === 'function') {
    //passed a function in penultimate argument then there is no opts
    out.cb = passedPenultimateArg;
  } else if (typeof passedUltimateArg === 'function') {
    out.opts = passedPenultimateArg;
    out.cb = passedUltimateArg;
  } else if (passedPenultimateArg) {
    out.opts = passedPenultimateArg;
  }

  return out;
}

module.exports  = function(clientOrNodeRedis,key,passedOptsOrCb,passedCb) {
  let
    constructorlastArgs = optionalOptsCbHandler(passedOptsOrCb,passedCb),
    client,
    checked             = false,
    rediSearchObj;
  
  if (clientOrNodeRedis.constructor.name === 'RedisClient') {
    //client passed in first agrument
    client = clientOrNodeRedis;
  } if (typeof clientOrNodeRedis.RedisClient === 'function') {
    //redis module passed in first agrument
    rediSearchBindings(clientOrNodeRedis);
    client = clientOrNodeRedis.createClient(constructorlastArgs.opts.clientOptions || {});
  }
  if (constructorlastArgs.cb !== noop) {
    client.on('ready',constructorlastArgs.cb);
  }
  
  
  const chainer = function(cObj) {
    return cObj.constructor.name === s.mulitConstructor ? cObj : rediSearchObj;
  }
  const clientCheck = function() {
    if (!client.ft_create) { throw new Error('Redis client instance has not enabled RediSearch.'); }
    checked = true;
  };



  const createIndex = function(fields,passedOptsOrCb,passedCb) {
    var
      createArgs  = [],
      indexOpts   = {},
      cb;

    if (arguments.length === 2) {
      if (typeof passedOptsOrCb === 'function') {
        cb = passedOptsOrCb;
      } else {
        indexOpts = passedOptsOrCb;
      }
    } else if (arguments.length === 3) {
      cb = passedCb;
      indexOpts = passedOptsOrCb;
    }
    
    if (!checked) { clientCheck(); }
    
    if (indexOpts.noOffsets) { createArgs.push(s.noOffsets); }
    if (indexOpts.noFields) { createArgs.push(s.noFields); }
    if (indexOpts.noFreqs) { createArgs.push(s.noFreqs); }
    //add check if stopwords is an array
    if (indexOpts.noStopWords) {
      createArgs.push(s.stopwords, 0);
    } else if (indexOpts.stopwords) {
      createArgs.push(s.stopwords, indexOpts.stopwords.length);
      createArgs = createArgs.concat(indexOpts.stopwords);
    }
    createArgs.push(s.schema);
    fields.forEach(function(aField) {
      createArgs = createArgs.concat(aField);
    });
    client.ft_create(key,createArgs,cb);
  };
  const deinterleave = function(doc) {                                // `doc` is an array like this `['fname','kyle','lname','davis']`
    return  _(doc)                                                    // Start the lodash chain with `doc`
      .chunk(2)                                                       // `chunk` to convert `doc` to `[['fname','kyle'],['lname','davis']]`
      .fromPairs()                                                    // `fromPairs` converts paired arrays into objects `{ fname : 'kyle', lname : 'davis }`
      .value();                                                       // Stop the chain and return it back
  }
  const docResultParserFactory = function(opts) {
    //`opts` is being reserved later functionality
    return function(doc) {
      return {
        doc     : deinterleave(doc)
      }
    }
  };
  const searchResultParserFactory = function(opts) {
    return function(results) {
      let 
        totalResults = results[0];
      
      results = _(results.slice(1))
        .chunk(2)
        .map(function(aResult) {
          return {
            docId     : aResult[0],
            doc       : deinterleave(aResult[1])
          };
        })
        .value();
      return {
        results       : results,
        totalResults  : totalResults,
        offset        : opts.offset || 0,
        resultSize    : results.length,        
        requestedResultSize    
                      : opts.numberOfResults || defaultNumberOfResults,
      };
    };
  };

  const searchFactory = function(cObj) {
    return function(queryString, passedOptsOrCb, passedCb) {
      let 
        lastArgs = optionalOptsCbHandler(passedOptsOrCb,passedCb),
        searchArgs = [],
        parser = searchResultParserFactory(lastArgs.opts);

      if (!checked) { clientCheck(); }
      
      searchArgs.push(queryString);
      if (lastArgs.opts.offset || lastArgs.opts.numberOfResults) {
        searchArgs.push(s.limit, lastArgs.opts.offset || 0, lastArgs.opts.numberOfResults || defaultNumberOfResults);
      }
      if (cObj.constructor.name === s.mulitConstructor) {
        cObj.parsers = !cObj.parsers ? {} : cObj.parsers;
        cObj.parsers['c'+cObj.queue.length] = parser;
      }
      cObj.ft_search(key,searchArgs,function(err,results) {
        if (err) { lastArgs.cb(err); } else {
          if (lastArgs.cb !== noop) {
            lastArgs.cb(err,parser(results));
          }
        }
      });

      return chainer(cObj);
    };
  };

  const addFactory = function(cObj) {
    //cObj is either a `client` or pipeline instance
    return function(docId, values, passedOptsOrCb, passedCb) {
      let
        lastArgs = optionalOptsCbHandler(passedOptsOrCb,passedCb),
        addArgs   = [docId];
      
      if (!checked) { clientCheck(); }
        
      addArgs.push(lastArgs.opts.score || 1, s.fields); 
      Object.keys(values).forEach(function(aField) {
        addArgs.push(aField,values[aField]);
      });
      cObj.ft_add(key,addArgs,lastArgs.cb);
      return chainer(cObj);
    };
  };

  const getDocFactory = function(cObj) {
    return function(docId, passedOptsOrCb, passedCb) {
      let 
        lastArgs  = optionalOptsCbHandler(passedOptsOrCb,passedCb),
        parser    = docResultParserFactory(lastArgs.opts);


      if (!checked) { clientCheck(); }

      if (cObj.constructor.name === s.mulitConstructor) {
        cObj.parsers = !cObj.parsers ? {} : cObj.parsers;
        cObj.parsers['c'+cObj.queue.length] = parser;
      }
      cObj.ft_get(key, docId, function(err,doc){
        if (err) { lastArgs.cb(err); } else {
          if (lastArgs.cb !== noop) {
            lastArgs.cb(err,parser(doc));
          }
        }
      });
      return chainer(cObj,rediSearchObj);
    };
  }


  const pipelineFactory = function(fn) {
    return function(passedPipeline) {
      let 
        ctx = passedPipeline || client[fn]();
      
      Object.getPrototypeOf(ctx).rediSearch = {
        add     : addFactory(ctx),
        exec    : execFactory(ctx),
        getDoc  : getDocFactory(ctx),
        search  : searchFactory(ctx)
      };
      return ctx;
    };
  };

  const execFactory = function(cObj) {
    return function(cb) {
      cObj.exec(function(err,results) {
        if (err) { cb(err); } else {
          for (let i = 0; i < results.length; i += 1) {
            if (cObj.parsers['c'+i]) {
              results[i] = cObj.parsers['c'+i](results[i]);
            }
          }
          cb(err,results);
        }
      });
    };
  };

  const dropIndex = function(cb) {
    if (!checked) { clientCheck(); }
    
    client.ft_drop(key,cb);
  };

  /* Schema functions */
  const genericField = function(fieldType) {
    return function(name,sortable) {
      let field = [name, fieldType];
      if (sortable) { field.push(s.sortable); }
      return field;
    };
  };
  const fieldDefinition = {
    text        : function(name, sortable, textOpts) {
      let field = [name, s.text];
      if (textOpts && textOpts.noStem) { field.push(s.noStem); }
      if (textOpts && textOpts.weight) { field.push(s.weight, textOpts.weight); }
      if (sortable) { field.push(s.sortable); }
      return field;
    },
    numeric     : genericField(s.numeric),
    geo         : genericField(s.geo)
  };


  return rediSearchObj = {
    createIndex       : createIndex,
    dropIndex         : dropIndex,
    
    add               : addFactory(client),
    getDoc            : getDocFactory(client),    
    search            : searchFactory(client),

    batch             : pipelineFactory('batch'),
    
    fieldDefinition   : fieldDefinition,
    
    client            : client
  };
};