var NODESQLDB_HOME = __dirname+'/../'
var fs = require('fs');
var _ = require('lodash');
var config = require('../config.js')
var NODE_ENV = config.NODE_ENV
var SchemaFilename = require('../SchemaFilename.js')
function isCacheNotSet(cacheKey){
  if( typeof process[cacheKey] == 'undefined' ) return true;
  return _.keys( ( process[cacheKey] || {} ) ).length === 0;
}

function schemaQuery(dbName,tableSchema){
  tableSchema = tableSchema ? tableSchema : 'def'


  var schemaQuery = "select trim(ist.table_name) as tablename, concat('[',trim(group_concat( concat('{ \"column_name\":\"', isc.column_name,'\", \"data_type\":\"', isc.data_type,'\", \"js_type\":\"', (case \
    when isc.data_type in('text','varchar','char','binary','varbinary','blob','enum','set') then 'string' \
    when isc.data_type in('integer','int','smallint','tinyint','mediumint','bigint','decimal','numeric','float','double','bit','real', 'double precision') then 'number' \
    when isc.data_type in('date') then 'date' \
    when isc.data_type in('timestamp') then 'time' \
    when isc.data_type in('json') then 'object' end) \
    ,'\", \"is_primary_key\":\"', (case when isc.column_key = 'PRI' then true else false end),'\"}') )\
  ),']') as tableschema \
  from information_schema.tables ist join information_schema.columns isc on isc.table_name = ist.table_name where ist.TABLE_SCHEMA = '"+dbName+"' AND ist.TABLE_CATALOG='"+tableSchema+"' group by ist.table_name";

  return schemaQuery
}

module.exports = function MysqlDatabaseSchemaCache(dbName,dbAddr,dbPasswd,dbPort,dbUser,dbConnection,dbAdapter){
  // var start = process.hrtime();
  var DB = dbName.toUpperCase();
  var CACHE_KEY = SchemaFilename(dbName,dbAddr,dbPort,dbUser,dbAdapter);
  var schemaTmp = {};
  // console.log("process[CACHE_KEY]",process[CACHE_KEY])
  var setCache = isCacheNotSet(CACHE_KEY)
  if( ! setCache ) return process[CACHE_KEY];

  process[CACHE_KEY] = process[CACHE_KEY] || {};
  var data = dbConnection.querySync("select 1 first_db_call_test, '"+dbConnection.databaseAddress+"' as db_address,'"+dbConnection.databaseName+"' as db_name")
  if( data.error ) {
    var e = data.error instanceof Error ? data.error : new Error(data.error)
    console.error(e.stack);
  }
  try {
    schemaTmp = dbConnection.querySync( schemaQuery(dbName) );
    if( schemaTmp.error ){
      var e = schemaTmp.error instanceof Error ? schemaTmp.error : new Error(schemaTmp.error)
      console.error(e.stack);
    }


    for( var row in schemaTmp.rows ){
      schemaTmp.rows[row].tableschema = JSON.parse(schemaTmp.rows[row].tableschema)
      for( var columnData in schemaTmp.rows[row].tableschema ){
        if( schemaTmp.rows[row].tableschema[columnData].is_primary_key == '1' ){
          schemaTmp.rows[row].tableschema[columnData].is_primary_key = true;
        } else {
          schemaTmp.rows[row].tableschema[columnData].is_primary_key = false
        }
      }
    }
    // console.log("schemaTmp",JSON.stringify(schemaTmp) )
  } catch(e){
    console.error(e.stack);
  }

  var pathToFileCache = NODESQLDB_HOME + 'schemas/'+CACHE_KEY+'.json';
  if( schemaTmp.error || !( schemaTmp instanceof Object ) || ! ( schemaTmp.rows instanceof Array ) || schemaTmp.rows.length === 0  ){
    var schemaFromFile;
    try {
      //abstractDBLog("Using file DB cache");
      schemaFromFile = JSON.parse( fs.readFileSync(pathToFileCache).toString('utf8') );
    }
    catch(e){

      var err = pathToFileCache + " AbstractTable readFileSync\n"+e.stack;

      if( err || schemaTmp.error ){
        if( schemaTmp.error ) console.error("Schema Loading error =>",schemaTmp.error);
        if ( err ) console.error("Schema Loading error =>",err);
      }
      fs.writeFileSync(pathToFileCache,JSON.stringify({}))
      if( NODE_ENV === 'development' ) {
        throw err;
      }
    }
    schemaTmp = schemaFromFile;
  }
  else {
    schemaTmp = schemaTmp.rows;
    fs.writeFileSync(pathToFileCache,JSON.stringify(schemaTmp));
  }

  _.each(schemaTmp,function(obj){
    process[CACHE_KEY][obj.tablename] = _.cloneDeep( obj.tableschema );
  });
  return process[CACHE_KEY];;
}
