var NODESQLDB_HOME = __dirname+'/../'
var config = require('../config.js')
var NODE_ENV = config.NODE_ENV
var fs = require('fs');
var _ = require('lodash');
var SchemaFilename = require('../SchemaFilename.js')
function isCacheNotSet(cacheKey){
  if( typeof process[cacheKey] == 'undefined' ) return true;
  return _.keys( ( process[cacheKey] || {} ) ).length === 0;
}

function schemaQuery(dbName,tableSchema){
  tableSchema = tableSchema ? tableSchema : 'public'
  return "select * from (select tt.table_name as tablename, array_agg( \
(select row_to_json(_) from ( select col.column_name,col.data_type, \
case when col.data_type in ('text','varchar','character varying') then 'string' \
when col.data_type in ('bigint','integer','numberic','real','double precision') then 'number' \
when col.data_type in ('timestamp','timestamp without time zone','timestamp with time zone') then 'time' \
when col.data_type in ('date') then 'date' \
when col.data_type in ('boolean') then 'boolean' \
when col.data_type in ('json','ARRAY') then 'object' \
end as js_type, \
case when col.column_name = ccu.column_name then true else false end as is_primary_key \
) as _ ) ) as tableschema \
from information_schema.tables tt \
join information_schema.columns col on col.table_name = tt.table_name \
left join information_schema.table_constraints tc on tc.table_name = tt.table_name and tc.constraint_type = 'PRIMARY KEY' \
left JOIN information_schema.constraint_column_usage AS ccu ON tc.constraint_name = ccu.constraint_name \
where tt.table_catalog = '"+dbName+"' \
and tt.table_schema = '"+tableSchema+"' \
group by tt.table_name \
) tables \
order by tables.tablename;";
}

module.exports = function CreatePostgresqlDatabaseSchema(dbName,dbAddr,dbPasswd,dbPort,dbUser,dbConnection,dbAdapter){
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
