var config = require('./config.js')
var getTimestamp = require('./getTimestamp.js')
var DB_LOG_ON = config.DB_LOG_ON
var DB_LOG_SLOW_QUERIES_ON = config.DB_LOG_SLOW_QUERIES_ON
var IS_DEV_ENV = config.IS_DEV_ENV

/** Helper Function to log timing of query functions **/
function logQueryPrint(message,query,valuesQuery,seconds,milliseconds){
  console.log(message + "Query took: %d:%ds  => " + query + valuesQuery, seconds, milliseconds); console.log();
}

function logQuery(startTime, query, values){
  var clientConnectionID = this.clientConnectionID >=0 ? this.clientConnectionID : 'null';
  if( ! ( DB_LOG_ON || DB_LOG_SLOW_QUERIES_ON   ) ) return;
  var t = process.hrtime(startTime);
  var valuesQuery = values instanceof Array && values.length > 0 ? (" , queryParams => " + JSON.stringify(values) + "" ) : "";
  var seconds = t[0];
  var milliseconds = t[1];
  var isSlowTiming = ( seconds + (milliseconds/1e9) ) >= 1;
  var message = "Connection "+this.databaseAddress+" ID: "+clientConnectionID+" - "+getTimestamp()+" - "
  if (  ( DB_LOG_SLOW_QUERIES_ON ||  IS_DEV_ENV ) && isSlowTiming  ){
    message = "Connection "+this.databaseAddress+" ID: "+clientConnectionID+" SLOW!! - "+getTimestamp()+" - "
    return logQueryPrint(message,query,valuesQuery,seconds,milliseconds)
  }
  if ( DB_LOG_ON ){ return logQueryPrint(message,query,valuesQuery,seconds,milliseconds) }
};

module.exports = logQuery;
