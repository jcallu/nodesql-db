var dbAdapterName = 'mysql';
var dbPortDefault = 3306;

var async = require('async')
var Q = require('q');
var _ = require('lodash');
var config = require('../config.js')
var logQuery = require('../logQuery.js')
var MySQL = require('mysql')
var mysql  = MySQL.createPoolCluster();
var mysqlSync = require('./mysqlSync.js')

var mysqlConnections = {
  pool: {},
  client: {}
};
var mysqlData = {}
var defaults = {
  reapIntervalMillis: config.NODESQLDB_REAP_INTERVAL_MILLIS,
  poolIdleTimeout: config.NODESQLDB_POOL_IDLE_TIMEOUT,
  poolSize: config.NODESQLDB_POOL_SIZE,
  parseInt8: parseInt,
  DB_CONNECTION_ID: mysqlData.DB_CONNECTION_ID>=0 ? mysqlData.DB_CONNECTION_ID : 1
}
var LOG_CONNECTIONS = config.LOG_CONNECTIONS;
var SYNC_LOGOUT_TIMEOUT = config.NODESQLDB_POOL_IDLE_TIMEOUT;
var DB_LOG_ON = config.DB_LOG_ON
var DB_LOG_SLOW_QUERIES_ON = config.DB_LOG_SLOW_QUERIES_ON
var IS_DEV_ENV = config.IS_DEV_ENV

function DBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.setConnectionParams.bind(this)(databaseName,databaseAddress,databasePassword,databasePort,databaseUser);
  this.mysqlClientDefaults = _.cloneDeep(defaults);
  // console.log("this.mysqlClientDefaults",this.mysqlClientDefaults)
  this.mysqlClientIntervalTimer = 0;
  this.clientConnectionID = ( parseInt(mysqlData.DB_CONNECTION_ID) >= 1 ? parseInt(mysqlData.DB_CONNECTION_ID) : 1 );
}

DBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser){
  this.setDatabaseName(databaseName);
  this.setDatabaseAddress(databaseAddress);
  this.setDatabasePort(databasePort);
  this.setDatabaseUser(databaseUser);
  this.setDatabasePassword(databasePassword);
}

DBConnection.prototype.setDatabaseName = function(dbName){
  this.databaseName = dbName || ''
}

DBConnection.prototype.setDatabaseAddress = function(dbAddr){
  this.databaseAddress = dbAddr || ''
}

DBConnection.prototype.setDatabasePort = function(dbPort){
  this.databasePort = dbPort || dbPortDefault;
}

DBConnection.prototype.setDatabaseUser = function(dbUser){
  this.databaseUser = dbUser || '';
}

DBConnection.prototype.setDatabasePassword = function(dbPasswd){
  this.databasePassword = dbPasswd || '';
}
/** Generate and return a connection string using database name and address **/
DBConnection.prototype.getConnectionString = function(){
  var user = this.databaseUser;
  var password = this.databasePassword;
  var address = this.databaseAddress;
  var port = this.databasePort;
  var name = this.databaseName;
  var connectionString = "mysql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ;
  return connectionString;
};



/** Async Client has died **/
DBConnection.prototype.poolConnectionFailure = function(err,done,callback){
  done = typeof done === 'function' ? done : function(){}; // make sure client can be killed without any syntax errors.
  callback = typeof callback == 'function' ? callback : function(){};
  err = err ? ( err instanceof Error ? err : new Error("Pool Connection Failure "+err) ) : new Error("Pool Connection Failure"); // make sure error is an Error instance.
  if(err) {
    try { self.end.bind(self)(); } catch(e){ /* console.error(e.stack) */ };
  } // kill the async client and reload the connection
  try { done(); } catch(e){ /* console.error(e.stack) */ }
  callback(err);
}



DBConnection.prototype.logQuery = logQuery


function isMYSQLPoolDisconnected(conStr){
  // console.log("?",mysql._nodes instanceof Object,mysql)
  return typeof mysqlConnections.pool['"'+conStr+'"'] == 'undefined' && ( !( mysql instanceof Object ) || !( mysql._nodes instanceof Object ) || mysql._closed || mysql._nodes[conStr] == 'undefined' )
}

/** Setup a new Asynchronous MYSQL Client **/
DBConnection.prototype.MYSQLNewPool = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  var dbConnectionString = this.getConnectionString.bind(this)();
  if( isMYSQLPoolDisconnected(dbConnectionString) ){
    mysqlConnections.pool['"'+dbConnectionString+'"'] = true;
    var poolConfig = {
      connectTimeout  : this.mysqlClientDefaults.poolIdleTimeout,
      connectionLimit : this.mysqlClientDefaults.poolSize,
      host            : this.databaseAddress,
      user            : this.databaseUser,
      password        : this.databasePassword,
      database        : this.databaseName,
      port            : this.databasePort
    }
    mysql.add(dbConnectionString,poolConfig);
  }
  if(  LOG_CONNECTIONS != false ) console.log(this.databaseName,"MYSQL Client Async Size = " + this.mysqlClientDefaults.poolSize + " :  DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}




/** Query using the Asynchronous MYSQL Client **/
DBConnection.prototype.query = function(query, params, callback){
  var self = this;
  var startTime = process.hrtime();
  var dbConnectionString = self.getConnectionString.bind(self)();

  if( isMYSQLPoolDisconnected(dbConnectionString) ) { /* If MYSQL Async Client is disconnected, connect that awesome, piece of awesomeness!!! */
    self.MYSQLNewPool.bind(self)();

  }

  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  mysql.getConnection(function(err, client) {
    var done = function(){};
    try { done = client.release.bind(client); } catch(e){}
    var isConnected = !err && client instanceof Object  && typeof client.query === 'function' && typeof done === 'function'
    if ( isConnected ) { /*  Check if client connected then run the query */
      return client.query.bind(client)(query, params, function(err, data) {
        try { done(); } catch(e){ err = err ? err : e }
        // console.log("data",data)
        self.logQuery.bind(self)(startTime, query, params)
        if(err){  err.message = err.message + "\r" + query + (params ? (", "+JSON.stringify(params)) : '');  }
        var results = { rows: [] }
        try {
          var keys = Object.keys(data[0])
          results.rows = _.map(data,function(r){
            var o = {}
            for( var col = 0; col < keys.length; col++ ){
              var column = keys[col]
              o[column] = r[column]
            }
            return o;
          })
        } catch(e){
          err = err || e;
        }
        callback(err, results);
      });
    }
    self.poolConnectionFailure.bind(self)(err, done, callback); /*  The Client died, didn't connect, or errored out; so make sure it gets buried properly, and resurrected immediately for further querying. */
  })
};

DBConnection.prototype.MYSQLPoolEnd = function(){
  try {
    mysql.end();
  } catch(e){} // Force log out of async MYSQL clients
  try {
    var conString = this.getConnectionString.bind(this)();
    delete mysqlConnections.pool["'"+conString+"'"];
  } catch(e){}
}

/** Wrapper to end database connection **/
DBConnection.prototype.end = function(){
  var self = this;
  self.MYSQLPoolEnd.bind(self)()
  self.MYSQLClientEnd.bind(self)()
}


function isMYSQLClientDisconnected(conString){
  return typeof mysqlConnections.client['"'+conString+'"'] === 'undefined' ||  typeof mysqlSync == 'undefined' || !( mysqlSync.mq instanceof Object) || ( !mysqlSync.mq.connected  )
}

/** Setup a new Synchronous MYSQL Client **/
DBConnection.prototype.MYSQLNewClient = function(dbConnectionString){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  if( isMYSQLClientDisconnected(dbConnectionString) ){
    try {
      mysqlSync.connectSync( dbConnectionString )
      mysqlConnections.client['"'+dbConnectionString+'"'] = true
    } catch(e){
    }
    this.MYSQLClientReaper.bind(this)()
  }
  if( LOG_CONNECTIONS != false )   console.log(this.databaseName,"MYSQL Client Sync Size = "+1+" :  DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Query using the Synchronous MYSQL Client **
 * WARNING PADWAN!!! : This is for object/array/string initialization using data from the database only.
 * Never use this for regular querying because it will starve CPU for the rest of application
 */
DBConnection.prototype.querySync = function(query,params,callback){
  var startTime = process.hrtime();
  clearInterval( this.mysqlClientIntervalTimer );   /* Prevent logout if it has not happened yet. */
  var dbConnectionString = this.getConnectionString.bind(this)()
  if( isMYSQLClientDisconnected(dbConnectionString) ){
    this.MYSQLNewClient.bind(this)(dbConnectionString);
  }
  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  var ret = { error: null, rows: [] };
  var err = null
  var query_ = query;
  try {
    var connected = mysqlSync.connectSync( dbConnectionString );
    query_ = mysqlSync.getSyncQuery(query,params)
    ret.rows  = mysqlSync.querySync( query_, params );
  } catch(e) {
    err = err || e;
    err.message += "\n"+query_
    ret.error = err;
    ret.rows = []
  } // run blocking/synchronous query to db, careful because it throws errors so we try/catched dem' bugs
  this.MYSQLClientReaper.bind(this)();
  this.logQuery.bind(this)(startTime, query_, params);
  callback(ret.error, ret.rows);
  return ret;
}



/** Force Sync Clients to die after certain time **/
DBConnection.prototype.MYSQLClientReaper = function(){
  clearInterval( this.mysqlClientIntervalTimer ); // Just killed MYSQL Sync Client.
  this.mysqlClientIntervalTimer = setInterval( this.MYSQLClientEnd.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.mysqlClientIntervalTimer.unref.bind(this)()
}

/** Force Sync Client to die **/
DBConnection.prototype.MYSQLClientEnd = function(){
  var conString = this.getConnectionString.bind(this)();
  try {
    clearInterval( this.mysqlClientIntervalTimer );
    mysqlSync.end();
  } catch(e){ /*console.error(e.stack);*/ } // Force log out of  sync MYSQL clients
  delete mysqlConnections.client["'"+conString+"'"];
}


module.exports = DBConnection;
