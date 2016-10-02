var dbAdapterName = 'mysql';
var dbPortDefault = 3306;

var async = require('async')
var Q = require('q');
var _ = require('lodash');
var config = require('../config.js')
var logQuery = require('../logQuery.js')
var MySQL = require('mysql')
var mysql  = { getConnection: function(cb){ cb(new Error("pool not ready")) }}
var mysqlSync = require('./mysqlSync.js')

var connectionsMap = {
  pool: {},
  client: {}
};
var mysqlData = {}
var defaults = {
  reapIntervalMillis: config.NODESQLDB_REAP_INTERVAL_MILLIS,
  poolIdleTimeout: config.NODESQLDB_POOL_IDLE_TIMEOUT,
  poolSize: config.NODESQLDB_POOL_SIZE,
  parseInt8: parseInt,
  dbConnectionId: mysqlData.dbConnectionId>=0 ? mysqlData.dbConnectionId : 1
}
var LOG_CONNECTIONS = config.LOG_CONNECTIONS;
var SYNC_LOGOUT_TIMEOUT = config.NODESQLDB_POOL_IDLE_TIMEOUT;
var DB_LOG_ON = config.DB_LOG_ON
var DB_LOG_SLOW_QUERIES_ON = config.DB_LOG_SLOW_QUERIES_ON
var IS_DEV_ENV = config.IS_DEV_ENV

function DBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.setConnectionParams.bind(this)(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol);
  this.clientDefaults = _.cloneDeep(defaults);
  this.clientEndIntervalTimer = 0;
  this.clientPoolEndIntervalTimer = 0;
  this.clientConnectionID = ( parseInt(mysqlData.dbConnectionId) >= 1 ? parseInt(mysqlData.dbConnectionId) : 1 );
}

DBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.setDatabaseName.bind(this)(databaseName);
  this.setDatabaseAddress.bind(this)(databaseAddress);
  this.setDatabasePort.bind(this)(databasePort);
  this.setDatabaseUser.bind(this)(databaseUser);
  this.setDatabasePassword.bind(this)(databasePassword);
  this.setDatabaseProtocol.bind(this)(databaseProtocol);
}

DBConnection.prototype.setDatabaseProtocol = function(databaseProtocol){
  if( typeof databaseProtocol == 'undefined' ) {
    var e = new Error("unrecognized protocol -> "+databaseProtocol)
    throw e
  }
  this.databaseProtocol = databaseProtocol || dbAdapterName;
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
  // console.log("connectionString",connectionString)
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



function isClientPoolDisconnected(conStr){
  // console.log("?",mysql._nodes instanceof Object,mysql)
  return typeof connectionsMap.pool['"'+conStr+'"'] == 'undefined' && ( !( mysql instanceof Object ) || !( mysql._nodes instanceof Object ) || mysql._closed || mysql._nodes[conStr] == 'undefined' )
}

/** Setup a new Asynchronous MYSQL Client **/
DBConnection.prototype.ClientNewPool = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  var dbConnectionString = this.getConnectionString.bind(this)();

  if( isClientPoolDisconnected(dbConnectionString) ){
    connectionsMap.pool['"'+dbConnectionString+'"'] = true;
    var poolConfig = {
      connectionLimit : this.clientDefaults.poolSize,
      host            : this.databaseAddress,
      user            : this.databaseUser,
      password        : this.databasePassword,
      database        : this.databaseName,
      port            : this.databasePort
    }
    mysql = MySQL.createPoolCluster();
    mysql.add(dbConnectionString,poolConfig);
  }
  if(  LOG_CONNECTIONS != false ) console.log(this.databaseProtocol,this.databaseName,"Pool Size = " + this.clientDefaults.poolSize + " - DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}




/** Query using the Asynchronous MYSQL Client **/
DBConnection.prototype.query = function(query, params, callback){
  clearInterval( this.clientPoolEndIntervalTimer );
  var self = this;
  var startTime = process.hrtime();
  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  clearInterval( self.clientPoolEndIntervalTimer );
  async.waterfall([
    function runQuery(wcb){
      var dbConnectionString = self.getConnectionString.bind(self)();
      if( isClientPoolDisconnected(dbConnectionString) ) { /* If MYSQL Async Client is disconnected, connect that awesome, piece of awesomeness!!! */
        self.ClientNewPool.bind(self)();
      }
      mysql.getConnection(function(err, client) {
        var done = function(){};
        try { done = client.release.bind(client); } catch(e){}
        var isConnected = !err && client instanceof Object  && typeof client.query === 'function' && typeof done === 'function'
        if ( isConnected ) { /*  Check if client connected then run the query */
          return client.query.bind(client)(query, params, function(err, data) {
            try { done(); } catch(e){ err = err ? err : e }
            self.logQuery.bind(self)(startTime, query, params)
            if(err){  err.message = err.message + "\r" + query + (params ? (", "+JSON.stringify(params)) : '');  }
            var results = { rows: [] }
            var keys = [];
            if( data instanceof Array && data.length > 0 ){
              keys = Object.keys(data[0])
            }
            if( keys.length > 0 ){
              results.rows = _.map(data,function(r){
                var o = {}
                for( var col = 0; col < keys.length; col++ ){
                  var column = keys[col]
                  o[column] = r[column]
                }
                return o;
              })  
            }

            wcb(err, results);
          });
        }
        self.poolConnectionFailure.bind(self)(err, done, wcb); /*  The Client died, didn't connect, or errored out; so make sure it gets buried properly, and resurrected immediately for further querying. */
      })
    }
  ],function(err,data){
    self.ClientPoolReaper.bind(self)();
    callback(err,data);
  })
};

DBConnection.prototype.ClientPoolEnd = function(){
  // console.log("ClientPoolEnd ended",mysql)
  try {
    mysql.end();
  } catch(e){
    // console.error(e.stack);
  } // Force log out of async MYSQL clients
  try {
    var conString = this.getConnectionString.bind(this)();
    delete connectionsMap.pool['"'+conString+'"'];
  } catch(e){
    console.error(e.stack);
  }
  // console.log('connectionsMap.pool',connectionsMap.pool)
}

/** Force Sync Clients to die after certain time **/
DBConnection.prototype.ClientPoolReaper = function(){
  clearInterval( this.clientPoolEndIntervalTimer ); // Just killed MYSQL Sync Client.
  this.clientPoolEndIntervalTimer = setInterval( this.ClientPoolEnd.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.clientPoolEndIntervalTimer.unref()
}

/** Wrapper to end database connection **/
DBConnection.prototype.end = function(){
  var self = this;
  self.ClientPoolEnd.bind(self)()
  self.ClientEnd.bind(self)()
}


function isClientDisconnected(conString){
  return typeof connectionsMap.client['"'+conString+'"'] === 'undefined' ||  typeof mysqlSync == 'undefined' || !( mysqlSync.mq instanceof Object) || ( !mysqlSync.mq.connected  )
}

/** Setup a new Synchronous MYSQL Client **/
DBConnection.prototype.NewClient = function(dbConnectionString){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  if( isClientDisconnected(dbConnectionString) ){
    try {
      mysqlSync.connectSync( dbConnectionString )
      connectionsMap.client['"'+dbConnectionString+'"'] = true
    } catch(e){
    }
    this.ClientReaper.bind(this)()
  }
  if( LOG_CONNECTIONS != false )   console.log(this.databaseProtocol,this.databaseName,"Client Size = 1 - DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Query using the Synchronous MYSQL Client **
 * WARNING PADWAN!!! : This is for object/array/string initialization using data from the database only.
 * Never use this for regular querying because it will starve CPU for the rest of application
 */
DBConnection.prototype.querySync = function(query,params,callback){
  var startTime = process.hrtime();
  clearInterval( this.clientEndIntervalTimer );   /* Prevent logout if it has not happened yet. */
  var dbConnectionString = this.getConnectionString.bind(this)()
  if( isClientDisconnected(dbConnectionString) ){
    this.NewClient.bind(this)(dbConnectionString);
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
  this.ClientReaper.bind(this)();
  this.logQuery.bind(this)(startTime, query_, params);
  callback(ret.error, ret.rows);
  return ret;
}



/** Force Sync Clients to die after certain time **/
DBConnection.prototype.ClientReaper = function(){
  clearInterval( this.clientEndIntervalTimer ); // Just killed MYSQL Sync Client.
  this.clientEndIntervalTimer = setInterval( this.ClientEnd.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.clientEndIntervalTimer.unref()
}

/** Force Sync Client to die **/
DBConnection.prototype.ClientEnd = function(){
  var conString = this.getConnectionString.bind(this)();
  try {
    clearInterval( this.clientEndIntervalTimer );
    mysqlSync.end();
  } catch(e){ /*console.error(e.stack);*/ } // Force log out of  sync MYSQL clients
  delete connectionsMap.client['"'+conString+'"'];
}


module.exports = DBConnection;
