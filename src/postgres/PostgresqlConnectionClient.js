var Q = require('q');
var _ = require('lodash');
var moment = require('moment-timezone')

var config = require('../config.js')
var logQuery = require('../logQuery.js')
var PG = require('pg').native
var PGNative = require('pg-native');
var pg = PG;
var pgSync = {};
var connectionsMap = { pool: {}, client: {} }

var pgData = {}

var defaults = {
  reapIntervalMillis: config.NODESQLDB_REAP_INTERVAL_MILLIS,
  poolIdleTimeout: config.NODESQLDB_POOL_IDLE_TIMEOUT,
  poolSize: config.NODESQLDB_POOL_SIZE,
  parseInt8: parseInt,
  dbConnectionId: pgData.dbConnectionId>=0 ? pgData.dbConnectionId : 1
}

var LOG_CONNECTIONS = config.LOG_CONNECTIONS;
var SYNC_LOGOUT_TIMEOUT = config.NODESQLDB_POOL_IDLE_TIMEOUT;




function DBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol);
  this.clientEndIntervalTimer = 0;
  this.clientDefaults = _.cloneDeep(defaults);
  this.clientConnectionID = ( parseInt(pgData.dbConnectionId) >= 1 ? parseInt(pgData.dbConnectionId) : 1 );
}

DBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.setDatabaseName.bind(this)(databaseName);
  this.setDatabaseAddress.bind(this)(databaseAddress);
  this.setDatabasePort.bind(this)(databasePort);
  this.setDatabaseUser.bind(this)(databaseUser);
  this.setDatabasePassword.bind(this)(databasePassword);
  this.setDatabaseProtocol.bind(this)(databaseProtocol);
}

DBConnection.prototype.setDatabaseProtocol = function(dbProtocol){
  if( typeof dbProtocol == 'undefined' ) {
    var e = new Error("unrecognized protocol -> "+dbProtocol)
    throw e
  }
  this.databaseProtocol = dbProtocol || 'postgresql'
}

DBConnection.prototype.setDatabaseName = function(dbName){
  this.databaseName = dbName || ''
}

DBConnection.prototype.setDatabaseAddress = function(dbAddr){
  this.databaseAddress = dbAddr || ''
}

DBConnection.prototype.setDatabasePort = function(dbPort){
  this.databasePort = dbPort || 5432;
}

DBConnection.prototype.setDatabaseUser = function(dbUser){
  this.databaseUser = dbUser || 'postgres';
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
  var connectionString = "postgresql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ;
  return connectionString;
};
/** Async Client has died **/
DBConnection.prototype.poolConnectionFailure = function(err,done,callback){
  done = typeof done === 'function' ? done : function(){}; // make sure client can be killed without any syntax errors.
  callback = typeof callback == 'function' ? callback : function(){};
  err = err ? ( err instanceof Error ? err : new Error(err) ) : new Error(); // make sure error is an Error instance.
  if(err) {
    try { self.ClientPoolEnd.bind(self)(); } catch(e){ /* console.error(e.stack) */ };
  } // kill the async client and reload the connection
  try { done(); } catch(e){ /* console.error(e.stack) */ }
  callback(err);
}
DBConnection.prototype.logQuery = logQuery

function newClientPool(){
  pg = PG;
  pg.defaults.reapIntervalMillis = defaults.reapIntervalMillis; // check to kill every 5 seconds
  pg.defaults.poolIdleTimeout = defaults.poolIdleTimeout; // die after 1 minute
  pg.defaults.poolSize = defaults.poolSize;
  pg.defaults.parseInt8 = defaults.parseInt8;
  pgData.dbConnectionId = defaults.dbConnectionId;
  if( pg.listeners('error').length === 0 ){
    pg.on('error',function(e){
      e = e instanceof Error ? e : new Error("pg died")
      console.error("FAILURE - pg module crashed ",e.stack)
    })
  }
  return pg;
};
newClientPool();

function isClientPoolDisconnected(conString){
  return !( pg.pools instanceof Object ) || !( pg.pools.all instanceof Object ) || typeof pg.pools.all['"'+conString+'"'] === 'undefined'
}
/** Setup a new Asynchronous  Client **/
DBConnection.prototype.ClientNewPool = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  var dbConnectionString = this.getConnectionString.bind(this)();
  // console.log("dbConnectionString",dbConnectionString)
  if( isClientPoolDisconnected(dbConnectionString) ){
    connectionsMap.pool['"'+dbConnectionString+'"'] = true;
    newClientPool()
  }
  if(  LOG_CONNECTIONS != false ) console.log(this.databaseProtocol,this.databaseName,"Pool Size = " + pg.defaults.poolSize + " - DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}



/** Query using the Asynchronous  Client **/
DBConnection.prototype.query = function(query, params, callback){
  var startTime = process.hrtime();
  var dbConnectionString = this.getConnectionString();
  if( isClientPoolDisconnected(dbConnectionString) ) { /* If  Async Client is disconnected, connect that awesome, piece of awesomeness!!! */
    this.ClientNewPool.bind(this)();
  }
  if ( typeof params == 'function' ){  callback = params; params = null; }
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  pg.connect( dbConnectionString , function(err, client, done) {
    var isConnected = !err  && client instanceof Object  && typeof client.query === 'function' && typeof done === 'function'
    if ( isConnected ) { /*  Check if client connected then run the query */
      return client.query.bind(client)(query, params, function(err, result) {
        try{ done(); } catch(e){ err = err ? err : e }
        self.logQuery.bind(self)(startTime, query, params)
        if(err){  err.message = err.message + "\r" + query + (params ? (", "+JSON.stringify(params)) : '');  }
        callback(err, result);
      });
    }
    self.poolConnectionFailure.bind(self)(err, done, callback); /*  The Client died, didn't connect, or errored out; so make sure it gets buried properly, and resurrected immediately for further querying. */
  });

};
DBConnection.prototype.ClientPoolEnd = function(){
  var conString = this.getConnectionString.bind(this)();
  try { pg.end(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of async  clients
  delete connectionsMap.pool["'"+conString+"'"];
}

/** Wrapper to end database connection **/
DBConnection.prototype.end = function(){
  var self = this;
  self.ClientPoolEnd.bind(self)()
  self.ClientEnd.bind(self)()
}


function newClient(conString){
  pgSync = new PGNative();
  connectionsMap.client['"'+conString+'"'] = true;
  pgSync.defaults = !( pgSync.defaults instanceof Object ) ? _.cloneDeep(defaults) : pgSync.defaults;
  pgSync.defaults.poolSize = 1; //sync clients use single connection then die
  return pgSync
}
function isClientDisconnected(conString){
  return typeof connectionsMap.client['"'+conString+'"'] === 'undefined' && ( typeof pgSync == 'undefined' || !( pgSync.pq instanceof Object) || ( !pgSync.pq.connected  ) )
}
/** Setup a new Synchronous PG Client **/
DBConnection.prototype.NewClient = function(dbConnectionString){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  if( isClientDisconnected(dbConnectionString) ){
    this.ClientReaper.bind(this)()
    newClient(dbConnectionString);
  }
  if( LOG_CONNECTIONS != false )   console.log(this.databaseProtocol,this.databaseName,"Client Size = "+pgSync.defaults.poolSize+" - DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Query using the Synchronous PG Client **
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
  try {
    pgSync.connectSync( dbConnectionString );
    ret.rows = pgSync.querySync( query, params );
  } catch(e) {
    ret.error = e;
    ret.rows = []
  } // run blocking/synchronous query to db, careful because it throws errors so we try/catched dem' bugs
  this.ClientReaper.bind(this)();
  this.logQuery.bind(this)(startTime, query, params);
  callback(ret.error, ret);
  return ret;
}



/** Force Sync Clients to die after certain time **/
DBConnection.prototype.ClientReaper = function(){
  clearInterval( this.clientEndIntervalTimer ); // Just killed PG Sync Client.
  this.clientEndIntervalTimer = setInterval( this.ClientEnd.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.clientEndIntervalTimer.unref.bind(this)()
}

/** Force Sync Client to die **/
DBConnection.prototype.ClientEnd = function(){
  var conString = this.getConnectionString.bind(this)();
  try {
    clearInterval( this.clientEndIntervalTimer );
  } catch(e){ /*console.error(e.stack);*/ } // Force log out of  sync PG clients
  try {
    pgSync.end();
  } catch(e){}
  delete connectionsMap.client["'"+conString+"'"];
}



module.exports = DBConnection;
