/**
 * Provides a postgres access point for other functions
 * @module ConnectionClient
 * @author Jacinto Callu
 * @copyright Jacinto Callu 2016
 */
var Q = require('q');
var _ = require('lodash');
var async = require('async')
/* TheVGP Modules */
var logQuery = require('../logQuery.js')
var config = require('../config.js')
var moment = require('moment-timezone')
/** Constants **/
var PG_POOL_SIZE = config.PG_POOL_SIZE;
var NODE_ENV = config.NODE_ENV;
var DB_LOG_ON = config.DB_LOG;
var DB_LOG_SLOW_QUERIES_ON = config.DB_LOG_SLOW_QUERIES_ON;
var IS_DEV_ENV =  config.IS_DEV_ENV

var pgData = {}
var defaults = {
  reapIntervalMillis: config.NODESQLDB_REAP_INTERVAL_MILLIS ,
  poolIdleTimeout: config.NODESQLDB_POOL_IDLE_TIMEOUT ,
  poolSize: 1,
  parseInt8: parseInt,
  dbConnectionId: pgData.dbConnectionId>=0 ? pgData.dbConnectionId : 1
}

function TransactionDBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol){
  this.setConnectionParams.bind(this)(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol);
  this.clientDefaults = _.cloneDeep(defaults);
  this.Client = Client;
  this.Client.defaults = this.clientDefaults;
  this.clientConnectionID = 1
}


TransactionDBConnection.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.setDatabaseName.bind(this)(databaseName);
  this.setDatabaseAddress.bind(this)(databaseAddress);
  this.setDatabasePort.bind(this)(databasePort);
  this.setDatabaseUser.bind(this)(databaseUser);
  this.setDatabasePassword.bind(this)(databasePassword);
  this.setDatabaseProtocol.bind(this)(databaseProtocol);
}

TransactionDBConnection.prototype.setDatabaseProtocol = function(dbProtocol){
  if( typeof dbProtocol == 'undefined' ) {
    var e = new Error("unrecognized protocol -> "+dbProtocol)
    throw e
  }
  this.databaseProtocol = dbProtocol || 'postgresql'
}
TransactionDBConnection.prototype.setDatabaseName = function(dbName){
  this.databaseName = dbName || ''
}

TransactionDBConnection.prototype.setDatabaseAddress = function(dbAddr){
  this.databaseAddress = dbAddr || ''
}

TransactionDBConnection.prototype.setDatabasePort = function(dbPort){
  this.databasePort = dbPort || 5432;
}

TransactionDBConnection.prototype.setDatabaseUser = function(dbUser){
  this.databaseUser = dbUser || 'postgres';
}

TransactionDBConnection.prototype.setDatabasePassword = function(dbPasswd){
  this.databasePassword = dbPasswd || '';
}

/** Setup a new Asynchronous PG Client **/
TransactionDBConnection.prototype.ClientNewPool = function(){
  if( !this.databaseName ){    console.error( new Error( "TransactionDBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "TransactionDBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  console.log(this.databaseProtocol,this.databaseName,"Pool Size = " + this.Client.defaults.poolSize + " : DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Generate and return a connection string using database name and address **/
TransactionDBConnection.prototype.getConnectionString = function(){
  var user = this.databaseUser;
  var password = this.databasePassword;
  var address = this.databaseAddress;
  var port = this.databasePort;
  var name = this.databaseName;
  var connectionString = "postgresql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ; /* Create a TCP postgresql Call string using a database, password, port, and address; password and port are defaulted currently to config's */
  return connectionString;
};

/** Query using the Asynchronous PG Client **/
TransactionDBConnection.prototype.query = function(queryIn, paramsIn, callback){
  var self = this;
  var query = _.cloneDeep(queryIn);
  var params = paramsIn instanceof Array ? _.cloneDeep(paramsIn) : paramsIn;
  var startTime = process.hrtime();
  if ( typeof params == 'function' ){
    callback = params;
    params = null;
  }
  callback = typeof callback === 'function' ? callback : function(){};
  async.waterfall([
      function ifNotConnectedConnect(wcb){
        var isConnected = false
        try { isConnected = self.Client.native.pq.connected == true } catch(e){}
        if( isConnected ) return wcb();
        self.Client.connect.bind(self.Client)(wcb)
      },
      function queryCall(wcb){
        self.Client.query.bind(self.Client)(query, params, function(err, result) {
          if(err){ try{  err.message = err.message + "\r" + query;  } catch(e){ console.error(e.stack) } }
          self.logQuery.bind(self)(startTime, query, params)
          callback(err, result);
        });
      }
  ],callback)
};

TransactionDBConnection.prototype.querySync = function(queryIn, paramsIn, callback){
  var err = new Error("transaction querySync not supported")
  callback(err)
}

/** Wrapper to end database connection **/
TransactionDBConnection.prototype.end = function(){
  try { this.Client.end(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of async PG clients

}

/** Force Sync Clients to die after certain time **/
TransactionDBConnection.prototype.ClientReaper = function(){

}

/** Force Sync Client to die **/
TransactionDBConnection.prototype.ClientEnd = function(){
  try {  } catch(e){  } /* If PGSync Client is alive and well destory it. Feel the power of the darkside!!! */

}

TransactionDBConnection.prototype.ClientPoolEnd = function(){
  try { this.Client.end(); } catch(e){  } /* If PGSync Client is kill */
}

/** Async Client has died **/
TransactionDBConnection.prototype.poolConnectionFailure = function(err,done,callback){
  done = typeof done === 'function' ? done : function(){}; // make sure client can be killed without any syntax errors.
  callback = typeof callback == 'function' ? callback : function(){};
  err = err ? ( err instanceof Error ? err : new Error(err) ) : new Error(); // make sure error is an Error instance.
  return callback(err);
}
TransactionDBConnection.prototype.logQuery = logQuery

module.exports = TransactionDBConnection;
