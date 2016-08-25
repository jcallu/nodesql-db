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
var SYNC_LOGOUT_TIMEOUT = config.NODESQLDB_POOL_IDLE_TIMEOUT;

var pgData = {}
var defaults = {
  reapIntervalMillis: config.NODESQLDB_REAP_INTERVAL_MILLIS ,
  poolIdleTimeout: config.NODESQLDB_POOL_IDLE_TIMEOUT ,
  poolSize: 1,
  parseInt8: parseInt,
  dbConnectionId: pgData.dbConnectionId>=0 ? pgData.dbConnectionId : 1
}

function DBConnection(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol){
  this.setConnectionParams.bind(this)(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol);
  this.clientDefaults = _.cloneDeep(defaults);
  this.clientEndIntervalTimer = 0;
  this.Client = Client;
  this.ClientReaper.bind(this)();
  this.Client.defaults = this.clientDefaults;
  this.clientConnectionID = 1
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

/** Setup a new Asynchronous PG Client **/
DBConnection.prototype.ClientNewPool = function(){
  if( !this.databaseName ){    console.error( new Error( "DBConnection.databaseName not assigned -> " + this.databaseName + ", typeof -> " + (typeof this.databaseName) ) ); }
  if( !this.databaseAddress ){ console.error( new Error( "DBConnection.databaseAddress not assigned -> " + this.databaseAddress + ", typeof -> " + (typeof this.databaseAddress) ) ); }
  console.log(this.databaseProtocol,this.databaseName,"Pool Size = " + this.Client.defaults.poolSize + " : DB Client " + this.clientConnectionID + "  Connected",this.databaseAddress,this.databasePort);
}

/** Generate and return a connection string using database name and address **/
DBConnection.prototype.getConnectionString = function(){
  var user = this.databaseUser;
  var password = this.databasePassword;
  var address = this.databaseAddress;
  var port = this.databasePort;
  var name = this.databaseName;
  var connectionString = "postgresql://" + user + ":" + password + "@" + address + ":" + port + "/" + name ; /* Create a TCP postgresql Call string using a database, password, port, and address; password and port are defaulted currently to config's */
  return connectionString;
};
DBConnection.prototype.isConnected = function isConnected(){
  var self = this;
  var isConnected = false
  try { isConnected = self.Client.native.pq.connected == true } catch(e){}
  return isConnected;
}

/** Query using the Asynchronous PG Client **/
DBConnection.prototype.query = function(queryIn, paramsIn, callback){
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
      if( self.isConnected.bind(self)() ) return wcb();
      self.Client.connect.bind(self.Client)(function(err){
        wcb(err);
      })
    },
    function queryCall(wcb){
      self.Client.query.bind(self.Client)(query, params, function(err, result, done) {
        if(err){ try{  err.message = err.message + "\r" + query;  } catch(e){ console.error(e.stack) } }
        self.logQuery.bind(self)(startTime, query, params)
        setImmediate(function(){ callback(err, result); });
      });
    }
  ],function(err,ret){
    self.ClientReaper.bind(self)();
    callback(err,ret);
  })
};

DBConnection.prototype.querySync = function(queryIn, paramsIn, callback){
  var err = new Error("transaction querySync not supported")
  console.error(err.stack);
  callback(err)
}

/** Wrapper to end database connection **/
DBConnection.prototype.end = function(){
  try { this.ClientEnd.bind(this)(); } catch(e){ /*console.error(e.stack);*/ } // Force log out of async PG clients

}

/** Force Sync Clients to die after certain time **/
DBConnection.prototype.ClientReaper = function(){
  clearInterval( this.clientEndIntervalTimer ); // Just killed PG Sync Client.
  this.clientEndIntervalTimer = setInterval( this.ClientEnd.bind(this) , SYNC_LOGOUT_TIMEOUT);
  this.clientEndIntervalTimer.unref()
}

/** Force Sync Client to die **/
DBConnection.prototype.ClientEnd = function(){
  try {
    this.Client.end()
    this.Client.native.pq.connected = false
  } catch(e){

  }
}



DBConnection.prototype.logQuery = logQuery

module.exports = DBConnection;
