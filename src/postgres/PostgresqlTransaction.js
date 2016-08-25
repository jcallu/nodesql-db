var Q = require('q');
var _ = require('lodash');
var PGClient = require('pg').native
var SchemaFilename = require('../SchemaFilename.js')
var PostgresqlTransactionConnectionClient = require('./PostgresqlTransactionConnectionClient.js')
var AbstractTable = require('../AbstractTable.js')

function Transaction (databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnection,databaseProtocol, PostgresqlDatabaseSchemaCache){
  var fsSchemaCacheKey = SchemaFilename(databaseName,databaseAddress,databasePort,databaseUser,databaseProtocol)
  var schemaSet = PostgresqlDatabaseSchemaCache(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,dbConnection,databaseProtocol);
  // console.log("schemaSet",Object.keys(schemaSet).length,fsSchemaCacheKey)
  var schema =  schemaSet || process[fsSchemaCacheKey] || {};
  if( Object.keys(schema).length == 0 ){
    throw Error("Schema "+databaseName+" not initialized");
  }
  this.databaseName = databaseName;
  this.databaseAddress = databaseAddress;
  this.databasePassword = databasePassword
  this.databasePort = databasePort;
  this.databaseUser = databaseUser;
  this.dbConnectionString = dbConnection.getConnectionString.bind(dbConnection)()
  var TransactionClient = new PGClient.Client( this.dbConnectionString );
  var Client = new PostgresqlTransactionConnectionClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,TransactionClient,databaseProtocol)
  this.Setup.bind(this)();
  this.Client = Client;
  this.Promise = function dbPromise(){ return Q.fcall(function(){ return; }); };
  for( var tablename in schema ){
    this[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,PostgresqlDatabaseSchemaCache);
  }
  return this;
}
Transaction.prototype.Setup = function(){
  this.RolledBack = false;
  this.Closed = false;
  this.Begun = false;
  return this;
}

Transaction.prototype.GetClient = function(){
  return this.Client;
};

Transaction.prototype.GetDB = function(){
  return this.databaseName;
};


Transaction.prototype.Begin = function(){
  var q = Q.defer();
  var self = this;
  self.Setup.bind(self)();
  var beginQuery = "BEGIN";
  self.Client.query.bind(self.Client)(beginQuery,function(err,ret){
    if( err ) {
      return self.Rollback.bind(self)();
    }
    self.Begun = true;
    q.resolve(ret);
  });
  return q.promise;
};

Transaction.prototype.Boundary = function(promisedStep){
  var self = this;
  var q = Q.defer();
  if( !( promisedStep instanceof Object && typeof promisedStep.then === 'function' && typeof promisedStep.fail == 'function' ) ) {
    self.Rollback.bind(self)().fail(function(err){
      q.reject(new Error("Boundary function was not a promise"));
    });
  }
  else {
    promisedStep.then(function(ret){
      q.resolve(ret);
    })
    .fail(function(err){
      self.Rollback.bind(self)(err).fail(function(err){
        q.reject(err);
      });
    })
    .done();
  }

  return q.promise;
};

Transaction.prototype.Commit = function(){
  var self = this;
  var q = Q.defer();
  if( !self.Begun || self.Closed || self.RolledBack ){
    q.resolve()
  }
  else {
    var commitQuery = "COMMIT";
    self.Client.query.bind(self.Client)(commitQuery,function(err,ret){
      if(err){
        self.Rollback(err);
      } else {
        self.Client.end.bind(self.Client)();
        self.Closed = true;
        q.resolve(ret);
      }
    });
  }

  return q.promise;
};


var ROLLBACK_MSG = "<~ Transaction Client Closed And Rolled Back";


Transaction.prototype.Rollback = function(err){
  var self = this;

  // var s = process.hrtime();
  var q = Q.defer();
  var rollbackQuery = "ROLLBACK;";

  if( self.RolledBack == false ) {
    self.Client.query.bind(self.Client)(rollbackQuery,function(err2,ret){
      try {
        self.Client.end.bind(self.Client)();
      } catch(e){
      }
      self.RolledBack = true;
      self.Closed = true;
      if(err) {
        err.message += " FAILURE: Transaction ERROR " + ROLLBACK_MSG;
      }
      else if(err2){
        err2.message += " FAILURE: Client Query ERROR " + ROLLBACK_MSG;
        err = err2;
      }
      else {
        err = new Error('User Rollback');
        err.stack = 'NOTICE: Transaction Rolled Back by User';
      }
      q.reject(err);
    });
  } else {
    try {
      self.Client.end.bind(self.Client)();
    } catch(e){
    }
    self.Closed = true;
    q.reject(new Error("Transaction RolledBack -> "+self.RolledBack+", Transaction Closed -> "+self.Closed));
  }
  return q.promise;
};

module.exports = Transaction;
