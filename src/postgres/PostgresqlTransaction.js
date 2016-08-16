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
  var tableSchema =  schemaSet || process[fsSchemaCacheKey] || {};
  if( Object.keys(tableSchema).length == 0 ){
    throw Error("Schema "+databaseName+" not initialized");
  }
  this.databaseName = databaseName;
  this.databaseAddress = databaseAddress;
  this.databasePassword = databasePassword
  this.databasePort = databasePort;
  this.databaseUser = databaseUser;

  var dbConnectionString = dbConnection.getConnectionString.bind(dbConnection)()

  var TransactionClient = new PGClient.Client( dbConnectionString );
  var Client = new PostgresqlTransactionConnectionClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,TransactionClient,databaseProtocol)

  // Client.end();

  this.RolledBack = false;
  this.Closed = false;
  this.Begun = false
  this.Client = Client;
  this.Promise = function dbPromise(){ return Q.fcall(function(){ return; }); };

  _.forEach(tableSchema,function(value,tablename){
    var table = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,PostgresqlDatabaseSchemaCache);
    this[tablename] = table;
  });
  return this;
}

Transaction.prototype.GetClient = function(){
  return this.Client;
};

Transaction.prototype.GetDB = function(){
  return this.databaseName;
};


Transaction.prototype.Begin = function(){

  // var s = process.hrtime();
  var q = Q.defer();
  var self = this;

  var beginQuery = "BEGIN;";
  self.Client.query.bind(self.Client)(beginQuery,function(err,ret){
    // console.log("BEGIN",err,ret)
    if(err) {
      self.Rollback.bind(self)();
    } else {
      self.Begun = true;
      q.resolve();
    }
  });
  return q.promise;
};

Transaction.prototype.boundary = function(promisedStep){
  var self = this;
  var q = Q.defer();
  if( !( promisedStep instanceof Object ) && promisedStep.state !== 'pending' ) {
    self.Rollback.bind(self)().fail(function(err){
      q.reject(new Error("Boundary function was not a promise"));
    });
  } else {
    promisedStep.then(function(ret){
      q.resolve(ret);
    })
    .fail(function(err){
      self.Rollback.bind(self)(err).fail(function(err){
        q.reject(err);
      });
    }).done();
  }

  return q.promise;
};

Transaction.prototype.Commit = function(){
  // var s = process.hrtime();
  var self = this;
  var q = Q.defer();
  var commitQuery = "COMMIT;";
  self.Client.query(commitQuery,function(err,ret){
    self.Client.logQuery(s,commitQuery);
    if(err){
      self.Rollback(err);
    } else {
      self.Client.end();
      self.Closed = true;
      q.resolve(ret);
    }

  });
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
        self.Client.end();
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
      self.Client.end();
    } catch(e){
    }
    self.Closed = true;
    q.reject(null);
  }
  return q.promise;
};

module.exports = Transaction;
