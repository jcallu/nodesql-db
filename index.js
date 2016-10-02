
var AbstractTable = require(__dirname+'/src/AbstractTable.js');
var ConnectionUrlParser = require(__dirname+'/src/ConnectionUrlParser.js');
var SchemaFilename = require(__dirname+'/src/SchemaFilename.js')
var util = require('util');
var Q = require('q')
var Promise = function dbPromise(){ var q = Q.defer(); q.resolve(undefined); return q.promise; };
/* Postgres Connection, Schema Caching */
var PostgresqlTransaction = require(__dirname+'/src/postgres/PostgresqlTransaction.js');
var PostgresqlConnectionClient = require(__dirname+'/src/postgres/PostgresqlConnectionClient.js');
var PostgresqlDatabaseSchemaCache = require(__dirname+'/src/postgres/PostgresqlDatabaseSchemaCache.js')
var MysqlConnectionClient = require(__dirname+"/src/mysql/MysqlConnectionClient")
var MysqlDatabaseSchemaCache = require(__dirname+'/src/mysql/MysqlDatabaseSchemaCache.js')
module.exports = function(connStrOrObj_){
  var connStrOrObj = typeof connStrOrObj_ == 'string' || connStrOrObj_ instanceof Object ? JSON.parse(JSON.stringify(connStrOrObj_)) : connStrOrObj_;
  var configParsed = typeof connStrOrObj === 'string' ? ConnectionUrlParser.parse(connStrOrObj) : connStrOrObj instanceof Object ? connStrOrObj : {};
  var config = configParsed instanceof Object ? configParsed : {}
  var databaseName =  config.database || undefined;
  var databaseAddress =   config.host || '127.0.0.1';
  var databasePassword = config.password ||  "";
  var databasePort = config.port || undefined;
  var databaseUser = config.user || undefined;
  var databaseProtocol = config.protocol || undefined;
  if( ! databaseName ) {
    var err = new Error("database name in connection string invalid -> "+databaseName+", "+JSON.stringify(connStrOrObj))
    throw err
  }
  if( ! databaseAddress ) {
    var err = new Error("database host in connection string invalid -> "+databaseAddress+", "+JSON.stringify(connStrOrObj))
    throw err
  }
  if( ! databasePort ) {
    var err = new Error("database port in connection string invalid -> "+databasePort+", "+JSON.stringify(connStrOrObj))
    throw err
  }
  if( ! databaseUser ) {
    var err = new Error("database user in connection string invalid -> "+databaseUser+", "+JSON.stringify(connStrOrObj))
    throw err
  }
  switch (databaseProtocol){
    case 'postgresql':
      var Client = new PostgresqlConnectionClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol)
      var schemaSet = PostgresqlDatabaseSchemaCache(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol);
      var schemaKey = SchemaFilename(databaseName,databaseAddress,databasePort,databaseUser,databaseProtocol)
      var schema = schemaSet instanceof Object && Object.keys(schemaSet).length > 0 ? schemaSet : process[schemaKey] instanceof Object && Object.keys(process[schemaKey]).length > 0 ? process[schemaKey] : {};
      for( var tablename in schema ){
        this[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,PostgresqlDatabaseSchemaCache);
      }
      function Transaction(){
        return PostgresqlTransaction.call(this,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,PostgresqlDatabaseSchemaCache,AbstractTable);
      }
      util.inherits(Transaction,PostgresqlTransaction);
      this.Transaction = Transaction
      this.Promise = Promise
      this.Client = Client
      break;
    case 'mysql':
    case 'memsql':
      var Client = new MysqlConnectionClient(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol);
      var schemaKey = SchemaFilename(databaseName,databaseAddress,databasePort,databaseUser,databaseProtocol)
      var schemaSet = MysqlDatabaseSchemaCache(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol);
      var schema = schemaSet instanceof Object && Object.keys(schemaSet).length > 0 ? schemaSet : process[schemaKey] instanceof Object && Object.keys(process[schemaKey]).length > 0 ? process[schemaKey] : {};
      for( var tablename in schema ){
        this[tablename] = new AbstractTable(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,MysqlDatabaseSchemaCache);
      }
      this.Promise = Promise
      this.Client = Client
      break;
    default:
      break;
  }
  return this;
};
