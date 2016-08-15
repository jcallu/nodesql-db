var config = {}

var DEBUG = process.env.NODESQLDB_DEBUG == 'true';
config.NODESQLDB_POOL_SIZE = process.env.NODESQLDB_POOL_SIZE || process.env.PG_POOL_SIZE || 10;
config.NODESQLDB_NODE_ENV = process.env.NODE_ENV || 'development';
config.NODESQLDB_REAP_INTERVAL_MILLIS = process.env.NODESQLDB_REAP_INTERVAL_MILLIS || process.env.REAP_INTERVAL_MILLIS || 1000;
config.NODESQLDB_POOL_IDLE_TIMEOUT = process.env.NODESQLDB_POOL_IDLE_TIMEOUT || process.env.POOL_IDLE_TIMEOUT || 5e3;
config.NODESQLDB_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.NODESQLDB_DB_LOG_SLOW || false;
config.NODESQLDB_DB_LOG = process.env.DB_LOG || process.env.NODESQLDB_DB_LOG || false;
config.NODESQLDB_DB_LOG_CONNECTIONS = process.env.NODESQLDB_DB_LOG_CONNECTIONS || false;
switch(DEBUG){
  case 'debug':
    config.NODESQLDB_DB_LOG_SLOW = process.env.DB_LOG_SLOW || process.env.NODESQLDB_DB_LOG_SLOW || 'true';
    config.NODESQLDB_DB_LOG = process.env.DB_LOG || process.env.NODESQLDB_DB_LOG || 'true'
    config.NODESQLDB_DB_LOG_CONNECTIONS = process.env.NODESQLDB_DB_LOG_CONNECTIONS || 'true';
    break;
  default:
    break;
}
config.LOG_CONNECTIONS = config.NODESQLDB_DB_LOG_CONNECTIONS != 'false';
config.PG_POOL_SIZE = config.NODESQLDB_POOL_SIZE || 10;
config.MYSQL_POOL_SIZE = config.NODESQLDB_POOL_SIZE || 10;
config.NODE_ENV = config.NODESQLDB_NODE_ENV == 'true';
config.DB_LOG_ON = config.NODESQLDB_DB_LOG == 'true'
config.DB_LOG_SLOW_QUERIES_ON = config.NODESQLDB_DB_LOG_SLOW == 'true'
config.IS_DEV_ENV =  config.NODE_ENV === 'development'
config.TIMEZONE = process.env.NODESQLDB_TIMEZONE ? process.env.NODESQLDB_TIMEZONE : "America/Los_Angeles"
module.exports = config;
