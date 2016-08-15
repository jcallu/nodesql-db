var FS_DATABASE_SCHEMA_SUFFIX = "_TABLES_SCHEMA_CACHE";
module.exports = function SchemaFilename(dbName,dbAddr,dbPort,dbUser,dbProtocol){
  if( typeof dbProtocol !== 'string' ){
    var e = new Error("unknown database protocol -> "+dbProtocol)
    throw e;
  }
  var filename = dbProtocol+"_"+dbName+"_"+dbAddr.replace(/\./g,"-")+"_"+dbPort+"_"+dbUser + FS_DATABASE_SCHEMA_SUFFIX;
  filename = filename.toUpperCase()
  return filename;
}
