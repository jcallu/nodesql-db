var FS_DATABASE_SCHEMA_SUFFIX = "_TABLES_SCHEMA_CACHE";
module.exports = function SchemaFilename(dbName,dbAddr,dbPort,dbUser,dbAdapter){
  var filename = dbAdapter+"_"+dbName+"_"+dbAddr.replace(/\./g,"-")+"_"+dbPort+"_"+dbUser + FS_DATABASE_SCHEMA_SUFFIX;
  filename = filename.toUpperCase()
  return filename;
}
