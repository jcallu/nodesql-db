'use strict'
var utilityFunctions = require('./utils.js')
var _ = require('lodash')
function GenerateWhereObj(tableName,tableSchema,whereObjOrRawSQL,AND){
  var isRawSQL = typeof whereObjOrRawSQL === 'string' ? true : false;
  var where_CLAUSE = '';
  try {
    if( isRawSQL ){
     where_CLAUSE = '';
      var rawSQLStr = whereObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ").trim();
      // console.log("rawSQLStr ->",rawSQLStr)
      var isNotWhereAddOn = rawSQLStr.indexOf("WHERE TRUE") === -1;
      if ( AND ){
        if( rawSQLStr.indexOf("and") !== 0 ){
          where_CLAUSE += " AND " + whereObjOrRawSQL + " ";
        } else {
          where_CLAUSE += " " + whereObjOrRawSQL + " ";
        }
      } else {
        if(  rawSQLStr.indexOf("where true") >= 0 && isNotWhereAddOn ){
          where_CLAUSE += " " + whereObjOrRawSQL + " "; // Syntax Sugar query expected here "WHERE TRUE blah and blah"
          //debugLog("1st str whereParam =>",whereObjOrRawSQL);
        }
        else if (  rawSQLStr.indexOf("and") === -1 && rawSQLStr.indexOf('where') === -1 && rawSQLStr && isNotWhereAddOn ){
         where_CLAUSE += " WHERE TRUE AND "+ whereObjOrRawSQL + " "; //Where starts on first condition without "AND" insensitive case
          //debugLog("2nd str  whereParam =>",whereObjOrRawSQL);
        }
        else if ( rawSQLStr.indexOf("and") === 0 && isNotWhereAddOn  ) {
         where_CLAUSE += " WHERE TRUE "+whereObjOrRawSQL + " "; //Starts with "AND" insensitive case
          //debugLog("3rd str  whereParam =>",whereObjOrRawSQL);
        }
        else if (  rawSQLStr && isNotWhereAddOn ) {
         where_CLAUSE += " WHERE "+whereObjOrRawSQL+ " "; // ANY corner case not handled like passing white space
          //debugLog("4th str  whereParam =>",whereObjOrRawSQL);
        }
        else if ( !isNotWhereAddOn && rawSQLStr.indexOf("and") !== 0 ){
         where_CLAUSE += " AND " + whereObjOrRawSQL + " ";
          //debugLog("5th str  whereParam =>",whereObjOrRawSQL);
        }
        else {
         where_CLAUSE += " "+whereObjOrRawSQL+" ";
          //debugLog("6th str  whereParam =>",whereObjOrRawSQL);
        }
      }
    }
    else if ( whereObjOrRawSQL instanceof Object ){
      var schemaData = getTableSchemaDataMap(whereObjOrRawSQL,tableSchema)
      var keys = schemaData.columnNames;
      var paramsData = schemaData.paramsData;
      var columnNamesData = schemaData.columnDataMap
      where_CLAUSE = !AND ? " WHERE TRUE " : ''
      _.forEach(whereObjOrRawSQL, function(value,key){
        var paramData = columnNamesData[key] || {};
        switch(true){
          case ( key === 'raw_postgresql' || key === 'raw_sql' ):
            if( typeof value === 'string' ){
                where_CLAUSE += " " + value + " ";
            } else {
              console.error(new Error(tableName+ " "+key+ " not sql where clause ").stack)
            }
            break;
          case ( value instanceof Object && typeof value.condition === 'string' ):
            where_CLAUSE += " AND " + key + " "+ value.condition + " ";
            break;
          case _.isNull(value) || _.isUndefined(value):
            where_CLAUSE += " AND " + key + " IS NULL ";
            break;
          case ( paramData.js_type == 'number' ):
            if( !isNaN( parseInt(value) ) ){
              where_CLAUSE += " AND " + key + " = "+value+" ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'date'  ):
            if( (value instanceof Date) && value !== 'Invalid Date' ){
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::DATE ";
            } else if ( typeof value == 'string' && new Date(value) !== 'Invalid Date' ) {
              value = new Date(value)
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::DATE ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
          case ( paramData.js_type == 'time'  ):
            if( (value instanceof Date) && value !== 'Invalid Date' ){
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
            } else if ( typeof value == 'string' && new Date(value) !== 'Invalid Date' ) {
              value = new Date(value)
              where_CLAUSE += " AND "+ key +" = '"+ value.toISOString()+"'::TIMESTAMP ";
            } else {
              console.error(new Error(tableName+ " "+key+ " invalid "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'object'  ):
            if( (value instanceof Object || value instanceof Array) ){
              value = JSON.stringify(value)
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            } else {
              console.error(new Error(tableName+ " "+key+ " not an "+paramData.js_type+" "+value).stack)
            }
            break;
          case ( paramData.js_type == 'string'  ):
            if( typeof value === 'string' ){
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            }
            else if ( value != null && typeof value !== 'undefined' && typeof value.toString === 'function' && value.toString() ){
              where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value.toString()) + "' ";
            }
            else {
              console.error(new Error(tableName+ " "+key+ " not a "+paramData.js_type+" "+value).stack)
            }
            break;
          default:
            try {
              value = value.toString();
            } catch(e){
              console.error(e.stack);
              value = '';
            }
            where_CLAUSE += " AND " + key + " = '" + utilityFunctions.escapeApostrophes(value) + "' ";
            break;
        }
      });
    }
  } catch(e){
    console.error(e.stack);
  }
  this.abstractTableWhere = where_CLAUSE;
}

GenerateWhereObj.prototype.getWhere = function(){
  return this.abstractTableWhere;
};

module.exports = GenerateWhereObj
