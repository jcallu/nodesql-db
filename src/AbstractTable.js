'use strict'
var NODESQLDB_HOME = __dirname
var _ = require('lodash');
var async = require('async');
var Q = require('q');
var fs = require('fs');
var config = require("./config");
var inflection = require('inflection');
var SchemaFilename = require('./SchemaFilename.js')
var NODE_ENV = config.NODE_ENV;
var DB_LOG = config.DB_LOG_ON;
var utilityFunctions = require('./utils.js')
var debugLog = NODE_ENV !== 'production' && DB_LOG ? utilityFunctions.console.asyncLog : function(){};
var EXCEPTIONS = require('./Exceptions.js')
var GenerateWhereObj = require('./GenerateWhereObj.js')
var getTableSchemaDataMap = require('./getTableSchemaDataMap.js')
var getDateForZone = utilityFunctions.getDateForZone


var AbstractTable = function(tablename,databaseName,databaseAddress,databasePassword,databasePort,databaseUser,Client,databaseProtocol,setSqlSchemaCache){
  this.abstractTableDb = databaseName;
  this.databaseProtocol = databaseProtocol
  var DB = this.abstractTableDb.toUpperCase();
  var CACHE_KEY = SchemaFilename(databaseName,databaseAddress,databasePort,databaseUser,databaseProtocol);
  this.abstractTableDB = DB;
  this.databaseName = databaseName;
  this.databaseAddress = databaseAddress;
  this.databasePassword  = databasePassword;
  this.databasePort = databasePort;
  this.databaseUser = databaseUser;
  if(  typeof Client !== 'object' ) {
    var err = new Error("Abstract Table client property in constructor not an object -> "+Client+"")
    throw err;
  }
  Client.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol)
  this.Client = Client
  setSqlSchemaCache(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,this.Client,databaseProtocol);
  // console.log("process[CACHE_KEY]",process[CACHE_KEY])
  this.abstractTableTableSchema = [];
  try {
    this.abstractTableTableSchema = process[CACHE_KEY][tablename] || [];
  } catch(e){
    console.error(CACHE_KEY,e.stack)
  }
  if( this.abstractTableTableSchema.length === 0 ){
    console.error("this.abstractTableTableSchema was empty",this.abstractTableTableSchema)
  }
  this.abstractTableTableName = tablename || undefined;
  this.abstractTablePrimaryKey = (_(this.abstractTableTableSchema).chain().filter(function(s){ return s.is_primary_key }).compact().head().value() || {}).column_name || null
  this.initializeTable.bind(this)();
  this.exceptions = EXCEPTIONS[this.databaseProtocol]

  var escapeString = ( this.databaseProtocol === 'mysql' || this.databaseProtocol === 'memsql' ) ? utilityFunctions.escapeMySQLString : utilityFunctions.escapeApostrophes;
  if( typeof escapeString === 'undefined' ){
    var e = new Error("escapeString")
    console.error(e.stack)
  }
  this.escapeString = escapeString.bind(this)
  createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(this.abstractTableTableSchema);
};

AbstractTable.prototype.setConnectionParams = function(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol){
  this.Client.setConnectionParams(databaseName,databaseAddress,databasePassword,databasePort,databaseUser,databaseProtocol)
}

function createDynamicSearchByPrimaryKeyOrForeignKeyIdPrototypes(schema){

  var idColumns = _.compact(_.map(schema,function(obj){
    var endsIn_id =  obj.column_name.lastIndexOf('_id') === (obj.column_name.length-3) ;
    if( endsIn_id  )
      return { functionId: inflection.camelize(obj.column_name), colName: obj.column_name };
    return null;
  }));
  if( idColumns.length=== 0 ){
    return;
  }
  _.each(idColumns,function(camelizedColObj){
    //findAllByAnyTableId
    AbstractTable.prototype['findBy'+camelizedColObj.functionId] = function(idIntegerParam){
      this.initializeTable();

      var camelizedColName = camelizedColObj.colName;

      if( ! isNaN( parseInt(idIntegerParam) ) ) { this.error = new Error('findBy'+camelizedColObj.functionId + " first and only parameter must be a "+camelizedColName+" integer and it was => " + typeof idIntegerParam );
      }

      this.primaryKeyLkup = camelizedColName && camelizedColName === this.abstractTablePrimaryKey ? true : false;
      if( this.abstractTableQuery.trim().indexOf('select') === -1 )
        this.abstractTableQuery = "SELECT "+this.abstractTableTableName+".* FROM "+ this.abstractTableTableName + " " + this.abstractTableTableName;
      return this.where(camelizedColName+"="+idIntegerParam);
    };

    //getIds return 1 record if calling getAll<PrimaryKeyId>s without whereParams
    AbstractTable.prototype['getAll'+camelizedColObj.functionId+'s'] = function(whereParams){
      this.initializeTable();
      var camelizedColName = camelizedColObj.colName;
      this.primaryKeyLkup = _.isUndefined(whereParams) && camelizedColName === this.abstractTablePrimaryKey ? true : false;
      var DISTINCT = !_.isUndefined(whereParams) ? 'DISTINCT' : '';
      if(  this.primaryKeyLkup  ){
        DISTINCT = '';
      }
      this.abstractTableQuery = "SELECT "+DISTINCT+" "+this.abstractTableTableName+"."+camelizedColName+" FROM "+ this.abstractTableTableName + " " + this.abstractTableTableName;
      if( !_.isUndefined(whereParams) ){
        this.where(whereParams);
      }
      //console.log("this.abstractTableQuery",this.abstractTableQuery);
      return this;
    };
  });
}

AbstractTable.prototype.rawsql =  function (rawSql){
  this.initializeTable();
  this.abstractTableQuery = rawSql;
  return this;
};


AbstractTable.prototype.select =  function (selectParams){
  this.initializeTable();
  this.selecting = true;
  this.abstractTableWhere = '';
  var querySelect = '';
  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = selectParams.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if( rawSQLStr.indexOf('select') === 0 )
      querySelect = " "+rawSQLStr+" "; // select * from this.abstractTableTableName expected;
    else if ( rawSQLStr.indexOf('select') !== 0 )
      querySelect = "SELECT "+selectParams+" FROM "+this.abstractTableTableName;
    else
      querySelect = updateObjOrRawSQL;
  } else {
    if( ! ( selectParams instanceof Array ) ) selectParams = [];
    var tableName = this.abstractTableTableName;
    selectParams = _.map(selectParams,function(colName){
      if( colName.indexOf('.') > -1 ) return colName;
      else return tableName + "."+colName;
    });
    if( selectParams.length === 0 ) selectParams = ['*'];
    querySelect = "SELECT "+selectParams.join(' , ')+" FROM "+ this.abstractTableTableName+" "+this.abstractTableTableName + " ";
  }
  this.abstractTableQuery = querySelect;
  return this;
};

AbstractTable.prototype.selectAll = function(){
  this.initializeTable();
  return this.select("*");
};

AbstractTable.prototype.selectWhere = function(selectWhereParams,whereObjOrRawSQL){
  return this.select(selectWhereParams).where(whereObjOrRawSQL);
};

AbstractTable.prototype.externalJoinHelper = function(obj,schema){
  var self = this;
  var schemaData = getTableSchemaDataMap(obj,schema)
  var keys = schemaData.columnNames;
  var paramsData = schemaData.paramsData;
  var columnNamesData = schemaData.columnDataMap


  var onCondition = "";

  _.forEach(obj,function(value,key){
    var paramData = columnNamesData[key] || {}
    if( value instanceof Object && typeof value.condition === 'string' ){
      onCondition += " AND "+key+" "+value.condition+" ";
      return;
    }
    if( typeof value === 'boolean' && self.databaseProtocol == 'postgresql' ){
      onCondition += " AND "+key+" IS "+value+"  ";
      return;
    }
    if( typeof value === 'boolean' && ( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql' ) ){
      var value_uint = value ? "1" : "0";
      onCondition += " AND "+key+" = "+value_uint+"  ";
      return;
    }
    if( typeof value === 'number' ){
      onCondition += "  AND "+key+" = "+value+" ";
      return;
    }
    onCondition += " AND "+key + " = " + value + " ";
  });
  return onCondition;
}




AbstractTable.prototype.join = function(tablesToJoinOnObjs){
  var self = this;
  var DB = self.abstractTableDB;
  var CACHE_KEY = SchemaFilename(self.databaseName,self.databaseAddress,self.databasePort,self.databaseUser,self.databaseProtocol);
  var rawSql = typeof tablesToJoinOnObjs === 'string' ? tablesToJoinOnObjs : null;
  var joinSQL = '';
  if( rawSql){
    joinSQL = " " + rawSql + " ";
  } else {
    var tables = tablesToJoinOnObjs;
    if( !( tables instanceof Object ) ){
      tables = {};
    }
    var thisTableName = this.abstractTableTableName;
    _.forEach(tables,function(obj,tablename){
      var schema = process[CACHE_KEY][tablename] || [];

      if( schema.length == 0 ){
          console.log("schema",schema,tablename)
      }


      obj.on = obj.on instanceof Array ? obj.on : [];
      var tableName = tablename;
      var alias = obj.as || tablename;
      var onArray = _(obj.on).chain().map(function(joinOnColumnsOrObj){
        if( typeof joinOnColumnsOrObj === 'string' && _(schema).chain().filter(function(o){ return joinOnColumnsOrObj.indexOf(o.column_name) === joinOnColumnsOrObj.replace(o.column_name,"").length  }).compact().head().value() instanceof Object ){
          return " AND "+alias+"."+joinOnColumnsOrObj+" = " +thisTableName+"."+joinOnColumnsOrObj+" ";
        }
        if( joinOnColumnsOrObj instanceof Object && _.keys(joinOnColumnsOrObj).length >= 1 ){
          return self.externalJoinHelper.bind(self)(joinOnColumnsOrObj,schema);
        }
        return null;
      }).compact().value();
      // console.log("on Array",onArray)
      var onTrue = '';
      if( onArray.length === 0 ) onArray = ['false'];
      if( onArray.length > 0 ) onTrue = 'TRUE ';
      joinSQL = " "+( obj.type||'INNER' ).toUpperCase() +" "+"JOIN "+ tableName + " " + alias + " ON "+onTrue+" " + onArray.join(' ') + " ";
    });
  }

  this.abstractTableQuery += joinSQL;
  return this;
};




AbstractTable.prototype.insert = function(optionalParams){
  this.initializeTable();
  this.inserting = true;
  this.abstractTableQuery = "INSERT INTO " + this.abstractTableTableName + " ";
  if( optionalParams instanceof Object ){
    this.values(optionalParams);
  }
  return this;
};



AbstractTable.prototype.getTableSchemaDataMap = getTableSchemaDataMap

AbstractTable.prototype.values = function(params){
  var self = this;

  var count = 1;

  var schemaData = self.getTableSchemaDataMap.bind(self)(params)
  var keys = schemaData.columnNames;
  var paramsData = schemaData.paramsData;
  var columnNamesData = schemaData.columnDataMap


  if( keys.length === 0 )  {  this.error = new Error("No insert values passed"); return this; }
  var queryParams = "";
  var columnNames = [];
  var selectValuesAs = [];

  _.forEach(params, function(value,key){
    try {
      if( !key ) return;
      var paramData = columnNamesData[key] || {}
      var ofTypeColumn = '';
      var fieldValue = '';

      switch(true){
        case ( _.isNull(value) || _.isUndefined(value) ):
          ofTypeColumn = 'null';
          break;
        case ( value instanceof Object && typeof value.condition === 'string' ):
          ofTypeColumn = 'pgsql_condition';
          break;
        case ( value instanceof Object && value.pgsql_function instanceof Object ):
          var functionToRun = _.keys(value.pgsql_function)[0]  || "THROW_AN_ERROR";
          var pgFunctionInputs = [];
          if (!functionToRun && typeof functionToRun !== String) {
            console.error("functionToRun in Values is not a String or is undefined");
          }
          var values = _.values(value.pgsql_function)[0] || [];
          if (!values && (typeof functionToRun !== Array || values.length === 0)) {
            console.error("values in Values is not an Array or is length of zero");
          }
          pgFunctionInputs = _.map(values,function(val){
            if ( typeof val === 'string' )
              return "'" + self.escapeString(val) + "'";
            else
              return val;
          });
          ofTypeColumn = 'pgsql_function';
          value = functionToRun + "("+pgFunctionInputs.join(',')+") ";
          break;
        case  ( paramData.js_type === 'object' && (value instanceof Object || value instanceof Array ) ):
          ofTypeColumn = 'object';
          value = self.escapeString( JSON.stringify(value) );
          break;
        case ( paramData.js_type === 'time' && ( new Date(value) instanceof Date ) ):
          value = getDateForZone(value)
          ofTypeColumn = 'time';
          break;
        case ( paramData.js_type === 'date' && ( new Date(value) instanceof Date) ):
          value = getDateForZone(value)
          ofTypeColumn = 'date';
          break;
        case ( paramData.js_type === 'number' && !isNaN(parseInt(value)) ):
          ofTypeColumn = 'num' ;
          break;
        case ( paramData.js_type === 'boolean'   || (paramData.js_type === 'number' && typeof value === 'boolean') ):
          ofTypeColumn = 'bool';
          break;
        case ( paramData.js_type === 'string' ):
          ofTypeColumn = 'text';
          if( value !== null && typeof value !== 'undefined' && typeof value != 'string' && value.toString() ){
            value = value.toString()
          }
          value = self.escapeString(value);
          break;
        default:
          try {
            value = value.toString();
            value = self.escapeString(value);
            ofTypeColumn = 'text';
          } catch(e){
            console.error(e.stack)
            ofTypeColumn = 'null';
          }
          break;
      }
      //debugLog("INSERT VALUES => type="+ofTypeColumn+" , value="+value+", column_name="+key);
      switch(ofTypeColumn){
        case 'date':
          var cast = "::DATE";
          if( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql'){
            cast = "";
          }
          columnNames.push(key);
          fieldValue = value
          selectValuesAs.push(" '"+fieldValue+"'"+cast+" as "+key+" ");
          queryParams += " AND " + key + " = " + fieldValue + ""+cast+" ";
          break;
        case 'time':
          var cast = "::TIMESTAMP";
          if( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql'){
            cast = "";
          }
          columnNames.push(key);
          fieldValue = value
          selectValuesAs.push(" '"+fieldValue+"'"+cast+" as "+key+" ");
          queryParams += " AND " + key + " = " + fieldValue + ""+cast+" ";
          break;
        case 'bool':
          if ( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql' ) {
            columnNames.push(key);
            fieldValue = value ? "1" : "0";
            selectValuesAs.push(" "+fieldValue+" as " + key+" ");
            queryParams += " AND " + key + " = " + fieldValue + " ";
          }
          else {
            columnNames.push(key);
            fieldValue = value ? "TRUE" : "FALSE";
            selectValuesAs.push(" "+fieldValue+" as " + key+" ");
            queryParams += " AND " + key + " IS " + fieldValue + " ";
          }
          break;
        case 'num':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" "+fieldValue+" as " + key+" ");
          queryParams += " AND " + key + " = " + fieldValue + " ";
          break;
        case 'null':
          columnNames.push(key);
          fieldValue = null;
          selectValuesAs.push(" null as " + key+" ");
          queryParams += " AND " + key + " IS NULL ";
          break;
        case 'object':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" '"+fieldValue + "' as " + key+ " ");
          queryParams += " AND " + key + "::TEXT = '" + fieldValue + "'::TEXT ";
          break;
        case 'text':
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" '"+fieldValue + "' as " + key+ " ");
          queryParams += " AND " + key + " = '" + fieldValue + "' ";
          break;
        case 'pgsql_function':
          // actual switch
          columnNames.push(key);
          fieldValue = value;
          selectValuesAs.push(" "+fieldValue + " as " + key+ " ");
          queryParams += " AND " + key + " = " + fieldValue + " ";
          break;
        case 'pgsql_condition':
          columnNames.push(key);
          fieldValue = value.condition;
          var comparisonOperator = "="
          if(typeof value.operator === 'string'){
            comparisonOperator = value.operator
          }
          selectValuesAs.push(" "+fieldValue + " as " + key+ " ");
          queryParams += " AND " + key + " " +comparisonOperator+ " " + fieldValue + " ";
          break;
        default:
          console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid entry ").stack)
          break;
      }
    } catch(e){
      e.message = e.message+"\nabstractdb::values value => "+ value+", key => "+key;
      console.error(e.stack);
      this.error = e.stack;
    }
  });
  this.abstractTableQuery += " (" + columnNames.join(",") + ") SELECT " + selectValuesAs.join(', ') + " ";
  this.abstractTableWhereUniqueParams = queryParams;
  return this;
};

AbstractTable.prototype.unique = function(params){

  var whereUnique = " WHERE NOT EXISTS ( SELECT 1 FROM "+  this.abstractTableTableName + " WHERE true ";
  whereUnique += this.abstractTableWhereUniqueParams;
  whereUnique += " ) ";
  this.abstractTableQuery += whereUnique;

  return this;
};


AbstractTable.prototype.insertUnique = function(params){
  this.insert();
  this.values(params);
  this.unique();
  return this;
};

AbstractTable.prototype.update = function(updateObjOrRawSQL){

  this.initializeTable();
  this.updating = true;
  this.abstractTableQuery = '';
  var isRawSQL = typeof updateObjOrRawSQL === 'string' ? true : false;
  if( isRawSQL ){
    this.set(updateObjOrRawSQL);
  }
  this.abstractTableQuery  = 'UPDATE '+this.abstractTableTableName + ' ';

  if( updateObjOrRawSQL instanceof Object ) {
    this.set(updateObjOrRawSQL);
  }

  return this;
};


AbstractTable.prototype.set = function(updateObjOrRawSQL){
  var self = this;
  var isRawSQL = typeof selectParams === 'string' ? true : false;
  if( isRawSQL ){
    var rawSQLStr = updateObjOrRawSQL.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    if ( rawSQLStr.indexOf('set') !== 0 )
      this.abstractTableQuery += "SET "+updateObjOrRawSQL+" ";
    else
      this.abstractTableQuery += updateObjOrRawSQL.trim();
    return this;
  } else if ( updateObjOrRawSQL instanceof Object ){
    var sql = "SET ";
    var schemaData = this.getTableSchemaDataMap.bind(this)(updateObjOrRawSQL)
    var keys = schemaData.columnNames;
    var paramsData = schemaData.paramsData;
    var columnNamesData = schemaData.columnDataMap

    _.forEach(updateObjOrRawSQL,function(value,key){

      var paramData = columnNamesData[key] || {}
      /** LEAVE THE SPACE COMMA SPACE IN THE SQL+= CONCATENATION!!! **/
      switch (true) {
        case ( _.isNull(value) || _.isUndefined(value) ):
          sql +=  key + " = NULL " + " , ";
          break;
        case (  value instanceof Object && typeof value.condition === 'string' ):
          sql += key + " "+ value.condition + " , ";
          break;
        case (  paramData.js_type === 'date'   ):
          var cast = "::DATE";
          if( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql'){
            cast = "";
          }
          if(value instanceof Date && value != "Invalid Date"){
            var dateValue = getDateForZone(value)
            sql += key + " = '"+ dateValue + "'"+cast+" , ";
          }
          else if (typeof value === 'string' && new Date(value) != "Invalid Date" ){
            var dateValue = getDateForZone(value)
            sql += key + " = '"+ dateValue + "'"+cast+" , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case (  paramData.js_type === 'time'   ):
          var cast = "::TIMESTAMP";
          if( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql'){
            cast = "";
          }
          if(value instanceof Date && value != "Invalid Date"){
            var dateValue = getDateForZone(value)
            sql += key + " = '"+ dateValue+ "'"+cast+" , ";
          }
          else if (typeof value === 'string' && new Date(value) != "Invalid Date" ){
            var dateValue = getDateForZone(value)
            sql += key + " = '"+ dateValue + "'"+cast+" , ";
          }
          else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case( paramData.js_type === 'boolean' ):
          if( typeof value === 'boolean' && ( self.databaseProtocol == 'memsql' || self.databaseProtocol == 'mysql' ) ) {
            var value_uint = value ? "1" : "0"
            sql += key + " = " + value_uint + " , ";
          }
          else if ( typeof value === 'boolean' && ( self.databaseProtocol == 'postgresql' ) ) {
            sql += key + " = " + value + " , ";
          }
          else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case ( paramData.js_type === 'number' ):
          if( value === true ) value = 1;
          if( value === false ) value = 0;
          if( ! isNaN( parseInt(value) ) ){
              sql += key + " = "+ value + " , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case ( paramData.js_type === 'object' ):
          if( (value instanceof Object || value instanceof Array) ){
              value = JSON.stringify(value)
              value = self.escapeString(value)
              sql += key + " = " + " '"+value+"' " + " , ";
          }
          else if( typeof value === 'string' ){
            value = self.escapeString(value)
            sql += key + " = " + " '"+value+"' " + " , ";
          } else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value).stack)
          }
          break;
        case ( paramData.js_type === 'string' ):
          if( typeof value === 'string' ){
              value = self.escapeString(value)
              sql += key + " = " + " '"+value+"' " + " , ";
          }
          else if ( value != null && typeof value !== 'undefined' && typeof value.toString == 'function' && value.toString() ) {
            value = self.escapeString(value.toString())
            sql += key + " = " + " '"+value+"' " + " , ";
          }
          else {
            console.error(new Error(self.abstractTableTableName+ " "+key+ " invalid  " + paramData.js_type + " " + value + " toString() -> "+ value.toString() ).stack)
          }
          break;
        default:
          try {
            value = value.toString();
          } catch(e) {
            console.error(e.stack);
            value = '';
          }
          sql += key + " = " + " '"+self.escapeString(value)+"' " + " , ";
          break;
      }
    });
    sql = sql.slice(0, sql.lastIndexOf(" , "));
    this.abstractTableQuery += sql;

  }

  return this;
};


AbstractTable.prototype.deleteFrom = function(){
  this.initializeTable();
  this.deleting = true;
  //console.log("client");
  this.abstractTableQuery = "DELETE FROM "+this.abstractTableTableName+ " WHERE FALSE";
  return this;
};

AbstractTable.prototype.and = function(whereObjOrRawSQL){
  var self = this;
  var generatedWhereClause = new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereObjOrRawSQL,true,self.databaseProtocol);
  var whereQueryGenerated = generatedWhereClause.getWhere();
  //console.log("whereQueryGenerated",whereQueryGenerated)
  this.abstractTableWhere += ' '+whereQueryGenerated+' '
  this.abstractTableQuery += ' '+whereQueryGenerated+' ';
  return this;
}

AbstractTable.prototype.where = function(whereObjOrRawSQL){
  var self = this;
  var selectTmp = this.abstractTableQuery.toLowerCase().trim().replace(/(\s{1,})/gm," ");
  if( !selectTmp && selectTmp.indexOf('select') === -1 && selectTmp.indexOf('update '+this.abstractTableTableName) === -1  && selectTmp.indexOf('delete from') === -1  ) {
    this.abstractTableQuery = "SELECT * FROM "+this.abstractTableTableName + " "+this.abstractTableTableName+ " ";
  }
  if( ! whereObjOrRawSQL ){
    this.primaryKeyLkup = false;
  }
  var generatedWhereClause = new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereObjOrRawSQL,undefined,self.databaseProtocol);
  var whereQueryGenerated = generatedWhereClause.getWhere();
  //console.log("whereQueryGenerated",whereQueryGenerated);
  if( typeof whereQueryGenerated === 'string' && whereQueryGenerated.length > 7 && this.deleting ){ // unlocking delete safety
    this.abstractTableQuery = this.abstractTableQuery.replace("DELETE FROM "+this.abstractTableTableName+" WHERE FALSE","DELETE FROM "+this.abstractTableTableName+" ");
  }
  this.abstractTableWhere = whereQueryGenerated;
  //console.log("where ->",whereQueryGenerated,'from ->',whereObjOrRawSQL);
  this.abstractTableQuery += (whereQueryGenerated||'');
  //this.optimizeQuery();
  return this;
};


AbstractTable.prototype.orderBy = function(arrOrRawOrderBy){

  var isRawSQL = typeof arrOrRawOrderBy === 'string' ? true : false;
  var orderByStr= '';

  if( isRawSQL ) {
    var rawSQLStr = arrOrRawOrderBy.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    orderByStr += arrOrRawOrderBy;
  } else {
    arrOrRawOrderBy = arrOrRawOrderBy instanceof Array ? arrOrRawOrderBy : [];
    _.each(arrOrRawOrderBy,function(ele){
      if ( typeof ele === 'string' ){
        orderByStr += " "+ele+" asc";
      } else if( ele instanceof Object ){
        orderByStr += " "+ele+" asc";
      }
    });
    if( arrOrRawOrderBy.length === 0 )
      orderByStr += " 1 ";
  }

  this.abstractTableQuery += " ORDER BY ";
  orderByStr = orderByStr ? orderByStr : '1'; // default order by first param;
  this.abstractTableQuery += " " + orderByStr + " ";
  return this;
};

/*
 *                                                     // optional params          //optional param
 *  @usage .AndNotExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'})
 */
AbstractTable.prototype.AndNotExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL){
  return this.AndExists(tableNameExists,onColumnIds,whereExistsObjOrSQL,false);
}

/*
 *                                                     // optional params          //optional param
 *  @usage .AndExists('clean_title',null,{source_name:'common-sense',source_key:'avatar'},true)
 */
AbstractTable.prototype.AndExists = function(tableNameExists,onColumnIds,whereExistsObjOrSQL,NOT){
  var self = this;
  NOT = typeof NOT === 'boolean' && !NOT ? " NOT " : "";
  onColumnIds = _.isNull(onColumnIds) || _.isNull(onColumnIds) ? [] : onColumnIds;
  onColumnIds = onColumnIds instanceof Array ? onColumnIds : [onColumnIds];

  if( typeof whereExistsObjOrSQL === 'boolean' ) {
    NOT = whereExistsObjOrSQL ? '' : ' NOT ';
    whereExistsObjOrSQL = null;

  }
  var getWhereGenerateWhereObj = "";
  if( whereExistsObjOrSQL ){
    var genWhereObj = new GenerateWhereObj(self.abstractTableTableName,self.abstractTableTableSchema,whereExistsObjOrSQL,undefined,self.databaseProtocol)
    getWhereGenerateWhereObj = genWhereObj.getWhere()
  }
  var whereQuery = whereExistsObjOrSQL ? getWhereGenerateWhereObj : ' WHERE TRUE ';
  var mainTableName = this.abstractTableTableName;
  var whereOnColumnIdsAnd = onColumnIds.length > 0 ? " AND " : " ";
  whereQuery =  "AND "+NOT+" EXISTS (select 1 from "+tableNameExists+ " "+tableNameExists+" "+
                whereQuery+whereOnColumnIdsAnd+
                _.map(onColumnIds,function(colName){
                  return " "+tableNameExists+"."+colName+" = "+mainTableName+"."+colName + " ";
                }).join(" AND ") +
                " )";
  this.abstractTableWhere += " "+whereQuery+" ";
  this.abstractTableQuery += " "+whereQuery+" ";
  return this;
};

AbstractTable.prototype.groupBy = function(textOrObj){
  var isRawSQL = typeof textOrObj === 'string' ? true : false;
  var orderByStr = '';
  if( isRawSQL ){
    var rawSQLStr = textOrObj.toLowerCase().trim().replace(/(\s{1,})/gm," ");
    orderByStr += "GROUP BY "+textOrObj;
  }
  else if ( textOrObj instanceof Array ){
    orderByStr += "GROUP BY "+textOrObj.join(', ');
  }
  else {}
  this.abstractTableQuery += " " + orderByStr + " ";
  return this;
};

AbstractTable.prototype.having = function(i){
  this.abstractTableQuery += " HAVING "+i+" ";
  return this;
};

AbstractTable.prototype.offset = function(i){

  this.abstractTableQuery += " OFFSET "+(parseInt(i)||0)+" ";
  return this;
};


AbstractTable.prototype.limit = function(i){

  this.abstractTableQuery += " LIMIT "+(parseInt(i)||'ALL')+" ";
  return this;
};


AbstractTable.prototype.optimizeQuery = function(){
  if( this.primaryKeyLkup ){
    this.limit(1);
  }
  return this;
};

AbstractTable.prototype.dbQuery = function(query,callback){
  var self = this;
  self.setConnectionParams.bind(self)(self.databaseName,self.databaseAddress,self.databasePassword,self.databasePort,self.databaseUser,self.databaseProtocol)
  self.Client.query.bind(self.Client)(query,[],function(err,results){
    if(err) return callback(err,null);
    var data = [];
    try {
      data = results.rows || []
    } catch(e){

    }
    callback(null,data);
  });

};

AbstractTable.prototype.dbQuerySync = function(query){
  var self = this;
  self.setConnectionParams(self.databaseName,self.databaseAddress,self.databasePassword,self.databasePort,self.databaseUser,self.databaseProtocol)
  var ret = self.Client.querySync.bind( self.Client )(query);
  ret.failed = ret.error ? true : false;
  ret.Rows = function () {  return ret.rows; };
  ret.Error = function () {  return ret.error; };
  return ret;
};

AbstractTable.prototype.run = function(callback){
  var self = this;


  self.finalizeQuery();

  var QueryToRun = self.abstractTableQuery + self.returnIds;

  var IS_PROMISED = typeof callback !== 'function';
  var q;
  if( IS_PROMISED ) q = Q.defer();


  callback = typeof callback === 'function' ? callback : function(){};

  this.initializeTable();


  if( self.error ){
    if(IS_PROMISED ) q.reject(self.error);
    else callback(self.error,null);
    self.initializeTable();
  }
  else {
    self.dbQuery(QueryToRun,function(err,rows){
      if(err ) {
        //console.error("Error query =>",Query);
        if(IS_PROMISED ) q.reject(err);
        else callback(err,null);
      }
      else {
        if(IS_PROMISED ) q.resolve(rows);
        else callback(null,rows);
      }
      self.initializeTable();
    });
  }
  if( IS_PROMISED) return q.promise;
};




AbstractTable.prototype.runSync = function(callback){

  var self = this;
  self.finalizeQuery.bind(self)();
  var QueryToRun = self.abstractTableQuery + self.returnIds;
  var ret = self.dbQuerySync.bind(self)(QueryToRun);
  callback = typeof callback === 'function' ? callback : function(){};
  this.initializeTable();// unbind



  var retObj = {};
  var rows = ret.Rows()
  var error = ret.Error();



  retObj.Rows = function(){ return rows; }
  retObj.Error = function(){ return error; }
  retObj.results = { error: error, rows: rows };

  callback(error,rows);



  return retObj;
};

AbstractTable.prototype.finalizeQuery = function(){


  if( this.databaseProtocol != 'postgresql' ) return this;

  var querySet = this.abstractTableQuery;
  var querySetTrimmed = querySet.trim();
  var queryFinalized = querySet.toLowerCase().trim().replace(/\s{1,}/gmi," ").trim();
  // console.log("queryFinalized",queryFinalized);

  if ( queryFinalized.indexOf("insert into "+this.abstractTableTableName) === 0  || queryFinalized.indexOf("update "+this.abstractTableTableName ) === 0 || queryFinalized.indexOf("delete from "+this.abstractTableTableName) ===0 ) {

    if( querySetTrimmed.lastIndexOf(";") == querySetTrimmed.length-1 && querySetTrimmed.length > 0  ){
      var query = this.abstractTableQuery.trim();
      this.abstractTableQuery = query.substring(0,query.length-1).trim()
    }
    // console.log("\nthis.abstractTableTableName",this.abstractTableTableName)
    if ( ! _.isNull ( this.abstractTablePrimaryKey ) ) {
      this.returnIds = " RETURNING " + this.abstractTablePrimaryKey;
    }
    else if ( queryFinalized.indexOf("returning ") == -1 ) {
      this.returnIds = " RETURNING * ";
    }
  }
  return this;
};

AbstractTable.prototype.printQuery = function(ovrLog){
  var self = this;
  self.finalizeQuery.bind(self)();
  var QueryToPrint = self.abstractTableQuery + self.returnIds;
  var queryLog = "\nquery => " + QueryToPrint + "\n";
  if( ! ovrLog ){
    debugLog(queryLog);
    return this;
  }
  console.log(queryLog);
  return this;
};

AbstractTable.prototype.initializeTable = function(callback){
  callback = typeof callback === 'function' ? callback : function(){};
  this.abstractTableQuery = '';
  this.primaryKeyLkup = false;
  this.abstractTableWhere = '';
  this.abstractTableWhereUniqueParams = ''
  this.deleting = false;
  this.inserting = false;
  this.updating = false;
  this.returnIds = '';
  this.upserting = false;
  this.utilReady = false;
  this.error = null;
  callback();
  return this;
};


AbstractTable.prototype.util = function(){
  var self = this;
  this.initializeTable.bind(this)();
  self.initializeTable.bind(self)();
  self.utilReady = true;
  return self;
};

AbstractTable.prototype.upsert = function(setParams,whereParams,callback){
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  var q = Q.defer();
  var err = null;
  if( !self.utilReady ){
    err = new Error("Need to call util() before accessing utility functions")
  }
  else if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) {
    err = new Error("Can only insert or update object params")
  }

  var tableNameId = "*";
  var ret = [];
  try {
    tableNameId = self.abstractTablePrimaryKey || "*"
  } catch(e){
    err = e;
  }


  async.series([
    function checkReadyForUpsert(scb){
      if(err) return scb(err);
      scb();
    },
    function update(scb){
      self.update.bind(self)()
      .set(setParams)
      .where(whereParams)
      .run(function(err,results){
        //if(err) { console.error("update in upsert",err); }
        if( results instanceof Array && results.length > 0 ) {
          ret = results;
        }
        scb();
      });
    },
    function insert(scb){
      if( ret.length > 0 ) return scb();
      self.insert.bind(self)()
      .values(setParams)
      .run(function(err,results){
        var isDupe = err instanceof Object && self.exceptions[err.sqlState]
        if( results instanceof Array && results.length > 0 )
          ret = results;
        if( ! isDupe )
          return scb(err);
        scb();
      });
    },function select(scb){
      if( ret.length > 0 ) return scb();
      self.select.bind(self)([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    self.initializeTable.bind(self)();
    if(err) { q.reject(err); return callback(err); }
    q.resolve(ret||[]);
    callback(err||null,ret||[]);
  });


  return q.promise;
};


AbstractTable.prototype.upsertUsingColumnValues = function(setParams,whereParams,callback){
  callback = typeof callback === 'function' ? callback : function(){};
  var self = this;
  // console.log("self",_.keys(self))
  var err = null;
  var q = Q.defer();
  if( !self.utilReady ){
    err = new Error("Need to call util() before accessing utility functions");
  }
  else if( ! ( setParams instanceof Object ) || !( whereParams instanceof Object ) ) {
    err = new Error("Can only insert or update object params")
  }


  var tableNameId = "*";
  var ret = [];
  try {
    tableNameId = self.abstractTablePrimaryKey || "*"
  } catch(e){
    err = e;
  }

  async.series([
    function checkReadyForUpsert(scb){
      if(err) return scb(err);
      scb();
    },
    function update(scb){
      self.update.bind(self)()
      .set(setParams)
      .where(whereParams)
      .run(function(err,results){
        //if(err) { console.error("update in upsert",err); }
        if( results instanceof Array && results.length > 0 ) {
          ret = results;
        }
        scb();
      });
    },
    function insert(scb){
      if( ret.length > 0 ) return scb();
      self.insert.bind(self)()
      .values(_.merge(setParams,whereParams))
      .run(function(err,results){
        var isDupe = err instanceof Object && self.exceptions[err.sqlState]
        if( results instanceof Array && results.length > 0 )
          ret = results;
        if( ! isDupe )
          return scb(err);
        scb();
      });
    },function select(scb){
      if( ret.length > 0 ) return scb();
      self.select.bind(self)([tableNameId])
      .where(whereParams)
      .run(function(err,tableIdFound){
        if(tableIdFound instanceof Array){
          ret = tableIdFound;
        }
        scb(err);
      });
    }
  ],function(err){
    self.initializeTable.bind(self)();
    if(err) { q.reject(err); return callback(err); }
    q.resolve(ret||[]);
    callback(err||null,ret||[]);
  });

  return q.promise;
};




module.exports = AbstractTable;
