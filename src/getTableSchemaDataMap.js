var _ = require('lodash')
module.exports = function getTableSchemaDataMap(params,schema){
  var self = this;
  var tableschema = schema || self.abstractTableTableSchema || []
  var retObj = {}
  var paramObj = {}
  var columnNames = [];
  _(_.keys(params)).chain().map(function(col){
    var colObj =  _(tableschema).chain().filter(function(o){ return col.indexOf(o.column_name) === col.replace(o.column_name,"").length }).compact().head().value()
    var isObj = colObj instanceof Object
    if( isObj ) {
      retObj[col] = colObj
      columnNames.push(colObj.column_name);
      paramObj[col] = params[col];
    }
  }).compact().value()

  var ret =  { paramsData: paramObj, columnNames: columnNames,  columnDataMap: retObj,  };

  // console.log("ret",ret)
  return ret;
}
