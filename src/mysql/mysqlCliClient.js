var ConnectionUrlParser = require('../ConnectionUrlParser.js')
var exec = require('child_process').execSync
var mysql = require('mysql')
var Q = require('q')
var query = process.argv[3]
var connectionString = process.argv[2]

// mysql://root:@127.0.0.1:3306/test;

//select ist.table_name, group_concat( concat('{ column_name:"', isc.column_name,'", data_type:"', isc.data_type,'" column_key:"', isc.column_key,'"}') ) from information_schema.tables ist join information_schema.columns isc on isc.table_name = ist.table_name where ist.TABLE_SCHEMA = 'test' group by ist.table_name;


var conConfig = ConnectionUrlParser.parse(connectionString)
// console.log("conConfig",conConfig)
var connection = mysql.createConnection({
  user:conConfig.user,
  password:conConfig.password,
  port:conConfig.port,
  host:conConfig.host,
  database:conConfig.database
})
Q.fcall(function(){
  return Q.nfcall(connection.query.bind(connection),query);
})
.then(function(dataOrig){
  dataOrig = dataOrig instanceof Array ? (dataOrig[0]||[]) : []
  // console.log("dataOrig",dataOrig)
  var data = { rows: [], columns: {} }
  try {
    var keys = Object.keys(dataOrig[0])
    data.columns = {}
    for( var col1 = 0; col1 < keys.length; col1++){
      var column = keys[col1]
      var field = dataOrig[0][column]
      var type = 'string';
      switch(true){
        case field instanceof Date:
          type = 'date';
          break;
        case field === null:
          type = 'null';
          break;
        case typeof field === 'boolean':
          type = 'boolean';
          break;
        case typeof field === 'number':
          type = 'number';
          break;
        case typeof field === 'object':
          type = 'object';
          break;
        default:
          type = 'string';
          break;
      }
      data.columns[column] = type;
    }

    for( var i = 0; i < dataOrig.length; i++){
      var o = {}
      for( var col = 0; col < keys.length; col++ ){
        var column = keys[col]
        var field = dataOrig[i][column]
        o[column] = field;
      }
      if( Object.keys(o).length > 0 ){
        data.rows.push(o);
      }
    }
  } catch(e){
    console.error(e.stack)
    process.exit(1);
  }
  console.log(JSON.stringify(data))
  process.exit(0);
})
.fail(function(err){
  console.error(err.stack);
  process.exit(1);
})
.done()
