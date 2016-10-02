var createQuery = require('mysql/lib/Connection.js').createQuery
var spawnSync = require('child_process').spawnSync;
var LexerTokenize = require('sql-parser').lexer.tokenize
var _ = require('lodash')
var fs = require('fs')
var syncOutputDataFile = __dirname+"/output/sync.log"
var syncInputDataFile = __dirname+"/input/sync.log"
var outputFD = null

var Iconv  = require('iconv').Iconv
var iconv = new Iconv( "UTF-8", "UTF-8");

var mysqlSync = {
  connectionString: '',
  connectSync: function(connectionString){
    this.connectionString = connectionString;
    if( !outputFD ){
      this.mq.connected = true;
      outputFD = outputFD || fs.openSync(syncOutputDataFile, 'w+');
    }
  },
  getSyncQuery: function(query,params){
    params = params instanceof Array ? params : []
    var queryRaw = _.cloneDeep( query );
    var queryTokens = []
    try {
      queryTokens = LexerTokenize(queryRaw.replace(/(\=\s{1,}\?)/gmi,"= '?'"));
    } catch(e){ }
    queryTokens = queryTokens instanceof Array ? queryTokens : []
    var whereQueryTokenize = '';
    var queryRawFormat = queryRaw.toLowerCase().replace(/\s{1,}/gmi," ").trim()
    for( var l= 0; l < queryTokens.length ; l++){
      if( queryTokens[l][0] === 'WHERE' ){
        whereQueryTokenize = _.map(queryTokens.slice(l),function(t){ return t[1] }).join(" ").replace(/\s{1}\.\s{1}/gmi,".").toLowerCase().replace(/\s{1,}/gmi," ").trim()
        break;
      }
    }

    var paramToReplace = queryRaw.match(/ \=\s{1,}(\?)/)
    var totalParamsAllowed = (queryRaw.match(/\?/gmi) || []).length
    if( queryRaw.toLowerCase().indexOf(whereQueryTokenize) > -1 && paramToReplace instanceof Array && totalParamsAllowed !== params.length ){
      throw "Invalid Params"
    }
    for ( var i = 0; i < totalParamsAllowed; i++ ){
      var param = null;
      switch(true){
        case params[i] === null || typeof params[i] == 'undefined':
          param = null;
          break;
        case typeof params[i] === 'number':
          param = params[i];
          break;
        case typeof params[i] === 'boolean':
          param = params[i] ? true : false;
          break;
        case params[i] instanceof Array || params[i] instanceof Object:
          param = JSON.stringify(params[i]);
          break;
        case typeof params[i] === 'string':
        default:
          param = "'"+params[i]+"'";
          break;
      }
      queryRaw = queryRaw.replace("?",param)
    }
    return queryRaw;
  },
  querySync: function(query){
    var inRet = { stderr: new Buffer('') };
    var stdoutData = "";
    var error = null;
    var results = { rows: [], columns: {} }

    try {
      fs.writeFileSync(syncInputDataFile,"",{encoding:"utf8"})
      fs.writeFileSync(syncOutputDataFile,"",{encoding:"utf8"})
      fs.writeFileSync(syncInputDataFile,query,{encoding:'utf8'});
      inRet = spawnSync("node",[__dirname+"/mysqlCliClient.js",this.connectionString,syncInputDataFile],{ env: process.env, maxBuffer: 1e9, encoding:'utf8', stdio: [0,outputFD,'pipe'] })
    } catch(e){
      error = e;
    }
    if( inRet.stderr.toString('utf8').trim().length > 0 ){
      error = error instanceof Error ? error : new Error(inRet.stderr.toString('utf8').trim())
    }
    if(error){
      this.end();
      throw error;
    }

    try{
      var buffer = fs.readFileSync(syncOutputDataFile);
      var result = iconv.convert(buffer).toString("utf8").trim();
      result = result.replace(/\\u0000/gmi,"")
      result = result.trim();
      var re = /\0/g;
      stdoutData = result.toString().replace(re, "") || JSON.stringify({});
      var data = JSON.parse( stdoutData );
      results = data;
      try { fs.writeFileSync(syncOutputDataFile,'') } catch(e){}
    }
    catch(e){
      error = error !== null ? error : new Error();
      var message = _.cloneDeep( "QUERY("+ query +") - DATA(" + stdoutData + ") - " + e.stack);
      error.message += message;
    }

    if(error){
      this.end();
      throw error;
    }
    for( var r = 0; r < results.rows.length; r++ ){
      for( var f in results.columns ){
        var type = results.columns[f];
        switch(type){
          case 'date':
            results.rows[r][f] = new Date(results.rows[r][f])
            break;
          default:
            break;
        }
      }
    }
    return results.rows;
  },
  end: function(){
    // console.log("end#outputFD",outputFD)
    try {
      fs.closeSync(outputFD);
    } catch(e){

    }
    outputFD = null;
    this.mq.connected = undefined;
  },
  defaults: {},
  mq: {
    connected: undefined
  }
};
module.exports = mysqlSync
