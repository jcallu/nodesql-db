var createQuery = require('../../node_modules/mysql/lib/Connection.js').createQuery
var spawnSync = require('child_process').spawnSync;
var LexerTokenize = require('sql-parser').lexer.tokenize
var _ = require('lodash')
var fs = require('fs')


var mysqlSync = {
  connectionString: '',
  connectSync: function(connectionString){
    this.connectionString = connectionString;
    this.mq.connected = true;
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
    var ret = {};
    try {
      fs.writeFileSync('./output/out.log','')
      var output = fs.openSync('./output/out.log', 'w+');
      ret = spawnSync("node",[__dirname+"/mysqlCliClient.js",this.connectionString,query],{ env: process.env, maxBuffer: 1e9, stdio: [0,output,'pipe'] })
    } catch(e){
      ret = {error: e}
    }
    // console.log("ret",ret)
    var error = ret.error || ret.stderr.toString('utf8') || null;
    var results = { rows: [] }

    try{
      var stdout = fs.readFileSync('./output/out.log','utf8')
      fs.writeFileSync('./output/out.log','')
      results = JSON.parse(stdout);
    } catch(e){
      error = error || e;
    }
    if(error){
      this.end();
      error = error instanceof Error ? error : new Error(error)
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
    // this.end();
    // console.log("results.rows",results.rows)
    return results.rows;
  },
  end: function(){
    this.mq.connected = undefined;
  },
  defaults: {},
  mq: {
    connected: undefined
  }
};
module.exports = mysqlSync
