'use strict'
var _ = require('lodash')
module.exports = {
  console: { asyncLog: function(){
      var args = _.values(arguments);
      var ltm = setTimeout(function(){
        console.log.apply(this,args)
      },22)
      ltm.unref();
    }
  },
  escapeApostrophes: function(str){
    if( typeof str != 'string' ){
      throw "not a string"
    }
    return str.replace(/\'/gm,"''")
  }
}
