'use strict'
var _ = require('lodash')
var dayLightSavingHoursMatches = new Date().toString().match(/(\-)?(\+)?0(\d)00/)
var dayLightSign = dayLightSavingHoursMatches[1] == '-' ? -1 : 1;
var dayLightHours = dayLightSavingHoursMatches[3]
var dayLightOffset = !isNaN(parseInt(dayLightHours)) ? dayLightSign*parseInt(dayLightHours) : 0;

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
  },
  escapeMySQLString:  function mysql_real_escape_string (str) {
    if( typeof str != 'string' ){
      throw "not a string"
    }
    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
            case "%":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });
  },
  getDateForZone: function getDateForZone(dateStr){
    var d = new Date(dateStr)
    d.setHours(d.getHours() + dayLightOffset)
    return d.toISOString();
  }
}
