var config = require('./config.js')
var tzName = config.TIMEZONE;
var moment = require('moment-timezone')
var monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
var momentRegion = tzName;
module.exports = function getTimestamp(){
  var date = new Date();
  var dateS = date.toISOString();
  var momentO = moment(dateS).tz(momentRegion);
  var momentS = momentO.format('DD, YYYY hh:mm:ss');
  var dateM = new Date(momentO.format());
  return monthNames[dateM.getMonth()] + " " + momentS + " (PST)"
}
