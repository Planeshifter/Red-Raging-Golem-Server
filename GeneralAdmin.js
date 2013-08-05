var cronJob = require('cron').CronJob;
var child = require('child_process');

GeneralAdmin = {};

/* Crontab Syntax

*    *     *     *   *    *        command to be executed
    -     -     -   -    -
    |     |     |   |    |
    |     |     |   |    +----- day of week (0 - 6) (Sunday=0)
    |     |     |   +------- month (1 - 12)
    |     |     +--------- day of        month (1 - 31)
    |     +----------- hour (0 - 23)
    +------------- min (0 - 59)
---------------- sec (0 - 59)

00 30 11 * * 1-5
Runs every weekday (Monday through Friday)
at 11:30:00 AM. It does not run on Saturday
or Sunday.

*/


GeneralAdmin.Admin = function()
{
  
var self = this;

this.init = function()
  {
  /* new cronJob('10,20,30,40,50 * * * * *', function(){
    console.log('You will see this message every ten seconds');
  }, null, true, "America/Los_Angeles"); */
  
  
  new cronJob('00 03 01 * * *', function(){
    child.fork("WebScraperNova.js");
  }, null, true, "Europe/Berlin"); 
  
  
    
  }
  
self.init();
  
}

var generaladmin = new GeneralAdmin.Admin();