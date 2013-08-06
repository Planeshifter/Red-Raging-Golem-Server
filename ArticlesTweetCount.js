var sqlite3 = require('sqlite3').verbose();
var $       = require('jquery'); 
var util = require('util');


twitter_db = new sqlite3.Database('tweetdata.db');   
articles_db = new sqlite3.Database('scraperdata.db');   

//var s = 'ALTER TABLE Article ADD COLUMN tweet_count INTEGER';
//articles_db.run(s);

var days = 7;
var list = [];
var iterator = 0;

function get_tweet_counts()
{
if(iterator < list.length)
  {
  var stmt = "SELECT COUNT(Id) AS count FROM Tweet WHERE article_id =" + list[iterator];  
  twitter_db.all(stmt, function(err,rows)
    {
    var stm = "UPDATE Article SET tweet_count = " + rows[0].count + " WHERE Id = " + list[iterator];
    articles_db.run(stm, function() {  
      iterator ++;
      console.log(iterator);
      get_tweet_counts();
      });
    
  
    });
  }
}

function load_articles()
{
iterator = 0;
var today = new Date().getTime();
var date_range = days * 24 * 60 * 60 * 1000;
var stmt = "SELECT Id FROM Article WHERE load_date >= " + parseInt((today - date_range) / 1000);

articles_db.all(stmt, function(err, rows) {
        rows.forEach(function (row) {      
        list.push(row.Id);
        });
        
get_tweet_counts();
});


}


load_articles();
console.log("hat geklappt");

