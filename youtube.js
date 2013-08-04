var sqlite3 = require('sqlite3').verbose();
var $       = require('jquery'); 
var util = require('util');

Youtube = {};

Youtube.Crawler = function()
  {
  var self = this;  
  this.db  = null;  
  
  this.store_videos_metadata = function(proc, results)
    {
    for (var i = 0; i < results.length; i++)
      {
       
      var p = results[i];  
     // self.get_user(p.user); 
      
      var video = {};
          
      video.Id  = p.id;
      video.url = p.url;
      video.title = p.title;
      video.content = p.content;
      video.author = p.author;
      video.duration = p.duration;
      video.comment_count = p.comment_count;
      video.category = p.categories.category;
      
      console.log(util.inspect(video));
  Video(Id, url, title, content, author VARCHAR(32), duration INTEGER, comment_count INTEGER, category VARCHAR(64))
         
       var stm = 'INSERT OR IGNORE INTO Tweet(Id, favorited, truncated, created_at, in_reply_to_user_id, text, retweet_count, retweeted, in_reply_to_user_id, user_id, article_id, sentiment) VALUES(' + tweet.Id + ',' +    tweet.favorited + ',' + tweet.truncated + ',"' + tweet.created_at + '",' + tweet.in_reply_to_user_id + ',"' + tweet.text +  '",' + tweet.retweet_count + ',' + tweet.retweeted + ',' + tweet.in_reply_to_user_id + ',' + tweet.user_id + ',' + tweet.article_id + ',' + tweet.sent + ')';
       
      //self.db.run(stm);
      }
    }
    
  this.get_public_query_string = function(keyword)
    {
    var prefix = 'http://query.yahooapis.com/v1/public/yql/';
    var question = '?q=';
    var query = 'select * from youtube.search where query="'
    var keyword = keyword;
    var json = '"&format=json&env=';
    var datatables = encodeURIComponent('store://datatables.org/alltableswithkeys');
    var ret = prefix + question + query + keyword + json + datatables;  
    return ret;
    }
    
  this.get_all_videos = function(proc)
  {
  var str = self.get_public_query_string(proc.keyword); 
  console.log(str);
  $.get(str, function() {
  }).done(function(data) 
    {   
    console.log("success" + data.query.count); 
    self.store_videos_metadata(proc, data.query.results.video);    
    })
   .fail(function() 
      {        
      console.log("error"); 
      })

  }

  this.db_integrity = function()
    {
    self.db.run("CREATE TABLE IF NOT EXISTS Video(Id VARCHAR(16) PRIMARY KEY, url VARCHAR(128), title VARCHAR(128), content VARCHAR(512), author VARCHAR(32), duration INTEGER, comment_count INTEGER, category VARCHAR(64))");  
    
    self.db.run("CREATE TABLE IF NOT EXISTS User(Id VARCHAR(32) PRIMARY KEY,  title VARCHAR(128), description VARCHAR(128), created VARCHAR(256), updated VARCHAR(64), hometown VARCHAR(64), location VARCHAR(2), subscribers INTEGER, views INTEGER, totalviews INTEGER)");  
    
    self.db.run("CREATE TABLE IF NOT EXISTS Comment(Id INTEGER PRIMARY KEY, video VARCHAR(16), published VARCHAR(256), title VARCHAR(128), content VARCHAR(512), author VARCHAR(32))");
    }
  
  this.init = function()
    {
    if (!self.db) self.db = new sqlite3.Database('youtube.db');
    self.db_integrity();
    }
  
  self.init();    
    
  }
  
var youtube_crawler = new Youtube.Crawler();

my_proc = {};
my_proc.keyword = "Eko Fresh";

youtube_crawler.get_all_videos(my_proc);