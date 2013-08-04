var sqlite3 = require('sqlite3').verbose();
var $       = require('jquery'); 

var util = require('util');
var OAuth = require('oauth').OAuth;

var analyze = require('Sentimental').analyze,
positivity = require('Sentimental').positivity,
negativity = require('Sentimental').negativity;


Scraper = {};



var twitterConsumerKey    = 'EpjzrG3byXS8WgqsuvKNgw'; 
var twitterConsumerSecret = '72p81JaGu7rghYNYt4V5F5Q3Uizz0SWzS8FjdvwYdI'; 

var oa = new OAuth("https://api.twitter.com/oauth/request_token",
    "https://api.twitter.com/oauth/access_token",
    twitterConsumerKey,
    twitterConsumerSecret,
    "1.0A",
    "http://toskana.ludicmedia.de/auth/twitter/callback",
    "HMAC-SHA1");
    
var access_token_key = '206795751-SR9LnpG1RwNXwoqDf3Y1Z3Is4sdoHaAkosEoszew';
var access_token_secret = '529vBAUMretn5KjByOlJhWsRjRAtMAXaloMRBQjMHo';


if (oa) console.log("oa existiert" + typeof(oa));

function twitterSearch(key, number)
{
var prefix = 'https://api.twitter.com/1.1/search/tweets.json?q=';
prefix += encodeURIComponent(key);

var count = '&result_type=recent&count=' + number;

var url = prefix + count;
  
console.log(url);  

//https://api.twitter.com/1.1/search/tweets.json?q=%23freebandnames&since_id=24012619984051000&max_id=250126199840518145&result_type=mixed&count=4 


oa.get(
      url,
      access_token_key, //test user token
      access_token_secret, //test user secret            
      function (e, data, res){
        if (e) console.error(e);        
        // console.log(require('util').inspect(data));     
      });  
}




// twitterSearch("Obama", 20);



Scraper.TwitterMonitor = function()
{
var self = this;
this.counter = 0;

this.blocked = true;
this.next_start = 0;

this.queue = [];

this.critical_time = 0; // Wird sie überschritten, gibts eine Art Reset

this.daily_twitter_counter = 0;    // wird nach der critical_time auf 0 gesetzt
this.daily_error_counter = 0;    // wird nach der critical_time auf 0 gesetzt

this.twitter_phase = 0;             // nimmt den timestamp der Suchphase auf
this.twitter_phase_duration = 1 * 60 *1000;  // 15 minuten
this.twitter_query_counter = 0; // um das Limit pro 15 m nicht zu brechen
this.allowed_tweets = 20;

this.article_db = null;
this.twitter_db = null;


this.job_done = false; // Das ist der tägliche Job

this.fetch_url = function(item, fq)
  {    
  var format = "json";
  var yql ="http://query.yahooapis.com/v1/public/yql/?q=";

  var query = yql + encodeURIComponent(fq) + "&format=json&diagnostics=true";
    
    $.getJSON(
	  query, 
	  function(data){		

      var result; 
       var redirect = data.query.diagnostics.redirect;
       if (! redirect) 
          {
          result = data.query.diagnostics.url.content;
          }
       else         
         {
        // resirect existiert   
         if (redirect.content) result = redirect.content;
         else
          {
          var x = redirect.length - 1;
          result = redirect[x].content;
          }
         
         }
       
      console.log(result);
       
      // TwitterSearch mit dem result 
      // console.log( util.inspect(item) );
      self.twitter_query_counter ++;
      self.daily_twitter_counter ++;
      console.log("Counter steht bei " + self.twitter_query_counter + " Daily " + self.daily_twitter_counter);
      
     
     if (self.twitter_query_counter < self.allowed_tweets && self.blocked ==   false)
      // Hier werden die Tweets für den Artikel geholt
      twitterbot.article_search(result, item);        
      else 
         {
         // Wenn der Counter die Anzahl der zulässigen Tweets überschreitet
         self.twitter_query_counter = 0;
         self.blocked = true;
         self.next_start = new Date().getTime() + self.twitter_phase_duration;
         console.log("JETZT WIRD GEBLOCKT");
         }
	    });  
    
    

    
  }


this.article_twitter_resonance = function(item)
  {
  var prefix = 'select * from html where url="';  
  var suffix = '" and xpath = "/query/diagnostics"';  
    
  var final = prefix + item.url + suffix;  
  
  self.fetch_url(item, final);
                     
  }

// Hier werden, wenn der Queue leer ist, die Artikel der letzten 3 Tage in den Queue eingeladen. Es gibt ein Queue-Objekt, das sich über den Typus definiert, in diesem Falle article

this.get_articles_to_queue = function()
  {
    if (! self.article_db) 
        self.article_db = new sqlite3.Database('scraperdata.db'); 
     
    
    var dateOffset = (24*60*60*1000) * 3; 
    var d = parseInt( (new Date().getTime() - dateOffset) / 1000);   
    
    console.log("Zeit ist " + d);
    
    // obj.load_date  = parseInt(new Date().getTime() / 1000);
        
    var stmt = "SELECT * FROM Article WHERE load_date >= " + d;
  
    self.article_db.all(stmt, function(err, rows) {
        rows.forEach(function (row) {
        
        var q = {};
        q.type = "article";
        q.url  = row.url;
        q.Id   = row.Id;


        self.queue.push(q);
        });
   
    // console.log(require('util').inspect(self.queue));  
      
    });


  
  }


this.get_search_processes_to_queue = function()
  {
    if (! self.twitter_db) 
        self.twitter_db = new sqlite3.Database('tweetdata.db');   
    
  var stmt = 'SELECT * FROM SearchProcess WHERE typus = "twitter_job"';
  console.log(stmt);
  
  var list = [];
  
  self.twitter_db.all(stmt, function(err, rows) {
          
       console.log(rows.length);
       rows.forEach(function (row) {
        
        var q = {};
        q.type = "twitter_job";
        q.query  = row.query;
        q.search_process_id   = row.Id;
        q.count = row.count;
        q.language = row.language;
        q.lat = row.lat;
        q.lng = row.lng;
        q.radius = row.radius;

        self.queue.push(q);
        });
  
       });
  }
  

this.check_date_change = function()   // Eine Art Reset-Funktion
  {
  var d = new Date().getTime();
  if (d > self.critical_time) 
    {
    console.log("---- Download Process restarted ---");  
      
    self.job_done = false;
    // var offset = 24 * 60 * 60 * 1000;
    var offset = self.download_phase;
    self.critical_time = d + offset;
    }
  else 
    {
    var remaining = self.critical_time - d;
    console.log("Remaining Time " + remaining);
    }  
  }


// Der Scraper hat sein Geschäft erledigt, Logdaten schreiben
this.write_log_data = function()
{
  
}


// Die entscheidende Funktion
this.execute = function()
  {
  // wenn der Queue und der Job noch nicht erledigt ust
  if (self.queue.length == 0 && self.job_done == false)
    {
    // Einladen der Artikel - nach dem täglichen RESET
    self.get_articles_to_queue(); 
    self.get_search_processes_to_queue();
    }
  else
    {
    // Der Queue ist größer als 0
    if (self.job_done == false)
      {      
      // DasItem an der Stelle 0 des Arrays wird extrahiert
      var item = self.queue.shift();   // es wird auf dem Array gelöscht
      
      // Wenn das Array abgearbeitet sein sollte
      if (self.queue.length == 0)  
        {
        // Job wird als erledigt markiert
        self.job_done = true;
        console.log("Job ist erledigt ");
        // Logdaten für den Tag werden geschrieben
        self.write_log_data();  
        }
      
      //
      switch(item.type)
        {
        case "article":
           // holt die Resonanzen für die Artikel
           self.article_twitter_resonance(item);
        break;
        
        case "twitter_job":
          // holt die Tweets für SearchProcesses
          twitterbot.get_tweets_for_search_process(item);
        break;
        }

      }
    else 
      {
      // IDLE - der Job ist erledigt, jetzt wird gefragt: 
      self.check_date_change(); // ist Tagesrythmus überschritten?  
      
      console.log("---- CHECK FOR DATE CHANGE ----");
      }
    
    }
  }


// Das ist die periodisch aufgerufene Funktion 

this.check = function()
  {
  // Man ist blocked, wenn man zu viele TwitterCalls gemacht hat
  if (self.blocked == true)
    {
    // habe ich die Freiphase betreten?
    var d = new Date().getTime();
    // wenn ja, wird der BLOCK aufgeoben
    if (d > self.next_start)
       {
        console.log("ENT-BLOCKEN");
        self.twitter_counter = 0;
        self.blocked = false; 
       }
    }
   else self.execute();  // Hier ist man ENTBLOCKT - kann exekutieren
  }

this.download_phase = 15*60*1000; 

this.init = function()
  {
  // var offset = 24*60*60*1000;
  var offset = self.download_phase;
  var d = new Date().getTime();
  self.critical_time = d + offset;
  }

self.init();
}

twitter_monitor = new Scraper.TwitterMonitor();




Scraper.TwitterBot = function()
{
var self = this;
this.oa = oa;

this.search_results = [];

this.db = null;

this.load_search_process_tweets = function(title)
{
  
}

this.store_search_process_tweet = function(item)
  {
     
    var p = item;  
    self.get_user_admin_unit(p.user);
    
    var sent = self.sentiment_analysis(p.text);;
    
    var tweet = {};
    
    tweet.Id        = p.id;
    tweet.favorited = self.cv_boolean(p.favorited);
    tweet.truncated = self.cv_boolean(p.truncated);
    tweet.created_at = p.created_at;
    tweet.in_reply_to_user_id = p.in_reply_to_user_id;
    tweet.text       = p.text.double_quotes();

    tweet.retweet_count       = p.retweet_count;
    tweet.tweet_id            = p.id;
    tweet.retweeted           = self.cv_boolean(p.retweeted);    
 
    tweet.in_reply_to_status_id = p.in_reply_to_status_id;
 
    tweet.user_id             = p.user.id;
    tweet.search_process_id = p.search_process_id;

    tweet.sent                = sent;
   
     var stm = 'INSERT OR IGNORE INTO Tweet(Id, favorited, truncated, created_at, in_reply_to_user_id, text, retweet_count, retweeted, in_reply_to_user_id, user_id, search_process_id, sentiment) VALUES(' + tweet.Id + ',' +    tweet.favorited + ',' + tweet.truncated + ',"' + tweet.created_at + '",' + tweet.in_reply_to_user_id + ',"' + tweet.text +  '",' + tweet.retweet_count + ',' + tweet.retweeted + ',' + tweet.in_reply_to_user_id + ',' + tweet.user_id + ',' + tweet.search_process_id + ',' + tweet.sent + ')';     
     
    // console.log(stm);  

    self.db.run(stm);  
    
  }

// holt die Tweets, die im Frontend als SearchProcesses definiert worden sind
this.get_tweets_for_search_process = function(config)
  {
  console.log("--------------------------------------")
  twitter_monitor.twitter_query_counter ++;
  twitter_monitor.daily_twitter_counter ++;
  
  console.log("Counter steht bei " + twitter_monitor.twitter_query_counter + " Daily " + twitter_monitor.daily_twitter_counter);
  
  var prefix = 'https://api.twitter.com/1.1/search/tweets.json?q=';
  prefix += encodeURIComponent(config.query);
  var count = '&result_type=recent&count=' + config.count;
  var url = prefix + count;  
  if (config.language) url += "&lang=" + config.language;
  if (config.lat)
      {
      console.log("hier ein Lokalwert");  
      url += "&geocode="  + config.lat + "," + config.lng + "," + config.radius + "km";
      }
 console.log(url); 
 
 self.oa.get(
      url,
      access_token_key, //test user token
      access_token_secret, //test user secret            
      function (e, data, res){
        if (e) console.error(e);        
        else
          {
          var res = JSON.parse(data);
          
          for (var i = 0; i < res.statuses.length; i++)
            {
            var item =  res.statuses[i];
            item.search_process_id = config.search_process_id;
            self.store_search_process_tweet(item);            
            }
          console.log("Datenanzahl " + res.statuses.length);       
          }
   
      }); 
  }
  

this.load_user_tweets = function(user_id)
{
var stm = "SELECT * FROM Tweet WHERE user_id=" + user_id;  
var list = [];


self.db.all(stm, function(err, rows) {
        rows.forEach(function (row) {

        list.push(row);
        });
   
     
   
    console.log(require('util').inspect(list));  
      
    });

}


this.load_user = function(user_id)
{
console.log("User " + user_id);
var stm = "SELECT * FROM User WHERE Id=" + user_id;

self.db.all(stm, function(err, rows) {
   if (rows.length > 0)
        {
        console.log("Wir haben den Nutzer identifiziert " + rows[0].Id);  
        }
    else console.log("Er ist nicht da");
      

         
    });



}


this.load_tweets = function(config)
{
var list = [];  
  
// var stm = "SELECT * FROM Tweet WHERE ";
var stm = "SELECT * FROM Tweet T JOIN User U ON(T.user_id = U.Id)  WHERE ";


if (config.query) stm += 'T.text LIKE "%' + config.query + '%"';  // evtl. 
// if (config.user_id) stm += 'user_id =' + config.user_id;  // evtl. problematisch

if (self.db) console.log("Datenbank existiert");

self.db.all(stm, function(err, rows) {
        rows.forEach(function (row) {
        list.push(row);
        });
      
    
    });

// REGEXP '[[:<:]]LIM[[:>:]]'

console.log("hier wird geladen " + stm);  
}




// Startpunkt, von dem aus die Twitter-Suchen erfolgen - search ist outdated
this.complex_search = function(config)
 {
 twitter_monitor.counter ++;   
   
   
 var search_process = config; 
 search_process.list = [];
 
 var prefix = 'https://api.twitter.com/1.1/search/tweets.json?q=';
 prefix += encodeURIComponent(config.query);
 var count = '&result_type=recent&count=' + config.count;
 var url = prefix + count;   
 
 if (config.language) url += "&lang=" + config.language;
 if (config.lat)
      {
      console.log("hier ein Lokalwert");  
      url += "&geocode="  + config.lat + "," + config.lng + "," + config.radius + "km";
      }
 
   
 console.log(url); 
 
 self.oa.get(
      url,
      access_token_key, //test user token
      access_token_secret, //test user secret            
      function (e, data, res){
        if (e) console.error(e);        
        else
          {
          var res = JSON.parse(data);
          
          for (var i = 0; i < res.statuses.length; i++)
            {
            var item =  res.statuses[i];
            item.search_key =  
            
            search_process.list.push(item);
            
            
            }
          console.log("Datenanzahl " + res.statuses[0].text ); 
          self.store_process(search_process);
          
          }
        

        
      }); 
 
 
 }



// Tweets, die sich auf Artikel beziehen
this.article_search = function(query, article)
 {
 var search_process = {}; 
 search_process.type = "article";
 search_process.query  = query;
 search_process.list = [];
 search_process.article = article;
 search_process.title = "article_tweet_grabber";
 search_process.sentiment = true;
   
 var prefix = 'https://api.twitter.com/1.1/search/tweets.json?q=';
 prefix += encodeURIComponent(query);
 var count = '&result_type=recent&count=' + 100;
 var url = prefix + count;   
   
 console.log(url);  
   
 self.oa.get(
      url,
      access_token_key, //test user token
      access_token_secret, //test user secret            
      function (e, data, res){
        if (e) console.error(e);        
        else
          {
          var res = JSON.parse(data);
          
          for (var i = 0; i < res.statuses.length; i++)
            {
            var item =  res.statuses[i];
            item.search_key =  
            
            search_process.list.push(item);
            
            
            }
          
          self.store_process(search_process);     
          }
        

        
        // console.log(require('util').inspect(j));  
      });   
 }



// search ist outdated
this.search = function(query, number)
 {
 var search_process = {}; 
 search_process.type = "search";
 search_process.query  = query;
 search_process.list = [];
   
 var prefix = 'https://api.twitter.com/1.1/search/tweets.json?q=';
 prefix += encodeURIComponent(query);
 var count = '&result_type=recent&count=' + number;
 var url = prefix + count;   
   
 console.log(url);  
   
 self.oa.get(
      url,
      access_token_key, //test user token
      access_token_secret, //test user secret            
      function (e, data, res){
        if (e) console.error(e);        
        else
          {
          var res = JSON.parse(data);
          
          for (var i = 0; i < res.statuses.length; i++)
            {
            var item =  res.statuses[i];
            item.search_key =  
            
            search_process.list.push(item);
            
            
            }
          
          self.store_process(search_process);     
          }
        

        
        // console.log(require('util').inspect(j));  
      });   
 }




this.store_process = function(process)
  {
  if (process.title)
  {
  console.log("bin drin " + process.title);
    
  var stmt = 'SELECT * FROM SearchProcess WHERE title="' + process.title + '"';
    
    self.db.all(stmt, function(err, rows) {
          
         if (rows.length > 0) self.store(process, rows[0].Id);
         else                 self.store(process, 0);
  
       });
     }
  }
  

this.cv_boolean = function(val)
{
if (val == false) return 0;
else              return 1;
}



this.show_all_users = function()
{
var stm = "SELECT * FROM User";  

self.db.all(stm, function(err, rows) {

    rows.forEach(function (row) {

        console.log(row.name + " Location " + row.location);
        }); 
         
    });

}


this.get_user_admin_unit = function(user)
{
var s = 'http://api.geonames.org/postalCodeSearchJSON?placename=' +
user.location + '&username=planeshifter';
user.admin_unit ="";

    $.getJSON(
    s, 
    function(data){	
    
    if (data.postalCodes)
      {
      if (data.postalCodes[0] != null)
        {
          
        var admin_unit = data.postalCodes[0].adminName1;
        var admin_unit2 = data.postalCodes[0].adminName2;
        var admin_unit3 = data.postalCodes[0].adminName3;
        
        user.admin_unit = admin_unit;    
        if(admin_unit2 === undefined) user.admin_unit2 = " ";
          else user.admin_unit2 = admin_unit2;
        if(admin_unit3 === undefined) user.admin_unit3 = " ";
          else user.admin_unit3 = admin_unit3;
        }

      }
      
    self.store_user(user);  
	  });
    

}

this.store_user = function(user)
{
if (user.id)
  {
  var guy = {};  
  guy.id                    = user.id;  
  guy.name                  = user.name;
  guy.profile_image_url     = user.profile_image_url;
  guy.created_at            = user.created_at;
  guy.location              = user.location;
  guy.followers_count       = user.followers_count;
  guy.favourites_count      = user.favourites_count;
  guy.language              = user.language;
  guy.listed_count          = user.listed_count;
  guy.description           = user.description;
  guy.statuses_count        = user.statuses_count;
  guy.friends_count         = user.friends_count;
  guy.protected             = self.cv_boolean(user.proteted);
  guy.verified              = self.cv_boolean(user.verified);
  
  guy.admin_unit            = user.admin_unit;
  guy.admin_unit2           = user.admin_unit2;
  guy.admin_unit3           = user.admin_unit3;
  
    
  var stm = 'INSERT OR IGNORE INTO User(Id, name, profile_image_url, created_at, location, followers_count, favourites_count, language, listed_count, description, statuses_count, friends_count, protected, verified, admin_unit, admin_unit2, admin_unit3) VALUES(' + guy.id +',"' + guy.name + '","' + guy.profile_image_url + '","' + guy.created_at + '","' + guy.location + '",' + guy.followers_count + ',' + guy.favourites_count + ',"' + guy.language + '",' + guy.listed_count + ',"' + guy.description + '",' + guy.statuses_count + ',' + guy.friends_count +  ',' + guy.protected + ',' + guy.verified +  ',"' + guy.admin_unit + '","' + guy.admin_unit2 + '","' + guy.admin_unit3 + '")';    
  
  // console.log(stm);
  
  self.db.run(stm); 
  }
}


this.sentiment_analysis = function(text)
 {
 var result = analyze(text);
 return result.score;
 }


this.store = function(proc, id)
  {
  // console.log("QUERY " + proc.query);
  

  if (id == 0)
    {
    var s = 'IF NOT EXISTS(SELECT 1 FROM SearchProcess WHERE title = "' + proc.title + '"';
    
    console.log(s);
    
      
    var stm = 'INSERT INTO SearchProcess(typus, title, query, language, count, lat, lng,     radius) VALUES("' + proc.typus + '","' +    proc.title + '","' + proc.query + '","' + proc.language + '","' + proc.count + '","' + proc.lat +  '","' + proc.lng + '","' + proc.radius + '")'; 
  
  self.db.run(stm);
  }
  
  if (! proc.list) console ("ALARM ");

   // console.log("Anzahl der Items " + proc.list.length);
   for (var i = 0; i < proc.list.length; i++)
    {
     
    var p = proc.list[i];  
    // self.store_user(p.user); 
    self.get_user_admin_unit(p.user);
    
    if (proc.sentiment) var sent = self.sentiment_analysis(p.text);
    else sent = -999;
    
    var tweet = {};
    
    tweet.Id        = p.id;
    tweet.favorited = self.cv_boolean(p.favorited);
    tweet.truncated = self.cv_boolean(p.truncated);
    tweet.created_at = p.created_at;
    tweet.in_reply_to_user_id = p.in_reply_to_user_id;
    tweet.text       = p.text.double_quotes();

    tweet.retweet_count       = p.retweet_count;
    tweet.tweet_id            = p.id;
    tweet.retweeted           = self.cv_boolean(p.retweeted);    
 
    tweet.in_reply_to_status_id = p.in_reply_to_status_id;
 
    tweet.user_id             = p.user.id;

    tweet.sent                = sent;

    // wird im TwitterMonitor fetch_url übergeben
    if (proc.article) tweet.article_id = proc.article.Id;
    else              tweet.article_id = 0;

     // var stm = 'INSERT INTO Tweet(Id) VALUES(' + tweet.Id + ')'; 

     
     var stm = 'INSERT OR IGNORE INTO Tweet(Id, favorited, truncated, created_at, in_reply_to_user_id, text, retweet_count, retweeted, in_reply_to_user_id, user_id, article_id, sentiment) VALUES(' + tweet.Id + ',' +    tweet.favorited + ',' + tweet.truncated + ',"' + tweet.created_at + '",' + tweet.in_reply_to_user_id + ',"' + tweet.text +  '",' + tweet.retweet_count + ',' + tweet.retweeted + ',' + tweet.in_reply_to_user_id + ',' + tweet.user_id + ',' + tweet.article_id + ',' + tweet.sent + ')';     
    
    
    
    // console.log(stm);
    



    self.db.run(stm);
    }
  }


this.db_integrity = function()
  {
  self.db.run("CREATE TABLE IF NOT EXISTS LogData (Id INTEGER PRIMARY KEY,   tweet_counts INTEGER, errors INTEGER)");  


self.db.run("CREATE TABLE IF NOT EXISTS SearchProcess (Id INTEGER PRIMARY KEY, typus VARCHAR(64), author CHAR(64), title VARCHAR(128), query VARCHAR(512), language VARCHAR(2), count INTEGER, lat REAL, lng REAL, radius REAL  )");  
 
  self.db.run("CREATE TABLE IF NOT EXISTS Tweet (Id INTEGER PRIMARY KEY, favorited INTEGER, truncated INTEGER, created_at VARCHAR(128), tweet_id INTEGER, text VARCHAR(512), retweet_count INTEGER, retweeted INTEGER, in_reply_to_status_id INTEGER, in_reply_to_user_id INTEGER, article_id INTEGER, user_id INTEGER, sentiment INTEGER, FOREIGN KEY(user_id) REFERENCES User(Id) )");  
 
self.db.run("CREATE TABLE IF NOT EXISTS User (Id INTEGER PRIMARY KEY, name VARCHAR(128), profile_image_url VARCHAR(128), created_at VARCHAR(128), location VARCHAR(128), tweeter_id INTEGER, followers_count INTEGER, favourites_count INTEGER, language VARCHAR(2), listed_count INTEGER, description VARCHAR(512), statuses_count INTEGER, friends_count INTEGER, protected INTEGER, verified INTEGER, admin_unit VARCHAR(64), admin_unit2 VARCHAR(64), admin_unit3 VARCHAR(64))");

  //var s = 'ALTER TABLE Tweet ADD COLUMN search_process_id INTEGER';
  //var s = 'ALTER TABLE Tweet ADD COLUMN sentiment INTEGER';
  // var s = 'ALTER TABLE User ADD COLUMN admin_unit VARCHAR(64)';
  // var s = 'ALTER TABLE User ADD COLUMN admin_unit2 VARCHAR(64)';
  // var s = 'ALTER TABLE User ADD COLUMN admin_unit3 VARCHAR(64)';
  //var s = "INSERT INTO SearchProcess(title) VALUES ('article_grabber')"
  //self.db.run(s);
  //var s2 = "INSERT INTO SearchProcess(title) VALUES ('article_tweet_grabber')"
  //self.db.run(s2);
  }


this.init = function()
  {
  if (! self.db) self.db = new sqlite3.Database('tweetdata.db');
  self.db_integrity();
  }
  
self.init();  
}


var twitterbot = new Scraper.TwitterBot();
conf = {};

conf.query = "tea party";
conf.language = "en";
conf.count = 20;

/*
// 37.781157,-122.398720,1mi
conf.lat = 48.51;
conf.lng = 2.21;
conf.radius = 1000;
*/
conf.title = "Tea party";

//twitterbot.load_tweets(conf);
// twitterbot.complex_search(conf);
// t.show_all_users();


// t.load_user(1171213596);
// t.load_user_tweets(1171213596);








String.prototype.stripTags = function () {
   return this.replace(/<([^>]+)>/g,'');
}

String.prototype.removeSpaces = function(){

  nsText = this.replace(/(\n\r|\n|\r)/gm,"<1br />");
  nsText = nsText.replace(/\t/g," ");
	re1 = /\s+/g;
	nsText = nsText.replace(re1," ");
	re2 = /\<1br \/>/gi;
	nsText = nsText.replace(re2, "\n");
	return nsText; 
}

// this is create double quotes so that it does not break
String.prototype.escape_quotes = function() 
{ 
return this.replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}

String.prototype.double_quotes = function() 
{ 
return this.replace(/[\\"]/g, '""');
}






Scraper.Article = function(obj)
{
this.url            = obj.url;  
this.content        = obj.content;
this.title          = obj.title;
this.author         = obj.author;
this.abstract       = obj.abstract;

this.published_in    = obj.published_in;

this.load_date      = obj.load_date;
this.pub_date       = obj.pub_date;
this.empty_content  = false;
}


// die einzelne Quelle
Scraper.RssSource = function(obj)
{
this.name  		       = obj.name;
this.url			       = obj.url;
this.rss			       = obj.rss;
this.xpath	         = obj.xpath;
this.group           = obj.group    || null;
this.language        = obj.language || "en";

this.art_it = 0; // der Artikel-Iterator für verketteten Aufrufe

this.article_links   = [];   // bekommen über RSS Links auf die Artikel
this.articles        = [];
}


// organisiert die Arbeit
Scraper.Work = function(db_name)
{
var self = this;  
this.db_name = db_name;
this.db      = null;
  
this.sources = [];  
 
// Helper Funktion - löscht Dinge,die als Parameter übergeben werden und die url unansprechbar machen 
this.clean_url = function(str)
{
var ret_str = str.replace("^([^?]+).*", "\\1");
return ret_str;
}


 // falls es verschiedene Schreibweisen für den Link gibt, Helper
 this.get_url = function(link_obj)
 {
 var final; 
   
 if (link_obj.href==null)
    {
    if (link_obj.link==null)
      {
      if (link_obj.url ==null) {console.log("wir haben ein problem")}
      else final = link_obj.url;
      }
      else final = link_obj.link;  
    }
    else final = link_obj.href;
    
 if (typeof(final) == "string") return(final)  
 else console.log("ein Array, hier sollten wir was raussuchen ");
 
 // Säubern des Strings von Suffixen 
 final = self.clear_url(final);
 
 return final;
 }
 
                 
                      
// helper-Funktion -erzeugt einen sauberen Download-String                      
this.download_string = function(query)
 {
 var yql    = 'http://query.yahooapis.com/v1/public/yql/?q=';
 var suffix = encodeURIComponent(query) + '&format=xml&callback=?';
 var download_url = yql + suffix;
 return download_url;
 }                  
         



this.cleansing = function(text)
  {
  text = text.stripTags();      
  text = text.replace(/(\r\n|\n|\r)/gm,"");
  text = text.removeSpaces();
  text = text.double_quotes();
  return text;
 }


// Helper, wandelt Datums-String in einen Integer (sekunden nach 1970)
this.get_date = function(date)
{
if (typeof(date) == "string") 
  {
  console.log("DATUM");  
  var d = new Date(date); 
  return parseInt(d.getTime() * 0.001); 
  }
}

// erzeugt und speichert Artikel
this.create_new_article = function(src, plaintext)
 {
 alink = src.article_links[src.art_it];
 console.log("Aktueller Link " + self.get_url(alink) );
 
 if (plaintext != "")
  {
  var obj = {};
  obj.url = self.get_url(alink);     
  obj.content = plaintext;  

  if (alink.description ) obj.abstract     = alink.description;
  if (alink.title )       obj.title        = alink.title; 
  if (alink.pubDate )     obj.pub_date     = alink.pubDate; 
  
   
  if (obj.abstract)      obj.abstract    = self.cleansing(obj.abstract);
  if (obj.title)         obj.title       = self.cleansing(obj.title); 
  
  obj.published_in                          = src.name;
  
  
  obj.pub_date = self.get_date(obj.pub_date);
  
  obj.load_date  = parseInt(new Date().getTime() / 1000);
  
  var stm = 'INSERT INTO Article(url, title, content, abstract, pub_date, load_date, published_in) VALUES("' + obj.url + '","' +    obj.title + '","' + obj.content + '","' + obj.abstract + '","' + obj.pub_date +  '","' + obj.load_date + '","' + obj.published_in + '")'; 

  
 console.log(stm);  
 
 
 
 self.db.run(stm);
 
 // CHAINING, Iterator wird hochgezählt, nächster Artikel heruntergeladen
 
 console.log(" ----------- CHAINING ------------")
 src.art_it ++;
 self.fetch_article(src);
  }
else
  {
  console.log("EIN LEERER ARTIKEL ");  
  src.art_it ++;
  self.fetch_article(src);   
  }
 
 }

// holt die XML Daten der einzelnen Artikel
this.yql_get_article = function(src, yql_query)
  {
  console.log(yql_query); 

  
  	$.getJSON(
		  yql_query, 
		  function(data){		 
		    
        var plain_text = data.results[0];
        plain_text   = self.cleansing(plain_text);
        self.create_new_article(src, plain_text);
        
		    });
  
  }
  


 // präpariert und holt den YQL-String
 // ist der Artikel schon da, springt er zum nächsten Link
 this.fetch_article = function(src)
 {
  if (src.art_it < src.article_links.length) 
    {
    var article_link = src.article_links[src.art_it]; 
   
    
    var query = 'select * from html where url="';   
    var url = self.get_url(article_link);

    var connex = '" and xpath="';
    var xpath = src.xpath;
                            
    var final_string = query + url + connex + xpath + '"';
    final_string = self.download_string(final_string);
    
    if (! article_link.existence) self.yql_get_article(src, final_string);
    else
      {
      console.log("Artikel da - Sprung zum nächsten ");  
      src.art_it ++; 
      self.fetch_article(src);
      }
    }
  else
    {
    console.log("Alle Artikel der Quelle eingeladen ");  
    self.source_it ++; 
    self.source_fetch_articles();
    }
    
 }



// 3. Stufe - jetzt werden nacheinander die Artikel eingeladen 
this.source_fetch_articles = function()
  {
  if (self.source_it < self.sources.length)
    {
    console.log("EINLESEN QUELLE " + self.source_it)  
    var s = self.sources[self.source_it];  
    self.fetch_article(s);
    
    }    
  }
 
 
this.db_integrity = function()
  {
  self.db.run("CREATE TABLE IF NOT EXISTS Article (Id INTEGER PRIMARY KEY, title   VARCHAR(512), author CHAR(164), abstract TEXT, content TEXT, url VARCHAR(512), load_date INTEGER, published_in CHAR(80), pub_date INTEGER, empty_content INTEGER  )");  
  }
 
 

this.check_existence  = function(list,item)
 {
 for (var j = 0; j < list.length; j++)
   {
   if (item.link == list[j]) item.existence = true;
   }
 }

 
this.existing_articles = function(src)
  {
  // var stmt = "SELECT * FROM Article WHERE published_in LIKE 'New%'";
  var stmt = "SELECT url FROM Article WHERE published_in ='" + src.name + "' AND date(load_date, 'unixepoch') >= date('now','-3 DAYS')";
  

  var list = [];
  
  self.db.all(stmt, function(err, rows) {
        rows.forEach(function (row) {
  	    var url 				= row.url;	
		    list.push(url);
            
            
        console.log(url);
        });
   
    src.last_stored_links = list;
    console.log("Urls sind eingelesen ");
    
    for (var i = 0; i < src.article_links.length; i++)
      {
      var link = src.article_links[i]; 
      self.check_existence(list, link);
      }
      
    // Nächste Source wird überprüft  
    self.source_it ++;
    self.check_article_links();
      
    });
  
  }
 
 
// 2. Schritt - sind die Links als Artikel schon vorhanden 
this.check_article_links = function()
  {
  if (self.source_it < self.sources.length)
    {
    var s = self.sources[self.source_it];  
    self.existing_articles(s); 
    }
  else
    {
    console.log("Alle Artikel-Links sind überprüft worden -> Schritt 3");
    self.source_it = 0;
    self.source_fetch_articles();      // PHASE 3
    }   
  }
 

this.get_public_query_string = function(rss)
  {
  var prefix = 'http://query.yahooapis.com/v1/public/yql/';
  var question = '?q=';
  var query = 'select * from rss where url="'
  var r = rss;
  var json = '"&format=json';
  var ret = prefix + question + query + r + json;  
  return ret;
  }

this.get_all_links = function(src)
  {
  var str = self.get_public_query_string(src.rss); 
  
 $.get(str, function() {

  }).done(function(data) 
    {   
    console.log("success" + data.query.count); 
    self.create_article_links(src, data.query.results);
    
    self.source_it++;
    self.get_links_for_sources();
    
    })
   .fail(function() 
      {
      self.source_it++;
      self.get_links_for_sources();  
        
      console.log("error"); 
      })

  }
 
 
// hat die JSON-Daten für die Links erhalten
this.create_article_links = function(src, results)
  {
  src.article_links = results.item;
  }
 
 
self.source_it = 0; 
 
// holt die Links für die Sources ab,YQL-Aufruf
this.get_links_for_sources = function()
  {
  if (self.source_it < self.sources.length)
    {
    var s = self.sources[self.source_it];  
    self.get_all_links(s); 
    }
  else
    {
    // Schritt 2: Check der bestehenden Artikel
    self.source_it = 0;
    self.check_article_links();
    }
  }
 
 
// liest alle Quellen in die sources-Liste ein 
this.get_sources = function()
  {
  var stmt = "SELECT * FROM rss_source"; 
  
  self.db.all(stmt, function(err, rows) {
        rows.forEach(function (row) {
            
        var obj = {};
  	    obj.rss 				= row.rss;
		    obj.id 					= row.rss_sourceid;
		    obj.name 			  = row.name;
		    obj.url		      = row.url;
		    obj.xpath			  = row.xpath;
  			
        source = new Scraper.RssSource(obj);
		    self.sources.push(source);
            
            
        console.log(obj.id + ": " + row.rss);
        });
        // closeDb();
        
          // self.db.close();  	
          // self.db = null;
     
          self.get_links_for_sources();
      
    });  
  }
 
 
this.twitter_echo = function()
   {
   console.log("Jetzt kommt Twitter"); 
   }
 
this.init = function()
  {
  console.log(self.db_name); 
  
  if (! self.db) self.db = new sqlite3.Database('scraperdata.db');
  
  self.db_integrity();
  self.get_sources();
  // self.twitter_echo();


  }
 self.init();
}





function zeit()
  {
  twitter_monitor.check(); 
  }

setInterval(zeit, 2500);



var db = new Scraper.Work("scraperdata.db")