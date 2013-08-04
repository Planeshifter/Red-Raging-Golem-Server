// UNSER SERVER

var http    = require("http"); 
var url     = require("url"); 
var sqlite3 = require('sqlite3').verbose();
var util    = require('util');
var rio     = require('rio');
var fs = require('fs');
var spawn = require('child_process').spawn;
var path = require('path');
var mime = require('mime');
var req = require('request');
var querystring = require('querystring');

String.prototype.double_quotes = function() 
{ 
return this.replace(/[\\"]/g, '');
}

console.log("unsere Datenbank");


var tweet_db = null;
var db;

function init_database()
{
db = new sqlite3.Database('scraperdata.db');
tweet_db = new sqlite3.Database('tweetdata.db');
console.log("Datenbanken offenbar erfolgreich ");
}


init_database();

/*
var download = function(uri, filename){
  req.head(uri, function(err, res, body){
    console.log('content-type:', res.headers['content-type']);
    console.log('content-length:', res.headers['content-length']);

    req(uri).pipe(fs.createWriteStream(filename));
  });
};
*/

function execute_query_download (query, filename, n, response, dbase)
{
var actual_dbase = "";
switch(dbase)
  {
  case "articles":
   actual_dbase = "scraperdata.db"; 
  break;
  case "tweets":
   actual_dbase = "tweetdata.db"; 
  break;
  }
  
var query_decoded = decodeURIComponent(query);
console.log("bin in query2")
init_R_string = "setwd(\"/home/martin/node/node-v0.10.4/RedRagingGolemServer\"); require(RSQLite); getwd(); driver <- dbDriver('SQLite'); con <- dbConnect(driver,'" + actual_dbase +  "');  dbGetQuery(con,\"PRAGMA ENCODING = 'UTF-8'\");";
init_R_string += "x <- dbSendQuery(con,'" + query_decoded + "');";
init_R_string += filename + "<- fetch(res=x, n=" + n + ");";

var p = "/home/tc/twinkle-toskana/current/public/Downloads/";

init_R_string += "save(" + filename + ",file=\"" + p + filename + ".RData\");";

fs.writeFile('executeQuery.R', init_R_string, function (err) {
    if (err) return console.log(err);
    var deploySh = spawn('sh', [ 'executeQuery.sh' ], {
   
  });
  
      	
   
   var link = p + filename + ".RData";
   console.log("DOWNLOAD ausgeführt " + link); 
   
    response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
     });
        

    var msg = JSON.stringify(link);
    response.end(msg);
   
   
   
});

//console.log(init_R_string)
//rio.evaluate(init_R_string);

// rio.evaluate(query_R_string);
console.log("rio beendet")
  
}

function db_query(query, name)
{
if (db) console.log("Datenbank bekannt")

var list = [];

var name = new sqlite3.Database(name);
name.run("CREATE TABLE IF NOT EXISTS Article (Id INTEGER PRIMARY KEY, title   VARCHAR(512), author CHAR(164), abstract TEXT, content TEXT, url VARCHAR(512), load_date INTEGER, published_in CHAR(80), pub_date INTEGER, empty_content INTEGER, clean_url VARCHAR(512)  )"); 

name.run("PRAGMA ENCODING = 'UTF-8'");


db.all(query, function(err, rows) {
       
        rows.forEach(function (obj) {    
          
          
          obj.content = obj.content.double_quotes();
          obj.abstract = obj.abstract.double_quotes();
          obj.title = obj.title.double_quotes();          
        
          
          console.log( util.inspect(obj) );
        /*
          var stm = 'INSERT INTO Article(url, title, content, abstract, pub_date, load_date, published_in, clean_url) VALUES("' + obj.url + '","' +    obj.title + '","' + obj.content; + '","' + obj.abstract + '","' + obj.pub_date +  '","' + obj.load_date + '","' + obj.published_in + '","' + obj.clean_url + '")'; 
          */
          
          
                    var stm = 'INSERT INTO Article(url, title, content, abstract) VALUES("' + obj.url + '","' +    obj.title + '","' +    obj.content + '","' +    obj.abstract + '")'; 
          
        console.log(stm);  
        name.run(stm);  
        });
   
    console.log("ALLES GETAN")  
      
    });

 
}


// db_query2('SELECT * FROM Article WHERE content LIKE "%Obama%"', "Obama.db", 200);

var package_list = [];

function split_packages(list, chunk)
  {
  package_list.length = 0;
  var i,j,temparray;
  for (i=0,j=list.length; i<j; i+=chunk) 
      {
      temparray = list.slice(i,i+chunk);
      package_list.push(temparray);
      }
  return package_list;
  }

function get_package(query, response)
  {
  list = package_list[query];
 
  response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
     });
      
  var msg = JSON.stringify(list);
  response.end(msg);  
  }


function execute_query(query, response, database)
{
var dbase = null;
switch(database)
  {
    case "articles":
     dbase = db;
    break;
    
    case "tweets":
     dbase = tweet_db;
    break;
    
  }

console.time('t');  
console.log(query);  
var stmt = decodeURIComponent(query);
console.log(stmt);

var list =[];



dbase.all(stmt, function(err, rows) {
       
        rows.forEach(function (obj) {    
          
        list.push(obj);
       
        });
   
     list = split_packages(list,2000)[0];
 
     console.timeEnd('t');
     console.log("Timer beendet");
     response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
     });
        

  
     var msg = JSON.stringify(list);
     response.end(msg);
    
    
    
    
    });


}


function demo(response)
{
 console.log("THIS is the answer ");
  
 response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
   });
        
  var t = {};
  t.str = "I am the JSON speaking server";
  
  var msg = JSON.stringify(t);

  
  response.end(msg);
}

function new_search_process(proc,response)
{
console.log(util.inspect(proc));
proc.count = 1000;
var stmt = 'INSERT INTO SearchProcess(typus, title, query, language, count, lat, lng,     radius) VALUES("' + proc.typus + '","' +    proc.title + '","' + proc.query + '","' + proc.language + '","' + proc.count + '","' + proc.lat +  '","' + proc.lng + '","' + proc.radius + '")'; 
tweet_db.run(stmt);

var t = {};
t.str = "Search Process successfully stored";
var msg = JSON.stringify(t);
response.end(msg);
}

function update_search_process(proc,response)
{
console.log(util.inspect(proc));
var stmt = 'UPDATE SearchProcess SET ';
stmt += 'title ="' + proc.title + '",';
stmt += 'query ="' + proc.query + '",';
stmt += 'language ="' + proc.language + '"';
if (proc.lat) stmt += ', lat =' + proc.lat;
if (proc.lng) stmt += ', lng =' + proc.lng;
if (proc.radius) stmt += ', radius =' + proc.radius;
stmt += ' WHERE Id = ' + proc.Id;

tweet_db.run(stmt);

response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
     });

var t = {};
t.str = "Search Process successfully updated";
var msg = JSON.stringify(t);
response.end(msg);
}


function load_search_processes(response)
{
var stmt = 'SELECT * FROM SearchProcess WHERE typus="twitter_job"' 
var list = [];

tweet_db.all(stmt, function(err, rows) {
       
        rows.forEach(function (obj) {           
        list.push(obj);
        });
   
     response.writeHead(200, {
      'Content-Type':  'application/json',
      'Access-Control-Allow-Origin' : '*',
     });
   
    console.log("ALLES EINGELADEN" + list.length)  

    var msg = JSON.stringify(list);
    console.log(util.inspect(list));
    response.end(msg);  
    });
  
}

// Create the server. 
http.createServer(function (request, response) { 

  // var pathname = url.parse(request.url).pathname;
  console.log("1. der Server bekommt eine Anfrage - noch unspezifisch ");

  var queryData = "";

   if(request.method == 'GET') 
      {
      console.log("Hier kommt ein GET");
      var pathname = url.parse(request.url).pathname;
      var query    = url.parse(request.url).query;
      
      
      switch(pathname)
        {
               
        case "/all_search_processes":  
          load_search_processes(response);
        break;
        
        case "/get_package":
         get_package(parseInt(query),response);
        break;
        
        case "/query":
          execute_query(query, response,"articles");
        break;
        
        case "/tweet_query":
          execute_query(query, response,"tweets");
        break;
        
        
        case "/query_download":
          var name = "Download";
          var no = 2000000;
          execute_query_download(query, name, no, response, "articles");
          //test_download(response);
        break;
        
        case "/tweet_download":
          var name = "Download";
          var no = 2000000;
          execute_query_download(query, name, no, response, "tweets");
          //test_download(response);
        break;
        
        case "/tweets":
          console.log("TWEET");
          // demo(response);
        break;
        
        
        default:
          console.log(pathname);
        break;
        }
      
      };





   if(request.method == 'POST') {
        request.on('data', function(data) {
            queryData += data;
            if(queryData.length > 1e6) {
                queryData = "";
                response.writeHead(413, {'Content-Type': 'text/plain'}).end();
                request.connection.destroy();
            }
        });
        
        
  
    request.on('end', function() {
      
        var pathname = url.parse(request.url).pathname;
        response.post = querystring.parse(queryData);
        var x = JSON.parse(response.post.data);   

        switch(pathname)
          {
          case "/new_search_process":
            console.log("Daten sind angekommen");
            new_search_process(x,response);
          break;
          
          case "/update_search_process":
            update_search_process(x,response);
          break;
          }
              
        
        // callback();
    })      

  }


}).listen(10000);