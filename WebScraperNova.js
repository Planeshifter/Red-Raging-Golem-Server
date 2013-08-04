var sqlite3 = require('sqlite3').verbose();
var $       = require('jquery'); 
var util    = require('util');
var winston = require('winston');



  var logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)(),
      new (winston.transports.File)({ filename: 'scraping_log.log' })
    ]
  });

logger.log('info', 'WINSTON'); 
 


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

// this is create double quotes that it does not break
String.prototype.escape_quotes = function() 
{ 
return this.replace(/[\\"']/g, '\\$&').replace(/\u0000/g, '\\0');
}

String.prototype.double_quotes = function() 
{ 
return this.replace(/[\\"]/g, '""');
}



Scraper = {};


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
str = str.split("?");
str = str[0];
  
if (typeof(str) != "string") logger.log('info', 'CLEAN_URL KEIN STRING');    
/*  
console.log("clean url " + str);  
var ret_str = str.replace("^([^?]+).*", "\\1");
return ret_str;
*/
return str;
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
    
    
    
 if (typeof(final) == "string")
    {
    final = self.clean_url(final);  
    
    logger.log('info', final);  
    return(final); 
    
    }
 else 
     {
     logger.log('info', 'KEIN STRING ');     
       
       
     if (link_obj.length > 0) 
        {
        logger.log('info', 'ARRAY');  
        logger.log('info', util.inspect(link_obj)     );  
        
        }
     
     if (typeof(link_obj) == "object")
       {
       console.log ("========================================");       
       console.log ("INSPEKTION " + util.inspect( link_obj));
       console.log ("========================================"); 


        logger.log('info', 'OBJECT');  
        logger.log('info', 'inspect', util.inspect(link_obj)     );  
       }
    
     
     
     // final = link_obj[0];
     var final = self.clean_url(link_obj[0]);
     console.log("ein Array, VORSICHT  " + final );
     }
 
 // Säubern des Strings von Suffixen 
 
 
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
  if (text && typeof(text) == "string")
    {
  
    text = text.stripTags();      
    text = text.replace(/(\r\n|\n|\r)/gm,"");
    text = text.removeSpaces();
    text = text.double_quotes();
    return text;
    }
  else return "";
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
 console.log("sauber Link " + alink.clean_url);
 
 
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
  
  
  obj.clean_url                           = alink.clean_url;
  
  
  var stm = 'INSERT INTO Article(url, title, content, abstract, pub_date, load_date, published_in, clean_url) VALUES("' + obj.url + '","' +    obj.title + '","' + obj.content + '","' + obj.abstract + '","' + obj.pub_date +  '","' + obj.load_date + '","' + obj.published_in + '","' + obj.clean_url + '")'; 

  
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

// holt die saubere url und schreibt dann den Artikel
this.get_clean_url = function(src, plain_text)
  {   
  console.log("SAUBERE URL " );  
    
    
  var article_link = src.article_links[src.art_it]; 
  var url = self.get_url(article_link);  
     
     
     
  var prefix = 'select * from html where url="';  
  var suffix = '" and xpath = "/query/diagnostics"';   
    
  var fq = prefix + url + suffix;    
    
    
  var format = "json";
  var yql ="http://query.yahooapis.com/v1/public/yql/?q=";
  var query = yql + encodeURIComponent(fq) + "&format=json&diagnostics=true";

  console.log(query);


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
       
      src.article_links[src.art_it].clean_url = result;
      console.log("Resultat " + result);
       
       
      self.create_new_article(src, plain_text); 
       
	    });  
    
  
    
    
    
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
        
        self.get_clean_url(src, plain_text);
        
        // self.create_new_article(src, plain_text);
        
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
  self.db.run("CREATE TABLE IF NOT EXISTS Article (Id INTEGER PRIMARY KEY, title   VARCHAR(512), author CHAR(164), abstract TEXT, content TEXT, url VARCHAR(512), load_date INTEGER, published_in CHAR(80), pub_date INTEGER, empty_content INTEGER, clean_url VARCHAR(512)  )");  
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
  if (!results) console.log("LEERES RESULT " + src.name);
 
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
 
 
this.init = function()
  {
  console.log(self.db_name); 
  
  if (! self.db) self.db = new sqlite3.Database('scraperdata.db');
  
  self.db_integrity();
  self.get_sources();

  }
 self.init();
}



var db = new Scraper.Work("scraperdata.db")