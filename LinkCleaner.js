var sqlite3 = require('sqlite3').verbose();
var url     = require("url"); 
var querystring = require('querystring');
var util     = require("util"); 

// organize the links for single web source

Array.prototype.contains = function (elem) {

for (var n = 0; n < this.length; n++)
    {
    if (elem == this[n].link_source) 
        {
        return true;
        }
    }

return false;
};

Array.prototype.getElem = function(elem)
{
for (var n = 0; n < this.length; n++)
    {
    if (elem == this[n].link_source) 
        {
        return this[n];
        }
    }
return null;
}

Links = {};

Links.Work = function(source_name)
{
var self = this;
this.article_db = new sqlite3.Database('scraperdata.db'); 
this.source_name = source_name;
this.Links = {};
this.retlist = [];
this.link_frequencies = [];

this.select_all_links = function(source)
  {  
  var stmt = "SELECT links FROM Article WHERE published_in ='" + self.source_name + "'";
  
    self.article_db.all(stmt, function(err, rows) {

        rows.forEach(function (row) {
        
        var s = decodeURI(row.links);
        var list_obj = JSON.parse(s);
        if(list_obj !== null)
          {
          for (var i = 0; i < list_obj.length; i++)
            {
            if (typeof(list_obj[i]) == "string")
              {
              var parsed_url    = url.parse(list_obj[i]);
              if (parsed_url.hostname !== null) 
                { self.retlist.push(parsed_url.hostname); }
              }
            }
          }
        });
      self.digest_source()
    });
  
  }
  
this.digest_source = function()
  {
  var list = self.retlist;
  // console.log(util.inspect(list));
  
  for (var j = 0; j < list.length; j++)
      {
      elem = list[j];
      //console.log(elem);
      if (self.link_frequencies.contains(elem))
        {
        var a = self.link_frequencies.getElem(elem);
        a.references += 1;
        }
      else 
        {
        var e = {};
        e.link_source = list[j];
        e.references = 1;
        self.link_frequencies.push(e);
        }
      }
  console.log(util.inspect(self.link_frequencies));  
  }
  
self.select_all_links();
}

new Links.Work("American Thinker");

var source = "http://www.guardian.co.uk"
var s = "%5B%22http://www.pinknews.co.uk/2009/01/21/tom-hanks-attacks-prop-8-mormons-as-un-american/%22,%22http://www.pinknews.co.uk/2007/07/01/1408/%22%5D";

var source2 = "http://www.theblaze.com"
var s2 = "%5B%22http://www.pinknews.co.uk/2009/01/21/tom-hanks-attacks-prop-8-mormons-as-un-american/%22,%22http://www.pinknews.co.uk/2007/07/01/1408/%22%5D";

function cleaner(source, link_list)
{
var obj = decodeURI(s);
var list = JSON.parse(obj);

var ret_obj = {};
ret_obj.source = source;
ret_obj.list = [];

for (var i = 0; i < list.length; i++)
  {
  var hostname    = url.parse(list[i]).hostname;
  ret_obj.list.push(hostname);
  }
return ret_obj;
}

function digest_sources(ret_objs)
  {
  var singletons = [];
  console.log(ret_objs.length)
  for (var i = 0; i < ret_objs.length; i++)
    {
    var list = ret_objs[i].list;
    console.log(util.inspect(list));
    for (var j = 0; j < list.length; j++)
      {
      elem = list[j];
      console.log(elem);
      if (singletons.contains(elem))
        {
        var a = singletons.getElem(elem);
        a.references += 1;
        }
      else 
        {
        var e = {};
        e.link_source = list[j];
        e.references = 1;
        singletons.push(e);
        }
      }
    }
  return singletons;
  }

