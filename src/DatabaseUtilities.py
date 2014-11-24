'''
    Jared Smith
    PyMiner, Version 0.1
    DatabaseUtilities.py

    Authored under MIT License.
'''

# Built-in Modules
import logging
import urllib2
import re
# 3rd-party imports
import MySQLdb
from django.utils.encoding import smart_str, smart_unicode

# SQL class handles all database operations
class SQL:

    # Initialize database connection and parameters for processing tweets into database
    def __init__(self, host, db_user, db_pass, shorteners_file = None):

        # Default url shorteners, to be used for url parsing or tweet urls
        self.shorteners = ['bit.ly', 'goo.gl', 'tiny.cc', 
                           't.co', 'tinyurl.com', 'fb.me']

        # Load URL shorteners from file if there is one        
        if shorteners_file is not None:
            regex = re.compile(' +|,+|\n')
            with open(file) as shorteners_file: 
                ids = regex.split(shorteners_file.read().rstrip())
            ids = filter(None, ids)

        self.logger = logging.getLogger('TwitterCollector')
        
        # Try to connect to the database, connect any exceptions and exit if connection fails
        try:
            self.db_con = MySQLdb.Connect(host, db_user, db_pass)
            self.db_con.autocommit(True)
        except Exception, e:
            print e, "\nCheck DB password/user in config file.\nExiting..."
            self.logger.error("Error: %s\nCheck DB pass/user in config file.\nExiting..." % str(e))
            exit(0)
        
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                                      START: Database Insert Methods
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    def __insert_user(self, tweet):
        ''' Insert user into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        try:
            sql = "INSERT INTO users (user_id, created_at) VALUES"\
            "(%d, '%s')"\
            %(
              tweet['user']['user_id'],
              tweet['user']['created_at'].strftime('%Y-%m-%d %H:%M:%S')
              )
            
            #exec stms
            self.cursor.execute(sql)
            
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.self.print_err('__insert_user', str(e))
    
    
    def __insert_user_info(self, tweet):
        ''' Insert user info into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        try:
            sql = "INSERT INTO user_info VALUES"\
            "(%d, '%s', '%s', %d, %d, '%s', '%s', '%s', '%s')"\
             %(
               tweet['user']['user_id'],
               MySQLdb.escape_string(tweet['user']['screen_name'].encode('utf-8').strip()),
               MySQLdb.escape_string(tweet['user']['name'].encode('utf-8').strip()),
               tweet['user']['followers_count'],
               tweet['user']['friends_count'],
               MySQLdb.escape_string(tweet['user']['description'].encode('utf-8').strip()),
               MySQLdb.escape_string(tweet['user']['image_url'].encode('utf-8').strip()),
               tweet['user']['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
               MySQLdb.escape_string(tweet['user']['location'].encode('utf-8').strip())
               )
             
            self.cursor.execute(sql)
            
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.print_err('__insert_info', str(e))
                    

    def __insert_tweet(self, tweet):
        ''' Insert tweet into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        try:
            sql = "INSERT INTO tweets VALUES"\
            "(%d, '%s', '%s', %d, %d, %d, '%s', %d, %d)"\
            %(
              tweet['tweet']['tweet_id'],
              MySQLdb.escape_string(tweet['tweet']['tweet_text'].encode('utf-8').strip()),
              tweet['tweet']['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
              tweet['tweet']['geo_lat'],
              tweet['tweet']['geo_long'],
              tweet['user']['user_id'],
              MySQLdb.escape_string(tweet['tweet']['tweet_url'].encode('utf-8').strip()),
              tweet['tweet']['retweet_count'],
              tweet['tweet']['original_tweet_id']
              )
            
            self.cursor.execute(sql)
            
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.print_err('__insert_tweet', str(e))
                    

    def __insert_hashtags(self, tweet):
        ''' Insert hashtags into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        try:
            sql = "INSERT INTO tweet_hashtags VALUES(%d, '%s', '%s', %d)"
            
            for hashtag in tweet['tweet']['hashtags']:
                tmp_sql = sql\
                %(
                  tweet['tweet']['tweet_id'],
                  #MySQLdb.escape_string(hashtag['text']).encode('utf-8').strip(),
                  MySQLdb.escape_string(smart_str(hashtag['text'])).strip(),
                  tweet['tweet']['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                  tweet['user']['user_id']
                 )
                
                self.cursor.execute(tmp_sql)
                
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.print_err('__insert_hashtags', str(e))
                
                    
    def __insert_mentions(self, tweet):
        ''' Insert mentions info into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        sql = "INSERT INTO tweet_mentions VALUES(%d, %d, %d)"
        
        #insert mentions
        try:
            for mention in tweet['tweet']['mentions']:
                tmp_sql = sql\
                %(
                  tweet['tweet']['tweet_id'],
                  tweet['user']['user_id'],
                  mention['id']
                 )
                
                self.cursor.execute(tmp_sql)
                
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.print_err('__insert_mentions', str(e))
    
    def __insert_urls(self, tweet):
        ''' Insert urls into db.
            
            Args:
            tweet: Tweet object containing parsed tweet
        '''
        
        sql = "INSERT INTO tweet_links VALUES(%d, %d, '%s', '%s', '%s')"
        
        #insert urls
        try:
            for url in tweet['tweet']['urls']:
                if url is None or url == '':
                    continue
                
                tmp_sql = sql\
                %(
                  tweet['tweet']['tweet_id'],
                  tweet['user']['user_id'],
                  MySQLdb.escape_string(self.__expand_url(url['expanded_url'])),
                  MySQLdb.escape_string(url['url']),
                  tweet['tweet']['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                 )
                
                self.cursor.execute(tmp_sql) 
                
        except Exception, e:
            if "or read-only" not in str(e) and "1062" not in str(e):
                self.print_err('__insert_urls', str(e))
                    
    def __insert_raw_JSON(self, tweet):
        if tweet['raw_json'] is not None:
            insert_raw_json = "INSERT INTO tweet_json_cache VALUES(%d, \"%s\")"\
                    %(tweet['tweet']['tweet_id'], 
                       MySQLdb.escape_string(tweet['raw_json']
                                            .encode('utf-8').strip()))
            self.cursor.execute(insert_raw_json)
           
    def insert_into(self, db, tweet):
        ''' Insert tweet info into db.
            
            Args:
            db: Name of db to use
            tweet: Tweet object containing parsed tweet
        '''
        try:
            #select db
            self.db_con.select_db(db)
            self.cursor = self.db_con.cursor()
        except Exception, e:
            self.self.print_err('insert_into', str(e))
            
        self.__insert_user(tweet)
        self.__insert_user_info(tweet)
        self.__insert_tweet(tweet)
        self.__insert_hashtags(tweet)
        self.__insert_mentions(tweet)
        self.__insert_urls(tweet)
        self.__insert_raw_JSON(tweet)
    
    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                                      END: Database Insert Methods
    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
    # Method to print error if error occurs in SQL execution
    def print_err(self, func, msg):
        err = "\nDB insert error\n@ %s IN SQL.py\nMSG( %s )" % (func, msg)
        print err;
        self.logger.error(err)
            
            
    def testDB(self, db):
        try:
            # Select Database
            self.db_con.select_db(db)
            self.cursor = self.db_con.cursor()
            
            # Test Database
            sql = "SELECT * FROM users LIMIT 1"
            self.cursor.execute(sql)
        
        # Catch exception in config file
        except Exception, e:
            print e, "\nCheck DB name in config file.\nExiting..."
            self.logger.error("Error: %s\nCheck DB name in config file.\nExiting..." % str(e))
            exit(0)
        
    # Method to expand shortened url to valid full url
    # Uses urllib2 library
    def __expand_url(self, url):
        
        if url is None:
            return "N/A"
        
        url = url.replace("https://", "")
        url = url.replace("http://", "")
        url = url.replace("www.", "")
        
        # Check for known shorteners
        isShort = [shortener for shortener in self.shorteners 
                    if shortener.lower() in url.lower()]
        
        if len(isShort) == 0 \
            and len(url[:url.index(".")]) > 7:
            return url
        
        url = "http://"+url
            
        try:
            # Send request using short url
            req = urllib2.Request(url)
            res = urllib2.urlopen(req)
            
            # Return final url
            url = res.geturl()
            
            if "http://" not in url and "https://" not in url:
                url = "http://"+url
                
            return url 
        
        except Exception, e:
            return "404/Invalid URL(",url,")"
