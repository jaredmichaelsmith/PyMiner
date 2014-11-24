'''
    Jared Smith
    PyMiner, Version 0.1
    Streamer.py

    Authored under MIT License.
'''

# Built-in Imports
from __future__ import print_function
import json
import re
import logging
import threading
import sys
# 3rd-Party Imports
import tweepy
from tweepy.models import Status
from tweepy.utils import import_simplejson, urlencode_noplus

# Define global json to be from tweepy simplejson module
json = import_simplejson()

# Thread-safe print method for use iin printing tweets
print_thread = lambda x: sys.stdout.write("%s\n" % x)

# Stream class does handling of Twitter Stream API 
class Stream:
    
    # Initialize class
    def __init__(self, consumer_key, consumer_secret, 
                       key, secret):
        
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(key, secret)
        self.tweetsBuffer = TweetsBuffer()
        self.logger = logging.getLogger('TwitterCollector')

        # If credentials invalid for tokens provides, exit on error
        if not tweepy.API(self.auth).verify_credentials():
            print("Invalid credentials provided.\nExiting...")
            logging.error("Invalid credentials provided.\nExiting...")
            exit(0)

    # Run the stream in a thread and return the thread to Collector.py
    def run(self, filters, filter_type):
        
        # Initialize the listener and the buffer for the stream
        sl = StreamListener()
        sl.init(self.tweetsBuffer)
        
        # Insatiate the streamer
        streamer = tweepy.Stream(self.auth, sl)

        filter_list = filters.split(',')
    
        locations = [-124.848974, 24.396308, -66.885444, 49.384358, -140.9,55.1,-129.8,60.3, -169.6,53.3,-140.9,71.5, -172.0,10.6,-143.3,28.8]

        stream_thread = threading.Thread(target=streamer.filter, kwargs=dict(locations=locations))
        stream_thread.start()

        '''# Check for filter type provided form config file and for filter parameters.
        # This will either be a list of lat-lon pairs for geofencing, or keywords
        if filter_type == 'location':
            locations = filter_list
            for location in locations:
                location = float(location)

        elif filter_type == 'keyword':
            keywords = filter_list
            stream_thread = threading.Thread(target=streamer.filter, kwargs=dict(track=keywords))
            stream_thread.start()
        else: 
            print("Invalid argument provided for config property filter_type.\nExiting...")
            logging.error("Invalid argument provided for config property filter_type.\nExiting...")
            exit(0)'''
        
        return stream_thread

    # Returns the tweepy tweet buffer
    def getTweetsBuffer(self):
        return self.tweetsBuffer
    
# Listener class handles what to do when data comes through stream to our endpoint
class StreamListener(tweepy.StreamListener):
    
    # Initialize the tweet buffer
    def init(self, tweetsBuffer):
        self.tweetsBuffer = tweetsBuffer
            
    # Parses status into python dictionary object for tweet and user
    def parse_status(self, status, retweet = False):
        
        tweet = {
                     'tweet_id':status.id, 
                     'tweet_text':status.text,
                     'created_at':status.created_at,
                     'geo_lat':status.coordinates['coordinates'][0] 
                               if not status.coordinates is None 
                               else 0,
                     'geo_long': status.coordinates['coordinates'][1] 
                                 if not status.coordinates is None 
                                 else 0,
                     'user_id':status.user.id,
                     'tweet_url':"http://twitter.com/"+status.user.id_str+"/status/"+status.id_str,
                     'retweet_count':status.retweet_count,
                     'original_tweet_id':status.retweeted_status.id 
                                        if not retweet and (status.retweet_count > 0)
                                        else 0,
		             'urls': status.entities['urls'],
                     'hashtags':status.entities['hashtags'],
                     'mentions': status.entities['user_mentions']
                }
        
        user = {
                    'user_id':status.user.id,
                    'screen_name': status.user.screen_name,
                    'name': status.user.name,
                    'followers_count': status.user.followers_count,
                    'friends_count': status.user.friends_count,
                    'description': status.user.description 
                                   if not status.user.description is None
                                   else "N/A",
                    'image_url': status.user.profile_image_url,
                    'location': status.user.location
                                if not status.user.location is None
                                   else "N/A",
                    'created_at': status.user.created_at
                }
        
        return {'tweet':tweet, 'user':user}
    
    # When data is recieved from stream, this could be in the form of several things
    # Essentially, handle replys, deletes, and rate limiting responses from stream,
    # and everything else is unneccessary apart from statuses which will be handled
    # by on_status
    def on_data(self, data):
        
        if 'in_reply_to_status_id' in data:
            status = Status.parse(self.api, json.loads(data))
            if self.on_status(status, data) is False:
                return False
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                 return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
    
    # When status is recieved. print the status out, parse the tweets and retweets, and 
    # insert data into the tweet buffer
    def on_status(self, status, rawJsonData):

        keyword_string = 'christmas,holidays,holiday,santa,santa claus,tacky sweater,snowflake,drummer boy,little drummer boy,nutcracker,clause,santa clause,holly jolly,ho ho ho,ho,shepard,Lord,manger,kwanzaa,hanukkah,Apple,apple,windows,Windows,PC,pc,android,Android,Google,google,church,christmas eve,family,toys,decorating,mistletoe,stockings,presents,cookies milk,chimney,north,pole,ornaments,jingle bells,sleigh,elf,rudolph,reindeer,dasher,dixin,prancer,stocking stuffer,holiday spirit,joy,merry,Merry Christmas,happy,white elephant,greetings,new year,happy new year,season,jolly,silent night,wish list,iOS,gmail,samsung,htc,nexus,andorid L, android lollipop, google tablet,macbook pro, macbook air, ipad, ipad mini, ipad air,imac, macbook,yosemite,mavericks,osx,os x,10.10,iOS 8, iwork,ilife,iphoto,macpro,wp8,wp7,windows 8, windows 8.1, windows 7, windows phone,gift,secret santa,letter,christmas tree,christmas lights,ornaments,angel,nativity scene,jesus,jesus birthday,jubilee,festival,yule,yuletide,tidings,scrooge,noel,Noel,Frosty,cards,candy canes,wreath,holly,pageant,savior,wisemen,Lord,Mary,Joseph,Bethlehem,artificial,cedar,icicles,tinsel,pine,lights,fir,star,ribbon,wrapping paper,dolls,elves,sled,sleigh bells,excited,anticipating,exhausted,vixan,donner,cupid,comet,blitzer,carol,Christmas Carol,carolers,Christmas Dinner'
        keyword_list = keyword_string.split(',')
        
        keyword_list = [element.lower() for element in keyword_list]
        patterns = [r'\b%s\b' % re.escape(s.strip()) for s in keyword_list]
        rgx = re.compile('|'.join(patterns))

        try:
            if rgx.search(status.text.lower()):
                print_thread(status.text)
                tweet = self.parse_status(status)
                tweet['raw_json'] = rawJsonData
                self.tweetsBuffer.insert(tweet)
            
            if tweet['tweet']['retweet_count'] > 0:                           
                if rgx.search(status.retweeted_status.text.lower()):
                    retweet = self.parse_status(status.retweeted_status, True)
                    retweet['raw_json'] = None
                    self.tweetsBuffer.insert(retweet)
        
        # On exception (i.e. Unicode errors in tweet), ignore error and move on
        # to avoid halting collection process
        except Exception, e: 
            pass

# TweetsBuffer class handles the inserting and popping of tweets off the buffer
# for tweets. This is here to allow for a data structure from which to pop off 
# tweets and insert into database
class TweetsBuffer():
    tweetsBuffer = []
    
    # Initialize the lock
    def __init__(self):
        self.lock = threading.Lock()
        
    # Acquire the lock, insert the tweet, and release the lock
    def insert(self, tweet):
        self.lock.acquire()
        self.tweetsBuffer.append(tweet)
        self.lock.release()
    
    # Acquire the lock, pop off the tweet, release the lock, and return tweet to collector
    def pop(self):
        self.lock.acquire()
        tweet = self.tweetsBuffer.pop() if len(self.tweetsBuffer) > 0 else None
        self.lock.release()
        return tweet
    
