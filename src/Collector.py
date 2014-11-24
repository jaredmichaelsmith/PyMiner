'''
    Jared Smith
    PyMiner, Version 0.1
    Collector.py

    Authored under MIT License.
'''

# Built-in Imports
import logging
import argparse
import re
import threading
import time
import sys
import os
from ConfigParser import SafeConfigParser
# Project-Specific imports
from Streamer import Stream
from DatabaseUtilities import SQL

# Collector class handles the overall management of collecting tweets, parsing them,
# and inserting them into the database
class Collector():

	# Specify default config file
    DEFAULT_CONFIG_FILE = "collector.config"
    
    # Initialize the logger
    def __init__(self):
        self.setup_logger()
       
    # Setup the logger object
    def setup_logger(self):
        logging.basicConfig(filename="collector.log")
        self.logger = logging.getLogger('Collector')
        fh = logging.FileHandler("collector.log")
        formatter = logging.Formatter('[%(asctime)s]:')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.setLevel(logging.INFO)
        self.logger.info("Collector Started")
        
    # Read in the config file and return the configuration dictionary
    def setup_config(self, file = DEFAULT_CONFIG_FILE):
        config_parser = SafeConfigParser()
        
        # If config file not found, exit on error
        if not config_parser.read(file):
            print "Config file %s not found.\nExiting..." % file
            self.logger.error("Config file %s not found.\nExiting..." % file)
            exit(0)

        # Check if config file has database information
        if "db_info" not in config_parser.sections():
            print "Config file missing database info.\nExiting..."
            self.logger.error("Config file missing database info.\nExiting...")
            exit(0)
        
        config = {}

        # Read in the configuration file into two python dictionaries for database 
        # and general configuration information
        try:
            self.logger.info("Parsing config file: %s", file)
            sections = config_parser.sections(); sections.remove("db_info")
            
            # Get the MySQL Server information
            db_info = {
               		"db_host": config_parser.get("db_info", "db_host"),
                    "db_user": config_parser.get("db_info", "db_user"),
                    "db_pass": config_parser.get("db_info", "db_pass"),
        	}
            
            # Make sure that a connection can be established to MySQL
            self.sql = SQL(db_info['db_host'], db_info['db_user'], db_info['db_pass'])
            
            # Get the configuration informations
            for section in sections:
                config = {
            		"con_key"      : config_parser.get(section, "con_key"),
                    "con_secret"   : config_parser.get(section, "con_secret"),
                    "key"          : config_parser.get(section, "key"),
                    "secret"       : config_parser.get(section, "secret"),
                    "db"           : config_parser.get(section, "db"),
                    "filters"	   : config_parser.get(section, "filters"),
                    "filter_type"  : config_parser.get(section, "filter_type"),
        		}

                # Make sure there is a database with the name provided
                self.sql.testDB(config_parser.get(section, "db"))
            
        # Exit on error with config file
        except Exception, e:
            print "\nPlease fix the config file.\nExiting..."
            self.logger.error("%s\nPlease fix the config file.\nExiting..." % str(e))
            exit(0)

        return config
    
    # Parse arguments from command line, load configuration file, and start the collector
    def parse_arguments(self):

    	# Set up argument parse
        parser = argparse.ArgumentParser(description='Collect tweets from Twitter')
        parser.add_argument('-c','--config', 
                            help='Path to config file. Default config file will'+
                                  ' not be used.', 
                            required=False)
        # Parse the arguments
        args = vars(parser.parse_args())

        # If config file provided, set that up, otherwise run with default config file
        if args['config']:
            config = self.setup_config(args['config'])
        else:
            config = self.setup_config()

		# Start the collector            
        self.start(config)
        
        
    def start(self, config):        
        stream_threads = []
        
        # Insatiate the stream object
        sr = Stream(config['con_key'], config['con_secret'], 
            config['key'], config['secret'])

        # Get tweets buffer from the stream object
        buff = sr.getTweetsBuffer()

        # Start the stream and append the thread with stream onto list
        print "Starting..."
        stream = sr.run(config['filters'], config['filter_type'])
        stream_threads.append(stream)

        # Continue to check if tweet is available to pop off buffer,
        # and if so, then insert it and all its associated information 
        # into the database; otherwise, sleep and try again later
        while True:
            try:
                tweet = buff.pop()
                if not tweet:
                    time.sleep(1)
                else:
                    self.sql.insert_into(config['db'], tweet)
            
            # If ctrl+c is pressed, join the threads with the main thread, and exit the program
            except KeyboardInterrupt:
                print "Joining threads and exiting..."
                for stream_thread in stream_threads:
                	stream_thread.join()
                
                os._exit(0)

# Run main
if __name__ == "__main__":
    Collector().parse_arguments()
