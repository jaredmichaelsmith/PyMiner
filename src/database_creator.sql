CREATE TABLE users (user_id BIGINT(20) UNSIGNED, 
			created_at DATETIME, 
			PRIMARY KEY(user_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';
			
CREATE TABLE user_info (user_id BIGINT(20) UNSIGNED,
			screen_name VARCHAR(25),
			name VARCHAR(150),
			followers int(10) unsigned,
			friends int(10) unsigned,
			description VARCHAR(150),
			image_url VARCHAR(150),
			last_update DATETIME,
			location VARCHAR(150),
			FOREIGN KEY(user_id) REFERENCES users(user_id),
			PRIMARY KEY(user_id, screen_name, name, followers, 					  
			description, image_url, location, friends)) data directory = '/Volumes/My\ Passport/mysql_twitter';
			
CREATE TABLE tweets (tweet_id BIGINT(20) UNSIGNED PRIMARY KEY,
			tweet_text VARCHAR(1000),
			created_at DATETIME,
			geo_lat DECIMAL(10,5),
			geo_long DECIMAL(10,5),
			user_id BIGINT(10) UNSIGNED,
			tweet_url VARCHAR(250),
			retweet_count int(10),
			original_tweet_id BIGINT(20),
			FOREIGN KEY(user_id) REFERENCES users(user_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';
			
CREATE TABLE tweet_hashtags (tweet_id BIGINT(20) UNSIGNED PRIMARY KEY,
			tag VARCHAR(500),
			created_at DATETIME,
			user_id BIGINT(20),
			FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';
			
CREATE TABLE tweet_mentions (tweet_id BIGINT(20) UNSIGNED PRIMARY KEY,
				source_user_id BIGINT(20),
				target_user_id BIGINT(20),
			FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';


CREATE TABLE tweet_links (tweet_id BIGINT(20) UNSIGNED PRIMARY KEY,
                                user_id BIGINT(20),
                                longURL  VARCHAR(1000),
                                shortURL VARCHAR(500),
                                created_at DATETIME,
			FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';

CREATE TABLE tweet_json_cache (tweet_id BIGINT(20) UNSIGNED PRIMARY KEY, 
				json_raw TEXT, 
			FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id)) data directory = '/Volumes/My\ Passport/mysql_twitter';
			
CREATE TABLE user_list (user_id  BIGINT(20) UNSIGNED, list_name VARCHAR(200), list_owner VARCHAR(25), PRIMARY KEY(user_id, list_name)) data directory = '/Volumes/My\ Passport/mysql_twitter';
