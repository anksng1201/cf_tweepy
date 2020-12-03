import tweepy
import pandas as pd
import time
import tqdm
import re


class scrape_tweets_sandbox:
	"""Scrapes tweets using the sandbox API calls - last 7 days.  http://docs.tweepy.org/en/latest/api.html"""
	def __init__(self, consumer_key, consumer_secret, ):
		"""
		Parameters:
		-------------
		consumer_key : str,
			Consumer key from Twitter account. (GET FROM ADMIN)
		consumer_Secret : str,
			Consumer secret 
		
		"""
		# Init
		self.consumer_key = consumer_key
		self.consumer_secret = consumer_secret
		# authorize using credentials
		auth = tweepy.AppAuthHandler(self.consumer_key, self.consumer_secret)
		self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

	def _clean_tweet(self, TEXT):
		"""Tweet cleaning function"""
		# strips \n 
		TEXT = TEXT.replace('\n', ' ')
	    # strips links 
		TEXT = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', TEXT, flags=re.MULTILINE)
		return TEXT


	def scrape(self, query_list, count, lang ):
		"""
		Returns a dataframe for one query list with clean tweets.
		Parameters:
		---------------
		query_list : list,  DO NOT USE TOO MANY QUERIES ELSE CAN BE BLOCKED: IF BLOCKED CREATE NEW PROJECT.
			List of queries
		count :  int
			Number of tweets to retrieve per query. 
		lang : str
			language to  search tweets in 
		"""

		df_list = []
		for text_query in tqdm.tqdm(query_list):
			try:
				results = self.api.search(q=text_query, lang=lang, count=count, tweet_mode='extended')
				 # Pulling information from tweets iterable object - results
				tweets_list = [[tweet.created_at, tweet.id, tweet.full_text] for tweet in results]
				 # Creation of dataframe from tweets list
				 # Add or remove columns as you remove tweet information
				tweets_df = pd.DataFrame(tweets_list, columns=['date','id','tweet'])
				tweets_df.drop_duplicates(subset={'tweet'}).reset_index(drop=True)
				df_list.append(tweets_df)
			except BaseException as e:
			    print('failed on_status,',str(e))
			    time.sleep(3)
		# Post processing and cleaning 
		df = pd.concat([i for i in df_list])
		df  = df.reset_index(drop=True)
		df = df.drop_duplicates(subset={'tweet'}).reset_index(drop=True)
		df['tweet_clean'] = df['tweet'].apply(lambda x: self._clean_tweet(x))
		return df


class scrape_tweets_full_archive:
	"""Scrapes tweets using the PREMIUM API calls - From 2006. Read more here - http://docs.tweepy.org/en/latest/api.html"""

	def __init__(self, consumer_key, consumer_secret, ):
		"""
		Parameters:
		-------------
		consumer_key : str,
			Consumer key from Twitter account. (GET FROM ADMIN)
		consumer_Secret : str,
			Consumer secret

		"""
		# Init
		self.consumer_key = consumer_key
		self.consumer_secret = consumer_secret
		# authorize using credentials
		auth = tweepy.AppAuthHandler(self.consumer_key, self.consumer_secret)
		self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


	def _clean_tweet(self, TEXT):
		"""Tweet cleaning function"""
		# strips \n
		TEXT = TEXT.replace('\n', ' ')
		# strips links
		TEXT = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', TEXT, flags=re.MULTILINE)
		return TEXT

	def scrape(self, environment_name, query, fromDate='200603210000', toDate='202011010000', maxResults=500):
		"""
		Returns a dataframe for one query list with clean tweets.
		Parameters:
		---------------
		environment_name : str
		    environment name created in Twitter account. **Ask ADMIN**
			query : str,
				List of queries
			fromDate :  str
				Date from or after: 200603210000 (March 2006) .
		    Format -> yyyymmdd0000
			toDate : str
				Date frrom or after: 200603210000 .
		    Format : yyyymmdd0000
		maxResults : int
		    Permitted values between 10-500. Default 500.
		"""

		try:
			results = self.api.search_full_archive(environment_name, query, fromDate, toDate, maxResults)
			# Pulling information from tweets iterable object - results
			tweets_list = [[tweet.created_at, tweet.id, tweet.full_text] for tweet in results]
			# Creation of dataframe from tweets lis
			tweets_df = pd.DataFrame(tweets_list, columns=['date', 'id', 'tweet'])
			# Post processing and cleaning
			tweets_df = tweets_df.drop_duplicates(subset={'tweet'}).reset_index(drop=True)
			tweets_df['tweet_clean'] = tweets_df['tweet'].apply(lambda x: self._clean_tweet(x))

		except BaseException as e:
			print('failed on_status,', str(e))
			time.sleep(3)
		if tweets_df:
			return tweets_df
		else:
			pass
