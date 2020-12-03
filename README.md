# cf_tweepy

* Using tweepy API in one call. 
* Supports free 7 day *search* API  (a.k.a sandbox) and *search_full_archive* (premium)
* Search *TWEETS* by passing *queries*. 
* Just pass the *consumer_secret* and *consumer_key* to authenticate. (See examples)

# Requirements
* tweepy -  ```pip install tweepy```

# Usage
* Download the project folder to the working DIR.

* Look at the ```cf_tweepy_usage.ipynb``` for usage examples.

## Using the FREE sandbox
```python
from cf_tweepy import scrape_tweets_sandbox
# Authorize
sand_box = scrape_tweets_sandbox(consumer_key='<key>',
                                 consumer_secret='<secret>')
# Params
query_list = ['Vegan milk', 'Vegan meat']
count = 1000
lang = 'en'

df = sand_box.scrape(query_list,count,lang)
df.head()

```

## Using the Premium full archive search

```python
from cf_tweepy import scrape_tweets_full_archive

sand_box = scrape_tweets_full_archive(consumer_key='<key>',
                                      consumer_secret='<secret>')
                                      
# Params
environment_name = 'Discourse'
query = 'tierhaltungsform'
fromDate='200603210000'
toDate = '202011010000'   
maxResults= 500

df = sand_box.scrape(environment_name, query, fromDate, toDate, maxResults)
df.head()

```
