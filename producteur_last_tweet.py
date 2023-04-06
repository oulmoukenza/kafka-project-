
from tqdm.notebook import tqdm
import snscrape.modules.twitter as sntwitter


# import the required libraries
from kafka import KafkaProducer
from datetime import datetime
from json import dumps
from time import sleep
import numpy as np
import datetime

# define the Kafka producer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                          value_serializer=lambda x: dumps(x).encode('utf-8'),
                          api_version=(2,0,2))






query = "(bank OR bnp OR JPMorgan OR SocieteGeneral OR goldmanSachs OR DeutcheBank) (#bank OR #bnp OR #JPMorgan OR #SocieteGeneral OR #goldmanSachs OR #DeutcheBank OR #hsbc OR #bnpparibas ) lang:en"

sent_tweets = []


while True:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tweets = sntwitter.TwitterSearchScraper(query).get_items()
    
    tweet_date = next(tweets).date.strftime('%Y-%m-%d %H:%M:%S')
    latest_tweet = next(tweets).rawContent
   
   
    if latest_tweet not in sent_tweets:
        # if the tweet is new, send its content and sentiment to Kafka and add it to the list of sent tweets
        data = {
            "date": tweet_date,
            "content": latest_tweet
           
        }
        producer.send('topictweet', value=data)
        print(f"{timestamp} - Sent tweet: ", data)
        sent_tweets.append(latest_tweet)

    sleep(5)
