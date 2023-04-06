import pandas as pd
from tqdm.notebook import tqdm
import snscrape.modules.twitter as sntwitter


scraper = sntwitter.TwitterSearchScraper("#bnp paribas lang:en")
tweets1= []
for i,tweet in enumerate(scraper.get_items()):
    data = [
        tweet.date,
        tweet.rawContent
    ]
    tweets1.append(data)
    if i > 100: 
        break


tweet_df1=pd.DataFrame(tweets1, columns=["date","content"])
pd.set_option('display.max_colwidth', None)
#bnp paribas lang:en
tweet_df1.head(10)


# import the required libraries
from kafka import KafkaProducer
from datetime import datetime
from json import dumps
import pandas as pd
from time import sleep
import numpy as np
import datetime
# define the Kafka producer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                          value_serializer=lambda x: dumps(x).encode('utf-8'),
                          api_version=(2,0,2))


# iterate over the rows of the DataFrame and send each row's content and sentiment to Kafka
for i, row in tweet_df1.iterrows():
   
    data = {
        "date": row['date'].strftime("%Y-%m-%d %H:%M:%S"),
        "content": row['content']
    }
    
    producer.send('topictweet', value=data)

    print(data)
    sleep(3)
