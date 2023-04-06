import json
from kafka import KafkaConsumer
from time import sleep

# define the Kafka consumer
consumer = KafkaConsumer('topictweet', bootstrap_servers=['kafka:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))


import snscrape.modules.twitter as sntwitter
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from scipy.special import softmax
from pyspark.sql import SparkSession


import json
from kafka import KafkaConsumer 
from pymongo import MongoClient

# load model and tokenizer
roberta = "cardiffnlp/twitter-roberta-base-sentiment"
model = AutoModelForSequenceClassification.from_pretrained(roberta)
tokenizer = AutoTokenizer.from_pretrained(roberta)

labels = ['Negative', 'Neutral', 'Positive']

# define the sentiment analysis function
def analyze_sentiment(tweet):
    tweet_words = []
    for word in tweet.split(' '):
        if word.startswith('@') and len(word) > 1:
            word = '@user'
        elif word.startswith('http'):
            word = "http"
        tweet_words.append(word)
    tweet_proc = " ".join(tweet_words)

    encoded_tweet = tokenizer(tweet_proc, return_tensors='pt')
    output = model(**encoded_tweet)

    scores = output[0][0].detach().numpy()
    scores = softmax(scores)

    # map sentiment labels to numeric values
    if labels[int(scores.argmax())] == 'Negative':
        return -1
    elif labels[int(scores.argmax())] == 'Neutral':
        return 0
    else:
        return 1



# Connexion à MongoDB Atlas
client = MongoClient("mongodb+srv://Rayan:Rayan1@cluster0.7x3tj73.mongodb.net/?retryWrites=true&w=majority")
db = client["DB"]
collection = db["TweetCollection"]

# consume messages from the Kafka topic
total_sentiment = 0
for message in consumer:
    data = message.value
    tweet_content = data['content']
    tweet_date = data['date']
    tweet_sentiment = analyze_sentiment(tweet_content)
    total_sentiment += tweet_sentiment
    tweet = {
        'date': tweet_date,
        'content': tweet_content,
        'sentiment': tweet_sentiment,
        'total': total_sentiment
    }
    collection.insert_one(tweet)
    print(f"Received tweet N°{message.offset}: {tweet_date},{tweet_content}, sentiment: {tweet_sentiment}, total: {total_sentiment}")
