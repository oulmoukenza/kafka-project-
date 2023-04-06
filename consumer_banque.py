import json
from kafka import KafkaConsumer
from time import sleep
import datetime
from pymongo import MongoClient

# define the Kafka consumer
consumer = KafkaConsumer('topicstock', bootstrap_servers=['kafka:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
# Connexion à MongoDB Atlas
client = MongoClient("mongodb+srv://Rayan:Rayan1@cluster0.7x3tj73.mongodb.net/?retryWrites=true&w=majority")
db = client["DB"]
collection_banque = db["BanqueCollection"]


# consume messages from the Kafka topic
for message in consumer:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = message.value
    bitcoin_price_usd = data.get("bitcoin_price_usd")
    bnp = data.get("bnp")
    societeG = data.get("societeG")
    goldman = data.get("goldman")
    hsbc = data.get("hsbc")
    jpmorgan = data.get("jpmorgan")
    print(f"{timestamp} - Received prices: bitcoin_price_usd={bitcoin_price_usd}, bnp={bnp}, societeG={societeG}, goldman={goldman}, hsbc={hsbc}, jpmorgan={jpmorgan}")
    collection_banque.insert_one(document=data)
