from kafka import KafkaProducer
from json import dumps
from time import sleep
from yahoo_fin import stock_info as si
from pycoingecko import CoinGeckoAPI


producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                          value_serializer=lambda x: dumps(x).encode('utf-8'),
                          api_version=(2,0,2))


import datetime
cg = CoinGeckoAPI()

while True:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {
        "timestamp": timestamp,
        "bnp": si.get_live_price("BNP.PA"),
        "societeG": si.get_live_price("GLE.TI"),
        "goldman": si.get_live_price("GS"),
        "hsbc": si.get_live_price("HSBC"),
        "jpmorgan": si.get_live_price("JPM"),
        "bitcoin_price_usd": cg.get_price(ids='bitcoin', vs_currencies='usd')['bitcoin']['usd']
    }
    
    
    
    
    producer.send('topicstock', value=data)
    print(f"{timestamp} - Sent prices: ", data)
    sleep(5)