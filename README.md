# archi_distrib
Projet architecture distribuée


équipe :

Kenza
Nabil
Redha
Rayan


procedure lancement 

Lancer le docker-compose: docker-compose up -d

Le fichier Docker Compose créer 
-Spark Master
-deux Spark workers
-jupyer notebook
-kafka
-Zookeeper



lancer le container spark en it: 
apt update
apt install pip
pip install -r requirment.txt

lancer dans différents container les produceur et les consumer

producteur_tweet.py + producteur_last_tweet.py -> consumer_tweets.py
producteur_banque.py -> consumer_banque.py



pour voir le dashboard, lien dans "dashboard.txt"
