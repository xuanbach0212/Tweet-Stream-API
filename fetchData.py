import tweepy
import json
import sys
from kafka import KafkaProducer
from configparser import ConfigParser
import time
import requests


class MyStream(tweepy.StreamingClient):

    def __init__(self, bearer_token, *, return_type=..., wait_on_rate_limit=False, **kwargs):
        super().__init__(bearer_token, return_type=return_type, wait_on_rate_limit=False, **kwargs)
        self.count = 0
        self.producer = producer
        self.topic_name = topic_name

    def on_data(self, raw_data):
        self.process_data(raw_data)
    
    def process_data(self, raw_data):
        print("process data .....")
        data = json.loads(raw_data.decode('utf-8'))
        text = data['data']['text']
        
        if "extended_tweet" in data:
            text = data["extended_tweet"]["full_text"]
        if '#' in text:
            producer.send(topic_name, value={"text": text})
            print("______________________________________________________________________________________")
            print(text)
            self.count += 1
            print(self.count)
            if self.count % 100 == 0:
                print("Number of tweets sent = ", self.count)


    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

if __name__ == "__main__":

    config = ConfigParser()
    config.read(r"conf\app.conf")

    bootstap_server = config['Kafka_param']['bootstrap.servers']


    producer = KafkaProducer(bootstrap_servers=[bootstap_server],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    topic_name = config['Resources']['app_topic_name']
    consumer_key = config['API_details']['consumer_key']
    consumer_secret = config['API_details']['consumer_secret']
    access_token = config['API_details']['access_token']
    access_secret = config['API_details']['access_secret']
    bearer_token = r"AAAAAAAAAAAAAAAAAAAAAJ43lwEAAAAAK3buAj9orM%2BtBFOab70FH6vZq5o%3DNLy734oAsOfd8ckGv5dI2X68VIjGXbUVwHYmTeagKiO5ekFkug"

    client = tweepy.Client(bearer_token,consumer_key,consumer_secret,access_token,access_secret)

    auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret,access_token,access_secret)

    # auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    stream = MyStream(bearer_token = bearer_token)
    
    # for rule in stream.get_rules()[0]:
    #     stream.delete_rules(rule.id)

    stream.add_rules(tweepy.StreamRule("(VietNam OR Trending) lang:en has:hashtags"), dry_run = False)
    stream.filter(tweet_fields=["entities"])

    


    