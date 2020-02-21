import tweepy
import json
import sys
from kafka import KafkaProducer
from configparser import ConfigParser


class Listener(tweepy.StreamListener):

    def __init__(self, kafka_producer, topic_name):
        self.producer = kafka_producer
        self.topic_name = topic_name
        self.count = 0

    def on_data(self, raw_data):
        self.process_data(raw_data)

    def process_data(self, raw_data):
        data = json.loads(raw_data)
        text = data["text"]
        if "extended_tweet" in data:
            text = data["extended_tweet"]["full_text"]
        if '#' in text:
            self.producer.send(self.topic_name, value={"text": text})
            self.count += 1
            if self.count % 100 == 0:
                print("Number of tweets sent = ", self.count)

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False


class StreamTweets():

    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth, listener)

    def start(self, location, language, track_keywords):
        self.stream.filter(languages=language,
                           track=track_keywords, locations=location)


if __name__ == "__main__":

    config = ConfigParser()
    config.read("..\conf\hashtags.conf")

    bootstap_server = config['Kafka_param']['bootstrap.servers']

    # bootstrap_servers=[‘localhost:9092’] : sets the host and port the producer
    # should contact to bootstrap initial cluster metadata. It is not necessary to set this here,
    # since the default is localhost:9092.
    #
    # value_serializer=lambda x: dumps(x).encode(‘utf-8’): function of how the data
    # should be serialized before sending to the broker. Here, we convert the data to
    # a json file and encode it to utf-8.
    producer = KafkaProducer(bootstrap_servers=[bootstap_server],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    listener = Listener(producer, config['Resources']['app_topic_name'])

    consumer_key = config['API_details']['consumer_key']
    consumer_secret = config['API_details']['consumer_secret']
    access_token = config['API_details']['access_token']
    access_secret = config['API_details']['access_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamTweets(auth, listener)

    # Converting string to float to get cordinates
    location = [float(x) for x in config['API_param']['location'].split(',')]
    language = config['API_param']['language'].split(' ')
    track_keywords = config['API_param']['track_keywords'].split(' ')
    stream.start(location, language, track_keywords)
