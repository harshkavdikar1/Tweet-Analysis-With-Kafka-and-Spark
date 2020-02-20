import tweepy
import json
import sys
from kafka import KafkaProducer

consumer_key = "fEXkjFzTUu4QNPExgdThQjhdg"
consumer_secret = "gKmpJp2FqhdYaambKL69NHxirtY3E2tiiGX0Fw7rXKiNhG4evX"
access_token = "882245118182408192-PwUJYU0Clh9zIfwkOM4AP2473vRmOlO"
access_secret = "J6GFLRBxX1OepK0IXYUxGYWWu0mWrxCmytjQaMSGad3PK"

topic_name = "twitter"


class Listener(tweepy.StreamListener):

    def __init__(self, kafka_producer):
        self.producer = kafka_producer
        self.count = 0

    def on_data(self, raw_data):
        self.process_data(raw_data)

    def process_data(self, raw_data):
        data = json.loads(raw_data)
        text = data["text"]
        if "extended_tweet" in data:
            text = data["extended_tweet"]["full_text"]
        if '#' in text:
            self.producer.send(topic_name, value={"text": text})
            # self.producer.flush()
        # for i in data:
        #     print(i, " == ", data[i])
            print(self.count, text)
            self.count += 1
        # if self.count >= 50:
        #     sys.exit()

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

    # bootstrap_servers=[‘localhost:9092’] : sets the host and port the producer
    # should contact to bootstrap initial cluster metadata. It is not necessary to set this here,
    # since the default is localhost:9092.
    #
    # value_serializer=lambda x: dumps(x).encode(‘utf-8’): function of how the data
    # should be serialized before sending to the broker. Here, we convert the data to
    # a json file and encode it to utf-8.
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    listener = Listener(producer)

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamTweets(auth, listener)

    # San Fransisco
    location = [-122.75, 36.8, -121.75, 37.8]
    language = ['en']
    track_keywords = ['#']
    stream.start(location, language, track_keywords)
