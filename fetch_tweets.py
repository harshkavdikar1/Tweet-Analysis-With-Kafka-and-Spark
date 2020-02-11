import tweepy, json
from kafka import KafkaProducer

consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""

topic_name = ""

class Listener(tweepy.StreamListener):

    def __init__(self, kafka_producer):
        self.producer = kafka_producer
        self.file = open("data.txt", "w")

    def on_data(self, raw_data):
        self.process_data(raw_data)
    
    def process_data(self, raw_data):
        self.producer.send(topic_name, value=raw_data)
        json.dump(raw_data,self.file)
        self.producer.flush()

class StreamTweets():

    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth, listener)

    def start(self, keywords):
        self.stream.filter(track = keywords)



if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    listener = Listener(producer)

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamTweets(auth, listener)

    stream.start(["liverpool"])