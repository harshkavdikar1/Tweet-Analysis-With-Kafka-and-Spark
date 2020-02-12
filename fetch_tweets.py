import tweepy, json, sys
from kafka import KafkaProducer

topic_name = "twitter"

class Listener(tweepy.StreamListener):

    def __init__(self, kafka_producer):
        self.producer = kafka_producer

    def on_data(self, raw_data):
        self.process_data(raw_data)
    
    def process_data(self, raw_data):
        data = json.loads(raw_data)
        text = data["text"]
        if "extended_tweet" in data:
            text = data["extended_tweet"]["full_text"]
        self.producer.send(topic_name, value={"text":text})
        self.producer.flush()

class StreamTweets():

    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth, listener)

    def start(self, keywords):
        self.stream.filter(track = keywords)



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

    stream.start(["#"])