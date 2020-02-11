import tweepy, json

consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""

class Listener(tweepy.StreamListener):

    def __init__(self):
        self.file = open("data.txt", "w")

    def on_data(self, raw_data):
        self.process_data(raw_data)
    
    def process_data(self, raw_data):
        json.dump(raw_data,self.file)        

class StreamTweets():

    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth, listener)

    def start(self, keywords):
        self.stream.filter(track = keywords)

if __name__ == "__main__":
    listener = Listener()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamTweets(auth, listener)

    stream.start(["liverpool"])