from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json, os

topic_name = "twitter"

if __name__ == "__main__":

    # import kafka libraries to run code from terminal
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    # Setup spark conf
    sparkConf = SparkConf("TwitterDataAnalysis")

    # Create spark context from above configuration
    sc = SparkContext(conf=sparkConf)

    # Set log level to error
    sc.setLogLevel("ERROR")

    # Create Streaming context
    # Get data from stream every 5 secs
    ssc = StreamingContext(sc, 5)

    # Setup checkpoint for RDD recovery
    ssc.checkpoint("checkpointTwitterApp")

    # Parameters for connecting to kafka
    kafkaParam = {
            "zookeeper.connect": 'localhost:2181',
            "group.id": 'twitter_data_analysis',
            "zookeeper.connection.timeout.ms": "10000",
            "bootstrap.servers": "localhost:9092"
    }

    # Creating Dstream by taking input from Kafka
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic_name], kafkaParams = kafkaParam, valueDecoder=lambda x: json.loads(x.decode('utf-8')))
    
    # Print count of tweets in a particular batch
    kafkaStream.count().pprint()

    # Tweet from user
    tweets = kafkaStream.map(lambda v: v[1]["text"])

    # print tweets    
    tweets.pprint()

    # Start Streaming Context
    ssc.start()
    ssc.awaitTermination()