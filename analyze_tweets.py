from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext
import json
import sys
from os import environ

topic_name = "twitter"

dashboard_topic_name = "processedtweets"


def sum_all_tags(new_values, last_sum):
    if last_sum is None:
        return sum(new_values)
    return sum(new_values) + last_sum


def getSparkSessionInstance(spark_context):
    # Creating the gloabal instance of SQL context only once
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SQLContext(spark_context)
    return globals()['sparkSessionSingletonInstance']


def getKafkaInstance():
    # Creating the gloabal instance of Kafka Producer only once
    if ('kafkaSingletonInstance' not in globals()):
        globals()['kafkaSingletonInstance'] = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return globals()['kafkaSingletonInstance']


def process_hashtags(time, rdd):
    print("---------{}--------".format(time))
    try:
        # Get the Spark SQL context
        spark_sql = getSparkSessionInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda tag: Row(hashtag=tag[0], frequency=tag[1]))

        # Create Dataset
        hashtagsDataFrame = spark_sql.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        hashtagsDataFrame.createOrReplaceTempView("hashtags")

        # Select top 10 hashtags according to frequency
        hashtagCountsDataFrame = spark_sql.sql(
            "select hashtag, frequency from hashtags order by frequency desc limit 10")
        # hashtagCountsDataFrame.show()

        send_to_kafka(hashtagCountsDataFrame)

    except:
        e = sys.exc_info()[0]
        print(e)


def send_to_kafka(hashtagCountsDataFrame):

    top_hashtags = {}
    for hashtag, frequency in hashtagCountsDataFrame.collect():
        top_hashtags[hashtag] = frequency

    producer = getKafkaInstance()

    producer.send(dashboard_topic_name, value=top_hashtags)


if __name__ == "__main__":

    # import kafka libraries to run code from terminal
    environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    # Setup spark conf
    sparkConf = SparkConf("TwitterDataAnalysis")

    # Number of receivers = 2
    # One for kafka and other for rdd processing
    sparkConf.setMaster("local[2]")

    # Create spark context from above configuration
    sc = SparkContext(conf=sparkConf)

    # Set log level to error
    sc.setLogLevel("ERROR")

    # Create Streaming context
    # Get data from stream every 5 secs
    ssc = StreamingContext(sc, 2)

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
    tweets = KafkaUtils.createDirectStream(
        ssc, [topic_name], kafkaParams=kafkaParam, valueDecoder=lambda x: json.loads(x.decode('utf-8')))

    # Print count of tweets in a particular batch
    # tweets.count().pprint()

    # Split tweets into words
    words = tweets.map(lambda v: v[1]["text"]).flatMap(lambda t: t.split(" "))

    # Get hashtags from tweet and create a new DStream by adding their count to previos DStream count
    hashtags = words.filter(lambda tag: len(
        tag) > 2 and '#' == tag[0]).countByValue().updateStateByKey(sum_all_tags)

    hashtags.foreachRDD(process_hashtags)

    # Start Streaming Context
    ssc.start()
    ssc.awaitTermination()
