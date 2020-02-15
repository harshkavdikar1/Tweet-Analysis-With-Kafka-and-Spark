from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext
import json, sys
from os import environ

topic_name = "twitter"

# class AnalyzeTweets():
def sum_all_tags(new_values, last_sum):
    if last_sum is None:
        return sum(new_values)
    return sum(new_values) + last_sum

def getSparkSessionInstance(spark_context):
    # Creaating the gloabal instance only once
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SQLContext(spark_context)
    return globals()['sparkSessionSingletonInstance']

def process_hashtags(time, rdd):
    print("---------{}--------".format(time))
    try:
        # Get the Spark SQL context
        spark_sql = getSparkSessionInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda tag: Row(hashtag=tag[0], frequency=tag[1]))

        #Create Dataset
        wordsDataFrame = spark_sql.createDataFrame(rowRdd)

        # # # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("hashtags")

        # # Select top 10 hashtags according to frequency
        wordCountsDataFrame = spark_sql.sql("select hashtag, frequency from hashtags order by frequency desc limit 10")
        wordCountsDataFrame.show()

    except:
        e = sys.exc_info()[0]
        print(e)

if __name__ == "__main__":

    # import kafka libraries to run code from terminal
    environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

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
    tweets = KafkaUtils.createDirectStream(ssc, [topic_name], kafkaParams = kafkaParam)
    
    # Print count of tweets in a particular batch
    # tweets.count().pprint()

    # Split tweets into words
    words = tweets.map(lambda v: v[1]).flatMap(lambda t: t.split(" "))

    # Get hashtags from tweet and create a new DStream by adding their count to previos DStream count
    hashtags = words.filter(lambda tag: len(tag)>2 and '#' == tag[0]).countByValue().updateStateByKey(sum_all_tags)

    hashtags.foreachRDD(process_hashtags)

    # Start Streaming Context
    ssc.start()
    ssc.awaitTermination()