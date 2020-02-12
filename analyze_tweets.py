from kafka import KafkaConsumer
import json

topic_name = "twitter"

if __name__ == "__main__":

    # bootstrap_servers = [‘localhost:9092’]  : same as our producer
    # auto_offset_reset = ’earliest’          : one of the most important arguments. It handles where the consumer restarts reading after breaking down or being turned off and can be set either to earliest or latest. When set to latest, the consumer starts reading at the end of the log. When set to earliest, the consumer starts reading at the latest committed offset. And that’s exactly what we want here.
    # enable_auto_commit = True               : makes sure the consumer commits its read offset every interval.
    # auto_commit_interval_ms=1000ms          : sets the interval between two commits. Since messages are coming in every five second, committing every second seems fair.
    # group_id='twitter_data_analysis'        :this is the consumer group to which the consumer belongs. Remember from the introduction that a consumer needs to be part of a consumer group to make the auto commit work.
    # The value deserializer deserializes the data into a common json format, the inverse of what our value serializer was doing.
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='twitter_data_analysis',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for msg in consumer:
        print(msg.value)
