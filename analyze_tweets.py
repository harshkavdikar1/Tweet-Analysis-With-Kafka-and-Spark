from kafka import KafkaConsumer
import json

topic_name = "twitter"

if __name__ == "__main__":
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='twitter_data_analysis',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for msg in consumer:
        print(msg.value)
