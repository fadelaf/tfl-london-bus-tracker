from kafka import KafkaProducer
import json
import logging
import time
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
import os

load_dotenv()

"""declare the kafka bootstrap servers and topic"""
bootstrap_servers = [os.getenv("BOOTSTRAP_SERVERS")]

""" topic for sending raw data into Transformation process """
topicName_API = "tfl.source.data_API"

""" declare kafka producers """
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Auto encode
                        )

""" function to send raw data into Transformation phase"""
def source_producer(data):
    try:
        producer.send(topicName_API, data)
        producer.flush()
        logger.info("Messages from source data API sent successfully!")
    except:
        logger.error("Message broker from data source error")


if __name__ == "__main__":
    source_producer()

