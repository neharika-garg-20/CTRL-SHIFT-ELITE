import logging

from kafka import KafkaConsumer
from kq import Worker

# Set up logging.
formatter = logging.Formatter("[%(levelname)s] %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger("kq.worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

# Set up a Kafka consumer.
consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id="group",
    auto_offset_reset="latest"
)

# Set up a worker.
worker = Worker(topic="mindcrafter", consumer=consumer)
worker.start()