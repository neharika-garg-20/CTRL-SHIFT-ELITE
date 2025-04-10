import requests
from kafka import KafkaProducer
from kq import Queue

# Set up a Kafka producer.
producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

# Set up a queue.
queue = Queue(topic="mindcrafter", producer=producer)  # Use the same topic as in worker

# Enqueue a function call.
job = queue.enqueue(requests.get, "https://www.google.com")
print(f"Job {job.id} sent to Kafka queue.")
