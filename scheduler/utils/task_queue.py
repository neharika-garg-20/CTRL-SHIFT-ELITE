# utils/task_queue.py
from kafka import KafkaProducer
from kq import Queue

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")
queue = Queue(topic="mindcrafter", producer=producer)

def fetch_google():
    import requests
    response = requests.get("https://www.google.com")
    return response.status_code

def enqueue_google_fetch():
    job = queue.enqueue(fetch_google)
    print(f"Job {job.id} sent to Kafka queue.")
