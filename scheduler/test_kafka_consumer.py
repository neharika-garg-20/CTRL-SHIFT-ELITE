from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}
c = Consumer(**conf)
c.subscribe(['test-topic'])

print("Listening for messages...")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print(f"Received: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    c.close()
