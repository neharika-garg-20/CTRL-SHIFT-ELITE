from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
p = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

print("Type messages to send to Kafka (type 'exit' to quit):")

try:
    while True:
        msg = input("> ")
        if msg.lower() == "exit":
            break
        p.produce('test-topic', key='key1', value=msg, callback=delivery_report)
        p.poll(0)  # Serve delivery callback
except KeyboardInterrupt:
    pass
finally:
    print("Flushing remaining messages...")
    p.flush()
