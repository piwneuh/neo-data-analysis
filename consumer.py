from kafka import KafkaConsumer

# create a Kafka consumer instance
consumer = KafkaConsumer(
    'near_earth_objects', 
    bootstrap_servers=['kafka:9092']
)

# continuously poll for new messages
for message in consumer:
    print(message.value)