from kafka import KafkaConsumer

consumer = KafkaConsumer('topictest')
for message in consumer:
    print(message)