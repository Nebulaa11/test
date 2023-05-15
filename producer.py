from kafka import KafkaProducer



producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('topictest',b'hey')
producer.flush()