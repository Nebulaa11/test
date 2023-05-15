from confluent_kafka import Consumer, Producer

from django.conf import settings

KAFKA_SETTINGS = settings.KAFKA_SETTINGS

class KafkaConsumer:
    def __init__(self, topic):
        self.consumer = Consumer(KAFKA_SETTINGS)
        self.consumer.subscribe([topic])

    def consume(self):
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print('Error while consuming message: {}'.format(msg.error()))

            print('Received message: {}'.format(msg.value().decode('utf-8')))
            
class KafkaProducer:
    def __init__(self, topic):
        self.producer = Producer(KAFKA_SETTINGS)
        self.topic = topic
        
    def produce(self, message):
        self.producer.produce(self.topic, key=None, value=message)
        self.producer.flush()
