from django.http import HttpResponse
from kafka_client import KafkaConsumer, KafkaProducer

def consume_kafka(request):
    consumer = KafkaConsumer('topictest')
    consumer.consume()
    return HttpResponse('Kafka messages consumed!')

def produce_kafka(request):
    producer = KafkaProducer('topictest')
    message = b'Hello, Kafka!'
    producer.produce(message)
    return HttpResponse('Kafka message produced!')



