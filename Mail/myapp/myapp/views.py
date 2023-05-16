

from django.http import HttpResponse
from .kafka_client import KafkaConsumer, KafkaProducer
from .kafka_email_worker import KafkaEmailWorker
from kafka import KafkaProducer
from .gmail_client import get_gmail_client


def consume_kafka(request):
    consumer = KafkaConsumer('topictest')
    consumer.consume()
    return HttpResponse('Kafka messages consumed!')

def produce_kafka(request):
    producer = KafkaProducer('topictest')
    message = b'Hello, Kafka!'
    producer.produce(message)
    return HttpResponse('Kafka message produced!')

def send_email_message_to_kafka(message):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('topictest', message.encode())
    producer.flush()


def send_email(request):
    if request.method == 'POST':
        recipient = request.POST.get('recipient')
        subject = request.POST.get('subject')
        message = request.POST.get('message')

        email_message = f'{subject}: {message}'
        gmail_client = get_gmail_client()
        gmail_client.send_email(recipient, subject, message)

        # Publish the email message to Kafka
        producer = KafkaProducer('topictest')
        producer.produce(email_message.encode())

        return HttpResponse('Email sent')


