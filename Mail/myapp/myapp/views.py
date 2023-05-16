

from django.http import HttpResponse
from .kafka_client import KafkaConsumer, KafkaProducer

import requests



def consume_kafka(request):
    consumer = KafkaConsumer('topictest')
    consumer.consume()
    return HttpResponse('Kafka messages consumed!')

def produce_kafka(request):
    producer = KafkaProducer('topictest')
    message = b'Welcome User!'
    producer.produce(message)
    return HttpResponse('Kafka message produced!')

def send_email_message_to_kafka(message):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('topictest', message.bytes(message, encoding='utf-8'))
    producer.flush()


import requests

def send_email(request):
    if request.method == 'POST':
        # Get the email data from the request
        subject = request.POST['subject']
        message = request.POST['message']
        sender = request.POST['sender']
        recipient = request.POST['recipient']

        # Set up the Mailgun API endpoint and authentication
        url = 'https://api.mailgun.net/v3/sandboxd59be11bd668466d8163946736d0b6e0.mailgun.org/messages'
        auth = ('api', '412786bad81c11cc3bc7824a1a91283d-db4df449-c37c47bb')

        # Set up the email data
        data = {
            'from': sender,
            'to': recipient,
            'subject': subject,
            'text': message
        }

        # Send the email using the Mailgun API
        response = requests.post(url, auth=auth, data=data)

        # Check if the email was sent successfully
        if response.status_code == 200:
            # Publish the email message to Kafka
            producer = KafkaProducer('topictest')
            producer.produce(message.encode())
            return HttpResponse('Email sent')
        else:
            return HttpResponse('Error sending email')



