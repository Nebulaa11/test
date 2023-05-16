from confluent_kafka import Consumer
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

class KafkaEmailWorker:
    def __init__(self, bootstrap_servers, group_id, topic, recipient):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([topic])
        self.recipient = recipient

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print('Error while consuming message: {}'.format(msg.error()))
                continue

            message = msg.value().decode('utf-8')
            self.send_email(message)

    def send_email(self, message):
        try:
            creds = Credentials.from_authorized_user_file('token.json', ['https://www.googleapis.com/auth/gmail.send'])
        except:
            print("Error loading credentials")

        service = build('gmail', 'v1', credentials=creds)

        msg = MIMEMultipart()
        text = MIMEText(message)
        msg.attach(text)
        # Uncomment the below lines to attach an image file to the email
        # img = open('image.png', 'rb').read()
        # image = MIMEImage(img, name='image.png')
        # msg.attach(image)

        message = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode()}
        message['to'] = self.recipient
        message['subject'] = 'New Email from Kafka'

        try:
            message = (service.users().messages().send(userId="me", body=message).execute())
            print(F'Sent message to {self.recipient} Message Id: {message["id"]}')
        except Exception as error:
            print(F'An error occurred: {error}')
