from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from django.conf import settings

def send_email_to_recipient(to, subject, body):
    try:
        credentials = Credentials.from_authorized_user_file(settings.GOOGLE_OAUTH2_CLIENT_SECRETS_FILE, ['https://www.googleapis.com/auth/gmail.send'])

        service = build('gmail', 'v1', credentials=credentials)

        message = MIMEMultipart()
        message['to'] = to
        message['subject'] = subject

        text = MIMEText(body)
        message.attach(text)

        # Base64 encode the message
        message_bytes = message.as_bytes()
        message_b64 = base64.urlsafe_b64encode(message_bytes).decode('utf-8')

        # Send the message
        send_message = service.users().messages().send(userId='me', body={'raw': message_b64}).execute()

        print(F'The message was sent to {to} with email Id: {send_message["id"]}')

    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None

    return send_message
