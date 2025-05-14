import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class EmailSender:
    def __init__(self, smtp_server = None, smtp_port = None):
        # Read credentials from environment variables
        print()
        self.email_config = {
            'sender_email': os.getenv('SENDER_EMAIL'),
            'sender_password': os.getenv('SENDER_PASSWORD'),
            'smtp_server': smtp_server,
            'smtp_port': smtp_port
        }

    def send_email(self, recipient_email, subject, body):
        """
        Sends an email using the provided subject and body.
        :param subject: Subject of the email.
        :param body: Body of the email.
        """
        msg = MIMEMultipart()
        msg['From'] = self.email_config['sender_email']
        msg['To'] = recipient_email
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        # Use SSL for security
        with smtplib.SMTP_SSL(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            server.send_message(msg)


