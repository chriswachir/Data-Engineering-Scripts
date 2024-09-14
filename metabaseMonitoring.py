import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

logging.basicConfig(filename='metabase_monitoring.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Define the Metabase node details
metabase_node = {"name": "Metabase Node", "host": "host.com", "port": 3000}


# Define the email settings
email_config = {
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 465,
    "smtp_username": "",
    "smtp_password": r"pass",
    "sender_email": "",
    "receiver_email": "" 
    #"receiver_email": "o"
}

def send_email(subject, body):
    """Send email using SMTP."""
    try:
        server = smtplib.SMTP_SSL(email_config['smtp_host'], email_config['smtp_port'])
        server.login(email_config['smtp_username'], email_config['smtp_password'])
        
        message = MIMEMultipart()
        message["From"] = email_config["sender_email"]
        message["To"] = email_config["receiver_email"]
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))

        server.sendmail(email_config["sender_email"], email_config["receiver_email"], message.as_string())
        server.quit()

        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")

def check_metabase_status(metabase_node, email_config):
    try:
        response = requests.get(f"http://{metabase_node['host']}:{metabase_node['port']}", timeout=5)
        response.raise_for_status()
        print("Metabase service is up and running.")
        logging.info("Metabase service is up and running.")
    except requests.exceptions.RequestException as e:
        # If the service is down, send a failure email
        subject = "Metabase Service Status - DOWN"
        body = f"The Metabase service on {metabase_node['host']}:{metabase_node['port']} is down.\n\nError: {str(e)}"
        send_email(subject, body)

        logging.error(f"Metabase service is down. Error: {str(e)}")

        # Print the error message to the console
        print(f"Metabase service is down. Error: {str(e)}")

# Call the 'check_metabase_status' function
check_metabase_status(metabase_node, email_config)
