import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import configparser
import psycopg2 as pg
from datetime import datetime as dt, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

# Define the receiver email
receiver_email = 'email@gmail.com'

# Define the name of the configuration files
DUPLICATES_CONFIG_FILE = '/app/secrets/r_duplicates.ini'
EMAIL_CONFIG_FILE = '/app/secrets/r_emailConfig.ini'

# Define the email subject
EMAIL_SUBJECT = "***Redshift Duplicate Alerts***"

# Function to read the server configuration from the .ini file
def read_config(file_path, section_name):
    config = configparser.ConfigParser()
    config.read(file_path)
    return dict(config.items(section_name))

# Function to send email alert
def send_email_alert(subject, body):
    email_config = read_config(EMAIL_CONFIG_FILE, 'email_config')

    msg = MIMEMultipart()
    msg['From'] = email_config['sender_email']
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP_SSL(email_config['smtp_host'], int(email_config['smtp_port'])) as server:
        server.login(email_config['smtp_username'], email_config['smtp_password'])
        server.sendmail(email_config['sender_email'], receiver_email, msg.as_string())

# Function to send Slack notification
def send_slack_notification(message):
    try:
        slack_webhook_url = read_config(EMAIL_CONFIG_FILE, 'email_config')['slack_webhook_url']
        payload = {'text': message}
        response = requests.post(slack_webhook_url, json=payload)
        if response.status_code != 200:
            print(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"An error occurred while sending Slack notification: {str(e)}")

# Function to get duplicates from Redshift and send email alert if duplicates are found
def get_duplicates_and_alert():
    try:
        duplicates_config = read_config(DUPLICATES_CONFIG_FILE, 'yoda_r_lake')

        conn = pg.connect(host=duplicates_config['host'], database=duplicates_config['database'],
                          user=duplicates_config['user'], password=duplicates_config['password'],
                          port=duplicates_config['port'])
        cur = conn.cursor()

        # Loop through sections in duplicates_config
        for section in duplicates_config:
            if section.startswith('yoda_hub'):
                table_config = read_config(DUPLICATES_CONFIG_FILE, section)
                table_name = table_config['table']
                unique_key = table_config['unique_key']

                # Query to find duplicates in Unique_key column
                query = f"""
                    SELECT COUNT(*) AS duplicate_count, {unique_key}
                    FROM {table_config['database']}.{table_name}
                    GROUP BY {unique_key}
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC;
                """

                # Execute the query and fetch the results
                cur.execute(query)
                duplicate_results = cur.fetchall()

                if duplicate_results:
                    # Construct email body
                    body = f"Duplicates found in {table_config['database']}.{table_name} at {dt.now()}.\n\n"
                    body += "DETAILS:\n"
                    body += f"Source Host: {table_config['host']}\n"
                    body += f"Source Database: {table_config['database']}\n"
                    body += f"Source Table: {table_name}\n"
                    body += f"Source Column: {unique_key}\n\n"
                    body += f"Total number of rows = {len(duplicate_results)}\n\n"

                    for duplicate_count, row_key in duplicate_results:
                        body += f"{row_key} has {duplicate_count} duplicate(s).\n"

                    # Send email alert
                    send_email_alert(EMAIL_SUBJECT, body)
                    # Send Slack notification
                    send_slack_notification(body)
                else:
                    print(f"No duplicates found in {table_config['database']}.{table_name} at {dt.now()}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

# Define the DAG
dag = DAG(
    'redshift_duplicate_alert',
    description='A DAG to send email alerts for duplicate entries in Redshift tables',
    schedule_interval='0 6 * * 1',  # Every Monday at 6 AM
    start_date=dt(2024, 3, 19),
    catchup=False
)

# Define the task
send_duplicate_alert_task = PythonOperator(
    task_id='redshift_duplicate_alert',
    python_callable=get_duplicates_and_alert,
    dag=dag
) 

