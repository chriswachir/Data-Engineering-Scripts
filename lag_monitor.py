import psycopg2 as pg
import socket
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from configparser import ConfigParser

# Define default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 19),
}

# Define the DAG
dag = DAG(
    'redshift_lag_check',
    default_args=default_args,
    description='Check Redshift lag and send alerts',
    schedule_interval='*/30 * * * *',
    max_active_runs=1,
    catchup = False
)

# Function to send email using SMTP
def send_email(subject, body, to_email, **kwargs):
    """Send email using SMTP."""
    # Fetch email configuration from XCom
    email_config = kwargs['ti'].xcom_pull(task_ids='fetch_email_config')
    smtp_host = email_config['smtp_host']
    smtp_port = email_config['smtp_port']
    smtp_username = email_config['smtp_username']
    smtp_password = email_config['smtp_password']
    sender_email = email_config['sender_email']

    # Connect to SMTP server
    server = smtplib.SMTP_SSL(smtp_host, smtp_port)

    # Create email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Login to SMTP server and send email
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, to_email, msg.as_string())
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}")
    finally:
        server.quit()

# Function to send Slack notification
def send_slack_notification(message):
    """Send Slack notification."""
    try:
        # Fetch Slack webhook URL from config
        slack_webhook_url = server_config(CONFIG_FILE_NAME, 'slack_config')['slack_webhook_url']
        payload = {'text': message}
        response = requests.post(slack_webhook_url, json=payload)
        if response.status_code != 200:
            print(f"Failed to send Slack notification. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"An error occurred while sending Slack notification: {str(e)}")

# Function to fetch email configuration
def fetch_email_config():
    """Fetch email configuration from the config file."""
    parser = ConfigParser()
    parser.read("/app/secrets/r_emailConfig.ini")
    email_config = dict(parser['email_config'])
    return email_config

# Function to connect to Redshift and check lag
def get_lag_and_alert(**kwargs):
    """Connect to Redshift and check lag for defined tables."""
    config_parser = ConfigParser()
    config_parser.read('/app/secrets/r_lagMonitor.ini')

    redshift_config = server_config('/app/secrets/r_lagMonitor.ini', 'yoda_r_lake')
    conn = None
    try:
        conn = pg.connect(**redshift_config)
        cur = conn.cursor()

        for section in config_parser.sections():
            if section.startswith('yoda_hub'):
                table_config = server_config('/app/secrets/r_lagMonitor.ini', section)

                table_name = table_config['table']
                database_name = table_config['database']
                host_name = table_config['host']
                replication_task = table_config.get('replication_task', None)

                if table_config.get('timezone', 'UTC') == 'UTC':
                    q_get_lag = f"SELECT DATEDIFF(minute, MAX(dateCreated), GETDATE()) FROM {database_name}.{table_name}"

                cur.execute(q_get_lag)
                lag_result = cur.fetchone()
                lag = lag_result[0]

                # Check if lag exceeds thresholds and send alerts
                if int(lag) > 15:
                    subject = f"Redshift Lag Alert: {database_name}.{table_name} at {datetime.now()}"
                    body = f"Redshift has a lag of {lag} minutes for {database_name}.{table_name} at {datetime.now()}.\n\n"
                    body += f"DETAILS: \n"
                    body += f"Source Host: {host_name}\n"
                    body += f"Source Database: {database_name}\n"
                    body += f"Source Table: {table_name}\n"
                    body += f"Replication Task: {replication_task}\n"

                    # Send email and Slack notification
                    send_email(subject, body, to_email="abc@gmail.com", **kwargs)
                    send_slack_notification(body)

                    # Send additional alert if lag exceeds a higher threshold
                    if int(lag) > 240:
                        subject = f"Redshift Lag Alert: {database_name}.{table_name} at {datetime.now()}"
                        body = f"Redshift has a lag of {lag} minutes for {database_name}.{table_name} at {datetime.now()}.\n\n"
                        body += f"DETAILS: \n"
                        body += f"Source Host: {host_name}\n"
                        body += f"Source Database: {database_name}\n"
                        body += f"Source Table: {table_name}\n"
                        body += f"Replication Task: {replication_task}\n"

                        # Send email and Slack notification
                        send_email(subject, body, to_email="abc@gmail.com", **kwargs)
                        send_slack_notification(body)

        cur.close()
    except Exception as err:
        print(f"Fetching lag from Redshift failed with error ({err})")
    finally:
        if conn is not None:
            conn.close()

# Function to read server configuration from a file
def server_config(filename, section):
    """Read server configuration from a file."""
    parser = ConfigParser()
    parser.read(filename)

    if parser.has_section(section):
        return dict(parser.items(section))
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

# Define tasks
with dag:
    fetch_email_config_task = PythonOperator(
        task_id='fetch_email_config',
        python_callable=fetch_email_config
    )

    get_lag_and_alert_task = PythonOperator(
        task_id='get_lag_and_alert',
        python_callable=get_lag_and_alert
    )

    fetch_email_config_task >> get_lag_and_alert_task
