import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2 as pg
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
from configparser import ConfigParser
import requests  # Import requests for Slack notifications

# Define the receiver email for alerts
receiver_email = 'email@gmail.com'

# Define the path to configuration files
DUPLICATES_CONFIG_FILE = '/app/secrets/r_duplicates.ini'
EMAIL_CONFIG_FILE = '/app/secrets/r_emailConfig.ini'

# Define the email subject for alerts
EMAIL_SUBJECT = "***Redshift Duplicate Alerts***"

# Function to read configuration from a file
def read_config(file_path, section_name):
    config = ConfigParser()
    config.read(file_path)
    return dict(config.items(section_name))

# Function to send email alerts
def send_email(subject, body, to_email):
    email_config = read_config(EMAIL_CONFIG_FILE, 'email_config')

    server = smtplib.SMTP_SSL(email_config['smtp_host'], int(email_config['smtp_port']))
    server.login(email_config['smtp_username'], email_config['smtp_password'])

    msg = MIMEMultipart()
    msg['From'] = email_config['sender_email']
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server.sendmail(email_config['sender_email'], to_email, msg.as_string())
    server.quit()

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

# Main function to remove duplicates from Redshift tables
def remove_duplicates():
    parser = ConfigParser()
    parser.read(DUPLICATES_CONFIG_FILE)

    conn = None
    try:
        # Read database connection parameters from config file
        params = read_config(DUPLICATES_CONFIG_FILE, 'redshift_r_lake')
        conn = pg.connect(**params)
        conn.autocommit = True
        cur = conn.cursor()

        # Iterate over each table configuration in the config file
        for section in parser.sections():
            if section.startswith('redshift_hub'):
                table_config = read_config(DUPLICATES_CONFIG_FILE, section)

                table_name = table_config['table']
                database_name = table_config['database']
                unique_key = table_config['unique_key']

                max_retries = 2
                retry_delay = 10

                # Retry mechanism for handling transient errors
                for _ in range(max_retries):
                    try:
                        # Fetch the column names of the table
                        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{database_name}'")
                        columns = [row[0] for row in cur.fetchall()]
                        column_list = ', '.join(columns)
                        temp_column_list = ', '.join([f't.{col}' for col in columns])

                        # Check for duplicates in the table
                        duplicate_check_query = f"""
                            SELECT {unique_key}
                            FROM {database_name}.{table_name}
                            GROUP BY {unique_key}
                            HAVING COUNT(*) > 1;
                        """
                        cur.execute(duplicate_check_query)
                        duplicate_keys = cur.fetchall()  # Fetch all duplicate keys

                        if not duplicate_keys:
                            print(f"No duplicates found in {database_name}.{table_name}. Skipping...\n")
                            break  # Move to the next table if no duplicates are found

                        print(f"Duplicates found in {database_name}.{table_name} with key '{duplicate_keys[0][0]}'. Processing...")

                        temp_table_name = f"{table_name}_duplicates_{int(time.time())}"  # Unique temp table name

                        # Step 1: Insert duplicates into a temporary table
                        create_temp_table_query = f"""
                            CREATE TABLE {temp_table_name} AS
                            SELECT {column_list}
                            FROM (
                                SELECT
                                    {column_list},
                                    ROW_NUMBER() OVER (PARTITION BY {unique_key} ORDER BY {unique_key}) AS row_num
                                FROM {database_name}.{table_name}
                            ) t
                            WHERE row_num > 1;
                        """
                        cur.execute(create_temp_table_query)

                        # Step 2: Remove duplicates from the main table
                        delete_duplicates_query = f"""
                            DELETE FROM {database_name}.{table_name}
                            USING {temp_table_name}
                            WHERE {database_name}.{table_name}.{unique_key} = {temp_table_name}.{unique_key};
                        """
                        cur.execute(delete_duplicates_query)

                        # Step 3: Insert back the unique rows
                        insert_back_query = f"""
                            INSERT INTO {database_name}.{table_name} ({column_list})
                            SELECT {temp_column_list} FROM {temp_table_name};
                        """
                        cur.execute(insert_back_query)

                        # Step 4: Drop the temporary table
                        drop_temp_table_query = f"DROP TABLE {temp_table_name};"
                        cur.execute(drop_temp_table_query)

                        print(f"Duplicates removed in {database_name}.{table_name} at {dt.datetime.now()} with key: '{duplicate_keys[0][0]}'\n")
                        break  # Move to the next table after successful processing

                    except pg.DatabaseError as e:
                        # Handle database error, rollback and retry
                        print(f"Retrying due to error: {e}")
                        time.sleep(retry_delay)  # Wait before retrying
                        continue

                else:
                    # If all retries fail, send an email alert and Slack notification
                    print(f"All retries failed for {database_name}.{table_name}. Sending email alert...")
                    subject = f"Error in Removing Duplicates: {database_name}.{table_name} at {dt.datetime.now()}"
                    body = f"An error occurred while removing duplicates in {database_name}.{table_name} at {dt.datetime.now()}.\n"
                    body += f"Error Details:\nAll retries failed. Please check the database.\n"
                    send_email(subject, body, receiver_email)
                    send_slack_notification(body)  # Send Slack notification

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()

# Define DAG
dag = DAG(
    'Drop_redshift_duplicates',
    description='A DAG to remove duplicates from Redshift tables and send email alerts if retries fail',
    schedule_interval='0 5 * * *',  # Every day at 5 AM
    start_date=dt.datetime(2024, 3, 19),
    catchup=False
)

# Define the task
remove_duplicates_task = PythonOperator(
    task_id='Drop_redshift_duplicates',
    python_callable=remove_duplicates,
    dag=dag
)
