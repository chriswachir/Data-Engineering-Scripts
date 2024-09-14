import psycopg2 as pg
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import requests

# Function to read server configuration
def server_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

# Email server login parameters
email_config = server_config('/app/secrets/r_emailConfig.ini', 'email_config')
smtp_host = email_config['smtp_host']
smtp_port = int(email_config['smtp_port'])
smtp_username = email_config['smtp_username']
smtp_password = email_config['smtp_password']
fromaddr = email_config['sender_email']
toaddr = 'receiver_email'
today = str(datetime.datetime.now())
subject = "Data Lake Data Fetch Failure Alert"
msg = MIMEMultipart("alternative")
msg['From'] = fromaddr
msg['To'] = toaddr
msg['Subject'] = subject + " " + today

# Define email server login parameters
#server = smtplib.SMTP_SSL(smtp_host, smtp_port)
#server.login(smtp_username, smtp_password)

# Get Redshift server details
redshift_details = server_config('/path/r_validation.ini', 'redshift')
host = redshift_details["host"]
dbname = redshift_details["database"]
user = redshift_details["user"]
password = redshift_details["password"]
port = redshift_details["port"]

# Get Slack webhook URL
slack_webhook_url = email_config['slack_webhook_url']

def fetch_insert_lake_data():
    source = 'redshift_schema_table'
    config = server_config('/path/r_validation.ini', 'redshift')
    

    conn = None
    try:
        # Connect to the Redshift database
        params = config
        conn = pg.connect(**params)
        cur = conn.cursor()

        today = datetime.datetime.now()
        first_of_month = today.replace(day=1)
        yesterday = today - timedelta(days=1)
        first_of_month_str = first_of_month.strftime("%Y-%m-%d")
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")

        if today.day == 1:
            last_month_end = (first_of_month - timedelta(days=1)).strftime("%Y-%m-%d")
            last_month_start = (first_of_month - timedelta(days=(first_of_month - timedelta(days=1)).day)).strftime("%Y-%m-%d")
            delete_query = f"""
            DELETE FROM schema.table
            WHERE pull_date >= '{last_month_start}' AND pull_date <= '{last_month_end}'
            AND source = '{source}';
            """
            cur.execute(delete_query)
            conn.commit()
            print(f"Deleted existing data for the range {last_month_start} to {last_month_end}.")
            data_start_point = last_month_start
            data_end_point = last_month_end
        elif today.day == 2:
            data_start_point = first_of_month.strftime("%Y-%m-%d")
            data_end_point = data_start_point
        else:
            data_start_point = first_of_month.strftime("%Y-%m-%d")
            day_before_yesterday = today - timedelta(days=2)
            day_before_yesterday_str = day_before_yesterday.strftime("%Y-%m-%d")
            delete_query = f"""
            DELETE FROM schema.table
            WHERE pull_date >= '{data_start_point}' AND pull_date <= '{day_before_yesterday_str}'
            AND source = '{source}';
            """
            cur.execute(delete_query)
            conn.commit()
            print(f"Deleted existing data for the range {data_start_point} to {day_before_yesterday_str}.")
            data_end_point = yesterday_str

        print(f"Using date range from {data_start_point} to {data_end_point}")

        q_fetch_data = f"""
               query

        """

        # Execute the query
        cur.execute(q_fetch_data)
        results = cur.fetchall()  # Fetch the data from Redshift
        print(f"Fetched {len(results)} rows.")

        # Insert data into Redshift in chunks
        insert_query = """
            INSERT INTO schema.table (
                pull_date, source, 
                paymentid, serviceid, clientid, paymentdate, countryid, currencyid,
                invoiceid, requestlogid, networkid, time_id, payment_amountpaid,
                msisdn, req_overallstatus, req_paymentpushedstatus, req_numberofsends,
                req_overallsettstatus, req_overallstatushistory, req_paymentpusheddesc,
                req_dateescalated, req_datecreated, req_paymentnextsend,
                req_paymentfirstsend, req_paymentlastsend, statusfirstsend,
                statuslastsend, statusnextsend, statuspusheddesc, statuspushed,
                requestoriginid, accountnumber, chargeamount, receiptnumber,
                receivernarration, paymentackdate, payertransactionid, customername,
                payernarration, extradata, datecreated, chargerequestid,
                paymentmode, beep_transactionid, originator_clientid,
                ins_costaccountnumber, storecode, countercode, paybillnumber,
                request_origin, row_num
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """

        # Iterate over the fetched results and insert each row dynamically
        rows_inserted = 0  # Initialize the counter for inserted rows
        print(f"Inserting data to redshift")

        for data in results:
            try:
                # Prepare the data values for insertion
                data_values = (today_str, source, *data)  # Include today_str and source

                
                # Execute the insert query
                cur.execute(insert_query, data_values)
                
                # Increment the rows inserted counter
                rows_inserted += 1
            except Exception as e:
                error_msg = f"Error inserting data: {e}"  # Initialize error_msg here
                print(error_msg)
                print(f"Problematic data: {data}")  # Print the problematic data
                print(f"Data types: {[type(item) for item in data]}")  # Print data types
                print(f"Insert query: {insert_query}")  # Print the insert query
                break  # Exit the loop after handling the error


        print(f"Inserted {rows_inserted} rows successfully.")



        # Commit the transaction after processing all rows
        conn.commit()
        print(f"Inserted {rows_inserted} rows into Redshift.")

        
        
    except Exception as err:
        # Handle exceptions and send email alert
        dag_id = 'dag_id'  # Define the DAG ID
        timestamp = datetime.datetime.now().isoformat()  # Get the current timestamp
        print(f"Data lake data fetch failed with error ({str(err)})")
        print(f"Sending email alert............ {str(datetime.datetime.now())}")

        # Prepare the error message
        text = (
            f"An error occurred when fetching and inserting data into Redshift at {timestamp}\n"
            f"DAG ID: {dag_id}\n"
            f"Source: {source}\n"
            f"Error: {err}"
        )
        
        # Set the email subject
        subject = f"Error in DAG: {dag_id} at {timestamp}"

        # Initialize email message
        msg = MIMEMultipart("alternative")  # Initialize msg here
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = subject
        msg.attach(MIMEText(text, "plain"))  # Attach the error message

        try:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
            server.login(smtp_username, smtp_password)
            server.sendmail(fromaddr, toaddr, msg.as_string())
            server.quit()  # Ensure server is closed
        except Exception as email_err:
            print(f"Failed to send email alert: {email_err}")

        # Send Slack alert
        slack_message = {
            "text": text
        }
        try:
            response = requests.post(slack_webhook_url, data=json.dumps(slack_message), headers={'Content-Type': 'application/json'})
            if response.status_code != 200:
                print(f"Failed to send Slack alert: {response.status_code}, {response.text}")
        except Exception as slack_err:
            print(f"Failed to send Slack alert: {slack_err}")
    finally:
        # Close the database connection
        if conn is not None:
            conn.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_id',
    default_args=default_args,
    description='Fetch and insert data into Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1
)

# Define the task
fetch_and_insert_task = PythonOperator(
    task_id='fetch_insert_yoda_lake_data',
    python_callable=fetch_insert_lake_data,
    dag=dag,
)

