from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import pymysql
import psycopg2
import sys

# Function to read server configuration
def server_config(filename, section):
    parser = ConfigParser()
    config = {}
    parser.read(filename)
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

# Function to setup email server
def setup_email():
    email_config = server_config('/path/r_emailConfig.ini', 'email_config')
    smtp_host = email_config['smtp_host']
    smtp_port = int(email_config['smtp_port'])
    smtp_username = email_config['smtp_username']
    smtp_password = email_config['smtp_password']
    fromaddr = email_config['sender_email']
    toaddr = 'receiver_email'
    today = str(datetime.datetime.now())
    #subject = "Data Lake Data Fetch Failure Alert"
    msg = MIMEMultipart("alternative")
    msg['From'] = fromaddr
    msg['To'] = toaddr
    #msg['Subject'] = subject + " " + today
    server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    server.login(smtp_username, smtp_password)
    return server, msg, fromaddr, toaddr

# Function to send Slack alerts
def send_slack_alert(message):
    slack_config = server_config('/path/r_emailConfig.ini', 'email_config')
    webhook_url = slack_config['slack_webhook_url']
    payload = {
        "text": message
    }
    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()

def fetch_insert_yoda_lake_data():
    mysql_conn = None
    redshift_conn = None
    source = 'mysql_db'
    chunk_size = 100  # Set the chunk size to a low number to avoid serializable errors

    try:
        # Get MySQL connection details
        yoda_mysql_details = server_config('/path/r_validation.ini', 'hub4_hub_ke_mysql')
        mysql_host = yoda_mysql_details["host"]
        mysql_dbname = yoda_mysql_details["database"]
        mysql_user = yoda_mysql_details["user"]
        mysql_password = yoda_mysql_details["password"]
        mysql_port = int(yoda_mysql_details["port"])

        # Get Redshift connection details
        yoda_redshift_details = server_config('/path/r_validation.ini', 'redshift')
        redshift_host = yoda_redshift_details["host"]
        redshift_dbname = yoda_redshift_details["database"]
        redshift_user = yoda_redshift_details["user"]
        redshift_password = yoda_redshift_details["password"]
        redshift_port = int(yoda_redshift_details["port"])

        # Connect to MySQL
        mysql_params = {
            'host': mysql_host,
            'user': mysql_user,
            'password': mysql_password,
            'database': mysql_dbname,
            'port': mysql_port
        }
        mysql_conn = pymysql.connect(**mysql_params)
        mysql_cur = mysql_conn.cursor()

        # Connect to Redshift
        redshift_params = "dbname='{}' user='{}' password='{}' host='{}' port='{}'".format(
            redshift_dbname, redshift_user, redshift_password, redshift_host, redshift_port
        )
        redshift_conn = psycopg2.connect(redshift_params)
        redshift_cur = redshift_conn.cursor()

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
            redshift_cur.execute(delete_query)
            redshift_conn.commit()

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
            redshift_cur.execute(delete_query)
            redshift_conn.commit()

            data_end_point = yesterday_str

        q_fetch_data = f"""
            SELECT * FROM (
                SELECT
                    CONCAT('KE_', S_PAYMENTS.paymentid) AS PAYMENTID,
                    CONCAT('KE_', S_PAYMENTS.payerclientid) AS CLIENTID,
                    CONCAT('KE_', S_PAYMENTS.serviceid) AS SERVICEID,
                    CONCAT('KE_', S_PAYMENTS.requestlogid) AS REQUESTLOGID,
                    117 AS COUNTRYID,
                    70 AS CURRENCYID,
                    NULL AS NETWORKID,
                    S_PAYMENTS.amountpaid AS PAYMENT_AMOUNTPAID,
                    S_PAYMENTS.chargeamount AS chargeamount,
                    S_PAYMENTS.paymentdate AS PAYMENTDATE,
                    SUBSTRING(S_PAYMENTS.paymentdate, 12, 8) AS TIME_ID,
                    S_PAYMENTS.statuspushed AS STATUSPUSHED,
                    S_PAYMENTS.statuspusheddesc AS STATUSPUSHEDDESC,
                    S_PAYMENTS.statusfirstsend AS STATUSFIRSTSEND,
                    S_PAYMENTS.statuslastsend AS STATUSLASTSEND,
                    S_PAYMENTS.statusnextsend AS STATUSNEXTSEND,
                    S_REQUESTLOGS.invoicenumber AS INVOICEID,
                    S_REQUESTLOGS.accountnumber AS ACCOUNTNUMBER,
                    S_REQUESTLOGS.msisdn AS MSISDN,
                    S_REQUESTLOGS.requestoriginid AS REQUESTORIGINID,
                    S_REQUESTLOGS.overallstatus AS REQ_OVERALLSTATUS,
                    S_REQUESTLOGS.dateescalated AS REQ_DATEESCALATED,
                    S_REQUESTLOGS.datecreated AS REQ_DATECREATED,
                    S_REQUESTLOGS.datemodified AS REQ_DATEMODIFIED,
                    S_REQUESTLOGS.paymentpushedstatus AS REQ_PAYMENTPUSHEDSTATUS,
                    S_REQUESTLOGS.paymentpusheddesc AS REQ_PAYMENTPUSHEDDESC,
                    S_REQUESTLOGS.paymentnextsend AS REQ_PAYMENTNEXTSEND,
                    S_REQUESTLOGS.paymentfirstsend AS REQ_PAYMENTFIRSTSEND,
                    S_REQUESTLOGS.paymentlastsend AS REQ_PAYMENTLASTSEND,
                    S_REQUESTLOGS.numberofsends AS REQ_NUMBEROFSENDS,
                    S_PAYMENTS.customername AS CUSTOMERNAME,
                    LEFT(S_PAYMENTS.payernarration, 210) AS PAYERNARRATION,
                    LEFT(S_PAYMENTS.receivernarration, 210) AS RECEIVERNARRATION,
                    S_PAYMENTS.payertransactionid AS PAYERTRANSACTIONID,
                    S_PAYMENTS.paymentackdate AS PAYMENTACKDATE,
                    S_PAYMENTS.receiptnumber AS RECEIPTNUMBER,
                    LEFT(S_PAYMENTS.extradata, 50) AS EXTRADATA,
                    S_PAYMENTS.datecreated AS DATECREATED,
                    NULL AS CHARGEREQUESTID,
                    S_REQUESTLOGS.overallsettlementstatus AS REQ_OVERALLSETTSTATUS,
                    NULL AS REQ_OVERALLSTATUSHISTORY,
                    S_REQUESTLOGS.accountNumber AS INS_COSTACCOUNTNUMBER,
                    NULL AS STORECODE,
                    NULL AS COUNTERCODE,
                    NULL AS PAYBILLNUMBER,
                    S_REQUESTLOGS.paymentMode AS PAYMENTMODE,
                    CONCAT('KE_', S_REQUESTLOGS.requestLogID) AS BEEP_TRANSACTIONID,
                    NULL AS ORIGINATOR_CLIENTID,
                    R.requestoriginname AS REQUEST_ORIGIN,
                    @row_num := IF(@prev_value = S_PAYMENTS.paymentid, @row_num + 1, 1) AS ROW_NUM,
                    @prev_value := S_PAYMENTS.paymentid
                FROM hub_ke.s_payments AS S_PAYMENTS
                LEFT OUTER JOIN hub_ke.s_requestLogs AS S_REQUESTLOGS
                    ON S_PAYMENTS.requestlogid = S_REQUESTLOGS.requestlogid
                LEFT JOIN hub_ke.requestOrigins AS R
                    ON S_REQUESTLOGS.REQUESTORIGINID = R.REQUESTORIGINID,
                (SELECT @row_num := 0, @prev_value := '') AS r  
                WHERE S_PAYMENTS.datecreated >= '{data_start_point}'
                AND S_PAYMENTS.dateCreated < '{data_end_point}'
            ) AS subquery;
        """

        mysql_cur.execute(q_fetch_data)
        results = mysql_cur.fetchall()
        print(f"Fetched {len(results)} rows from MySQL.")

        if not results:
            print("No data fetched from MySQL.")
            return

        # Insert data into Redshift
        insert_query = """
        INSERT INTO schema.table (
            pull_date, source, paymentid, clientid, serviceid, requestlogid, countryid, currencyid, networkid, payment_amountpaid,
            chargeamount, paymentdate, time_id, statuspushed, statuspusheddesc, statusfirstsend, statuslastsend, statusnextsend,
            invoiceid, accountnumber, msisdn, requestoriginid, req_overallstatus, req_dateescalated, req_datecreated, req_datemodified,
            req_paymentpushedstatus, req_paymentpusheddesc, req_paymentnextsend, req_paymentfirstsend, req_paymentlastsend, req_numberofsends,
            customername, payernarration, receivernarration, payertransactionid, paymentackdate, receiptnumber, extradata, datecreated,
            chargerequestid, req_overallsettstatus, req_overallstatushistory, ins_costaccountnumber, storecode, countercode, paybillnumber,
            paymentmode, beep_transactionid, originator_clientid, request_origin, row_num
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        pull_date = today_str  # Add the pull_date for each record
        for data in results:
            data_values = (
                pull_date, source, data[0], data[1], data[2], data[3], data[4],
                data[5], data[6], data[7], data[8], data[9], data[10], data[11],
                data[12], data[13], data[14], data[15], data[16], data[17],
                data[18], data[19], data[20], data[21], data[22], data[23],
                data[24], data[25], data[26], data[27], data[28], data[29],
                data[30], data[31], data[32], data[33], data[34], data[35],
                data[36], data[37], data[38], data[39], data[40], data[41],
                data[42], data[43], data[44], data[45], data[46], data[47],
                data[48], data[49]
            )
            redshift_cur.execute(insert_query, data_values)

        redshift_conn.commit()
        print(f"Inserted {len(results)} rows into Redshift.")

    except (Exception, pymysql.MySQLError, psycopg2.DatabaseError) as error:
        dag_id = 'mysql_schema_table'
        error_message = (
            f"Data fetch or insert failed. Error: {str(error)}"
            f"DAG ID: {dag_id}\n"
            f"Timestamp: {today}\n"
            
            )
        send_slack_alert(error_message)
        try:
            subject = f"Error in DAG: {dag_id} at {today}"  
            server, msg, fromaddr, toaddr = setup_email(subject)  
            msg.attach(MIMEText(error_message, 'plain'))
            server.sendmail(fromaddr, toaddr, msg.as_string())
            server.quit()
        except Exception as email_error:
            pass
    finally:
        if mysql_conn is not None:
            mysql_conn.close()
        if redshift_conn is not None:
            redshift_conn.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 8, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'mysql_schema_table',
    default_args=default_args,
    description='Fetch and insert data from MySQL to Redshift',
    schedule_interval=timedelta(days=1),
)

# Define the task using PythonOperator
fetch_insert_task = PythonOperator(
    task_id='fetch_insert_yoda_lake_data',
    python_callable=fetch_insert_yoda_lake_data,
    dag=dag,
)
