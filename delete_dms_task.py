import datetime
from datetime import date
from datetime import timedelta
from configparser import ConfigParser
import boto3

import time
from configparser import ConfigParser
import sys

#Config function
def server_config(filename, section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

#Get variable details from config file
dms_task_details = server_config('/path/iac_config.ini','delete_dms_task')
task_arn = dms_task_details["task_arn"]

def delete_dms_task():
    try:
        # Define dms client
        client = boto3.client('dms')
        response = client.delete_replication_task(
            ReplicationTaskArn=task_arn
        )
        print("DMS Task " + str(task_arn) + " deleted successfully with below response\n" + str(response))

    except Exception as err:
            print ("DMS Task deletion failed with error ( " + str(err) + " )")
def main():
    start_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n........Starting DMS Task deletion..................",start_time,"\n")
    delete_dms_task()
    end_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n.......Finished DMS Task deletion.......",end_time,"\n")
if __name__ == '__main__':
    main()