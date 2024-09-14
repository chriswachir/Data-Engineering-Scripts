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
repl_instance_details = server_config('/path/iac_config.ini','create_repl_instance')
repl_id = repl_instance_details["repl_id"]
storage = repl_instance_details["storage"]
storage = int(storage)
repl_class = repl_instance_details["repl_class"]
repl_engine_version = repl_instance_details["repl_engine_version"]
tag_key = repl_instance_details["tag_key"]
tag_value = repl_instance_details["tag_value"]
tag_arn = repl_instance_details["tag_arn"]
subnet_id = repl_instance_details["subnet_id"]
repl_engine_version = repl_instance_details["repl_engine_version"]
sg_id = repl_instance_details["sg_id"]

def create_repl_instance():
    try:
        # Define dms client
        client = boto3.client('dms')
        response = client.create_replication_instance(
                    ReplicationInstanceIdentifier=repl_id,
                    AllocatedStorage=storage,
                    ReplicationInstanceClass=repl_class,
                    VpcSecurityGroupIds=[
                        sg_id
                    ],
                    ReplicationSubnetGroupIdentifier=subnet_id,
                    PreferredMaintenanceWindow='',
                    MultiAZ=True,
                    EngineVersion=repl_engine_version,
                    AutoMinorVersionUpgrade=True,
                    Tags=[
                        {
                            'Key': tag_key,
                            'Value': tag_value,
                            'ResourceArn': tag_arn
                        },
                    ],
                    KmsKeyId='',
                    PubliclyAccessible=False,
                    DnsNameServers='',
                    ResourceIdentifier=''
                )
        print("Replication instance " + str(repl_id) + " created successfully with below response:\n" + str(response))

    except Exception as err:
            print ("Replication Instance creation failed with error ( " + str(err) + " )")
def main():
    start_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n........Starting Replication Instance creation..................",start_time,"\n")
    create_repl_instance()
    end_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n.......Finished Replication Instance creation.......",end_time,"\n")
if __name__ == '__main__':
    main()