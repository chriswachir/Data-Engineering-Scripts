import datetime
from datetime import date
from datetime import timedelta
from configparser import ConfigParser
import boto3

import time
from configparser import ConfigParser
import sys
import json
import random

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
dms_task_details = server_config('/path/iac_config.ini','create_dms_task')
task_id = dms_task_details["task_id"]
source = dms_task_details["source_arn"]
target = dms_task_details["target_arn"]
repl_ins = dms_task_details["repl_ins_arn"]
migration_type = dms_task_details["migration_type"]
tag_key = dms_task_details["tag_key"]
tag_value = dms_task_details["tag_value"]
tag_arn = dms_task_details["tag_arn"]

def create_dms_task():
    try:
        # Define dms client
        client = boto3.client('dms')
        #define Table Mappings
        """
            Replace all occurences of true with True, false with False, and null with None for python to interpret
        """
        table_mapping = {
                            "rules": [
                            {
                                "rule-type": "selection",
                                "rule-id": str(random.randint(100000000,999999999)),
                                "rule-name": str(random.randint(100000000,999999999)),
                                "object-locator": {
                                "schema-name": "hub_ke",
                                "table-name": "invoices"
                                },
                                "rule-action": "include",
                                "filters": []
                            },
                            {
                                "rule-type": "transformation",
                                "rule-id": str(random.randint(100000000,999999999)),
                                "rule-name": str(random.randint(100000000,999999999)),
                                "rule-target": "schema",
                                "object-locator": {
                                "schema-name": "%"
                                },
                                "rule-action": "add-prefix",
                                "value": "hub5_db1_ke_aurora_",
                                "old-value": None
                            }
                            ]
                        }
        #define Task settings
        """
            Replace all occurences of true with True, false with False, and None with None for python to interpret
        """
        task_settings = {
                "TargetMetadata": {
                "TargetSchema": "",
                "SupportLobs": True,
                "FullLobMode": False,
                "LobChunkSize": 64,
                "LimitedSizeLobMode": True,
                "LobMaxSize": 32,
                "InlineLobMaxSize": 0,
                "LoadMaxFileSize": 0,
                "ParallelLoadThreads": 0,
                "ParallelLoadBufferSize": 0,
                "BatchApplyEnabled": True,
                "TaskRecoveryTableEnabled": False,
                "ParallelLoadQueuesPerThread": 0,
                "ParallelApplyThreads": 0,
                "ParallelApplyBufferSize": 0,
                "ParallelApplyQueuesPerThread": 0
                },
                "FullLoadSettings": {
                "CreatePkAfterFullLoad": False,
                "StopTaskCachedChangesApplied": False,
                "StopTaskCachedChangesNotApplied": False,
                "MaxFullLoadSubTasks": 8,
                "TransactionConsistencyTimeout": 600,
                "CommitRate": 10000
                },
                "Logging": {
                "EnableLogging": False,
                "EnableLogContext": False,
                "LogComponents": [
                    {
                    "Id": "SOURCE_UNLOAD",
                    "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                    "Id": "SOURCE_CAPTURE",
                    "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                    "Id": "TARGET_LOAD",
                    "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                    "Id": "TARGET_APPLY",
                    "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                    "Id": "TASK_MANAGER",
                    "Severity": "LOGGER_SEVERITY_DEFAULT"
                    }
                ],
                "CloudWatchLogGroup": None,
                "CloudWatchLogStream": None
                },
                "ControlTablesSettings": {
                "ControlSchema": "",
                "HistoryTimeslotInMinutes": 5,
                "HistoryTableEnabled": False,
                "SuspendedTablesTableEnabled": False,
                "StatusTableEnabled": False
                },
                "StreamBufferSettings": {
                "StreamBufferCount": 3,
                "StreamBufferSizeInMB": 8,
                "CtrlStreamBufferSizeInMB": 5
                },
                "ChangeProcessingDdlHandlingPolicy": {
                "HandleSourceTableDropped": True,
                "HandleSourceTableTruncated": True,
                "HandleSourceTableAltered": True
                },
                "ErrorBehavior": {
                "DataErrorPolicy": "LOG_ERROR",
                "DataTruncationErrorPolicy": "LOG_ERROR",
                "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                "DataErrorEscalationCount": 0,
                "TableErrorPolicy": "SUSPEND_TABLE",
                "TableErrorEscalationPolicy": "STOP_TASK",
                "TableErrorEscalationCount": 0,
                "RecoverableErrorCount": -1,
                "RecoverableErrorInterval": 5,
                "RecoverableErrorThrottling": True,
                "RecoverableErrorThrottlingMax": 1800,
                "RecoverableErrorStopRetryAfterThrottlingMax": False,
                "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                "ApplyErrorInsertPolicy": "LOG_ERROR",
                "ApplyErrorUpdatePolicy": "LOG_ERROR",
                "ApplyErrorEscalationPolicy": "LOG_ERROR",
                "ApplyErrorEscalationCount": 0,
                "ApplyErrorFailOnTruncationDdl": False,
                "FullLoadIgnoreConflicts": True,
                "FailOnTransactionConsistencyBreached": False,
                "FailOnNoTablesCaptured": False
                },
                "ChangeProcessingTuning": {
                "BatchApplyPreserveTransaction": True,
                "BatchApplyTimeoutMin": 1,
                "BatchApplyTimeoutMax": 30,
                "BatchApplyMemoryLimit": 500,
                "BatchSplitSize": 0,
                "MinTransactionSize": 1000,
                "CommitTimeout": 1,
                "MemoryLimitTotal": 1024,
                "MemoryKeepTime": 60,
                "StatementCacheSize": 50
                },
                "ValidationSettings": {
                "EnableValidation": False,
                "ValidationMode": "ROW_LEVEL",
                "ThreadCount": 5,
                "FailureMaxCount": 10000,
                "TableFailureMaxCount": 1000,
                "HandleCollationDiff": False,
                "ValidationOnly": False,
                "RecordFailureDelayLimitInMinutes": 0,
                "SkipLobColumns": False,
                "ValidationPartialLobSize": 0,
                "ValidationQueryCdcDelaySeconds": 0,
                "PartitionSize": 10000
                },
                "PostProcessingRules": None,
                "CharacterSetSettings": None,
                "LoopbackPreventionSettings": None,
                "BeforeImageSettings": None,
                "FailTaskWhenCleanTaskResourceFailed": False
            }
        #Create the dms task
        response = client.create_replication_task(
            ReplicationTaskIdentifier=task_id,
            SourceEndpointArn=source,
            TargetEndpointArn=target,
            ReplicationInstanceArn=repl_ins,
            MigrationType=migration_type,
            TableMappings=json.dumps(table_mapping),
            ReplicationTaskSettings=json.dumps(task_settings),
            #CdcStartTime=datetime(2015, 1, 1),
            #CdcStartPosition='string',
            #CdcStopPosition='string',
            Tags=[
                {
                    'Key': tag_key,
                    'Value': tag_value,
                    'ResourceArn': tag_arn
                },
            ],
            #TaskData='string',
            ResourceIdentifier=tag_arn
        )
        print("DMS Task " + str(task_id) + " created successfully with below response:\n" + str(response))

    except Exception as err:
            print ("DMS Task creation failed with error ( " + str(err) + " )")
def main():
    start_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n........Starting DMS Task creation..................",start_time,"\n")
    create_dms_task()
    end_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print ("\n.......Finished DMS Task creation.......",end_time,"\n")
if __name__ == '__main__':
    main()