import json
import boto3

def lambda_handler(event, context):
    for record in event['Records']:
        # Extract the message payload from the SNS event record
        message = json.loads(record['Sns']['Message'])
        print("Received message:", message)

        # Extract relevant information from the DMS event message
        dms_event_status = message.get('Event Message', '').split("\n")[0].strip()
        task_id = message.get('Identifier Link', '').strip()
        task_name = message.get('SourceId', '').strip()
        print(f"DMS Event Status: {dms_event_status}")
        print(f"Task ID: {task_id}")
        print(f"Task name: {task_name}")

        # Check if the event status is "failed"
        if dms_event_status == 'Replication task has failed.':
            if task_id and task_name:
                # Perform actions to handle the failed DMS task here
                print("Handling the failed DMS task...")
                resume_failed_task(task_name)
            else:
                print("Failed to extract Task ID or Task Name. Cannot handle the failed DMS task.")

        # If needed, you can add additional checks for specific event categories
        # if message['Event Source'] == 'some-other-category':
        #     # Handle events in "some-other-category"
        #     pass

def resume_failed_task(task_name):
    # Initialize AWS DMS client
    dms_client = boto3.client('dms')

    # Get the task ARN based on the task ID
    filtersDict = {'Name': 'replication-task-id', 'Values': [task_name]}
    response = dms_client.describe_replication_tasks(Filters=[filtersDict])
    if 'ReplicationTasks' in response and len(response['ReplicationTasks']) > 0:
        task_arn = response['ReplicationTasks'][0]['ReplicationTaskArn']

        # Restart the failed task using 'resume-processing' mode
        try:
            response = dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='resume-processing'
            )
            print(f"Resumed DMS task: {task_name}")
        except Exception as e:
            print(f"Error resuming DMS task {task_name}: {e}")
    else:
        print(f"DMS task '{task_name}' not found.")





### Sample event
#{
#  "Records": [
#   {
#     "Sns": {
#       "Message": "{\"Event Source\":\"replication-task\",\"Event Time\":\"2023-07-26 06:36:15.325\",\"Identifier Link\":\"https://console.aws.amazon.com/dms/v2/home?region=eu-west-1#taskDetails/hub4-uba-slave-full-load-plus-cdc \",\"SourceId\":\"hub4-uba-slave-full-load-plus-cdc\",\"Event ID\":\"http://docs.aws.amazon.com/dms/latest/userguide/CHAP_Events.html#DMS-EVENT-0078 \",\"Event Message\":\"Replication task has failed.\\nReason: Last Error  Endpoint initialization failed.\\nTask error notification received from subtask 0, thread 1 [reptask/replicationtask.c:2883] [1020401]\\nServer name must be supplied; Failed while preparing stream component 'st_0_AQIPJWUNGIBZ7LYMMP63ZM23BUGPROUIGPEXCUY'.; Stream component 'st_0_AQIPJWUNGIBZ7LYMMP63ZM23BUGPROUIGPEXCUY' terminated [reptask/replicationtask.c:2891] [1020401]\\n Stop Reason FATAL_ERROR Error Level FATAL.\",\"Notification time\":\"2023-07-25 11:24:46.624\"}"
#     }
#   }
# ]
#}