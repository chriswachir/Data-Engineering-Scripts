
# Add SNS as trigger on lambda

#!/usr/bin/python3.6
import urllib3
import json

http = urllib3.PoolManager()


def lambda_handler(event, context):
    url = "slack webhook url"
    msg = {
        "channel": "#aws_sns_notifications",
        "username": "AWS Notifications and Alarms",
        "text": event["Records"][0]["Sns"]["Message"],
        "icon_emoji": "",
    }

    encoded_msg = json.dumps(msg).encode("utf-8")
    resp = http.request("POST", url, body=encoded_msg)
    print(
        {
            "message": event["Records"][0]["Sns"]["Message"],
            "status_code": resp.status,
            "response": resp.data,
        }
    )