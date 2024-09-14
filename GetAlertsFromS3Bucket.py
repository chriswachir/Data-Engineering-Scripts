# Add S# as trigger in AWS lambda

import boto3

# Create an SNS client
sns = boto3.client('sns')

# Manually set the topic ARN
topic_arn = 'sns arn'

# Lambda function code
def lambda_handler(event, context):
    # Get the S3 object details from the event
    s3 = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the object is in the desired path
    full_path = f"{bucket_name}/{object_key}"
    if 'path to the bucket' in full_path:
        # Get the size of the object
        object_size = s3.head_object(Bucket=bucket_name, Key=object_key)['ContentLength']
        
        # Check if the object size is less than 43.0 B
        if object_size <= 43.0:
            # Send an alert to the SNS topic
            sns.publish(
                TopicArn=topic_arn,
                Subject=' S3 Object Alert',
                Message='The S3 object {} in bucket {} is empty (43.0 B).'.format(object_key, bucket_name)
            )
            
        else:
          # Send an alert to the SNS topic
            sns.publish(
                TopicArn=topic_arn,
                Subject='S3 Object Alert',
                Message='The S3 object {} in bucket {} was successfully uploaded.'.format(object_key, bucket_name)
            )
            
    # Return a success response
    return {
        'statusCode': 200,
        'body': 'Success'
    }
