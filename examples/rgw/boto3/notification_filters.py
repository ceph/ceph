#!/usr/bin/python

import boto3
import sys

if len(sys.argv) != 4:
    print('Usage: ' + sys.argv[0] + ' <bucket> <topic ARN> <notification Id>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]
# topic ARN as second argument
topic_arn = sys.argv[2]
# notification id as third argument
notification_id = sys.argv[3]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

# regex filter on the object name and metadata based filtering are extension to AWS S3 API
# bucket and topic should be created beforehand

topic_conf_list = [{'Id': notification_id, 
                    'TopicArn': topic_arn, 
                    'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
                    'Filter': {
                        'Metadata': {
			    'FilterRules': [{'Name': 'x-amz-meta-foo', 'Value': 'bar'},
                                            {'Name': 'x-amz-meta-hello', 'Value': 'world'}]
                         },
                        'Tags': {
			    'FilterRules': [{'Name': 'foo', 'Value': 'bar'},
                                            {'Name': 'hello', 'Value': 'world'}]
                         },
                         'Key': {
                             'FilterRules': [{'Name': 'regex', 'Value': '([a-z]+)'}]
                         }
                    }}]

print(client.put_bucket_notification_configuration(Bucket=bucketname,
                                                   NotificationConfiguration={'TopicConfigurations': topic_conf_list}))
