#!/usr/bin/python

import boto3
import sys

if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <bucket> <notification>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]
    # notification name as second argument
notification_name = sys.argv[2]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

# getting a specific notification configuration is an extension to AWS S3 API

print(client.get_bucket_notification_configuration(Bucket=bucketname,
                                                   Notification=notification_name))
