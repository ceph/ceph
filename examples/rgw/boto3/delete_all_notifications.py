#!/usr/bin/python

import boto3
import sys

if len(sys.argv) == 2:
    # bucket name as first argument
    bucketname = sys.argv[1]
else:
    print('Usage: ' + sys.argv[0] + ' <bucket>')
    sys.exit(1)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

# an empty notification configuration deletes all notification configurations on a bucket
# this is the standard S3 API, so it does not need the RGW extensions
print(client.put_bucket_notification_configuration(Bucket=bucketname,
                                                   NotificationConfiguration={}))
