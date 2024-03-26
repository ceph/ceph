#!/usr/bin/python

import boto3
import sys

if len(sys.argv) != 2:
    print('Usage: ' + sys.argv[0] + ' <bucket>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

# getting an unordered list of objects is an extension to AWS S3 API

print(client.list_objects(Bucket=bucketname, AllowUnordered=True))
