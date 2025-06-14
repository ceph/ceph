#!/usr/bin/python

import boto3
import sys
import os
from botocore.exceptions import ClientError

if len(sys.argv) != 2:
    print(f'Usage: {sys.argv[0]} <bucket>')
    sys.exit(1)

# Bucket name as first argument
bucketname = sys.argv[1]

# Use AWS standard environment variables
endpoint = os.getenv('AWS_ENDPOINT_URL', 'http://127.0.0.1:8000')
access_key = os.getenv('AWS_ACCESS_KEY_ID', '0555b35654ad1656d804')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==')

client = boto3.client('s3',
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

try:
    client.create_bucket(
        Bucket=bucketname,
        CreateBucketConfiguration={
            'BucketIndex': {
                'Type': 'Indexless'
            }
        }
    )
    print(f"Bucket '{bucketname}' created successfully.")
except ClientError as e:
    print(f"Failed to create bucket: {e}")
    sys.exit(1)
