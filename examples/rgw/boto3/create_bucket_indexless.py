#!/usr/bin/python

import boto3
import sys
import os
from botocore.exceptions import ClientError

if len(sys.argv) != 2:
    print(f'Usage: {sys.argv[0]} <bucket-name>')
    sys.exit(1)

bucketname = sys.argv[1]

# Fetch from environment or fallback to default
endpoint = os.getenv('S3_ENDPOINT', 'http://127.0.0.1:8000')
access_key = os.getenv('S3_ACCESS_KEY', '0555b35654ad1656d804')
secret_key = os.getenv('S3_SECRET_KEY', 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==')

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
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print(f"Bucket '{bucketname}' already exists.")
    else:
        print(f"Failed to create bucket: {e}")
        sys.exit(1)
