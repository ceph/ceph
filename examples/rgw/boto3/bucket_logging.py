#!/usr/bin/python

import boto3
import sys

if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <bucket> <target bucket>')
    sys.exit(1)

# bucket name as first argument
bucket = sys.argv[1]
# target bucket name as the 2nd argument
target_bucket = sys.argv[2]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==' # notsecret

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)


# create the target bucket
response = client.create_bucket(Bucket=target_bucket)
print(response)

bucket_logging_conf = {'LoggingEnabled': {
    'TargetBucket': target_bucket,
    'TargetPrefix': 'log/',
    'TargetObjectKeyFormat': {
      'Simple': {}
    },
    'ObjectRollTime': 60,
    'LoggingType': 'Journal',
  }
}

response = client.put_bucket_logging(Bucket=bucket, BucketLoggingStatus=bucket_logging_conf)
print(response)

response = client.get_bucket_logging(Bucket=bucket)
print(response)

