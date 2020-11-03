#!/usr/bin/python

import boto3
import sys
from botocore.client import Config

if len(sys.argv) == 3:
    # topic arn as first argument
    topic_arn = sys.argv[1]
    # region name as second argument
    region_name = sys.argv[2]
elif len(sys.argv) == 2:
    # topic arn as first argument
    topic_arn = sys.argv[1]
    region_name = ""
else:
    print 'Usage: ' + sys.argv[0] + ' <topic arn> [region name]'
    sys.exit(1)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('sns',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
        config=Config(signature_version='s3'))

# to see the list of available "regions" use:
# radosgw-admin realm zonegroup list

# getting a list of topics is an extension to AWS S3 API

print client.list_topics(TopicArn=topic_arn)
