#!/usr/bin/python

import boto3
import sys
import urlparse
from botocore.client import Config

if len(sys.argv) == 3:
    # topic name as first argument
    topic_name = sys.argv[1]
    # region name as second argument
    region_name = sys.argv[2]
elif len(sys.argv) == 2:
    # topic name as first argument
    topic_name = sys.argv[1]
    region_name = ""
else:
    print 'Usage: ' + sys.argv[0] + ' <topic name> [region name]'
    sys.exit(1)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('sns',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        region_name=region_name,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3'))

# to see the list of available "regions" use:
# radosgw-admin realm zonegroup list

# this is standard AWS services call, using custom attributes to add AMQP endpoint information to the topic

endpoint_args = 'push-endpoint=amqp://127.0.0.1:5672&amqp-exchange=ex1&amqp-ack-level=broker'
attributes = {nvp[0] : nvp[1] for nvp in urlparse.parse_qsl(endpoint_args, keep_blank_values=True)}

print client.create_topic(Name=topic_name, Attributes=attributes)
