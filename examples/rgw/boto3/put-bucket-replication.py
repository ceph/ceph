#!/usr/bin/python

from __future__ import print_function
from botocore.client import Config
from pprint import pprint

import boto3
import json


# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8101'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(parameter_validation=False))

response = client.put_bucket_replication(
    Bucket='sample-bucket',
    ReplicationConfiguration={
        'Role': '',
        'Rules': [
            {
                'ID': 'sample-bucket-rule',
                "Status": "Enabled",
                "Filter" : { "Prefix": ""},
                "Source": { "Zones": ["zg1-1"] },
                "Destination": {"Zones": ["zg1-1", "zg1-2"], "Bucket": "*"}
            }
        ]
    }
)

pprint(response)

