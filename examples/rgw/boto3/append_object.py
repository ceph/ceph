#!/usr/bin/python
from __future__ import print_function

import boto3
import sys
import json

def js_print(arg):
    print(json.dumps(arg, indent=2))

if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <bucket> <key>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]
keyname = sys.argv[2]
# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key='0555b35654ad1656d804'
secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

print('deleting object first')
js_print(client.delete_object(Bucket=bucketname, Key=keyname))
print('appending at position 0')
resp = client.put_object(Bucket=bucketname, Key=keyname,
                         Append=True,
                         AppendPosition=0,
                         Body='8letters')

js_print(resp)
append_pos = resp['AppendPosition']
print('appending at position %d' % append_pos)
js_print(client.put_object(Bucket=bucketname, Key=keyname,
                           Append=True,
                           AppendPosition=append_pos,
                           Body='8letters'))
