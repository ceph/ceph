#!/usr/bin/python
import sys
import socket
import boto
import boto.s3.connection

inkeyfile = sys.argv[1]

access_key = ""
with open(inkeyfile) as fd_read:
    access_key = fd_read.read().strip()

boto.config.add_section('s3')
boto.config.set('s3', 'use-sigv2', 'True')
conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = '',
    host = socket.getfqdn(),
    port = 7280,
    is_secure=False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

bucket = conn.create_bucket('testuser-new-bucket')
for bucket in conn.get_all_buckets():
    print "{name}\t{created}".format(
        name = bucket.name,
        created = bucket.creation_date,
)

