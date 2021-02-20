#!/usr/bin/env python

#
# Ceph - scalable distributed file system
#
# Copyright (C) 2011 New Dream Network
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

"""
boto_del.py: simple bucket deletion program

A lot of common s3 clients can't delete weirdly named buckets.
But this little script can do it!
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from sys import stderr
import boto
import os
import sys

bucket_name = sys.argv[1]
conn = S3Connection(calling_format=OrdinaryCallingFormat(), is_secure=False,
                aws_access_key_id=os.environ["AKEY"],
                aws_secret_access_key=os.environ["SKEY"])
bucket = conn.lookup(bucket_name)
if (bucket == None):
    print("bucket '%s' no longer exists" % bucket_name)
    sys.exit(0)

print("deleting bucket '%s' ..." % bucket_name)
bucket.delete()
print("done.")
sys.exit(0)
