#!/usr/bin/python

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
from optparse import OptionParser
from sys import stderr
import boto
import os
import sys

def getenv(a):
    if os.environ.has_key(a):
        return os.environ[a]
    else:
        return None

parser = OptionParser("boto_tool.py: a simple s3 client")
parser.add_option("-c", "--create-bucket",
    dest="create_bucket", help="create the bucket")
parser.add_option("-d", "--delete-bucket",
    dest="delete_bucket", help="delete the bucket")
parser.add_option("--bucket-exists",
    dest="bucket_exists", help="test if a bucket exists")
parser.add_option("-l", "--list-buckets", action="store_true",
    dest="list_buckets", help="list all buckets")
parser.add_option("-L", "--list-buckets-detailed", action="store_true",
    dest="list_buckets_detailed", help="list all buckets with details")
(opts, args) = parser.parse_args()

if (len(args) != 1):
    print "expected one positional argument: a host"
    sys.exit(1)

host = args[0]

conn = S3Connection(calling_format=OrdinaryCallingFormat(), is_secure=False,
                host = host,
                aws_access_key_id=getenv("AKEY"),
                aws_secret_access_key=getenv("SKEY"))
if (opts.create_bucket):
    print "creating bucket '%s' ..." % opts.create_bucket
    bucket = conn.create_bucket(opts.create_bucket)
    print "done."
if (opts.delete_bucket):
    bucket = conn.lookup(opts.delete_bucket)
    if (bucket == None):
        print "bucket '%s' no longer exists" % opts.delete_bucket
        sys.exit(1)
    print "deleting bucket '%s' ..." % opts.delete_bucket
    bucket.delete()
    print "done."
if (opts.bucket_exists):
    bucket = conn.lookup(opts.bucket_exists)
    if (bucket == None):
        print "bucket '%s' does not exist"
        sys.exit(1)
    else:
        print "found bucket '%s'."
if (opts.list_buckets):
    blrs = conn.get_all_buckets()
    for b in blrs:
        print b
if (opts.list_buckets_detailed):
    blrs = conn.get_all_buckets()
    for b in blrs:
        print b.__dict__
