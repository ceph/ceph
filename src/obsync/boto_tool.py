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
boto_tool.py: s3cmd-like tool for operating on s3

A lot of common s3 clients can't handle weird names.
But this little script can do it!
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from sys import stderr
import boto
import os
import string
import sys

global conn

def getenv(a):
    if os.environ.has_key(a):
        return os.environ[a]
    else:
        return None

def strip_prefix(prefix, s):
    if not (s[0:len(prefix)] == prefix):
        return None
    return s[len(prefix):]

def list_all_buckets(args):
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="store_true",
        dest="verbose", default=False, help="verbose output")
    (opts, args) = parser.parse_args(args)
    blrs = conn.get_all_buckets()
    for b in blrs:
        if (opts.verbose):
            print b.__dict__
        else:
            print b

def mkbucket(args):
    if (len(args) < 1):
        print "must give an argument to mkbucket"
        return 255
    bucket_name = args[0]
    print "creating bucket '%s' ..." % bucket_name
    bucket = conn.create_bucket(bucket_name)
    print "done."
    return 0

def rmbucket(args):
    if (len(args) < 1):
        print "must give an argument to rmbucket"
        return 255
    bucket = conn.get_bucket(args[0])
    print "deleting bucket '%s' ..." % args[0]
    bucket.delete()
    print "done."
    return 0

def bucket_exists(args):
    if (len(args) < 1):
        print "must give an argument to exists"
        return 255
    bucket = conn.get_bucket(opts.bucket_exists)
    if (bucket == None):
        print "bucket '%s' does not exist"
        return 1
    else:
        print "found bucket '%s'."
    return 0

def put_obj(bucket_name, args):
    parser = OptionParser()
    parser.add_option("-f", "--filename", dest="filename",
                        help="file name (default stdin)")
    (opts, args) = parser.parse_args(args)
    if (len(args) < 1):
        print "put requires an argument: the object name"
        return 255
    obj_name = args[0]
    print "uploading to bucket: '%s', object name: '%s'" % (bucket_name, obj_name)
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    k.key = obj_name
    if (opts.filename == None):
        print "sorry, no support for put-from-stdin yet. use -f"
        return 255
    else:
        k.set_contents_from_filename(opts.filename)

def get_obj(bucket_name, args):
    parser = OptionParser()
    parser.add_option("-f", "--filename", dest="filename",
                        help="file name (default stdin)")
    (opts, args) = parser.parse_args(args)
    if (len(args) < 1):
        print "get requires an argument: the object name"
        return 255
    obj_name = args[0]
    print "downloading from bucket: '%s', object name: '%s'" % (bucket_name, obj_name)
    bucket = conn.get_bucket(bucket_name)
    k = Key(bucket)
    k.key = obj_name
    if (opts.filename == None):
        k.get_contents_to_file(sys.stdout)
    else:
        k.get_contents_to_filename(opts.filename)

def list_obj(bucket_name, args):
    if (len(args) < 1):
        prefix = None
    else:
        prefix = args[0]
    bucket = conn.get_bucket(bucket_name)
    for key in bucket.list(prefix = prefix):
        print key.name

def rm_obj(bucket_name, args):
    if (len(args) < 1):
        obj_name = None
    else:
        obj_name = args[0]
    print "removing from bucket: '%s', object name: '%s'" % (bucket_name, obj_name)
    bucket = conn.get_bucket(bucket_name)
    bucket.delete_key(obj_name)
    print "done."

def head_obj(bucket_name, args):
    parser = OptionParser()
    parser.add_option("-f", "--filename", dest="filename",
                        help="file name (default stdin)")
    (opts, args) = parser.parse_args(args)
    if (len(args) < 1):
        print "get requires an argument: the object name"
        return 255
    obj_name = args[0]
    print "downloading from bucket: '%s', object name: '%s'" % (bucket_name, obj_name)
    bucket = conn.get_bucket(bucket_name)
    k = bucket.get_key(k, obj_name)
    print k

def usage():
    print """
boto_tool.py
    ./boto_tool.py -h
    ./boto_tool.py --help
        Show this help
    ./boto_tool.py <host> ls
        Lists all buckets in a host
    ./boto_tool.py <host> <bucket> ls
        Lists all objects in a bucket
    ./boto_tool.py <host> <bucket> ls <prefix>
        Lists all objects in a bucket that have a given prefix
    ./boto_tool.py <host> mkbucket <bucket>
        Create a new bucket
    ./boto_tool.py <host> rmbucket <bucket>
        Remove a bucket
    ./boto_tool.py <host> exists <bucket>
        Tests if a bucket exists
    ./boto_tool.py <host> <bucket> put <object> [opts]
        Upload an object
        opts:
            -f filename            file name (default stdin)
    ./boto_tool.py <host> <bucket> get <object> [opts]
        Gets an object
        opts:
            -f filename            file name (default stdout)
    ./boto_tool.py <host> <bucket> head <object> [opts]
        Gets the headers of an object
"""

if (len(sys.argv) < 3):
    usage()
    sys.exit(255)

if (sys.argv[1] == "-h") or (sys.argv[1] == "--help"):
    usage()
    sys.exit(0)

host = sys.argv[1]

conn = S3Connection(calling_format=OrdinaryCallingFormat(), is_secure=False,
                host = host,
                aws_access_key_id=getenv("AKEY"),
                aws_secret_access_key=getenv("SKEY"))

if (sys.argv[2] == "ls"):
    sys.exit(list_all_buckets(sys.argv[3:]))
elif (sys.argv[2] == "mkbucket"):
    sys.exit(mkbucket(sys.argv[3:]))
elif (sys.argv[2] == "rmbucket"):
    sys.exit(rmbucket(sys.argv[3:]))
elif (sys.argv[2] == "exists"):
    sys.exit(bucket_exists(sys.argv[3:]))
else:
    # bucket operations
    wb = strip_prefix("bucket:", sys.argv[2])
    if (wb):
        bucket_name = wb
    else:
        bucket_name = sys.argv[2]
    if (len(sys.argv) < 4):
        print "too few arguments. -h for help."
        sys.exit(255)
    if (sys.argv[3] == "put"):
        sys.exit(put_obj(bucket_name, sys.argv[4:]))
    elif (sys.argv[3] == "get"):
        sys.exit(get_obj(bucket_name, sys.argv[4:]))
    elif (sys.argv[3] == "ls"):
        sys.exit(list_obj(bucket_name, sys.argv[4:]))
    elif (sys.argv[3] == "rm"):
        sys.exit(rm_obj(bucket_name, sys.argv[4:]))
    elif (sys.argv[3] == "head"):
        sys.exit(head_obj(bucket_name, sys.argv[4:]))
    else:
        print "unknown operation on bucket"
        sys.exit(255)

