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
osync.py: the object synchronizer
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from sys import stderr
import boto
import base64
import errno
import hashlib
import mimetypes
import os
import shutil
import string
import sys
import tempfile
import traceback

global opts

###### Helper functions #######
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        if (not os.path.isdir(path)):
            raise

def bytes_to_str(b):
    return ''.join(["%02x"% ord(x) for x in b]).strip()

def get_md5(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return "%s" % md5.hexdigest()

def strip_prefix(prefix, s):
    if not (s[0:len(prefix)] == prefix):
        return None
    return s[len(prefix):]

def etag_to_md5(etag):
    if (etag[:1] == '"'):
        start = 1
    else:
        start = 0
    if (etag[-1:] == '"'):
        end = -1
    else:
        end = None
    return etag[start:end]

def getenv(e):
    if os.environ.has_key(e):
        return os.environ[e]
    else:
        return None

###### NonexistentStore #######
class NonexistentStore(Exception):
    pass

###### Object #######
class Object(object):
    def __init__(self, name, md5, size):
        self.name = name
        self.md5 = md5
        self.size = int(size)
    def equals(self, rhs):
        if (self.name != rhs.name):
            return False
        if (self.md5 != rhs.md5):
            return False
        if (self.size != rhs.size):
            return False
        return True
    @staticmethod
    def from_file(obj_name, path):
        f = open(path)
        try:
            md5 = get_md5(f)
        finally:
            f.close()
        size = os.path.getsize(path)
        #print "Object.from_file: path="+path+",md5=" + bytes_to_str(md5) +",size=" + str(size)
        return Object(obj_name, md5, size)

###### Store #######
class Store(object):
    @staticmethod
    def make_store(url, create, akey, skey):
        s3_url = strip_prefix("s3://", url)
        if (s3_url):
            return S3Store(s3_url, create, akey, skey)
        file_url = strip_prefix("file://", url)
        if (file_url):
            return FileStore(file_url, create)
        if (url[0:1] == "/"):
            return FileStore(url, create)
        if (url[0:2] == "./"):
            return FileStore(url, create)
        raise Exception("Failed to find a prefix of s3://, file://, /, or ./ \
Cannot handle this URL.")
    def __init__(self, url):
        self.url = url

###### S3 store #######
class S3StoreLocalCopy(object):
    def __init__(self, path):
        self.path = path
    def __del__(self):
        self.remove()
    def remove(self):
        if (self.path):
            os.unlink(self.path)
            self.path = None

class S3StoreIterator(object):
    """S3Store iterator"""
    def __init__(self, blrs):
        self.blrs = blrs
    def __iter__(self):
        return self
    def next(self):
        # This will raise StopIteration when there are no more objects to
        # iterate on
        key = self.blrs.next()
        ret = Object(key.name, etag_to_md5(key.etag), key.size)
        return ret

class S3Store(Store):
    def __init__(self, url, create, akey, skey):
        # Parse the s3 url
        host_end = string.find(url, "/")
        if (host_end == -1):
            raise Exception("S3Store URLs are of the form \
s3://host/bucket/key_prefix. Failed to find the host.")
        self.host = url[0:host_end]
        bucket_end = url.find("/", host_end+1)
        if (bucket_end == -1):
            self.bucket_name = url[host_end+1:]
            self.key_prefix = ""
        else:
            self.bucket_name = url[host_end+1:bucket_end]
            self.key_prefix = url[bucket_end+1:]
        if (self.bucket_name == ""):
            raise Exception("S3Store URLs are of the form \
s3://host/bucket/key_prefix. Failed to find the bucket.")
        if (opts.more_verbose):
            print "self.host = '" + self.host + "', ",
            print "self.bucket_name = '" + self.bucket_name + "' ",
            print "self.key_prefix = '" + self.key_prefix + "'"
        self.conn = S3Connection(calling_format=OrdinaryCallingFormat(),
                host=self.host, is_secure=False,
                aws_access_key_id=akey, aws_secret_access_key=skey)
        self.bucket = self.conn.get_bucket(self.bucket_name)
        Store.__init__(self, "s3://" + url)
    def __str__(self):
        return "s3://" + self.host + "/" + self.bucket_name + "/" + self.key_prefix
    def make_local_copy(self, obj):
        k = Key(self.bucket)
        k.key = obj.name
        temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        try:
            k.get_contents_to_filename(temp_file.name)
        except:
            os.unlink(temp_file.name)
            raise
        return S3StoreLocalCopy(temp_file.name)
    def all_objects(self):
        blrs = self.bucket.list(prefix = self.key_prefix)
        return S3StoreIterator(blrs.__iter__())
    def locate_object(self, obj):
        k = self.bucket.get_key(obj.name)
        if (k == None):
            return None
        return Object(obj.name, etag_to_md5(k.etag), k.size)
    def upload(self, local_copy, obj):
        if (opts.more_verbose):
            print "UPLOAD: local_copy.path='" + local_copy.path + "' " + \
                "obj='" + obj.name + "'"
#        mime = mimetypes.guess_type(local_copy.path)[0]
#        if (mime == NoneType):
#            mime = "application/octet-stream"
        k = Key(self.bucket)
        k.key = obj.name
        #k.set_metadata("Content-Type", mime)
        k.set_contents_from_filename(local_copy.path)
    def remove(self, obj):
        self.bucket.delete_key(obj.name)
        if (opts.more_verbose):
            print "S3Store: removed %s" % obj.name

###### FileStore #######
class FileStoreIterator(object):
    """FileStore iterator"""
    def __init__(self, base):
        self.base = base
        self.generator = os.walk(base)
        self.path = ""
        self.files = []
    def __iter__(self):
        return self
    def next(self):
        while True:
            if (len(self.files) == 0):
                self.path, dirs, self.files = self.generator.next()
                continue
            path = self.path + "/" + self.files[0]
            self.files = self.files[1:]
            obj_name = path[len(self.base)+1:]
            if (not os.path.isfile(path)):
                continue
            return Object.from_file(obj_name, path)

class FileStoreLocalCopy(object):
    def __init__(self, path):
        self.path = path
    def remove(self):
        self.path = None

class FileStore(Store):
    def __init__(self, url, create):
        # Parse the file url
        self.base = url
        if (self.base[-1:] == '/'):
            self.base = self.base[:-1]
        if (create):
            mkdir_p(self.base)
        elif (not os.path.isdir(self.base)):
            raise NonexistentStore()
        Store.__init__(self, "file://" + url)
    def __str__(self):
        return "file://" + self.base
    def make_local_copy(self, obj):
        return FileStoreLocalCopy(self.base + "/" + obj.name)
    def all_objects(self):
        return FileStoreIterator(self.base)
    def locate_object(self, obj):
        path = self.base + "/" + obj.name
        found = os.path.isfile(path)
        if (opts.more_verbose):
            if (found):
                print "FileStore::locate_object: found object '" + \
                    obj.name + "'"
            else:
                print "FileStore::locate_object: did not find object '" + \
                    obj.name + "'"
        if (not found):
            return None
        return Object.from_file(obj.name, path)
    def upload(self, local_copy, obj):
        s = local_copy.path
        d = self.base + "/" + obj.name
        #print "s='" + s +"', d='" + d + "'"
        mkdir_p(os.path.dirname(d))
        shutil.copy(s, d)
    def remove(self, obj):
        os.unlink(self.base + "/" + obj.name)
        if (opts.more_verbose):
            print "FileStore: removed %s" % obj.name

###### Functions #######
def delete_unreferenced(src, dst):
    """ delete everything from dst that is not referenced in src """
    if (opts.more_verbose):
        print "handling deletes."
    for dobj in dst.all_objects():
        sobj = src.locate_object(dobj)
        if (sobj == None):
            dst.remove(dobj)

USAGE = """
osync synchronizes objects. The source and destination can both be local or
both remote.

Examples:
# copy contents of mybucket to disk
osync -v s3://myhost/mybucket file://mydir

# copy contents of mydir to an S3 bucket
osync -v file://mydir s3://myhost/mybucket

# synchronize two S3 buckets
SRC_AKEY=foo SRC_SKEY=foo \
DST_AKEY=foo DST_SKEY=foo \
osync -v s3://myhost/mybucket1 s3://myhost/mybucket2

Note: You must specify an AWS access key and secret access key when accessing
S3. osync honors these environment variables:
SRC_AKEY          Access key for the source URL
SRC_SKEY          Secret access key for the source URL
DST_AKEY          Access key for the destination URL
DST_SKEY          Secret access key for the destination URL

If these environment variables are not given, we will fall back on libboto
defaults.

osync (options) [source] [destination]"""

parser = OptionParser(USAGE)
#parser.add_option("-c", "--config-file", dest="config_file", \
    #metavar="FILE", help="set config file")
#parser.add_option("-n", "--dry-run", action="store_true", \
#    dest="dry_run")
parser.add_option("-S", "--source-config",
    dest="source_config", help="boto configuration file to use for the S3 source")
parser.add_option("-D", "--dest-config",
    dest="dest_config", help="boto configuration file to use for the S3 destination")
parser.add_option("-c", "--create-dest", action="store_true", \
    dest="create", help="create the destination if it doesn't already exist")
parser.add_option("--delete-before", action="store_true", \
    dest="delete_before", help="delete objects that aren't in SOURCE from \
DESTINATION before transferring any objects")
parser.add_option("-d", "--delete-after", action="store_true", \
    dest="delete_after", help="delete objects that aren't in SOURCE from \
DESTINATION after doing all transfers.")
parser.add_option("-v", "--verbose", action="store_true", \
    dest="verbose", help="be verbose")
parser.add_option("-V", "--more-verbose", action="store_true", \
    dest="more_verbose", help="be really, really verbose (developer mode)")
(opts, args) = parser.parse_args()
if (len(args) < 2):
    print >>stderr, "Expected two positional arguments: source and destination"
    print >>stderr, USAGE
    sys.exit(1)
elif (len(args) > 2):
    print >>stderr, "Too many positional arguments."
    print >>stderr, USAGE
    sys.exit(1)
if (opts.more_verbose):
    opts.verbose = True
    boto.set_stream_logger("stdout")
    boto.log.info("Enabling verbose boto logging.")
if (opts.delete_before and opts.delete_after):
    print >>stderr, "It doesn't make sense to specify both --delete-before \
and --delete-after."
    sys.exit(1)
src_name = args[0]
dst_name = args[1]
try:
    if (opts.more_verbose):
        print "SOURCE: " + src_name
    src = Store.make_store(src_name, False,
            getenv("SRC_AKEY"), getenv("SRC_SKEY"))
except NonexistentStore as e:
    print >>stderr, "Fatal error: Source " + src_name + " does not exist."
    sys.exit(1)
except Exception as e:
    print >>stderr, "error creating source: " + str(e)
    traceback.print_exc(100000, stderr)
    sys.exit(1)
try:
    if (opts.more_verbose):
        print "DESTINATION: " + dst_name
    dst = Store.make_store(dst_name, opts.create,
            getenv("DST_AKEY"), getenv("DST_SKEY"))
except NonexistentStore as e:
    print >>stderr, "Fatal error: Destination " + dst_name + " does " +\
        "not exist. Run with -c or --create-dest to create it automatically."
    sys.exit(1)
except Exception as e:
    print >>stderr, "error creating destination: " + str(e)
    traceback.print_exc(100000, stderr)
    sys.exit(1)

if (opts.delete_before):
    delete_unreferenced(src, dst)

for sobj in src.all_objects():
    if (opts.more_verbose):
        print "handling " + sobj.name
    dobj = dst.locate_object(sobj)
    upload = False
    if (dobj == None):
        if (opts.verbose):
            print "+ " + sobj.name
        upload = True
    elif not sobj.equals(dobj):
        if (opts.verbose):
            print "> " + sobj.name
        upload = True
    else:
        if (opts.verbose):
            print ". " + sobj.name
    if (upload):
        local_copy = src.make_local_copy(sobj)
        try:
            dst.upload(local_copy, sobj)
        finally:
            local_copy.remove()

if (opts.delete_after):
    delete_unreferenced(src, dst)

if (opts.more_verbose):
    print "finished."

sys.exit(0)
