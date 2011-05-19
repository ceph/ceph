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
obsync_test.py: a system test for obsync
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from sys import stderr
import ConfigParser
import atexit
import boto
import os
import shutil
import subprocess
import random
import sys
import tempfile
import xattr

global opts
global tdir
global nonce
nonce = 0

###### Constants #######
ACL_XATTR = "rados.acl"
CONTENT_TYPE_XATTR = "rados.content_type"

###### Helper functions #######
def get_nonce():
    global nonce
    if (opts.deterministic_nonce):
        nonce = nonce + 1
        return nonce
    else:
        return random.randint(9999, 99999)

def read_config():
    config = {}
    cfg = ConfigParser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError('To run tests, point environment ' + \
            'variable S3TEST_CONF to a config file.')
    with file(path) as f:
        cfg.readfp(f)

    for section in cfg.sections():
        try:
            (type_, name) = section.split(None, 1)
        except ValueError:
            continue
        if type_ != 's3':
            continue
        # TODO: support 'port', 'is_secure'

        config[name] = {}
        for var in [ 'access_key', 'host', 'secret_key', 'user_id',
                     'display_name', 'email', 'consistency', ]:
            try:
                config[name][var] = cfg.get(section, var)
            except ConfigParser.NoOptionError:
                pass
        # Make sure connection works
        try:
            conn = boto.s3.connection.S3Connection(
                aws_access_key_id = config[name]["access_key"],
                aws_secret_access_key = config[name]["secret_key"],
                host = config[name]["host"],
                # TODO support & test all variations
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                )
        except Exception, e:
            print >>stderr, "error initializing connection!"
            raise

        # Create bucket name
        try:
            template = cfg.get('fixtures', 'bucket prefix')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            template = 'test-{random}-'
        random.seed()
        try:
            config[name]["bucket_name"] = \
                template.format(random=get_nonce())
        except:
            print >>stderr, "error parsing bucket prefix template"
            raise
    return config

def obsync(src, dst, misc):
    env = {}
    full = ["./obsync"]
    if (isinstance(src, str)):
        full.append(src)
    else:
        src.to_src(env, full)
    if (isinstance(dst, str)):
        full.append(dst)
    else:
        dst.to_dst(env, full)
    full.extend(misc)
    if (opts.more_verbose):
        for k,v in env.items():
            print str(k) + "=" + str(v) + " ",
        print
        for f in full:
            print f,
        print
    return subprocess.call(full, stderr=opts.error_out, env=env)

def obsync_check(src, dst, opts):
    ret = obsync(src, dst, opts)
    if (ret != 0):
        raise RuntimeError("call to obsync failed!")

def cleanup_tempdir():
    if tdir != None and opts.keep_tempdir == False:
        shutil.rmtree(tdir)

def compare_directories(dir_a, dir_b, expect_same = True):
    if (opts.verbose):
        print "comparing directories %s and %s" % (dir_a, dir_b)
    full = ["diff", "-q"]
    full.extend(["-r", dir_a, dir_b])
    ret = subprocess.call(full)
    if ((ret == 0) and (not expect_same)):
        print "expected the directories %s and %s to differ, but \
they were the same!" % (dir_a, dir_b)
        raise Exception("compare_directories failed!")
    if ((ret != 0) and expect_same):
        print "expected the directories %s and %s to be the same, but \
they were different!" % (dir_a, dir_b)
        raise Exception("compare_directories failed!")

def count_obj_in_dir(d):
    """counts the number of objects in a directory (WITHOUT recursing)"""
    num_objects = 0
    for f in os.listdir(d):
        num_objects = num_objects + 1
    return num_objects

def xuser(src, dst):
    return [ "--xuser", config[src]["user_id"] + "=" + config[dst]["user_id"]]

def get_optional(h, k):
    if (h.has_key(k)):
        print "found " + str(h[k])
        return h[k]
    else:
        print "found nothing"
        return None

def xattr_sync_impl(file_name, meta):
    xlist = xattr.get_all(file_name, namespace=xattr.NS_USER)
    to_delete = []
    to_set = {}
    for k,v in meta.items():
        to_set[k] = v
    for k,v in xlist:
        if (k == ACL_XATTR):
            continue
        if (not meta.has_key(k)):
            to_delete.append(k)
        elif (meta[k] == v):
            del to_set[k]
    return to_delete, to_set

def xattr_sync(file_name, meta):
    """ Synchronize the xattrs on a file with a hash of our choosing """
    to_delete, to_set = xattr_sync_impl(file_name, meta)
    for k in to_delete:
        xattr.remove(file_name, k)
    for k,v in to_set.items():
        xattr.set(file_name, k, v, namespace=xattr.NS_USER)

def assert_xattr(file_name, meta):
    """ Raise an exception if the xattrs on a file are not what we expect """
    to_delete, to_set = xattr_sync_impl(file_name, meta)
    if (len(to_delete) == 0) and (len(to_set) == 0):
        return
    print "XATTRS DIFFER: ",
    print "EXPECTED: {",
    sep = ""
    for k,v in meta.items():
        print sep + str(k) + " : " + str(v),
        sep = ", "
    print "}",
    print "GOT: {",
    sep = ""
    for k,v in xattr.get_all(file_name, namespace=xattr.NS_USER):
        if (k == ACL_XATTR):
            continue
        print "%s%s:%s" % (sep, k, v),
        sep = ", "
    print "}",
    print
    raise Exception("extended attributes are not what we expect on %s" % file_name)

###### ObSyncTestBucket #######
class ObSyncTestBucket(object):
    def __init__(self, name, url, akey, skey, consistency):
        self.name = name
        self.url = url
        self.akey = akey
        self.skey = skey
        self.consistency = consistency
    def to_src(self, env, args):
        env["SRC_AKEY"] = self.akey
        env["SRC_SKEY"] = self.skey
        args.append(self.url)
    def to_dst(self, env, args):
        env["DST_AKEY"] = self.akey
        env["DST_SKEY"] = self.skey
        args.append(self.url)
        if (self.consistency != None):
            env["DST_CONSISTENCY"] = self.consistency

###### Main #######
# change directory to obsync directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# parse options
parser = OptionParser("""test-obsync.sh
A system test for obsync.

Important environment variables:
S3TEST_CONF: path to the S3-tests configuration file
""")
parser.add_option("-k", "--keep-tempdir", action="store_true",
    dest="keep_tempdir", default=False,
    help="create the destination if it doesn't already exist")
parser.add_option("-v", "--verbose", action="store_true",
    dest="verbose", default=False,
    help="run verbose")
parser.add_option("-V", "--more-verbose", action="store_true", \
    dest="more_verbose", help="be really, really verbose (developer mode)")
parser.add_option("-D", "--deterministic-nonce", action="store_true", \
    dest="deterministic_nonce", help="use a deterministic bucket nonce\
(good for predictability, bad for re-entrancy).")
(opts, args) = parser.parse_args()
if (opts.more_verbose):
    opts.verbose = True

# parse configuration file
config = read_config()
opts.buckets = []
opts.buckets.append(ObSyncTestBucket(config["main"]["bucket_name"], \
    "s3://" + config["main"]["host"] + "/" + config["main"]["bucket_name"], \
    config["main"]["access_key"], config["main"]["secret_key"],
    get_optional(config["main"], "consistency")))
opts.buckets.append(ObSyncTestBucket(config["alt"]["bucket_name"], \
    "s3://" + config["alt"]["host"] + "/" + config["alt"]["bucket_name"], \
    config["alt"]["access_key"], config["alt"]["secret_key"],
    get_optional(config["alt"], "consistency")))

if not config["main"]["user_id"]:
    raise Exception("You must specify a user_id for the main section.")
if not config["alt"]["user_id"]:
    raise Exception("You must specify a user_id for the alt section.")

# set up temporary directory
tdir = tempfile.mkdtemp()
if (opts.verbose):
    print "created temporary directory: %s" % tdir
atexit.register(cleanup_tempdir)

# set up a little tree of files
os.mkdir("%s/dir1" % tdir)
os.mkdir("%s/dir1/c" % tdir)
os.mkdir("%s/dir1/c/g" % tdir)
f = open("%s/dir1/a" % tdir, 'w')
f.write("a")
f.close()
f = open("%s/dir1/b" % tdir, 'w')
f.close()
f = open("%s/dir1/c/d" % tdir, 'w')
f.write("file d!")
f.close()
f = open("%s/dir1/c/e" % tdir, 'w')
f.write("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
f.close()
f = open("%s/dir1/c/f" % tdir, 'w')
f.write("file f.")
f.close()
f = open("%s/dir1/c/g/h" % tdir, 'w')
for i in range(0, 1000):
    f.write("%d." % i)
f.close()

if (opts.more_verbose):
    opts.error_out = sys.stderr
else:
    opts.error_out = open("/dev/null", 'w')

# copy this tree to somewhere else
subprocess.check_call(["cp", "-r", "%s/dir1" % tdir, "%s/dir1a" % tdir])

# make sure it's still the same
compare_directories("%s/dir1" % tdir, "%s/dir1a" % tdir)

# Run unit tests
ret = obsync("", "", ["--unit"])

# we should fail here, because we didn't supply -c
ret = obsync("file://%s/dir1" % tdir, "file://%s/dir2" % tdir, [])
if (ret == 0):
    raise RuntimeError("expected this call to obsync to fail, because \
we didn't supply -c. But it succeeded.")
if (opts.verbose):
    print "first call failed as expected."

# now supply -c and it should work
obsync_check("file://%s/dir1" % tdir, "file://%s/dir2" % tdir, ["-c"])
compare_directories("%s/dir1" % tdir, "%s/dir2" % tdir)

# test the alternate syntax where we leave off the file://, and it is assumed
# because the url begins with / or ./
obsync_check("file://%s/dir1" % tdir, "/%s/dir2" % tdir, ["-c"])

compare_directories("%s/dir1" % tdir, "%s/dir2" % tdir)

if (opts.verbose):
    print "successfully created dir2 from dir1"

if (opts.verbose):
    print "test a dry run between local directories"
os.mkdir("%s/dir1b" % tdir)
obsync_check("file://%s/dir1" % tdir, "file://%s/dir1b" % tdir, ["-n"])
if (count_obj_in_dir("/%s/dir1b" % tdir) != 0):
    raise RuntimeError("error! the dry run copied some files!")

if (opts.verbose):
    print "dry run didn't do anything. good."
obsync_check("file://%s/dir1" % tdir, "file://%s/dir1b" % tdir, [])
compare_directories("%s/dir1" % tdir, "%s/dir1b" % tdir)
if (opts.verbose):
    print "regular run synchronized the directories."

if (opts.verbose):
    print "test running without --delete-after or --delete-before..."
obsync_check("file://%s/dir1b" % tdir, "file://%s/dir1c" % tdir, ["-c"])
os.unlink("%s/dir1b/a" % tdir)
obsync_check("/%s/dir1b" % tdir, "file://%s/dir1c" % tdir, [])
if not os.path.exists("/%s/dir1c/a" % tdir):
    raise RuntimeError("error: running without --delete-after or \
--delete-before still deleted files from the destination!")
if (opts.verbose):
    print "test running _with_ --delete-after..."
obsync_check("/%s/dir1b" % tdir, "file://%s/dir1c" % tdir, ["--delete-after"])
if os.path.exists("/%s/dir1c/a" % tdir):
    raise RuntimeError("error: running with --delete-after \
failed to delete files from the destination!")

# test with --no-preserve-acls
obsync_check("file://%s/dir1" % tdir, "file://%s/dir1b2" % tdir,
            ["--no-preserve-acls", "-c"])


# Create synthetic ACL
obsync_check("file://%s/dir1" % tdir, "file://%s/dira" % tdir, ["-c"])
synthetic_xml1 = \
"<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
<Owner>\n\
<ID>" + config["main"]["user_id"] + "</ID>\n\
<DisplayName></DisplayName>\n\
</Owner>\n\
<AccessControlList>\n\
<Grant>\n\
  <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
xsi:type=\"CanonicalUser\">\n\
    <ID>" + config["main"]["user_id"] + "</ID>\n\
    <DisplayName></DisplayName>\n\
  </Grantee>\n\
  <Permission>FULL_CONTROL</Permission>\n\
</Grant>\n\
</AccessControlList>\n\
</AccessControlPolicy>"
xattr.set("%s/dira/a" % tdir, ACL_XATTR, synthetic_xml1,
            namespace=xattr.NS_USER)
print "set attr on %s" % ("%s/dira/a" % tdir)
# test ACL transformations
# canonicalize xml by parse + write out
obsync_check("file://%s/dira" % tdir, "file://%s/dira2" % tdir,
            ["-d", "-c"] + xuser("main", "alt"))
# test that ACL is preserved
obsync_check("file://%s/dira2" % tdir, "file://%s/dira3" % tdir,
            ["-d", "-c"])
synthetic_xml2 = xattr.get("%s/dira2/a" % tdir, ACL_XATTR,
                        namespace=xattr.NS_USER)
synthetic_xml3 = xattr.get("%s/dira3/a" % tdir, ACL_XATTR,
                        namespace=xattr.NS_USER)
if (synthetic_xml2 != synthetic_xml3):
    raise Exception("xml not preserved across obsync!")
# test ACL transformation
obsync_check("file://%s/dira3" % tdir, "file://%s/dira4" % tdir,
            ["-d", "-c"] + xuser("main", "alt"))
synthetic_xml4 = xattr.get("%s/dira4/a" % tdir, ACL_XATTR,
                        namespace=xattr.NS_USER)
if (synthetic_xml3 != synthetic_xml4):
    raise Exception("user translation was a no-op")
obsync_check("file://%s/dira4" % tdir, "file://%s/dira5" % tdir,
            ["-d", "-c"])
synthetic_xml5 = xattr.get("%s/dira5/a" % tdir, ACL_XATTR,
                        namespace=xattr.NS_USER)
if (synthetic_xml4 != synthetic_xml5):
    raise Exception("xml not preserved across obsync!")
# test ACL transformation back
obsync_check("file://%s/dira5" % tdir, "file://%s/dira6" % tdir,
            ["-d", "-c"] + xuser("alt", "main"))
if (synthetic_xml5 != synthetic_xml2):
    raise Exception("expected to transform XML back to original form \
through a double xuser")

# first, let's empty out the S3 bucket
os.mkdir("%s/empty1" % tdir)
if (opts.verbose):
    print "emptying out " + opts.buckets[0].name
obsync_check("file://%s/empty1" % tdir, opts.buckets[0],
            ["-c", "--delete-after"])

# make sure that the empty worked
obsync_check(opts.buckets[0], "file://%s/empty2" % tdir, ["-c"])
compare_directories("%s/empty1" % tdir, "%s/empty2" % tdir)
if (opts.verbose):
    print "successfully emptied out " + opts.buckets[0].name

if (opts.verbose):
    print "copying the sample directory to " + opts.buckets[0].name
# now copy the sample files to the test bucket
obsync_check("file://%s/dir1" % tdir, opts.buckets[0], [])

# make sure that the copy worked
obsync_check(opts.buckets[0], "file://%s/dir3" % tdir, ["-c"])
compare_directories("%s/dir1" % tdir, "%s/dir3" % tdir)
if (opts.verbose):
    print "successfully copied the sample directory to " + opts.buckets[0].name

# test --follow-symlinks
os.mkdir("%s/sym_test_dir" % tdir)
f = open("%s/sym_test_dir/a" % tdir, 'w')
f.write("a")
f.close()
os.symlink("./a", "%s/sym_test_dir/b" % tdir)
obsync_check("file://%s/sym_test_dir" % tdir,
    "file://%s/sym_test_dir2" % tdir,
    ["-c", "--follow-symlinks"])
os.unlink("%s/sym_test_dir2/a" % tdir)
f = open("%s/sym_test_dir2/b" % tdir, 'r')
whole_file = f.read()
f.close()
if (whole_file != "a"):
    raise RuntimeError("error! unexpected value in %s/sym_test_dir2/b" % tdir)
if (opts.verbose):
    print "successfully copied a directory with --follow-symlinks"

# test escaping
os.mkdir("%s/escape_dir1" % tdir)
f = open("%s/escape_dir1/$$foo" % tdir, 'w')
f.write("$foo")
f.close()
f = open("%s/escape_dir1/blarg$slash" % tdir, 'w')
f.write("blarg/")
f.close()
obsync_check("file://%s/escape_dir1" % tdir, opts.buckets[0], ["-d"])
obsync_check(opts.buckets[0], "file://%s/escape_dir2" % tdir, ["-c"])
compare_directories("%s/escape_dir1" % tdir, "%s/escape_dir2" % tdir)

# some more tests with --no-preserve-acls
obsync_check("file://%s/dir1" % tdir, opts.buckets[0],
            ["--no-preserve-acls"])
obsync_check(opts.buckets[0], "file://%s/dir1_no-preserve-acls" % tdir,
            ["--no-preserve-acls", "-c"])

if (opts.verbose):
    print "copying dir1 to " + opts.buckets[0].name
obsync_check("file://%s/dir1" % tdir, opts.buckets[0], ["--delete-before"])
if (opts.verbose):
    print "copying " + opts.buckets[0].name + " to " + opts.buckets[1].name
obsync_check(opts.buckets[0], opts.buckets[1], ["-c", "--delete-after"] + \
            xuser("main", "alt"))
if (opts.verbose):
    print "copying bucket1 to dir4..."
obsync_check(opts.buckets[1], "file://%s/dir4" % tdir, ["-c"])
compare_directories("%s/dir1" % tdir, "%s/dir4" % tdir)
if (opts.verbose):
    print "successfully copied " + opts.buckets[0].name + " to " + \
        opts.buckets[1].name
if (opts.verbose):
    print "adding another object to " + opts.buckets[1].name
os.mkdir("%s/small" % tdir)
f = open("%s/small/new_thing" % tdir, 'w')
f.write("a new object!!!")
f.close()
obsync_check("%s/small" % tdir, opts.buckets[1], [])
obsync_check(opts.buckets[0], "%s/bucket0_out" % tdir, ["-c"])
obsync_check(opts.buckets[1], "%s/bucket1_out" % tdir, ["-c"])
bucket0_count = count_obj_in_dir("/%s/bucket0_out" % tdir)
bucket1_count = count_obj_in_dir("/%s/bucket1_out" % tdir)
if (bucket1_count != bucket0_count + 1):
    raise RuntimeError("error! expected one extra object in bucket1! \
bucket0_count=%d, bucket1_count=%d" % (bucket0_count, bucket1_count))
if (opts.verbose):
    print "copying bucket0 to bucket1..."
obsync_check(opts.buckets[0], opts.buckets[1], ["-c", "--delete-before"] + \
        xuser("main", "alt"))
obsync_check(opts.buckets[0], "%s/bucket0_out" % tdir, ["--delete-after"])
obsync_check(opts.buckets[1], "%s/bucket1_out" % tdir, ["--delete-after"])
bucket0_count = count_obj_in_dir("/%s/bucket0_out" % tdir)
bucket1_count = count_obj_in_dir("/%s/bucket1_out" % tdir)
if (bucket0_count != bucket1_count):
    raise RuntimeError("error! expected the same number of objects \
in bucket0 and bucket1. bucket0_count=%d, bucket1_count=%d" \
% (bucket0_count, bucket1_count))

# Check that content-type is preserved
os.mkdir("%s/content_type_test" % tdir)
hamfile = "%s/content_type_test/hammy_thing" % tdir
eggfile = "%s/content_type_test/eggy_thing" % tdir
f = open(hamfile, 'w')
f.write("SPAM SPAM SPAM")
f.close()
xattr_sync(hamfile, { CONTENT_TYPE_XATTR : "Ham" })
assert_xattr(hamfile, { CONTENT_TYPE_XATTR : "Ham" })
f = open(eggfile, 'w')
f.write("eggs")
f.close()
xattr_sync(eggfile, { CONTENT_TYPE_XATTR : "Eggs" })
obsync_check("%s/content_type_test" % tdir, opts.buckets[0], ["--delete-after"])
obsync_check(opts.buckets[0], "%s/content_type_test2" % tdir, ["-c"])
assert_xattr("%s/content_type_test2/hammy_thing" % tdir,
        { CONTENT_TYPE_XATTR : "Ham" })
assert_xattr("%s/content_type_test2/eggy_thing" % tdir,
        { CONTENT_TYPE_XATTR : "Eggs" })

# Check that user-defined metadata is preserved
os.mkdir("%s/user_defined_md" % tdir)
sporkfile = "%s/user_defined_md/spork" % tdir
f = open(sporkfile, 'w')
f.write("SPAM SPAM SPAM")
f.close()
xattr_sync(sporkfile,
    { "rados.meta.tines" : "3", "rados.content_type" : "application/octet-stream" })
obsync_check("%s/user_defined_md" % tdir, opts.buckets[0], ["--delete-after"])
obsync_check(opts.buckets[0], "%s/user_defined_md2" % tdir, ["-c"])
assert_xattr("%s/user_defined_md2/spork" % tdir,
    { "rados.meta.tines" : "3", "rados.content_type" : "application/octet-stream" })

sys.exit(0)
