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

from optparse import OptionParser
import atexit
import os
import tempfile
import shutil
import subprocess
import sys

global opts
global tdir
global user1
global user2

###### Helper functions #######
def getenv(e):
    if os.environ.has_key(e):
        return os.environ[e]
    else:
        return None

def obsync(src, dst, misc):
    full = ["./obsync.py"]
    e = {}
    if (isinstance(src, ObSyncTestBucket)):
        full.append(src.url)
        e["SRC_AKEY"] = src.akey
        e["SRC_SKEY"] = src.skey
    else:
        full.append(src)
    if (isinstance(dst, ObSyncTestBucket)):
        full.append(dst.url)
        e["DST_AKEY"] = dst.akey
        e["DST_SKEY"] = dst.skey
    else:
        full.append(dst)
    full.extend(misc)
    if (opts.more_verbose):
        for f in full:
            print f,
        print
    return subprocess.call(full, stderr=opts.error_out, env=e)

def obsync_check(src, dst, opts):
    ret = obsync(src, dst, opts)
    if (ret != 0):
        raise RuntimeError("call to obsync failed!")

def cleanup_tempdir():
    if tdir != None and opts.keep_tempdir == False:
        shutil.rmtree(tdir)

def compare_directories(dir_a, dir_b, ignore_acl = True, expect_same = True):
    if (opts.verbose):
        print "comparing directories %s and %s" % (dir_a, dir_b)
    full = ["diff", "-q"]
    if (ignore_acl):
        full.extend(["-x", "*$acl"])
    full.extend(["-r", dir_a, dir_b])
    ret = subprocess.call(full)
    if ((ret == 0) and (not expect_same)):
        print "expected the directories %s and %s to differ, but \
they were the same!" % (dir_a, dir_b)
        sys.exit(1)
    if ((ret != 0) and expect_same):
        print "expected the directories %s and %s to be the same, but \
they were different!" % (dir_a, dir_b)
        sys.exit(1)

def count_obj_in_dir(d):
    """counts the number of objects in a directory (WITHOUT recursing)"""
    num_objects = 0
    for f in os.listdir(d):
        # skip ACL side files
        if (f.find(r'$acl') != -1):
            continue
        num_objects = num_objects + 1
    return num_objects

###### ObSyncTestBucket #######
class ObSyncTestBucket(object):
    def __init__(self, url, akey, skey):
        self.url = url
        self.akey = akey
        self.skey = skey

###### Main #######
# change directory to obsync directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# parse options
parser = OptionParser("""test-obsync.sh
A system test for obsync.

Important environment variables:
URL1, SKEY1, AKEY1: to set up bucket1 (optional)
URL2, SKEY2, AKEY2: to set up bucket2 (optional)""")
parser.add_option("-k", "--keep-tempdir", action="store_true",
    dest="keep_tempdir", default=False,
    help="create the destination if it doesn't already exist")
parser.add_option("-v", "--verbose", action="store_true",
    dest="verbose", default=False,
    help="run verbose")
parser.add_option("-V", "--more-verbose", action="store_true", \
    dest="more_verbose", help="be really, really verbose (developer mode)")
(opts, args) = parser.parse_args()
if (opts.more_verbose):
    opts.verbose = True

# parse environment
opts.buckets = []
if (not os.environ.has_key("URL1")):
    if (opts.verbose):
        print "no bucket urls were given. Running local tests only."
elif (not os.environ.has_key("URL2")):
    opts.buckets.append(ObSyncTestBucket(getenv("URL1"), getenv("AKEY1"),
                        getenv("SKEY1")))
    if (opts.verbose):
        print "have scratch1_url: will test bucket transfers"
    user1 = getenv("USER1")
    if (user1 == None):
        raise Exception("You must specify a USER1 to use with URL1")
else:
    opts.buckets.append(ObSyncTestBucket(getenv("URL1"), getenv("AKEY1"),
                        getenv("SKEY1")))
    opts.buckets.append(ObSyncTestBucket(getenv("URL2"), getenv("AKEY2"),
                getenv("SKEY2")))
    if (opts.verbose):
        print "have both scratch1_url and scratch2_url: will test \
bucket-to-bucket transfers."

if (len(opts.buckets) == 0):
    print >>sys.stderr, "invalid usage. -h for help."
    sys.exit(1)

user1 = getenv("USER1")
user2 = getenv("USER2")
if (user1 == None):
    raise Exception("You must specify a USER1. If URL1 exists, we will use \
USER1 with it.")
if (user2 == None):
    raise Exception("You must specify a USER2. If URL2 exists, we will use \
USER2 with it.")

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
synthetic_xml = \
"<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
<Owner>\n\
<ID>" + user1 + "</ID>\n\
<DisplayName></DisplayName>\n\
</Owner>\n\
<AccessControlList>\n\
<Grant>\n\
  <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
xsi:type=\"CanonicalUser\">\n\
    <ID>" + user1 + "</ID>\n\
    <DisplayName></DisplayName>\n\
  </Grantee>\n\
  <Permission>FULL_CONTROL</Permission>\n\
</Grant>\n\
</AccessControlList>\n\
</AccessControlPolicy>"
f = open("%s/dira/.a$acl" % tdir, "w")
try:
    f.write(synthetic_xml)
finally:
    f.close()
# test ACL transformations
obsync_check("file://%s/dira" % tdir, "file://%s/dirb" % tdir,
            ["-d", "-c", "--xuser", user1 + "=" + user2])
# The transformation should result in different directories
compare_directories("%s/dira" % tdir, "%s/dirb" % tdir, \
    ignore_acl=False, expect_same=False)
# Test ACL syncing. It should sync the ACLs even when the object data is
# the same!
obsync_check("file://%s/dirb" % tdir, "file://%s/dira" % tdir, ["-d"])
obsync_check("file://%s/dira" % tdir, "file://%s/dirb" % tdir, ["-d"])
compare_directories("%s/dira" % tdir, "%s/dirb" % tdir, \
    ignore_acl=False, expect_same=True)

if (len(opts.buckets) >= 1):
    # first, let's empty out the S3 bucket
    os.mkdir("%s/empty1" % tdir)
    if (opts.verbose):
        print "emptying out bucket1..."
    obsync_check("file://%s/empty1" % tdir, opts.buckets[0],
                ["-c", "--delete-after"])

    # make sure that the empty worked
    obsync_check(opts.buckets[0], "file://%s/empty2" % tdir, ["-c"])
    compare_directories("%s/empty1" % tdir, "%s/empty2" % tdir)
    if (opts.verbose):
        print "successfully emptied out the bucket."

    if (opts.verbose):
        print "copying the sample directory to the test bucket..."
    # now copy the sample files to the test bucket
    obsync_check("file://%s/dir1" % tdir, opts.buckets[0], [])

    # make sure that the copy worked
    obsync_check(opts.buckets[0], "file://%s/dir3" % tdir, ["-c"])
    compare_directories("%s/dir1" % tdir, "%s/dir3" % tdir)
    if (opts.verbose):
        print "successfully copied the sample directory to the test bucket."

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
    # test ACL transformations, again
    obsync_check("file://%s/dirb" % tdir, opts.buckets[0],
                ["-d", "-c", "--xuser", user2 + "=" + user1])

if (len(opts.buckets) >= 2):
    if (opts.verbose):
        print "copying dir1 to bucket0..."
    obsync_check("file://%s/dir1" % tdir, opts.buckets[0], ["--delete-before"])
    if (opts.verbose):
        print "copying bucket0 to bucket1..."
    obsync_check(opts.buckets[0], opts.buckets[1], ["-c", "--delete-after"])
    if (opts.verbose):
        print "copying bucket1 to dir4..."
    obsync_check(opts.buckets[1], "file://%s/dir4" % tdir, ["-c"])
    compare_directories("%s/dir1" % tdir, "%s/dir4" % tdir)
    if (opts.verbose):
        print "successfully copied one bucket to another."
    if (opts.verbose):
        print "adding another object to bucket1..."
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
    obsync_check(opts.buckets[0], opts.buckets[1], ["-c", "--delete-before"])
    obsync_check(opts.buckets[0], "%s/bucket0_out" % tdir, ["--delete-after"])
    obsync_check(opts.buckets[1], "%s/bucket1_out" % tdir, ["--delete-after"])
    bucket0_count = count_obj_in_dir("/%s/bucket0_out" % tdir)
    bucket1_count = count_obj_in_dir("/%s/bucket1_out" % tdir)
    if (bucket0_count != bucket1_count):
        raise RuntimeError("error! expected the same number of objects \
in bucket0 and bucket1. bucket0_count=%d, bucket1_count=%d" \
% (bucket0_count, bucket1_count))

sys.exit(0)
