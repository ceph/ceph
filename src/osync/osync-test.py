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
osync_test.py: a system test for osync
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

###### Helper functions #######
def getenv(e):
    if os.environ.has_key(e):
        return os.environ[e]
    else:
        return None

def osync(src, dst, misc):
    full = ["./osync.py"]
    e = {}
    if (isinstance(src, OsyncTestBucket)):
        full.append(src.url)
        e["SRC_AKEY"] = src.akey
        e["SRC_SKEY"] = src.skey
    else:
        full.append(src)
    if (isinstance(dst, OsyncTestBucket)):
        full.append(dst.url)
        e["DST_AKEY"] = dst.akey
        e["DST_SKEY"] = dst.skey
    else:
        full.append(dst)
    full.extend(misc)
    return subprocess.call(full, stderr=opts.error_out, env=e)

def osync_check(src, dst, opts):
    ret = osync(src, dst, opts)
    if (ret != 0):
        raise RuntimeError("call to osync failed!")

def cleanup_tempdir():
    if tdir != None and opts.keep_tempdir == False:
        shutil.rmtree(tdir)

def compare_directories(dir_a, dir_b):
    if (opts.verbose):
        print "comparing directories %s and %s" % (dir_a, dir_b)
    subprocess.check_call(["diff", "-r", dir_a, dir_b])

###### OsyncTestBucket #######
class OsyncTestBucket(object):
    def __init__(self, url, akey, skey):
        self.url = url
        self.akey = akey
        self.skey = skey

###### Main #######
# change directory to osync directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# parse options
parser = OptionParser("""osync-test.sh
A system test for osync.

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
    opts.buckets.append(OsyncTestBucket(getenv("URL1"), getenv("AKEY1"),
                        getenv("SKEY1")))
    if (opts.verbose):
        print "have scratch1_url: will test bucket transfers"
else:
    opts.buckets.append(OsyncTestBucket(getenv("URL1"), getenv("AKEY1"),
                        getenv("SKEY1")))
    opts.buckets.append(OsyncTestBucket(getenv("URL2"), getenv("AKEY2"),
                getenv("SKEY2")))
    if (opts.verbose):
        print "have both scratch1_url and scratch2_url: will test \
bucket-to-bucket transfers."

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

# we should fail here, because we didn't supply -c
ret = subprocess.call(["./osync.py", "file://%s/dir1" % tdir,
                "file://%s/dir2" % tdir], stderr=opts.error_out)
if (ret == 0):
    raise RuntimeError("expected this call to osync to fail, because \
we didn't supply -c. But it succeeded.")
if (opts.verbose):
    print "first call failed as expected."

# now supply -c and it should work
ret = subprocess.check_call(["./osync.py", "-c", "file://%s/dir1" % tdir,
                "file://%s/dir2" % tdir], stderr=opts.error_out)
compare_directories("%s/dir1" % tdir, "%s/dir2" % tdir)

# test the alternate syntax where we leave off the file://, and it is assumed
# because the url begins with / or ./
ret = subprocess.check_call(["./osync.py", "-c", "file://%s/dir1" % tdir,
                "/%s/dir2" % tdir], stderr=opts.error_out)
compare_directories("%s/dir1" % tdir, "%s/dir2" % tdir)

if (opts.verbose):
    print "successfully created dir2 from dir1"

if (opts.verbose):
    print "test a dry run between local directories"
os.mkdir("%s/dir1b" % tdir)
osync_check("file://%s/dir1" % tdir, "file://%s/dir1b" % tdir, ["-n"])
for f in os.listdir("/%s/dir1b" % tdir):
    raise RuntimeError("error! the dry run copied some files!")
if (opts.verbose):
    print "dry run didn't do anything. good."
osync_check("file://%s/dir1" % tdir, "file://%s/dir1b" % tdir, [])
compare_directories("%s/dir1" % tdir, "%s/dir1b" % tdir)
if (opts.verbose):
    print "regular run synchronized the directories."

if (opts.verbose):
    print "test running without --delete-after or --delete-before..."
osync_check("file://%s/dir1b" % tdir, "file://%s/dir1c" % tdir, ["-c"])
os.unlink("%s/dir1b/a" % tdir)
osync_check("/%s/dir1b" % tdir, "file://%s/dir1c" % tdir, [])
if not os.path.exists("/%s/dir1c/a" % tdir):
    raise RuntimeError("error: running without --delete-after or \
--delete-before still deleted files from the destination!")
if (opts.verbose):
    print "test running _with_ --delete-after..."
osync_check("/%s/dir1b" % tdir, "file://%s/dir1c" % tdir, ["--delete-after"])
if os.path.exists("/%s/dir1c/a" % tdir):
    raise RuntimeError("error: running with --delete-after \
failed to delete files from the destination!")

if (len(opts.buckets) >= 1):
    # first, let's empty out the S3 bucket
    os.mkdir("%s/empty1" % tdir)
    if (opts.verbose):
        print "emptying out bucket1..."
    osync_check("file://%s/empty1" % tdir, opts.buckets[0],
                ["-c", "--delete-after"])

    # make sure that the empty worked
    osync_check(opts.buckets[0], "file://%s/empty2" % tdir, ["-c"])
    compare_directories("%s/empty1" % tdir, "%s/empty2" % tdir)
    if (opts.verbose):
        print "successfully emptied out the bucket."

    if (opts.verbose):
        print "copying the sample directory to the test bucket..."
    # now copy the sample files to the test bucket
    osync_check("file://%s/dir1" % tdir, opts.buckets[0], [])

    # make sure that the copy worked
    osync_check(opts.buckets[0], "file://%s/dir3" % tdir, ["-c"])
    compare_directories("%s/dir1" % tdir, "%s/dir3" % tdir)
    if (opts.verbose):
        print "successfully copied the sample directory to the test bucket."

if (len(opts.buckets) >= 2):
    if (opts.verbose):
        print "copying dir1 to bucket0..."
    osync_check("file://%s/dir1" % tdir, opts.buckets[0], ["--delete-before"])
    if (opts.verbose):
        print "copying bucket0 to bucket1..."
    osync_check(opts.buckets[0], opts.buckets[1], ["-c", "--delete-after"])
    if (opts.verbose):
        print "copying bucket1 to dir4..."
    osync_check(opts.buckets[1], "file://%s/dir4" % tdir, ["-c"])
    compare_directories("%s/dir1" % tdir, "%s/dir4" % tdir)
    if (opts.verbose):
        print "successfully copied one bucket to another."

sys.exit(0)
