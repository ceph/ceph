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

def cleanup_tempdir():
    if tdir != None and opts.keep_tempdir == False:
        shutil.rmtree(tdir)

def compare_directories(dir_a, dir_b):
    if (opts.verbose):
        print "comparing directories %s and %s" % (dir_a, dir_b)
    subprocess.check_call(["diff", "-r", dir_a, dir_b])

# parse options
parser = OptionParser("osync-test.sh")
parser.add_option("-k", "--keep-tempdir", action="store_true",
    dest="keep_tempdir", default=False,
    help="create the destination if it doesn't already exist")
parser.add_option("-v", "--verbose", action="store_true",
    dest="verbose", default=False,
    help="run verbose")
parser.add_option("-L", "--local-only", action="store_true",
    dest="local_only", default=False,
    help="run local tests only (no actual S3)")
(opts, args) = parser.parse_args()

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

dev_null = open("/dev/null", 'w')
#e = { "BOTO_CONFIG" : ("%s/boto_config" % tdir) }

# copy this tree to somewhere else
subprocess.check_call(["cp", "-r", "%s/dir1" % tdir, "%s/dir1a" % tdir])

# make sure it's still the same
compare_directories("%s/dir1" % tdir, "%s/dir1a" % tdir)

# we should fail here, because we didn't supply -c
ret = subprocess.call(["./osync.py", "file://%s/dir1" % tdir,
                "file://%s/dir2" % tdir], stderr=dev_null, env=e)
if (ret == 0):
    raise RuntimeError("expected this call to osync to fail, because \
we didn't supply -c. But it succeeded.")
if (opts.verbose):
    print "first call failed as expected."

# now supply -c and it should work
ret = subprocess.check_call(["./osync.py", "-c", "file://%s/dir1" % tdir,
                "file://%s/dir2" % tdir], stderr=dev_null, env=e)
compare_directories("%s/dir1" % tdir, "%s/dir2" % tdir)
if (opts.verbose):
    print "successfully created dir2 from dir1"

if (not local_only):
    # first, let's empty out the S3 bucket
    ret = subprocess.check_call(["./osync.py", "-c", "--delete-after",
        "file://%s/empty" % tdir, "s3://test-bucket/"], stderr=dev_null, env=e)

    ret = subprocess.check_call(["./osync.py", "-c", "file://%s/dir1" % tdir,
                    "s3://test-bucket/"], stderr=dev_null, env=e)

sys.exit(0)
