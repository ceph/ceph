# Test of mult-tenancy

import json
import sys

from StringIO import StringIO

from boto.s3.connection import S3Connection, OrdinaryCallingFormat

# XXX once we're done, break out the common code into a library module
#     See  https://github.com/ceph/ceph/pull/8646
import test_multi as t

class TestException(Exception):
    pass

#
# Create a traditional user, S3-only, global (empty) tenant
#
def test2(cluster):
    uid = "tester2"
    display_name = "'Test User 2'"
    access_key = "tester2KEY"
    s3_secret = "test3pass"
    cmd = t.build_cmd('--uid', uid,
                    '--display-name', display_name,
                    '--access-key', access_key,
                    '--secret', s3_secret,
                    "user create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: user create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: user create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: user create --uid %s" % uid)
    if outj['user_id'] != uid:
        raise TestException(
            "command: user create --uid %s, returned user_id %s" %
            (uid, outj['user_id']))

#
# Create a tenantized user with --tenant foo
#
def test3(cluster):
    tid = "testx3"
    uid = "tester3"
    display_name = "Test_User_3"
    access_key = "tester3KEY"
    s3_secret = "test3pass"
    cmd = t.build_cmd(
        '--tenant', tid,
        '--uid', uid,
        '--display-name', display_name,
        '--access-key', access_key,
        '--secret', s3_secret,
        "user create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: user create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: user create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: user create --uid %s" % uid)
    tid_uid = "%s$%s" % (tid, uid)
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: user create --uid %s, returned user_id %s" %
            (tid_uid, outj['user_id']))

#
# Create a tenantized user with a subuser
#
# N.B. The aim of this test is not just to create a subuser, but to create
# the key with a separate command, which does not use --tenant, but extracts
# the tenant from the subuser. No idea why we allow this. There was some kind
# of old script that did this.
#
def test4(cluster):
    tid = "testx4"
    uid = "tester4"
    subid = "test4"

    display_name = "Test_User_4"
    cmd = t.build_cmd(
        '--tenant', tid,
        '--uid', uid,
        '--display-name', display_name,
        '--subuser', '%s:%s' % (uid, subid),
        '--key-type', 'swift',
        '--access', 'full',
        "user create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: user create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: user create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: user create --uid %s" % uid)
    tid_uid = "%s$%s" % (tid, uid)
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: user create --uid %s, returned user_id %s" %
            (tid_uid, outj['user_id']))

    # Note that this tests a way to identify a fully-qualified subuser
    # without --tenant and --uid. This is a historic use that we support.
    swift_secret = "test3pass"
    cmd = t.build_cmd(
        '--subuser', "'%s$%s:%s'" % (tid, uid, subid),
        '--key-type', 'swift',
        '--secret', swift_secret,
        "key create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: key create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: key create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: key create --uid %s" % uid)
    tid_uid = "%s$%s" % (tid, uid)
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: key create --uid %s, returned user_id %s" %
            (tid_uid, outj['user_id']))
    # These tests easily can throw KeyError, needs a try: XXX
    skj = outj['swift_keys'][0]
    if skj['secret_key'] != swift_secret:
        raise TestException(
            "command: key create --uid %s, returned swift key %s" %
            (tid_uid, skj['secret_key']))

#
# Access the cluster, create containers in two tenants, verify it all works.
#

def test5_add_s3_key(cluster, tid, uid):
    secret = "%spass" % uid
    if tid:
        tid_uid = "%s$%s" % (tid, uid)
    else:
        tid_uid = uid

    cmd = t.build_cmd(
        '--uid', "'%s'" % (tid_uid,),
        '--access-key', uid,
        '--secret', secret,
        "key create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: key create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: key create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: key create --uid %s" % uid)
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: key create --uid %s, returned user_id %s" %
            (uid, outj['user_id']))
    skj = outj['keys'][0]
    if skj['secret_key'] != secret:
        raise TestException(
            "command: key create --uid %s, returned s3 key %s" %
            (uid, skj['secret_key']))

def test5_add_swift_key(cluster, tid, uid, subid):
    secret = "%spass" % uid
    if tid:
        tid_uid = "%s$%s" % (tid, uid)
    else:
        tid_uid = uid

    cmd = t.build_cmd(
        '--subuser', "'%s:%s'" % (tid_uid, subid),
        '--key-type', 'swift',
        '--secret', secret,
        "key create")
    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: key create --uid %s" % uid)

    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: key create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: key create --uid %s" % uid)
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: key create --uid %s, returned user_id %s" %
            (uid, outj['user_id']))
    # XXX checking wrong thing here (S3 key)
    skj = outj['keys'][0]
    if skj['secret_key'] != secret:
        raise TestException(
            "command: key create --uid %s, returned s3 key %s" %
            (uid, skj['secret_key']))

def test5_make_user(cluster, tid, uid, subid):
    """
    :param tid: Tenant ID string or None for the legacy tenant
    :param uid: User ID string
    :param subid: Subuser ID, may be None for S3-only users
    """
    display_name = "'Test User %s'" % uid

    cmd = ""
    if tid:
        cmd = t.build_cmd(cmd,
            '--tenant', tid)
    cmd = t.build_cmd(cmd,
        '--uid', uid,
        '--display-name', display_name)
    if subid:
        cmd = t.build_cmd(cmd,
            '--subuser', '%s:%s' % (uid, subid),
            '--key-type', 'swift')
    cmd = t.build_cmd(cmd,
        '--access', 'full',
        "user create")

    out, ret = cluster.rgw_admin(cmd, check_retcode=False)
    if ret != 0:
        raise TestException("failed command: user create --uid %s" % uid)
    try:
        outj = json.loads(out)
    except ValueError:
        raise TestException("invalid json after: user create --uid %s" % uid)
    if not isinstance(outj, dict):
        raise TestException("bad json after: user create --uid %s" % uid)
    if tid:
        tid_uid = "%s$%s" % (tid, uid)
    else:
        tid_uid = uid
    if outj['user_id'] != tid_uid:
        raise TestException(
            "command: user create --uid %s, returned user_id %s" %
            (tid_uid, outj['user_id']))

    #
    # For now, this uses hardcoded passwords based on uid.
    # They are all different for ease of debugging in case something crosses.
    #
    test5_add_s3_key(cluster, tid, uid)
    if subid:
        test5_add_swift_key(cluster, tid, uid, subid)

def test5_poke_s3(cluster):

    bucketname = "test5cont1"
    objname = "obj1"

    # Not sure if we like useless information printed, but the rest of the
    # test framework is insanely talkative when it executes commands.
    # So, to keep it in line and have a marker when things go wrong, this.
    print("PUT bucket %s object %s for tenant A (empty)" %
          (bucketname, objname))
    c = S3Connection(
        aws_access_key_id="tester5a",
        aws_secret_access_key="tester5apass",
        is_secure=False,
        host="localhost",
        port = cluster.port,
        calling_format = OrdinaryCallingFormat())

    bucket = c.create_bucket(bucketname)

    key = bucket.new_key(objname)
    headers = { "Content-Type": "text/plain" }
    key.set_contents_from_string("Test5A\n", headers)
    key.set_acl('public-read')

    #
    # Now it's getting interesting. We're logging into a tenantized user.
    #
    print("PUT bucket %s object %s for tenant B" % (bucketname, objname))
    c = S3Connection(
        aws_access_key_id="tester5b1",
        aws_secret_access_key="tester5b1pass",
        is_secure=False,
        host="localhost",
        port = cluster.port,
        calling_format = OrdinaryCallingFormat())

    bucket = c.create_bucket(bucketname)
    bucket.set_canned_acl('public-read')

    key = bucket.new_key(objname)
    headers = { "Content-Type": "text/plain" }
    key.set_contents_from_string("Test5B\n", headers)
    key.set_acl('public-read')

    #
    # Finally, let's fetch a couple of objects and verify that they
    # are what they should be and we didn't get them overwritten.
    # Note that we access one of objects across tenants using the colon.
    #
    print("GET bucket %s object %s for tenants A and B" %
          (bucketname, objname))
    c = S3Connection(
        aws_access_key_id="tester5a",
        aws_secret_access_key="tester5apass",
        is_secure=False,
        host="localhost",
        port = cluster.port,
        calling_format = OrdinaryCallingFormat())

    bucket = c.get_bucket(bucketname)

    key = bucket.get_key(objname)
    body = key.get_contents_as_string()
    if body != "Test5A\n":
        raise TestException("failed body check, bucket %s object %s" %
                            (bucketname, objname))

    bucket = c.get_bucket("test5b:"+bucketname)
    key = bucket.get_key(objname)
    body = key.get_contents_as_string()
    if body != "Test5B\n":
        raise TestException(
            "failed body check, tenant %s bucket %s object %s" %
            ("test5b", bucketname, objname))

    print("Poke OK")


def test5(cluster):
    # Plan:
    # 0. create users tester5a and test5b$tester5b1 test5b$tester5b2
    # 1. create buckets "test5cont" under test5a and test5b
    # 2. create objects in the buckets
    # 3. access objects (across users in container test5b)

    test5_make_user(cluster, None, "tester5a", "test5a")
    test5_make_user(cluster, "test5b", "tester5b1", "test5b1")
    test5_make_user(cluster, "test5b", "tester5b2", "test5b2")

    test5_poke_s3(cluster)


# XXX this parse_args boolean makes no sense. we should pass argv[] instead,
#     possibly empty. (copied from test_multi, correct it there too)
def init(parse_args):

    #argv = []
    #if parse_args:
    #    argv = sys.argv[1:]
    #args = parser.parse_args(argv)

    #rgw_multi = RGWMulti(int(args.num_zones))
    #rgw_multi.setup(not args.no_bootstrap)

    # __init__():
    port = 8001
    clnum = 1  # number of clusters
    clid = 1   # 1-based
    cluster = t.RGWCluster(clid, port)

    # setup():
    cluster.start()
    cluster.start_rgw()

    # The cluster is always reset at this point, so we don't need to list
    # users or delete pre-existing users.

    try:
        test2(cluster)
        test3(cluster)
        test4(cluster)
        test5(cluster)
    except TestException as e:
        cluster.stop_rgw()
        cluster.stop()
        sys.stderr.write("FAIL\n")
        sys.stderr.write("%s\n" % str(e))
        return 1

    # teardown():
    cluster.stop_rgw()
    cluster.stop()
    return 0

def setup_module():
    return init(False)

if __name__ == "__main__":
    sys.exit(init(True))
