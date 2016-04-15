# Test of mult-tenancy

import json
import sys

# XXX once we're done, break out the common code into a library module
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
    # XXX maybe add some more checking here for keys and such

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
    skj = outj['swift_keys'][0]
    if skj['secret_key'] != swift_secret: 
        raise TestException(
            "command: key create --uid %s, returned swift key %s" %
            (tid_uid, skj['secret_key']))

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

    # The cluster is always reset at this point, so we don't need to list
    # users or delete pre-existing users.

    try:
        test2(cluster)
        test3(cluster)
        test4(cluster)
    except TestException as e:
        cluster.stop()
        sys.stderr.write("FAIL\n")
        sys.stderr.write("%s\n" % str(e))
        return 1

    # teardown():
    cluster.stop()
    return 0

def setup_module():
    return init(False)

if __name__ == "__main__":
    sys.exit(init(True))
