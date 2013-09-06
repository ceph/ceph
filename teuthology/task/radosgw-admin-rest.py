# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#

from cStringIO import StringIO
import logging
import json

import boto.exception
import boto.s3.connection
import boto.s3.acl

import requests
import time

from boto.connection import AWSAuthConnection
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def successful_ops(out):
    summary = out['summary']
    if len(summary) == 0:
        return 0
    entry = summary[0]
    return entry['total']['successful_ops']

def rgwadmin(ctx, client, cmd):
    log.info('radosgw-admin: %s' % cmd)
    testdir = teuthology.get_testdir(ctx)
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'radosgw-admin',
        '--log-to-stderr',
        '--format', 'json',
        ]
    pre.extend(cmd)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    proc = remote.run(
        args=pre,
        check_status=False,
        stdout=StringIO(),
        stderr=StringIO(),
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.info(' json result: %s' % j)
        except ValueError:
            j = out
            log.info(' raw result: %s' % j)
    return (r, j)


def rgwadmin_rest(connection, cmd, params=None, headers=None, raw=False):
    log.info('radosgw-admin-rest: %s %s' % (cmd, params))
    put_cmds = ['create', 'link', 'add']
    post_cmds = ['unlink', 'modify']
    delete_cmds = ['trim', 'rm', 'process']
    get_cmds = ['check', 'info', 'show', 'list']

    bucket_sub_resources = ['object', 'policy', 'index']
    user_sub_resources = ['subuser', 'key', 'caps']
    zone_sub_resources = ['pool', 'log', 'garbage']

    def get_cmd_method_and_handler(cmd):
        if cmd[1] in put_cmds:
            return 'PUT', requests.put
        elif cmd[1] in delete_cmds:
            return 'DELETE', requests.delete
        elif cmd[1] in post_cmds:
            return 'POST', requests.post
        elif cmd[1] in get_cmds:
            return 'GET', requests.get

    def get_resource(cmd):
        if cmd[0] == 'bucket' or cmd[0] in bucket_sub_resources:
            if cmd[0] == 'bucket':
                return 'bucket', ''
            else:
                return 'bucket', cmd[0]
        elif cmd[0] == 'user' or cmd[0] in user_sub_resources:
            if cmd[0] == 'user':
                return 'user', ''
            else:
                return 'user', cmd[0]
        elif cmd[0] == 'usage':
            return 'usage', ''
        elif cmd[0] == 'zone' or cmd[0] in zone_sub_resources:
            if cmd[0] == 'zone':
                return 'zone', ''
            else:
                return 'zone', cmd[0]

    """
        Adapted from the build_request() method of boto.connection
    """
    def build_admin_request(conn, method, resource = '', headers=None, data='',
            query_args=None, params=None):

        path = conn.calling_format.build_path_base('admin', resource)
        auth_path = conn.calling_format.build_auth_path('admin', resource)
        host = conn.calling_format.build_host(conn.server_name(), 'admin')
        if query_args:
            path += '?' + query_args
            boto.log.debug('path=%s' % path)
            auth_path += '?' + query_args
            boto.log.debug('auth_path=%s' % auth_path)
        return AWSAuthConnection.build_base_http_request(conn, method, path,
                auth_path, params, headers, data, host)

    method, handler = get_cmd_method_and_handler(cmd)
    resource, query_args = get_resource(cmd)
    request = build_admin_request(connection, method, resource,
            query_args=query_args, headers=headers)

    url = '{protocol}://{host}{path}'.format(protocol=request.protocol,
            host=request.host, path=request.path)

    request.authorize(connection=connection)
    result = handler(url, params=params, headers=request.headers)

    if raw:
        log.info(' text result: %s' % result.txt)
        return result.status_code, result.txt
    else:
        log.info(' json result: %s' % result.json)
        return result.status_code, result.json


def task(ctx, config):
    """
    Test radosgw-admin functionality through the RESTful interface
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task s3tests only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    # just use the first client...
    client = clients[0]

    ##
    admin_user = 'ada'
    admin_display_name = 'Ms. Admin User'
    admin_access_key = 'MH1WC2XQ1S8UISFDZC8W'
    admin_secret_key = 'dQyrTPA0s248YeN5bBv4ukvKU0kh54LWWywkrpoG'
    admin_caps = 'users=read, write; usage=read, write; buckets=read, write; zone=read, write'

    user1 = 'foo'
    user2 = 'fud'
    subuser1 = 'foo:foo1'
    subuser2 = 'foo:foo2'
    display_name1 = 'Foo'
    display_name2 = 'Fud'
    email = 'foo@foo.com'
    access_key = '9te6NH5mcdcq0Tc5i8i1'
    secret_key = 'Ny4IOauQoL18Gp2zM7lC1vLmoawgqcYP/YGcWfXu'
    access_key2 = 'p5YnriCv1nAtykxBrupQ'
    secret_key2 = 'Q8Tk6Q/27hfbFSYdSkPtUqhqx1GgzvpXa4WARozh'
    swift_secret1 = 'gpS2G9RREMrnbqlp29PP2D36kgPR1tm72n5fPYfL'
    swift_secret2 = 'ri2VJQcKSYATOY6uaDUX7pxgkW+W1YmC6OCxPHwy'

    bucket_name = 'myfoo'

    # legend (test cases can be easily grep-ed out)
    # TESTCASE 'testname','object','method','operation','assertion'
    # TESTCASE 'create-admin-user','user','create','administrative user','succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', admin_user,
            '--display-name', admin_display_name,
            '--access-key', admin_access_key,
            '--secret', admin_secret_key,
            '--max-buckets', '0',
            '--caps', admin_caps
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    remote_host = remote.name.split('@')[1]
    admin_conn = boto.s3.connection.S3Connection(
        aws_access_key_id=admin_access_key,
        aws_secret_access_key=admin_secret_key,
        is_secure=False,
        port=7280,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    # TESTCASE 'info-nosuch','user','info','non-existent user','fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {"uid": user1})
    assert ret == 404

    # TESTCASE 'create-ok','user','create','w/all valid info','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['user', 'create'],
            {'uid' : user1,
             'display-name' :  display_name1,
             'email' : email,
             'access-key' : access_key,
             'secret-key' : secret_key,
             'max-buckets' : '4'
            })

    assert ret == 200

    # TESTCASE 'info-existing','user','info','existing user','returns correct info'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})

    assert out['user_id'] == user1
    assert out['email'] == email
    assert out['display_name'] == display_name1
    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key
    assert not out['suspended']

    # TESTCASE 'suspend-ok','user','suspend','active user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' : user1, 'suspended' : True})
    assert ret == 200

    # TESTCASE 'suspend-suspended','user','suspend','suspended user','succeeds w/advisory'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert out['suspended']

    # TESTCASE 're-enable','user','enable','suspended user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' : user1, 'suspended' : 'false'})
    assert not err

    # TESTCASE 'info-re-enabled','user','info','re-enabled user','no longer suspended'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert not out['suspended']

    # TESTCASE 'add-keys','key','create','w/valid info','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['key', 'create'],
            {'uid' : user1,
             'access-key' : access_key2,
             'secret-key' : secret_key2
            })


    assert ret == 200

    # TESTCASE 'info-new-key','user','info','after key addition','returns all keys'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert len(out['keys']) == 2
    assert out['keys'][0]['access_key'] == access_key2 or out['keys'][1]['access_key'] == access_key2
    assert out['keys'][0]['secret_key'] == secret_key2 or out['keys'][1]['secret_key'] == secret_key2

    # TESTCASE 'rm-key','key','rm','newly added key','succeeds, key is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['key', 'rm'],
            {'uid' : user1,
             'access-key' : access_key2
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})

    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key

    # TESTCASE 'add-swift-key','key','create','swift key','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['subuser', 'create'],
            {'subuser' : subuser1,
             'secret-key' : swift_secret1,
             'key-type' : 'swift'
            })

    assert ret == 200

    # TESTCASE 'info-swift-key','user','info','after key addition','returns all keys'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert len(out['swift_keys']) == 1
    assert out['swift_keys'][0]['user'] == subuser1
    assert out['swift_keys'][0]['secret_key'] == swift_secret1

    # TESTCASE 'add-swift-subuser','key','create','swift sub-user key','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['subuser', 'create'],
            {'subuser' : subuser2,
             'secret-key' : swift_secret2,
             'key-type' : 'swift'
            })

    assert ret == 200

    # TESTCASE 'info-swift-subuser','user','info','after key addition','returns all sub-users/keys'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert ret == 200
    assert len(out['swift_keys']) == 2
    assert out['swift_keys'][0]['user'] == subuser2 or out['swift_keys'][1]['user'] == subuser2
    assert out['swift_keys'][0]['secret_key'] == swift_secret2 or out['swift_keys'][1]['secret_key'] == swift_secret2

    # TESTCASE 'rm-swift-key1','key','rm','subuser','succeeds, one key is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['key', 'rm'],
            {'subuser' : subuser1,
             'key-type' :'swift'
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert len(out['swift_keys']) == 1

    # TESTCASE 'rm-subuser','subuser','rm','subuser','success, subuser is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['subuser', 'rm'],
            {'subuser' : subuser1
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert len(out['subusers']) == 1

    # TESTCASE 'rm-subuser-with-keys','subuser','rm','subuser','succeeds, second subser and key is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['subuser', 'rm'],
            {'subuser' : subuser2,
             'key-type' : 'swift',
             '{purge-keys' :True
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert len(out['swift_keys']) == 0
    assert len(out['subusers']) == 0

    # TESTCASE 'bucket-stats','bucket','info','no session/buckets','succeeds, empty list'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'uid' :  user1})
    assert ret == 200
    assert len(out) == 0

    # connect to rgw
    connection = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        is_secure=False,
        port=7280,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    # TESTCASE 'bucket-stats2','bucket','stats','no buckets','succeeds, empty list'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'uid' : user1, 'stats' : True})
    assert ret == 200
    assert len(out) == 0

    # create a first bucket
    bucket = connection.create_bucket(bucket_name)

    # TESTCASE 'bucket-list','bucket','list','one bucket','succeeds, expected list'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'uid' : user1})
    assert ret == 200
    assert len(out) == 1
    assert out[0] == bucket_name

    # TESTCASE 'bucket-stats3','bucket','stats','new empty bucket','succeeds, empty list'
    (ret, out) = rgwadmin_rest(admin_conn,
            ['bucket', 'info'], {'bucket' : bucket_name, 'stats' : True})

    assert ret == 200
    assert out['owner'] == user1
    bucket_id = out['id']

    # TESTCASE 'bucket-stats4','bucket','stats','new empty bucket','succeeds, expected bucket ID'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'uid' : user1, 'stats' : True})
    assert ret == 200
    assert len(out) == 1
    assert out[0]['id'] == bucket_id    # does it return the same ID twice in a row?

    # use some space
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('one')

    # TESTCASE 'bucket-stats5','bucket','stats','after creating key','succeeds, lists one non-empty object'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'bucket' : bucket_name, 'stats' : True})
    assert ret == 200
    assert out['id'] == bucket_id
    assert out['usage']['rgw.main']['num_objects'] == 1
    assert out['usage']['rgw.main']['size_kb'] > 0

    # reclaim it
    key.delete()

    # TESTCASE 'bucket unlink', 'bucket', 'unlink', 'unlink bucket from user', 'fails', 'access denied error'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'unlink'], {'uid' : user1, 'bucket' : bucket_name})

    assert ret == 200

    # create a second user to link the bucket to
    (ret, out) = rgwadmin_rest(admin_conn,
            ['user', 'create'],
            {'uid' : user2,
            'display-name' :  display_name2,
            'access-key' : access_key2,
            'secret-key' : secret_key2,
            'max-buckets' : '1',
            })

    assert ret == 200

    # try creating an object with the first user before the bucket is relinked
    denied = False
    key = boto.s3.key.Key(bucket)

    try:
        key.set_contents_from_string('two')
    except boto.exception.S3ResponseError:
        denied = True

    assert not denied

    # delete the object
    key.delete()

    # link the bucket to another user
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'link'], {'uid' : user2, 'bucket' : bucket_name})

    assert ret == 200

    # try creating an object with the first user which should cause an error
    key = boto.s3.key.Key(bucket)

    try:
        key.set_contents_from_string('three')
    except boto.exception.S3ResponseError:
        denied = True

    assert denied

    # relink the bucket to the first user and delete the second user
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'link'], {'uid' : user1, 'bucket' : bucket_name})
    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'rm'], {'uid' : user2})
    assert ret == 200

    # TESTCASE 'object-rm', 'object', 'rm', 'remove object', 'succeeds, object is removed'

    # upload an object
    object_name = 'four'
    key = boto.s3.key.Key(bucket, object_name)
    key.set_contents_from_string(object_name)

    # now delete it
    (ret, out) = rgwadmin_rest(admin_conn, ['object', 'rm'], {'bucket' : bucket_name, 'object' : object_name})
    assert ret == 200

    # TESTCASE 'bucket-stats6','bucket','stats','after deleting key','succeeds, lists one no objects'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'bucket' : bucket_name, 'stats' : True})
    assert ret == 200
    assert out['id'] == bucket_id
    assert out['usage']['rgw.main']['num_objects'] == 0

    # create a bucket for deletion stats
    useless_bucket = connection.create_bucket('useless_bucket')
    useless_key = useless_bucket.new_key('useless_key')
    useless_key.set_contents_from_string('useless string')

    # delete it
    useless_key.delete()
    useless_bucket.delete()

    # wait for the statistics to flush
    time.sleep(60)

    # need to wait for all usage data to get flushed, should take up to 30 seconds
    timestamp = time.time()
    while time.time() - timestamp <= (20 * 60):      # wait up to 20 minutes
        (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'], {'categories' : 'delete_obj'})  # last operation we did is delete obj, wait for it to flush

        if successful_ops(out) > 0:
            break
        time.sleep(1)

    assert time.time() - timestamp <= (20 * 60)

    # TESTCASE 'usage-show' 'usage' 'show' 'all usage' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'])
    assert ret == 200
    assert len(out['entries']) > 0
    assert len(out['summary']) > 0
    user_summary = out['summary'][0]
    total = user_summary['total']
    assert total['successful_ops'] > 0

    # TESTCASE 'usage-show2' 'usage' 'show' 'user usage' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'], {'uid' : user1})
    assert ret == 200
    assert len(out['entries']) > 0
    assert len(out['summary']) > 0
    user_summary = out['summary'][0]
    for entry in user_summary['categories']:
        assert entry['successful_ops'] > 0
    assert user_summary['user'] == user1

    # TESTCASE 'usage-show3' 'usage' 'show' 'user usage categories' 'succeeds'
    test_categories = ['create_bucket', 'put_obj', 'delete_obj', 'delete_bucket']
    for cat in test_categories:
        (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'], {'uid' : user1, 'categories' : cat})
        assert ret == 200
        assert len(out['summary']) > 0
        user_summary = out['summary'][0]
        assert user_summary['user'] == user1
        assert len(user_summary['categories']) == 1
        entry = user_summary['categories'][0]
        assert entry['category'] == cat
        assert entry['successful_ops'] > 0

    # TESTCASE 'usage-trim' 'usage' 'trim' 'user usage' 'succeeds, usage removed'
    (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'trim'], {'uid' : user1})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'], {'uid' : user1})
    assert ret == 200
    assert len(out['entries']) == 0
    assert len(out['summary']) == 0

    # TESTCASE 'user-suspend2','user','suspend','existing user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' : user1, 'suspended' : True})
    assert ret == 200

    # TESTCASE 'user-suspend3','user','suspend','suspended user','cannot write objects'
    try:
        key = boto.s3.key.Key(bucket)
        key.set_contents_from_string('five')
    except boto.exception.S3ResponseError as e:
        assert e.status == 403

    # TESTCASE 'user-renable2','user','enable','suspended user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' :  user1, 'suspended' : 'false'})
    assert ret == 200

    # TESTCASE 'user-renable3','user','enable','reenabled user','can write objects'
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('six')

    # TESTCASE 'garbage-list', 'garbage', 'list', 'get list of objects ready for garbage collection'

    # create an object large enough to be split into multiple parts
    test_string = 'foo'*10000000

    big_key = boto.s3.key.Key(bucket)
    big_key.set_contents_from_string(test_string)

    # now delete the head
    big_key.delete()

    # TESTCASE 'rm-user-buckets','user','rm','existing user','fails, still has buckets'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'rm'], {'uid' : user1})
    assert ret == 409

    # delete should fail because ``key`` still exists
    try:
        bucket.delete()
    except boto.exception.S3ResponseError as e:
        assert e.status == 409

    key.delete()
    bucket.delete()

    # TESTCASE 'policy', 'bucket', 'policy', 'get bucket policy', 'returns S3 policy'
    bucket = connection.create_bucket(bucket_name)

    # create an object
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('seven')

    # should be private already but guarantee it
    key.set_acl('private')

    (ret, out) = rgwadmin_rest(admin_conn, ['policy', 'show'], {'bucket' : bucket.name, 'object' : key.key})
    assert ret == 200

    acl = key.get_xml_acl()
    assert acl == out.strip('\n')

    # add another grantee by making the object public read
    key.set_acl('public-read')

    (ret, out) = rgwadmin_rest(admin_conn, ['policy', 'show'], {'bucket' : bucket.name, 'object' : key.key})
    assert ret == 200

    acl = key.get_xml_acl()
    assert acl == out.strip('\n')

    # TESTCASE 'rm-bucket', 'bucket', 'rm', 'bucket with objects', 'succeeds'
    bucket = connection.create_bucket(bucket_name)
    key_name = ['eight', 'nine', 'ten', 'eleven']
    for i in range(4):
        key = boto.s3.key.Key(bucket)
        key.set_contents_from_string(key_name[i])

    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'rm'], {'bucket' : bucket_name, 'purge-objects' : True})
    assert ret == 200

    # TESTCASE 'caps-add', 'caps', 'add', 'add user cap', 'succeeds'
    caps = 'usage=read'
    (ret, out) = rgwadmin_rest(admin_conn, ['caps', 'add'], {'uid' :  user1, 'user-caps' : caps})
    assert ret == 200
    assert out[0]['perm'] == 'read'

    # TESTCASE 'caps-rm', 'caps', 'rm', 'remove existing cap from user', 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['caps', 'rm'], {'uid' :  user1, 'user-caps' : caps})
    assert ret == 200
    assert not out

    # TESTCASE 'rm-user','user','rm','existing user','fails, still has buckets'
    bucket = connection.create_bucket(bucket_name)
    key = boto.s3.key.Key(bucket)

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'rm'], {'uid' : user1})
    assert ret == 409

    # TESTCASE 'rm-user2', 'user', 'rm', user with data', 'succeeds'
    bucket = connection.create_bucket(bucket_name)
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('twelve')

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'rm'], {'uid' : user1, 'purge-data' : True})
    assert ret == 200

    # TESTCASE 'rm-user3','user','info','deleted user','fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert ret == 404

