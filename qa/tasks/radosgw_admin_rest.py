"""
Run a series of rgw admin commands through the rest interface.

The test cases in this file have been annotated for inventory.
To extract the inventory (in csv format) use the command:

   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'

"""
import logging


import boto.exception
import boto.s3.connection
import boto.s3.acl

import requests
import time

from boto.connection import AWSAuthConnection
from teuthology import misc as teuthology
from tasks.util.rgw import get_user_summary, get_user_successful_ops, rgwadmin

log = logging.getLogger(__name__)

def rgwadmin_rest(connection, cmd, params=None, headers=None, raw=False):
    """
    perform a rest command
    """
    log.info('radosgw-admin-rest: %s %s' % (cmd, params))
    put_cmds = ['create', 'link', 'add', 'set']
    post_cmds = ['unlink', 'modify', 'post']
    delete_cmds = ['trim', 'rm', 'process']
    get_cmds = ['check', 'info', 'show', 'list', 'get', '']

    def get_cmd_method_and_handler(cmd):
        """
        Get the rest command and handler from information in cmd and
        from the imported requests object.
        """
        if cmd[1] in put_cmds:
            return 'PUT', requests.put
        elif cmd[1] in delete_cmds:
            return 'DELETE', requests.delete
        elif cmd[1] in post_cmds:
            return 'POST', requests.post
        elif cmd[1] in get_cmds:
            return 'GET', requests.get

    def get_resource(cmd):
        """
        Always return (resource, subresource) as a 2-tuple.
        Accepts:
        ['user','info']                  -> ('user','')
        [('user','key'),'create']        -> ('user','key')
        ['info','']                      -> ('info','')
        """
        r = cmd[0]
        if isinstance(r, (list, tuple)):
            if len(r) == 1:
                return r[0], ''
            else:
                return r
        if isinstance(r, str):
            return r, ''
        raise TypeError(f'Unsupported resource type for {str(r)}')

    def build_admin_request(conn, method, resource = '', headers=None, data='',
            query_args=None, params=None):
        """
        Build an administative request adapted from the build_request()
        method of boto.connection
        """

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
        log.info(' text result: %s' % result.text)
        return result.status_code, result.text
    elif len(result.content) == 0:
        # many admin requests return no body, so json() throws a JSONDecodeError
        log.info(' empty result')
        return result.status_code, None
    else:
        log.info(' json result: %s' % result.json())
        return result.status_code, result.json()

def test_cap_user_info_without_keys_get_user_info_privileged_users(ctx, client, op, op_args, uid, display_name, access_key, secret_key, user_type):
    user_caps = 'user-info-without-keys=read'

    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', uid,
            '--display-name', display_name,
            '--access-key', access_key,
            '--secret', secret_key,
            '--caps', user_caps,
            user_type
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    endpoint = ctx.rgw.role_endpoints.get(client)

    privileged_user_conn = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        is_secure=True if endpoint.cert else False,
        port=endpoint.port,
        host=endpoint.hostname,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    (ret, out) = rgwadmin_rest(privileged_user_conn, op, op_args)
    # show that even though the cap is set, since the user is privileged the user can still see keys
    assert len(out['keys']) == 1
    assert out['swift_keys'] == []

    (err, out) = rgwadmin(ctx, client, [
            'user', 'rm',
            '--uid', uid,
            ])
    logging.error(out)
    logging.error(err)
    assert not err

def test_cap_user_info_without_keys_get_user_info(ctx, client, admin_conn, admin_user, op, op_args):
    true_admin_uid = 'a_user'
    true_admin_display_name = 'True Admin User'
    true_admin_access_key = 'true_admin_akey'
    true_admin_secret_key = 'true_admin_skey'

    system_uid = 'system_user'
    system_display_name = 'System User'
    system_access_key = 'system_akey'
    system_secret_key = 'system_skey'

    test_cap_user_info_without_keys_get_user_info_privileged_users(ctx, client, op, op_args, system_uid, system_display_name, system_access_key, system_secret_key, '--system')
    test_cap_user_info_without_keys_get_user_info_privileged_users(ctx, client, op, op_args, true_admin_uid, true_admin_display_name, true_admin_access_key, true_admin_secret_key, '--admin')

    # TESTCASE 'info-existing','user','info','existing user','returns no keys with user-info-without-keys cap set to read'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'user-info-without-keys=read'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (ret, out) = rgwadmin_rest(admin_conn, op, op_args)
    assert 'keys' not in out
    assert 'swift_keys' not in out

    # TESTCASE 'info-existing','user','info','existing user','returns no keys with user-info-without-keys cap set to read'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'user-info-without-keys=*'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (ret, out) = rgwadmin_rest(admin_conn, op, op_args)
    assert 'keys' not in out
    assert 'swift_keys' not in out

    # TESTCASE 'info-existing','user','info','existing user','returns keys with user-info-without-keys cap set to read but cap users is set to read'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'users=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (ret, out) = rgwadmin_rest(admin_conn, op, op_args)
    assert 'keys' in out
    assert 'swift_keys' in out

    # TESTCASE 'info-existing','user','info','existing user','returns 403 with user-info-without-keys cap set to write'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'rm',
            '--uid', admin_user,
            '--caps', 'users=read, write; user-info-without-keys=*'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'user-info-without-keys=write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    (ret, out) = rgwadmin_rest(admin_conn, op, op_args)
    assert ret == 403

    # remove cap user-info-without-keys permenantly for future testing
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'rm',
            '--uid', admin_user,
            '--caps', 'user-info-without-keys=write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    # reset cap users permenantly for future testing
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'users=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

def test_cap_user_info_without_keys(ctx, client, admin_conn, admin_user, user1):
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'rm',
            '--uid', admin_user,
            '--caps', 'users=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    op = ['user', 'info']
    op_args = {'uid' : user1}
    test_cap_user_info_without_keys_get_user_info(ctx, client, admin_conn, admin_user, op, op_args)

    # add caps that were removed earlier in the function back in
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'users=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

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
    client = next(iter(clients))

    ##
    admin_user = 'ada'
    admin_display_name = 'Ms. Admin User'
    admin_access_key = 'MH1WC2XQ1S8UISFDZC8W'
    admin_secret_key = 'dQyrTPA0s248YeN5bBv4ukvKU0kh54LWWywkrpoG'
    admin_caps = 'users=read, write; usage=read, write; buckets=read, write; zone=read, write; info=read; ratelimit=read, write; accounts=read, write'

    user1 = 'foo'
    user2 = 'fud'
    ratelimit_user = 'ratelimit_user'
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
    account_id = 'RGW00000000000000001'

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

    assert hasattr(ctx, 'rgw'), 'radosgw-admin-rest must run after the rgw task'
    endpoint = ctx.rgw.role_endpoints.get(client)
    assert endpoint, 'no rgw endpoint for {}'.format(client)

    admin_conn = boto.s3.connection.S3Connection(
        aws_access_key_id=admin_access_key,
        aws_secret_access_key=admin_secret_key,
        is_secure=True if endpoint.cert else False,
        port=endpoint.port,
        host=endpoint.hostname,
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

    # TESTCASE 'list-no-user','user','list','list user keys','user list object'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'list'], {'list' : '', 'max-entries' : 0})
    assert ret == 200
    assert out['count'] == 0
    assert out['truncated'] == True
    assert len(out['keys']) == 0
    assert len(out['marker']) > 0

    # TESTCASE 'list-user-without-marker','user','list','list user keys','user list object'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'list'], {'list' : '', 'max-entries' : 1})
    assert ret == 200
    assert out['count'] == 1
    assert out['truncated'] == True
    assert len(out['keys']) == 1
    assert len(out['marker']) > 0
    marker = out['marker']

    # TESTCASE 'list-user-with-marker','user','list','list user keys','user list object'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'list'], {'list' : '', 'max-entries' : 1, 'marker': marker})
    assert ret == 200
    assert out['count'] == 1
    assert out['truncated'] == False
    assert len(out['keys']) == 1

    # TESTCASE 'info-existing','user','info','existing user','returns correct info'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})

    assert out['user_id'] == user1
    assert out['email'] == email
    assert out['display_name'] == display_name1
    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key
    assert not out['suspended']
    assert out['tenant'] == ''
    assert out['max_buckets'] == 4
    assert out['caps'] == []
    assert out['op_mask'] == 'read, write, delete'
    assert out['default_placement'] == ''
    assert out['default_storage_class'] == ''
    assert out['placement_tags'] == []
    assert not out['bucket_quota']['enabled']
    assert not out['bucket_quota']['check_on_raw']
    assert out['bucket_quota']['max_size'] == -1
    assert out['bucket_quota']['max_size_kb'] == 0
    assert out['bucket_quota']['max_objects'] == -1
    assert not out['user_quota']['enabled']
    assert not out['user_quota']['check_on_raw']
    assert out['user_quota']['max_size'] == -1
    assert out['user_quota']['max_size_kb'] == 0
    assert out['user_quota']['max_objects'] == -1
    assert out['temp_url_keys'] == []
    assert out['type'] == 'rgw'
    assert out['mfa_ids'] == []
    # TESTCASE 'info-existing','user','info','existing user query with wrong uid but correct access key','returns correct info'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'access-key' : access_key, 'uid': 'uid_not_exist'})

    assert out['user_id'] == user1
    assert out['email'] == email
    assert out['display_name'] == display_name1
    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key
    assert not out['suspended']
    assert out['tenant'] == ''
    assert out['max_buckets'] == 4
    assert out['caps'] == []
    assert out['op_mask'] == "read, write, delete"
    assert out['default_placement'] == ''
    assert out['default_storage_class'] == ''
    assert out['placement_tags'] == []
    assert not out['bucket_quota']['enabled']
    assert not out['bucket_quota']['check_on_raw']
    assert out ['bucket_quota']['max_size'] == -1
    assert out ['bucket_quota']['max_size_kb'] == 0
    assert out ['bucket_quota']['max_objects'] == -1
    assert not out['user_quota']['enabled']
    assert not out['user_quota']['check_on_raw']
    assert out['user_quota']['max_size'] == -1
    assert out['user_quota']['max_size_kb'] == 0
    assert out['user_quota']['max_objects'] == -1
    assert out['temp_url_keys'] == []
    assert out['type'] == 'rgw'
    assert out['mfa_ids'] == []

    # TESTCASES for cap user-info-without-keys
    test_cap_user_info_without_keys(ctx, client, admin_conn, admin_user, user1)

    # TESTCASE 'suspend-ok','user','suspend','active user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' : user1, 'suspended' : True})
    assert ret == 200

    # TESTCASE 'suspend-suspended','user','suspend','suspended user','succeeds w/advisory'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert out['suspended']
    assert out['email'] == email

    # TESTCASE 're-enable','user','enable','suspended user','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'modify'], {'uid' : user1, 'suspended' : 'false'})
    assert not err

    # TESTCASE 'info-re-enabled','user','info','re-enabled user','no longer suspended'
    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' : user1})
    assert ret == 200
    assert not out['suspended']

    # TESTCASE 'add-keys','key','create','w/valid info','succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
            [('user', 'key'), 'create'],
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
            [('user', 'key'), 'rm'],
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
            [('user', 'subuser'), 'create'],
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
            [('user', 'subuser'), 'create'],
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
            [('user', 'key'), 'rm'],
            {'subuser' : subuser1,
             'key-type' :'swift'
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert len(out['swift_keys']) == 1

    # TESTCASE 'rm-subuser','subuser','rm','subuser','success, subuser is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            [('user', 'subuser'), 'rm'],
            {'subuser' : subuser1
            })

    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'info'], {'uid' :  user1})
    assert len(out['subusers']) == 1

    # TESTCASE 'rm-subuser-with-keys','subuser','rm','subuser','succeeds, second subser and key is removed'
    (ret, out) = rgwadmin_rest(admin_conn,
            [('user', 'subuser'), 'rm'],
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
        is_secure=True if endpoint.cert else False,
        port=endpoint.port,
        host=endpoint.hostname,
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
    assert out['tenant'] == ''
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

    # TESTCASE 'bucket-stats6', 'bucket', 'stats', 'non-existent bucket', 'fails, 'bucket not found error'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'bucket' : 'doesnotexist'})
    assert ret == 404
    assert out['Code'] == 'NoSuchBucket'

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
    (ret, out) = rgwadmin_rest(admin_conn,
            ['bucket', 'link'],
            {'uid' : user2,
             'bucket' : bucket_name,
             'bucket-id' : bucket_id,
            })

    assert ret == 200

    # try creating an object with the first user which should cause an error
    key = boto.s3.key.Key(bucket)

    try:
        key.set_contents_from_string('three')
    except boto.exception.S3ResponseError:
        denied = True

    assert denied

    # relink the bucket to the first user and delete the second user
    (ret, out) = rgwadmin_rest(admin_conn,
            ['bucket', 'link'],
            {'uid' : user1,
             'bucket' : bucket_name,
             'bucket-id' : bucket_id,
            })
    assert ret == 200

    (ret, out) = rgwadmin_rest(admin_conn, ['user', 'rm'], {'uid' : user2})
    assert ret == 200

    # TESTCASE 'object-rm', 'object', 'rm', 'remove object', 'succeeds, object is removed'

    # upload an object
    object_name = 'four'
    key = boto.s3.key.Key(bucket, object_name)
    key.set_contents_from_string(object_name)

    # now delete it
    (ret, out) = rgwadmin_rest(admin_conn, [('bucket', 'object'), 'rm'], {'bucket' : bucket_name, 'object' : object_name})
    assert ret == 200

    # TESTCASE 'bucket-stats6','bucket','stats','after deleting key','succeeds, lists one no objects'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'info'], {'bucket' : bucket_name, 'stats' : True})
    assert ret == 200
    assert out['id'] == bucket_id
    assert out['usage']['rgw.main']['num_objects'] == 0

    # create a bucket for deletion stats
    useless_bucket = connection.create_bucket('useless-bucket')
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

        if get_user_successful_ops(out, user1) > 0:
            break
        time.sleep(1)

    assert time.time() - timestamp <= (20 * 60)

    # TESTCASE 'usage-show' 'usage' 'show' 'all usage' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['usage', 'show'])
    assert ret == 200
    assert len(out['entries']) > 0
    assert len(out['summary']) > 0
    user_summary = get_user_summary(out, user1)
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

    (ret, out) = rgwadmin_rest(admin_conn, [('bucket', 'policy'), 'show'], {'bucket' : bucket.name, 'object' : key.key})
    assert ret == 200
    assert len(out['acl']['grant_map']) == 1

    # add another grantee by making the object public read
    key.set_acl('public-read')

    (ret, out) = rgwadmin_rest(admin_conn, [('bucket', 'policy'), 'show'], {'bucket' : bucket.name, 'object' : key.key})
    assert ret == 200
    assert len(out['acl']['grant_map']) == 2

    # TESTCASE 'rm-bucket', 'bucket', 'rm', 'bucket with objects', 'succeeds'
    bucket = connection.create_bucket(bucket_name)
    key_name = ['eight', 'nine', 'ten', 'eleven']
    for i in range(4):
        key = boto.s3.key.Key(bucket)
        key.set_contents_from_string(key_name[i])

    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'rm'], {'bucket' : bucket_name, 'purge-objects' : True})
    assert ret == 200

    # TESTCASE 'rm-bucket', 'bucket', 'rm', 'non-existent bucket', 'correct error'
    (ret, out) = rgwadmin_rest(admin_conn, ['bucket', 'rm'], {'bucket' : bucket_name})
    assert ret == 404
    assert out['Code'] == 'NoSuchBucket'

    # TESTCASE 'caps-add', 'caps', 'add', 'add user cap', 'succeeds'
    caps = 'usage=read'
    (ret, out) = rgwadmin_rest(admin_conn, [('user', 'caps'), 'add'], {'uid' :  user1, 'user-caps' : caps})
    assert ret == 200
    assert out[0]['perm'] == 'read'

    # TESTCASE 'caps-rm', 'caps', 'rm', 'remove existing cap from user', 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, [('user', 'caps'), 'rm'], {'uid' :  user1, 'user-caps' : caps})
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

    # TESTCASE 'info' 'display info' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['info', ''])
    assert ret == 200
    info = out['info']
    backends = info['storage_backends']
    name = backends[0]['name']
    fsid = backends[0]['cluster_id']
    # name is always "rados" at time of writing, but zipper would allow
    # other backends, at some point
    assert len(name) > 0
    # fsid is a uuid, but I'm not going to try to parse it
    assert len(fsid) > 0
    
    # TESTCASE 'ratelimit' 'user' 'info' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn,
        ['user', 'create'],
        {'uid' : ratelimit_user,
         'display-name' :  display_name1,
         'email' : email,
         'access-key' : access_key,
         'secret-key' : secret_key,
         'max-buckets' : '1000'
        })
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user})
    assert ret == 200

    # TESTCASE 'ratelimit' 'user' 'info'  'not existing user' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user + 'string'})
    assert ret == 404

    # TESTCASE 'ratelimit' 'user' 'info'  'uid not specified' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user'})
    assert ret == 400

    # TESTCASE 'ratelimit' 'bucket' 'info' 'succeeds'
    ratelimit_bucket = 'ratelimitbucket'
    connection.create_bucket(ratelimit_bucket)
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket})
    assert ret == 200

    # TESTCASE 'ratelimit' 'bucket' 'info'  'not existing bucket' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket + 'string'})
    assert ret == 404

    # TESTCASE 'ratelimit' 'bucket' 'info' 'bucket not specified' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket'})
    assert ret == 400

    # TESTCASE 'ratelimit' 'global' 'info' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'global' : 'true'})
    assert ret == 200

    # TESTCASE 'ratelimit' 'user' 'modify'  'not existing user' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user + 'string', 'enabled' : 'true'})
    assert ret == 404

    # TESTCASE 'ratelimit' 'user' 'modify'  'uid not specified' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'user'})
    assert ret == 400
    
    # TESTCASE 'ratelimit' 'bucket' 'modify'  'not existing bucket' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket + 'string', 'enabled' : 'true'})
    assert ret == 404

    # TESTCASE 'ratelimit' 'bucket' 'modify' 'bucket not specified' 'fails'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'enabled' : 'true'})
    assert ret == 400

    # TESTCASE 'ratelimit' 'user' 'modifiy' 'enabled' 'max-read-bytes = 2' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user, 'enabled' : 'true', 'max-read-bytes' : '2'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user})
    assert ret == 200
    user_ratelimit = out['user_ratelimit']
    assert user_ratelimit['enabled'] == True
    assert user_ratelimit['max_read_bytes'] ==  2

    # TESTCASE 'ratelimit' 'bucket' 'modifiy' 'enabled' 'max-write-bytes = 2' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket, 'enabled' : 'true', 'max-write-bytes' : '2'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket})
    assert ret == 200
    bucket_ratelimit = out['bucket_ratelimit']
    assert bucket_ratelimit['enabled'] == True
    assert bucket_ratelimit['max_write_bytes'] == 2

    # TESTCASE 'ratelimit' 'global' 'modify' 'anonymous' 'enabled' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'global': 'true', 'enabled' : 'true'})
    assert ret == 200

    # TESTCASE 'ratelimit' 'user' 'modify' 'max-list-ops = 100' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user, 'max-list-ops' : '100'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user})
    assert ret == 200
    user_ratelimit = out['user_ratelimit']
    assert user_ratelimit['max_list_ops'] == 100

    # TESTCASE 'ratelimit' 'user' 'modify' 'max-delete-ops = 50' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user, 'max-delete-ops' : '50'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user})
    assert ret == 200
    user_ratelimit = out['user_ratelimit']
    assert user_ratelimit['max_delete_ops'] == 50

    # TESTCASE 'ratelimit' 'bucket' 'modify' 'max-list-ops = 200' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket, 'max-list-ops' : '200'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket})
    assert ret == 200
    bucket_ratelimit = out['bucket_ratelimit']
    assert bucket_ratelimit['max_list_ops'] == 200

    # TESTCASE 'ratelimit' 'bucket' 'modify' 'max-delete-ops = 75' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket, 'max-delete-ops' : '75'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'bucket', 'bucket' : ratelimit_bucket})
    assert ret == 200
    bucket_ratelimit = out['bucket_ratelimit']
    assert bucket_ratelimit['max_delete_ops'] == 75

    # TESTCASE 'ratelimit' 'global' 'modify' 'bucket' 'max-list-ops = 500' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'global': 'true', 'max-list-ops' : '500'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'global' : 'true'})
    assert ret == 200
    # Check that global bucket ratelimit has the list ops set
    assert 'bucket_ratelimit' in out
    assert out['bucket_ratelimit']['max_list_ops'] == 500

    # TESTCASE 'ratelimit' 'global' 'modify' 'bucket' 'max-delete-ops = 300' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {'ratelimit-scope' : 'bucket', 'global': 'true', 'max-delete-ops' : '300'})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'global' : 'true'})
    assert ret == 200
    # Check that global bucket ratelimit has the delete ops set
    assert 'bucket_ratelimit' in out
    assert out['bucket_ratelimit']['max_delete_ops'] == 300

    # TESTCASE 'ratelimit' 'user' 'modify' 'multiple ops at once' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'modify'], {
        'ratelimit-scope' : 'user',
        'uid' : ratelimit_user,
        'max-read-ops' : '1000',
        'max-write-ops' : '800',
        'max-list-ops' : '600',
        'max-delete-ops' : '400',
        'enabled' : 'true'
    })
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['ratelimit', 'info'], {'ratelimit-scope' : 'user', 'uid' : ratelimit_user})
    assert ret == 200
    user_ratelimit = out['user_ratelimit']
    assert user_ratelimit['enabled'] == True
    assert user_ratelimit['max_read_ops'] == 1000
    assert user_ratelimit['max_write_ops'] == 800
    assert user_ratelimit['max_list_ops'] == 600
    assert user_ratelimit['max_delete_ops'] == 400

    # TESTCASE 'create account' 'account' 'post' 'creating a new account' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'post'], {'id' : account_id, 'name': account_id})
    assert ret == 200

    # TESTCASE 'account-info' 'account' 'info' 'getting the account info, including its quota, and checking for default values' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'info'], {'id' : account_id})
    assert ret == 200
    account_quota = out['quota']
    assert not account_quota["enabled"]
    assert account_quota["max_size"] == -1
    assert account_quota["max_objects"] == -1
    bucket_quota = out['bucket_quota']
    assert not bucket_quota["enabled"]
    assert bucket_quota["max_size"] == -1
    assert bucket_quota["max_objects"] == -1
    assert out["max_users"] == 1000
    assert out["max_roles"] == 1000
    assert out["max_groups"] == 1000
    assert out["max_buckets"] == 1000
    assert out["max_access_keys"] == 4

    # TESTCASE 'bucket-level-account-quota-set' 'account-quota-set' 'setting account quota at the bucket level. Other values should remain the same' 'succeeds'
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'quota-type' : 'bucket', 'max-size': 12345, 'max-objects' : 54321, 'enabled': True})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'info'], {'id' : account_id})
    assert ret == 200
    # verifying bucket-level quota has been changed
    bucket_quota = out['bucket_quota']
    assert bucket_quota["enabled"]
    assert bucket_quota["max_size"] == 12345
    assert bucket_quota["max_objects"] == 54321
    # verifying the rest of the values remain the same
    account_quota = out['quota']
    assert not account_quota["enabled"]
    assert account_quota["max_size"] == -1
    assert account_quota["max_objects"] == -1
    assert out["max_users"] == 1000
    assert out["max_roles"] == 1000
    assert out["max_groups"] == 1000
    assert out["max_buckets"] == 1000
    assert out["max_access_keys"] == 4
    # cleanup the account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'rm'], {'id' : account_id})
    assert ret == 200


    # TESTCASE 'account-level-quota-set' 'account-quota-set' 'setting account quota at the account level. Other values should remain the same' 'succeeds'
    # re-create the account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'post'], {'id' : account_id, 'name': account_id})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'quota-type' : 'account', 'max-size': 12345, 'max-objects' : 54321, 'enabled': True})
    assert ret == 200
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'info'], {'id' : account_id})
    assert ret == 200
    # verifying account-level quota has been changed
    account_quota = out['quota']
    assert account_quota["enabled"]
    assert account_quota["max_size"] == 12345
    assert account_quota["max_objects"] == 54321
    # verifying the rest of the values remain the same
    bucket_quota = out['bucket_quota']
    assert not bucket_quota["enabled"]
    assert bucket_quota["max_size"] == -1
    assert bucket_quota["max_objects"] == -1
    assert out["max_users"] == 1000
    assert out["max_roles"] == 1000
    assert out["max_groups"] == 1000
    assert out["max_buckets"] == 1000
    assert out["max_access_keys"] == 4
    # cleanup the account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'rm'], {'id' : account_id})
    assert ret == 200

    # TESTCASE 'account-quota-set-invalid-args' 'account-quota-set' 'verify set quota requests with invalid args fail' 'fails'
    # re-create the account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'post'], {'id' : account_id, 'name': account_id})
    assert ret == 200

    # verify invalid quota-type fails
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'quota-type' : 'invalid', 'max-size': 12345, 'max-objects' : 54321, 'enabled': True})
    assert ret == 400

    # verify empty quota-type fails
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'max-size': 12345, 'max-objects' : 54321, 'enabled': True})
    assert ret == 400

    # verify request with no account id fails
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'quota-type' : 'bucket', 'max-size': 12345, 'max-objects' : 54321, 'enabled': True})
    assert ret == 400

    # cleanup the account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'rm'], {'id' : account_id})
    assert ret == 200

    # TESTCASE 'rm account caps' 'user-caps-rm' 'verify access after removing user caps for account read and write' 'fails'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'rm',
            '--uid', admin_user,
            '--caps', 'accounts=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    # checking failed access to create account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'post'], {'id' : account_id, 'name': account_id})
    assert ret == 403

    # checking failed access to set quota
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'quota-type' : 'account', 'max-size': -1, 'max-objects' : -1, 'enabled': True})
    assert ret == 403

    # checking failed access to get account info
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'info'], {'id' : account_id})
    assert ret == 403

    # checking failed access to remove account
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'rm'], {'id' : account_id})
    assert ret == 403

    # TESTCASE 'add back account caps' 'user-caps-add' 'verify access after adding back user caps for account read and write' 'succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'caps', 'add',
            '--uid', admin_user,
            '--caps', 'accounts=read, write'
            ])
    logging.error(out)
    logging.error(err)
    assert not err

    # checking create account access
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'post'], {'id' : account_id, 'name': account_id})
    assert ret == 200

    # checking set quota access
    (ret, out) = rgwadmin_rest(admin_conn, [('account', 'quota'), 'set'], {'id': account_id, 'quota-type' : 'account', 'max-size': -1, 'max-objects' : -1, 'enabled': True})
    assert ret == 200

    # checking get account info access
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'info'], {'id' : account_id})
    assert ret == 200

    # checking remove account access
    (ret, out) = rgwadmin_rest(admin_conn, ['account', 'rm'], {'id' : account_id})
    assert ret == 200
