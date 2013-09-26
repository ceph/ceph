# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#

import copy
import json
import logging
import time

from cStringIO import StringIO

import boto.exception
import boto.s3.connection
import boto.s3.acl

import teuthology.task_util.rgw as rgw_utils

from teuthology import misc as teuthology
from teuthology.task_util.rgw import rgwadmin

log = logging.getLogger(__name__)


def successful_ops(out):
    summary = out['summary']
    if len(summary) == 0:
        return 0
    entry = summary[0]
    return entry['total']['successful_ops']


def task(ctx, config):
    """
    Test radosgw-admin functionality against a running rgw instance.
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

    multi_region_run = rgw_utils.multi_region_enabled(ctx)

    client = clients[0]; # default choice, multi-region code may overwrite this
    if multi_region_run:
        client = rgw_utils.get_master_client(ctx, clients)

    # once the client is chosen, pull the host name and  assigned port out of
    # the role_endpoints that were assigned by the rgw task
    (remote_host, remote_port) = ctx.rgw.role_endpoints[client]

    ##
    user1='foo'
    user2='fud'
    subuser1='foo:foo1'
    subuser2='foo:foo2'
    display_name1='Foo'
    display_name2='Fud'
    email='foo@foo.com'
    email2='bar@bar.com'
    access_key='9te6NH5mcdcq0Tc5i8i1'
    secret_key='Ny4IOauQoL18Gp2zM7lC1vLmoawgqcYP/YGcWfXu'
    access_key2='p5YnriCv1nAtykxBrupQ'
    secret_key2='Q8Tk6Q/27hfbFSYdSkPtUqhqx1GgzvpXa4WARozh'
    swift_secret1='gpS2G9RREMrnbqlp29PP2D36kgPR1tm72n5fPYfL'
    swift_secret2='ri2VJQcKSYATOY6uaDUX7pxgkW+W1YmC6OCxPHwy'

    bucket_name='myfoo'
    bucket_name2='mybar'

    # connect to rgw
    connection = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        is_secure=False,
        port=remote_port,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
    connection2 = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key2,
        aws_secret_access_key=secret_key2,
        is_secure=False,
        port=remote_port,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    # legend (test cases can be easily grep-ed out)
    # TESTCASE 'testname','object','method','operation','assertion'
    # TESTCASE 'info-nosuch','user','info','non-existent user','fails'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1])
    assert err

    # TESTCASE 'create-ok','user','create','w/all valid info','succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user1,
            '--display-name', display_name1,
            '--email', email,
            '--access-key', access_key,
            '--secret', secret_key,
            '--max-buckets', '4'
            ],
            check_status=True)

    # TESTCASE 'duplicate email','user','create','existing user email','fails'
    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user2,
            '--display-name', display_name2,
            '--email', email,
            ])
    assert err

    # TESTCASE 'info-existing','user','info','existing user','returns correct info'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1], check_status=True)
    assert out['user_id'] == user1
    assert out['email'] == email
    assert out['display_name'] == display_name1
    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key
    assert not out['suspended']

    # this whole block should only be run if regions have been configured
    if multi_region_run:
        rgw_utils.radosgw_agent_sync_all(ctx)
        # post-sync, validate that user1 exists on the sync destination host
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            dest_client = c_config['dest']
            (err, out) = rgwadmin(ctx, dest_client, ['metadata', 'list', 'user'])
            (err, out) = rgwadmin(ctx, dest_client, ['user', 'info', '--uid', user1], check_status=True)
            assert out['user_id'] == user1
            assert out['email'] == email
            assert out['display_name'] == display_name1
            assert len(out['keys']) == 1
            assert out['keys'][0]['access_key'] == access_key
            assert out['keys'][0]['secret_key'] == secret_key
            assert not out['suspended']

        # compare the metadata between different regions, make sure it matches
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err1, out1) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'user:{uid}'.format(uid=user1)], check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client,
                ['metadata', 'get', 'user:{uid}'.format(uid=user1)], check_status=True)
            assert out1 == out2

        # suspend a user on the master, then check the status on the destination
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err, out) = rgwadmin(ctx, source_client, ['user', 'suspend', '--uid', user1])
            rgw_utils.radosgw_agent_sync_all(ctx)
            (err, out) = rgwadmin(ctx, dest_client, ['user', 'info', '--uid', user1], check_status=True)
            assert out['suspended']

        # delete a user on the master, then check that it's gone on the destination
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err, out) = rgwadmin(ctx, source_client, ['user', 'rm', '--uid', user1], check_status=True)
            rgw_utils.radosgw_agent_sync_all(ctx)
            (err, out) = rgwadmin(ctx, source_client, ['user', 'info', '--uid', user1])
            assert out is None
            (err, out) = rgwadmin(ctx, dest_client, ['user', 'info', '--uid', user1])
            assert out is None

            # then recreate it so later tests pass
            (err, out) = rgwadmin(ctx, client, [
                'user', 'create',
                '--uid', user1,
                '--display-name', display_name1,
                '--email', email,
                '--access-key', access_key,
                '--secret', secret_key,
                '--max-buckets', '4'
                ],
                check_status=True)

        # now do the multi-region bucket tests

        # Create a second user for the following tests
        (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user2,
            '--display-name', display_name2,
            '--email', email2,
            '--access-key', access_key2,
            '--secret', secret_key2,
            '--max-buckets', '4'
            ],
            check_status=True)
        (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user2], check_status=True)
        assert out is not None

        # create a bucket and do a sync
        bucket = connection.create_bucket(bucket_name2)
        rgw_utils.radosgw_agent_sync_all(ctx)

        # compare the metadata for the bucket between different regions, make sure it matches
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err1, out1) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            assert out1 == out2

            # get the bucket.instance info and compare that
            src_bucket_id = out1['data']['bucket']['bucket_id']
            dest_bucket_id = out2['data']['bucket']['bucket_id']
            (err1, out1) = rgwadmin(ctx, source_client, ['metadata', 'get',
                'bucket.instance:{bucket_name}:{bucket_instance}'.format(
                bucket_name=bucket_name2,bucket_instance=src_bucket_id)],
                check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client, ['metadata', 'get',
                'bucket.instance:{bucket_name}:{bucket_instance}'.format(
                bucket_name=bucket_name2,bucket_instance=dest_bucket_id)],
                check_status=True)
            assert out1 == out2

        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']

            # Attempt to create a new connection with user1 to the destination RGW
            # and use that to attempt a delete (that should fail)
            exception_encountered = False
            try:
                (dest_remote_host, dest_remote_port) = ctx.rgw.role_endpoints[dest_client]
                connection_dest = boto.s3.connection.S3Connection(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    is_secure=False,
                    port=dest_remote_port,
                    host=dest_remote_host,
                    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                    )

                # this should fail
                connection_dest.delete_bucket(bucket_name2)
            except boto.exception.S3ResponseError as e:
                assert e.status == 301
                exception_encountered = True

            # confirm that the expected exception was seen
            assert exception_encountered

            # now delete the bucket on the source RGW and do another sync
            bucket.delete()
            rgw_utils.radosgw_agent_sync_all(ctx)

        # make sure that the bucket no longer exists in either region
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err1, out1) = rgwadmin(ctx, source_client, ['metadata', 'get',
                'bucket:{bucket_name}'.format(bucket_name=bucket_name2)])
            (err2, out2) = rgwadmin(ctx, dest_client, ['metadata', 'get',
                'bucket:{bucket_name}'.format(bucket_name=bucket_name2)])
            # Both of the previous calls should have errors due to requesting
            # metadata for non-existent buckets
            assert err1
            assert err2

        # create a bucket and then sync it
        bucket = connection.create_bucket(bucket_name2)
        rgw_utils.radosgw_agent_sync_all(ctx)

        # compare the metadata for the bucket between different regions, make sure it matches
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err1, out1) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            assert out1 == out2

        # Now delete the bucket and recreate it with a different user
        # within the same window of time and then sync.
        bucket.delete()
        bucket = connection2.create_bucket(bucket_name2)
        rgw_utils.radosgw_agent_sync_all(ctx)

        # compare the metadata for the bucket between different regions, make sure it matches
        # user2 should own the bucket in both regions
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err1, out1) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            assert out1 == out2
            assert out1['data']['owner'] == user2
            assert out1['data']['owner'] != user1

        # now we're going to use this bucket to test meta-data update propagation
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']

            # get the metadata so we can tweak it
            (err, orig_data) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)

            # manually edit mtime for this bucket to be 300 seconds in the past
            new_data = copy.deepcopy(orig_data)
            new_data['mtime'] =  orig_data['mtime'] - 300
            assert new_data != orig_data
            (err, out) = rgwadmin(ctx, source_client,
                ['metadata', 'put', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                stdin=StringIO(json.dumps(new_data)),
                check_status=True)

            # get the metadata and make sure that the 'put' worked
            (err, out) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            assert out == new_data

            # sync to propagate the new metadata
            rgw_utils.radosgw_agent_sync_all(ctx)

            # get the metadata from the dest and compare it to what we just set
            # and what the source region has.
            (err1, out1) = rgwadmin(ctx, source_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            (err2, out2) = rgwadmin(ctx, dest_client,
                ['metadata', 'get', 'bucket:{bucket_name}'.format(bucket_name=bucket_name2)],
                check_status=True)
            # yeah for the transitive property
            assert out1 == out2
            assert out1 == new_data

        # now we delete the bucket
        bucket.delete()

        # Delete user2 as later tests do not expect it to exist.
        # Verify that it is gone on both regions
        for agent_client, c_config in ctx.radosgw_agent.config.iteritems():
            source_client = c_config['src']
            dest_client = c_config['dest']
            (err, out) = rgwadmin(ctx, source_client,
                ['user', 'rm', '--uid', user2], check_status=True)
            rgw_utils.radosgw_agent_sync_all(ctx)
            # The two 'user info' calls should fail and not return any data
            # since we just deleted this user.
            (err, out) = rgwadmin(ctx, source_client, ['user', 'info', '--uid', user2])
            assert out is None
            (err, out) = rgwadmin(ctx, dest_client, ['user', 'info', '--uid', user2])
            assert out is None

    # end of 'if multi_region_run:'

    # TESTCASE 'suspend-ok','user','suspend','active user','succeeds'
    (err, out) = rgwadmin(ctx, client, ['user', 'suspend', '--uid', user1],
        check_status=True)

    # TESTCASE 'suspend-suspended','user','suspend','suspended user','succeeds w/advisory'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1], check_status=True)
    assert out['suspended']

    # TESTCASE 're-enable','user','enable','suspended user','succeeds'
    (err, out) = rgwadmin(ctx, client, ['user', 'enable', '--uid', user1], check_status=True)

    # TESTCASE 'info-re-enabled','user','info','re-enabled user','no longer suspended'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1], check_status=True)
    assert not out['suspended']

    # TESTCASE 'add-keys','key','create','w/valid info','succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'key', 'create', '--uid', user1,
            '--access-key', access_key2, '--secret', secret_key2,
            ], check_status=True)

    # TESTCASE 'info-new-key','user','info','after key addition','returns all keys'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1],
        check_status=True)
    assert len(out['keys']) == 2
    assert out['keys'][0]['access_key'] == access_key2 or out['keys'][1]['access_key'] == access_key2
    assert out['keys'][0]['secret_key'] == secret_key2 or out['keys'][1]['secret_key'] == secret_key2

    # TESTCASE 'rm-key','key','rm','newly added key','succeeds, key is removed'
    (err, out) = rgwadmin(ctx, client, [
            'key', 'rm', '--uid', user1,
            '--access-key', access_key2,
            ], check_status=True)
    assert len(out['keys']) == 1
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key

    # TESTCASE 'add-swift-key','key','create','swift key','succeeds'
    subuser_access = 'full'
    subuser_perm = 'full-control'

    (err, out) = rgwadmin(ctx, client, [
            'subuser', 'create', '--subuser', subuser1,
            '--access', subuser_access
            ], check_status=True)

    # TESTCASE 'add-swift-key','key','create','swift key','succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'subuser', 'modify', '--subuser', subuser1,
            '--secret', swift_secret1,
            '--key-type', 'swift',
            ], check_status=True)

    # TESTCASE 'subuser-perm-mask', 'subuser', 'info', 'test subuser perm mask durability', 'succeeds'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1])

    assert out['subusers'][0]['permissions'] == subuser_perm

    # TESTCASE 'info-swift-key','user','info','after key addition','returns all keys'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1], check_status=True)
    assert len(out['swift_keys']) == 1
    assert out['swift_keys'][0]['user'] == subuser1
    assert out['swift_keys'][0]['secret_key'] == swift_secret1

    # TESTCASE 'add-swift-subuser','key','create','swift sub-user key','succeeds'
    (err, out) = rgwadmin(ctx, client, [
            'subuser', 'create', '--subuser', subuser2,
            '--secret', swift_secret2,
            '--key-type', 'swift',
            ], check_status=True)

    # TESTCASE 'info-swift-subuser','user','info','after key addition','returns all sub-users/keys'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1], check_status=True)
    assert len(out['swift_keys']) == 2
    assert out['swift_keys'][0]['user'] == subuser2 or out['swift_keys'][1]['user'] == subuser2
    assert out['swift_keys'][0]['secret_key'] == swift_secret2 or out['swift_keys'][1]['secret_key'] == swift_secret2

    # TESTCASE 'rm-swift-key1','key','rm','subuser','succeeds, one key is removed'
    (err, out) = rgwadmin(ctx, client, [
            'key', 'rm', '--subuser', subuser1,
            '--key-type', 'swift',
            ], check_status=True)
    assert len(out['swift_keys']) == 1

    # TESTCASE 'rm-subuser','subuser','rm','subuser','success, subuser is removed'
    (err, out) = rgwadmin(ctx, client, [
            'subuser', 'rm', '--subuser', subuser1,
            ], check_status=True)
    assert len(out['subusers']) == 1

    # TESTCASE 'rm-subuser-with-keys','subuser','rm','subuser','succeeds, second subser and key is removed'
    (err, out) = rgwadmin(ctx, client, [
            'subuser', 'rm', '--subuser', subuser2,
            '--key-type', 'swift', '--purge-keys',
            ], check_status=True)
    assert len(out['swift_keys']) == 0
    assert len(out['subusers']) == 0

    # TESTCASE 'bucket-stats','bucket','stats','no session/buckets','succeeds, empty list'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'stats', '--uid', user1],
        check_status=True)
    assert len(out) == 0

    if multi_region_run:
        rgw_utils.radosgw_agent_sync_all(ctx)

    # TESTCASE 'bucket-stats2','bucket','stats','no buckets','succeeds, empty list'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'list', '--uid', user1], check_status=True)
    assert len(out) == 0

    # create a first bucket
    bucket = connection.create_bucket(bucket_name)

    # TESTCASE 'bucket-list','bucket','list','one bucket','succeeds, expected list'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'list', '--uid', user1], check_status=True)
    assert len(out) == 1
    assert out[0] == bucket_name

    # TESTCASE 'bucket-list-all','bucket','list','all buckets','succeeds, expected list'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'list'], check_status=True)
    assert len(out) >= 1
    assert bucket_name in out;

    # TESTCASE 'max-bucket-limit,'bucket','create','4 buckets','5th bucket fails due to max buckets == 4'
    bucket2 = connection.create_bucket(bucket_name + '2')
    bucket3 = connection.create_bucket(bucket_name + '3')
    bucket4 = connection.create_bucket(bucket_name + '4')
    # the 5th should fail.
    failed = False
    try:
        connection.create_bucket(bucket_name + '5')
    except Exception:
        failed = True
    assert failed

    # delete the buckets
    bucket2.delete()
    bucket3.delete()
    bucket4.delete()

    # TESTCASE 'bucket-stats3','bucket','stats','new empty bucket','succeeds, empty list'
    (err, out) = rgwadmin(ctx, client, [
            'bucket', 'stats', '--bucket', bucket_name], check_status=True)
    assert out['owner'] == user1
    bucket_id = out['id']

    # TESTCASE 'bucket-stats4','bucket','stats','new empty bucket','succeeds, expected bucket ID'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'stats', '--uid', user1], check_status=True)
    assert len(out) == 1
    assert out[0]['id'] == bucket_id    # does it return the same ID twice in a row?

    # use some space
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('one')

    # TESTCASE 'bucket-stats5','bucket','stats','after creating key','succeeds, lists one non-empty object'
    (err, out) = rgwadmin(ctx, client, [
            'bucket', 'stats', '--bucket', bucket_name], check_status=True)
    assert out['id'] == bucket_id
    assert out['usage']['rgw.main']['num_objects'] == 1
    assert out['usage']['rgw.main']['size_kb'] > 0

    # reclaim it
    key.delete()

    # TESTCASE 'bucket unlink', 'bucket', 'unlink', 'unlink bucket from user', 'fails', 'access denied error'
    (err, out) = rgwadmin(ctx, client,
        ['bucket', 'unlink', '--uid', user1, '--bucket', bucket_name],
        check_status=True)

    # create a second user to link the bucket to
    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user2,
            '--display-name', display_name2,
            '--access-key', access_key2,
            '--secret', secret_key2,
            '--max-buckets', '1',
            ],
            check_status=True)

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
    (err, out) = rgwadmin(ctx, client, ['bucket', 'link', '--uid', user2, '--bucket', bucket_name],
        check_status=True)

    # try to remove user, should fail (has a linked bucket)
    (err, out) = rgwadmin(ctx, client, ['user', 'rm', '--uid', user2])
    assert err

    # TESTCASE 'bucket unlink', 'bucket', 'unlink', 'unlink bucket from user', 'succeeds, bucket unlinked'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'unlink', '--uid', user2, '--bucket', bucket_name],
        check_status=True)

    # relink the bucket to the first user and delete the second user
    (err, out) = rgwadmin(ctx, client,
        ['bucket', 'link', '--uid', user1, '--bucket', bucket_name],
        check_status=True)

    (err, out) = rgwadmin(ctx, client, ['user', 'rm', '--uid', user2],
        check_status=True)

    # TESTCASE 'object-rm', 'object', 'rm', 'remove object', 'succeeds, object is removed'

    # upload an object
    object_name = 'four'
    key = boto.s3.key.Key(bucket, object_name)
    key.set_contents_from_string(object_name)

    # now delete it
    (err, out) = rgwadmin(ctx, client,
        ['object', 'rm', '--bucket', bucket_name, '--object', object_name],
        check_status=True)

    # TESTCASE 'bucket-stats6','bucket','stats','after deleting key','succeeds, lists one no objects'
    (err, out) = rgwadmin(ctx, client, [
            'bucket', 'stats', '--bucket', bucket_name],
            check_status=True)
    assert out['id'] == bucket_id
    assert out['usage']['rgw.main']['num_objects'] == 0

    # list log objects
    # TESTCASE 'log-list','log','list','after activity','succeeds, lists one no objects'
    (err, out) = rgwadmin(ctx, client, ['log', 'list'], check_status=True)
    assert len(out) > 0

    for obj in out:
        # TESTCASE 'log-show','log','show','after activity','returns expected info'
        if obj[:4] == 'meta' or obj[:4] == 'data':
            continue

        (err, log) = rgwadmin(ctx, client, ['log', 'show', '--object', obj],
            check_status=True)
        assert len(log) > 0

        # exempt bucket_name2 from checking as it was only used for multi-region tests
        assert log['bucket'].find(bucket_name) == 0 or log['bucket'].find(bucket_name2) == 0
        assert log['bucket'] != bucket_name or log['bucket_id'] == bucket_id
        assert log['bucket_owner'] == user1 or log['bucket'] == bucket_name + '5' or log['bucket'] == bucket_name2
        for entry in log['log_entries']:
            assert entry['bucket'] == log['bucket']
            assert entry['user'] == user1 or log['bucket'] == bucket_name + '5' or log['bucket'] == bucket_name2

        # TESTCASE 'log-rm','log','rm','delete log objects','succeeds'
        (err, out) = rgwadmin(ctx, client, ['log', 'rm', '--object', obj],
            check_status=True)

    # TODO: show log by bucket+date

    # need to wait for all usage data to get flushed, should take up to 30 seconds
    timestamp = time.time()
    while time.time() - timestamp <= (20 * 60):      # wait up to 20 minutes
        (err, out) = rgwadmin(ctx, client, ['usage', 'show', '--categories', 'delete_obj'])  # last operation we did is delete obj, wait for it to flush
        if successful_ops(out) > 0:
            break;
        time.sleep(1)

    assert time.time() - timestamp <= (20 * 60)

    # TESTCASE 'usage-show' 'usage' 'show' 'all usage' 'succeeds'
    (err, out) = rgwadmin(ctx, client, ['usage', 'show'], check_status=True)
    assert len(out['entries']) > 0
    assert len(out['summary']) > 0
    user_summary = out['summary'][0]
    total = user_summary['total']
    assert total['successful_ops'] > 0

    # TESTCASE 'usage-show2' 'usage' 'show' 'user usage' 'succeeds'
    (err, out) = rgwadmin(ctx, client, ['usage', 'show', '--uid', user1],
        check_status=True)
    assert len(out['entries']) > 0
    assert len(out['summary']) > 0
    user_summary = out['summary'][0]
    for entry in user_summary['categories']:
        assert entry['successful_ops'] > 0
    assert user_summary['user'] == user1

    # TESTCASE 'usage-show3' 'usage' 'show' 'user usage categories' 'succeeds'
    test_categories = ['create_bucket', 'put_obj', 'delete_obj', 'delete_bucket']
    for cat in test_categories:
        (err, out) = rgwadmin(ctx, client, ['usage', 'show', '--uid', user1, '--categories', cat],
            check_status=True)
        assert len(out['summary']) > 0
        user_summary = out['summary'][0]
        assert user_summary['user'] == user1
        assert len(user_summary['categories']) == 1
        entry = user_summary['categories'][0]
        assert entry['category'] == cat
        assert entry['successful_ops'] > 0

    # TESTCASE 'usage-trim' 'usage' 'trim' 'user usage' 'succeeds, usage removed'
    (err, out) = rgwadmin(ctx, client, ['usage', 'trim', '--uid', user1],
        check_status=True)
    (err, out) = rgwadmin(ctx, client, ['usage', 'show', '--uid', user1],
        check_status=True)
    assert len(out['entries']) == 0
    assert len(out['summary']) == 0

    # TESTCASE 'user-suspend2','user','suspend','existing user','succeeds'
    (err, out) = rgwadmin(ctx, client, ['user', 'suspend', '--uid', user1],
        check_status=True)

    # TESTCASE 'user-suspend3','user','suspend','suspended user','cannot write objects'
    try:
        key = boto.s3.key.Key(bucket)
        key.set_contents_from_string('five')
    except boto.exception.S3ResponseError as e:
        assert e.status == 403

    # TESTCASE 'user-renable2','user','enable','suspended user','succeeds'
    (err, out) = rgwadmin(ctx, client, ['user', 'enable', '--uid', user1],
        check_status=True)

    # TESTCASE 'user-renable3','user','enable','reenabled user','can write objects'
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('six')

    # TESTCASE 'gc-list', 'gc', 'list', 'get list of objects ready for garbage collection'

    # create an object large enough to be split into multiple parts
    test_string = 'foo'*10000000

    big_key = boto.s3.key.Key(bucket)
    big_key.set_contents_from_string(test_string)

    # now delete the head
    big_key.delete()

    # wait a bit to give the garbage collector time to cycle
    time.sleep(15)

    (err, out) = rgwadmin(ctx, client, ['gc', 'list'])

    assert len(out) > 0

    # TESTCASE 'gc-process', 'gc', 'process', 'manually collect garbage'
    (err, out) = rgwadmin(ctx, client, ['gc', 'process'], check_status=True)

    #confirm
    (err, out) = rgwadmin(ctx, client, ['gc', 'list'])

    assert len(out) == 0

    # TESTCASE 'rm-user-buckets','user','rm','existing user','fails, still has buckets'
    (err, out) = rgwadmin(ctx, client, ['user', 'rm', '--uid', user1])
    assert err

    # delete should fail because ``key`` still exists
    fails = False
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

    (err, out) = rgwadmin(ctx, client,
        ['policy', '--bucket', bucket.name, '--object', key.key],
        check_status=True)

    acl = key.get_xml_acl()

    assert acl == out.strip('\n')

    # add another grantee by making the object public read
    key.set_acl('public-read')

    (err, out) = rgwadmin(ctx, client,
        ['policy', '--bucket', bucket.name, '--object', key.key],
        check_status=True)

    acl = key.get_xml_acl()
    assert acl == out.strip('\n')

    # TESTCASE 'rm-bucket', 'bucket', 'rm', 'bucket with objects', 'succeeds'
    bucket = connection.create_bucket(bucket_name)
    key_name = ['eight', 'nine', 'ten', 'eleven']
    for i in range(4):
        key = boto.s3.key.Key(bucket)
        key.set_contents_from_string(key_name[i])

    (err, out) = rgwadmin(ctx, client,
        ['bucket', 'rm', '--bucket', bucket_name, '--purge-objects'],
        check_status=True)

    # TESTCASE 'caps-add', 'caps', 'add', 'add user cap', 'succeeds'
    caps='user=read'
    (err, out) = rgwadmin(ctx, client, ['caps', 'add', '--uid', user1, '--caps', caps])

    assert out['caps'][0]['perm'] == 'read'

    # TESTCASE 'caps-rm', 'caps', 'rm', 'remove existing cap from user', 'succeeds'
    (err, out) = rgwadmin(ctx, client, ['caps', 'rm', '--uid', user1, '--caps', caps])

    assert not out['caps']

    # TESTCASE 'rm-user','user','rm','existing user','fails, still has buckets'
    bucket = connection.create_bucket(bucket_name)
    key = boto.s3.key.Key(bucket)

    (err, out) = rgwadmin(ctx, client, ['user', 'rm', '--uid', user1])
    assert err

    # TESTCASE 'rm-user2', 'user', 'rm', 'user with data', 'succeeds'
    bucket = connection.create_bucket(bucket_name)
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('twelve')

    (err, out) = rgwadmin(ctx, client,
        ['user', 'rm', '--uid', user1, '--purge-data' ],
        check_status=True)

    # TESTCASE 'rm-user3','user','rm','deleted user','fails'
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user1])
    assert err

    # TESTCASE 'zone-info', 'zone', 'get', 'get zone info', 'succeeds, has default placement rule'
    #

    (err, out) = rgwadmin(ctx, client, ['zone', 'get'])
    orig_placement_pools = len(out['placement_pools'])

    # removed this test, it is not correct to assume that zone has default placement, it really
    # depends on how we set it up before
    #
    # assert len(out) > 0
    # assert len(out['placement_pools']) == 1

    # default_rule = out['placement_pools'][0]
    # assert default_rule['key'] == 'default-placement'

    rule={'key': 'new-placement', 'val': {'data_pool': '.rgw.buckets.2', 'index_pool': '.rgw.buckets.index.2'}}

    out['placement_pools'].append(rule)

    (err, out) = rgwadmin(ctx, client, ['zone', 'set'],
        stdin=StringIO(json.dumps(out)),
        check_status=True)

    (err, out) = rgwadmin(ctx, client, ['zone', 'get'])
    assert len(out) > 0
    assert len(out['placement_pools']) == orig_placement_pools + 1
