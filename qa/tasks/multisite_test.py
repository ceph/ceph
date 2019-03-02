import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
log = logging.getLogger(__name__)
import os
import pwd
import time
import argparse

"""
# Test yaml to test script mapper for boto3

tests_mapper_v2 = {'test_Mbuckets_basic': 'test_Mbuckets_basic',
                   'test_Mbuckets_with_Nobjects_basic': 'test_Mbuckets_with_Nobjects_basic',
                   'test_Mbuckets_with_Nobjects_delete': 'test_Mbuckets_with_Nobjects',
                   'test_Mbuckets_with_Nobjects_download': 'test_Mbuckets_with_Nobjects',
                   'test_Mbuckets_with_Nobjects_sharding': 'test_Mbuckets_with_Nobjects',
                   'test_Mbuckets_with_Nobjects_encryption': 'test_Mbuckets_with_Nobjects',
                   'test_bucket_lifecycle_config_disable': 'test_bucket_lifecycle_config_ops',
                   'test_bucket_lifecycle_config_modify': 'test_bucket_lifecycle_config_ops',
                   'test_bucket_lifecycle_config_read': 'test_bucket_lifecycle_config_ops',
                   'test_bucket_lifecycle_config_versioning': 'test_bucket_lifecycle_config_ops',
                   'test_acls': 'test_acls',
                   'test_bucket_policy_delete': 'test_bucket_policy_ops',
                   'test_bucket_policy_modify': 'test_bucket_policy_ops',
                   'test_bucket_policy_replace': 'test_bucket_policy_ops',
                   'test_bucket_request_payer': 'test_bucket_request_payer',
                   'test_bucket_request_payer_download': 'test_bucket_request_payer',
                   'test_dynamic_sharding_offline': 'test_dynamic_bucket_resharding',
                   'test_dynamic_sharding_online': 'test_dynamic_bucket_resharding',
                   'test_multitenant_access': 'test_multitenant_user_access',
                   'test_storage_policy_s3': 'test_storage_policy',
                   'test_storage_policy_swift': 'test_storage_policy',
                   'test_swift_basic_ops': 'test_swift_basic_ops',
                   'test_versioning_enable': 'test_versioning_with_objects',
                   'test_versioning_objects_copy': 'test_versioning_copy_objects',
                   'test_versioning_objects_delete': 'test_versioning_with_objects',
                   'test_versioning_objects_enable': 'test_versioning_with_objects',
                   'test_versioning_objects_suspend': 'test_versioning_with_objects',
                   'test_versioning_objects_suspend_reupload': 'test_versioning_with_objects',
                   }

"""


def get_remotes(ctx):

    rgws = ctx.cluster.only(teuthology.is_type('rgw'))
    haproxys = ctx.cluster.only(teuthology.is_type('haproxy'))
    remotes = []
    for remote, roles_for_host in rgws.remotes.iteritems():
        remotes.append(remote)
    for remote, roles_for_host in haproxys.remotes.iteritems():
        remotes.append(remote)

    return remotes


def user_creation(ctx, user_config, mclient, version):

    log.info('Create user on master client')

    temp_yaml_file = 'user_create_' + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

    #        temp_yaml_file = 'user_create.yaml'

    if user_config is None:
        assert isinstance(user_config, dict), "configuration not given"

    log.info('creating yaml from the config: %s' % user_config)
    local_file = '/tmp/' + temp_yaml_file
    with open(local_file,  'w') as outfile:
        outfile.write(yaml.dump(user_config, default_flow_style=False))

    log.info('copying yaml to the client node')
    destination_location = \
        ('rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/' % version + temp_yaml_file)
    mclient.put_file(local_file,  destination_location)
    mclient.run(args=['ls', '-lt',
                      'rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/' % version])
    mclient.run(args=['cat',
                      'rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/' % version + temp_yaml_file])

    #        mclient.run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

    mclient.run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/%s '
                '-c rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/%s '
                % (version, 'user_create.py', version, temp_yaml_file))])

    log.info('copy user_details file from source client into local dir')

    user_file = mclient.get_file('user_details', '/tmp')

    time.sleep(10)

    log.info('copy user_file to target client')

#    if mclient != tclient:
#        tclient.put_file(user_file, 'user_details')

    remotes = get_remotes(ctx)

    for remote in remotes:
        if remote != mclient:
            log.info('copy user_details to {}'.format(remote))
            remote.put_file(user_file, 'user_details')


def test_data(tclient, test_name, script_name, version):

    tclient.run(args=['ls', '-lt',
                      'rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/' % version])
    tclient.run(args=['cat',
                      'rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/' % version + test_name])

    tclient.run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/%s '
                '-c rgw-tests/ceph-qe-scripts/rgw/%s/tests/multisite/yamls/%s '
                % (version, script_name, version, test_name))])


def copy_file_from(src_node, dest_node, file_path='/home/ubuntu/io_info.yaml'):

    # copies to /tmp dir and then puts it in destination machines

    log.info('copy of io_info.yaml from %s initiated' % src_node)

#    io_info_file = src_node.get_file(file_path, '/tmp')

    io_info_file = teuthology.get_file(
                remote=src_node,
                path=file_path,
    )

    time.sleep(10)

    log.info('copy io_info_file to %s' % dest_node)

    teuthology.sudo_write_file(
        remote=dest_node,
        path=file_path,
        data=io_info_file)

#    dest_node.put_file(io_info_file, file_name)

    log.info('copy of io_info.yaml completed')


@contextlib.contextmanager
def pull_io_info(ctx, config):

    # copy file from the node running tests to all other rgw nodes
    """
        - multisite_test.pull_io_info:
    """

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    tclient = ctx.multisite_test.target

    remotes = get_remotes(ctx)

    for remote in remotes:
        if remote != tclient:
            copy_file_from(tclient, remote)

    yield


def cleanup(ctx):

    remotes = get_remotes(ctx)

    for remote in remotes:
        cleanup = lambda x: remote.run(args=[run.Raw('sudo rm -rf %s' % x)])

        soot = ['venv', 'rgw-tests', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                '*.key.*', 'user_details', 'io_info.yaml']

        map(cleanup, soot)


def clone_repo(ctx):
    remotes = get_remotes(ctx)

    for remote in remotes:
        remote.run(args=['mkdir', 'rgw-tests'])
        remote.run(
            args=[
                'cd',
                'rgw-tests',
                run.Raw(';'),
                'git',
                'clone',
                '-b',
                'add-encryption',
                'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git',
                ])

        remote.run(args=['virtualenv', 'venv'])
        remote.run(
            args=[
                'source',
                'venv/bin/activate',
                run.Raw(';'),
                run.Raw('pip install boto boto3 names python-swiftclient PyYaml psutil ConfigParser simplejson'),
                run.Raw(';'),
                'deactivate'])


@contextlib.contextmanager
def userexec(ctx, config):

    # Create user and copy the user_details to target client

    """
    -multisite-test.userexec:
        test_dir_version: v1
        master_client: source.rgw.0
        master_config:
            user_count: 3
    """

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    if not hasattr(ctx, 'userexec'):
        ctx.userexec = argparse.Namespace()

    assert isinstance(config, dict), \
        "task userexec only supports a dictionary for configuration"

    log.info('cloning the repo to client machines')

    cleanup(ctx)
    clone_repo(ctx)

    master_client = config['master_client']
    (mclient,) = ctx.cluster.only(master_client).remotes.iterkeys()

    user_config = config['master_config']

    user_data = None

    user_data = dict(
        config=dict(
            user_count=user_config['user_count'],
        )
    )

    if config['test_dir_version'] == 'v1':
        user_creation(ctx, user_data, mclient, version='v1')
    elif config['test_dir_version'] == 'v2':
        user_creation(ctx, user_data, mclient, version='v2')

    yield


def execute_v1(tclient, config):

    # Tests using boto2 here

    test_name = config['test-name'] + ".yaml"
    script_name = config['test-name'] + ".py"

    log.info('test name :%s' % config['test-name'])

    # Execute  test

    test_data(tclient, test_name, script_name, version='v1')


def execute_v2(tclient, config):

    # Tests using boto3 here

    test_name = config['test_name'] + ".yaml"
    script_name = config['script_name'] + ".py"

    log.info('test name :%s' % config['test_name'])

    # Execute  test

    test_data(tclient, test_name, script_name, version='v2')


@contextlib.contextmanager
def task(ctx, config):

    """
    - multisite-test:
      test-name: test_multipart_upload_download
      test_dir_version: v1
      test_client: c2.rgw.1
      target_config:
          bucket_count: 5
          min_file_size: 100
          max_file_size: 200

    - multisite-test:
      test_name: test_bucket_policy_replace
      script_name: test_bucket_policy_ops
      test_dir_version: v2
      test_client: c1.rgw.0
    """

    log.info('starting the task')

    log.info('config %s' % config)

    assert isinstance(config, dict), \
        "task multisite_test only supports a dictionary for configuration"

    # Target node where the tests will be run. Can be primary or secondary multisite zones.

    target_client = config['test_client']
    (tclient,) = ctx.cluster.only(target_client).remotes.iterkeys()

    if not hasattr(ctx, 'multisite_test'):
        ctx.multisite_test = argparse.Namespace()

    ctx.multisite_test.target = tclient
    ctx.multisite_test.version = config['test_dir_version']

    if not hasattr(ctx, 'userexec'):
        cleanup(ctx)
        clone_repo(ctx)

    log.info('test_dir_version: %s' % config['test_dir_version'])

    if config['test_dir_version'] == 'v1':
        execute_v1(tclient, config)

    if config['test_dir_version'] == 'v2':
        execute_v2(tclient, config)

    yield
