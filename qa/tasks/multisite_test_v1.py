import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
log = logging.getLogger(__name__)
import os
import pwd
import time


def user_creation(user_config, mclient, tclient):

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
        'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/' + temp_yaml_file
    mclient.put_file(local_file,  destination_location)
    mclient.run(args=['ls', '-lt',
                      'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/'])
    mclient.run(args=['cat',
                      'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/' + temp_yaml_file])

    #        mclient.run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

    mclient.run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/%s '
                '-c rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/%s ' % ('user_create.py', temp_yaml_file))])

    log.info('copy user_details file from source client into local dir')

    user_file = mclient.get_file('user_details', '/tmp')

    time.sleep(20)

    log.info('copy user_file to target client')

    if mclient != tclient:
        tclient.put_file(user_file, 'user_details')


def test_data(script_name, data_config, tclient):

    script_fname = script_name + ".py"

    yaml_fname = script_name + ".yaml"

    log.info('test: %s' % script_fname)

    temp_yaml_file = yaml_fname + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

    if data_config is None:
        assert isinstance(data_config, dict), "configuration not given"

    log.info('creating yaml from the config: %s' % data_config)
    local_file = '/tmp/' + temp_yaml_file
    with open(local_file,  'w') as outfile:
        outfile.write(yaml.dump(data_config, default_flow_style=False))

    log.info('copying yaml to the client node')
    destination_location = \
        'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/' + yaml_fname
    tclient.put_file(local_file,  destination_location)
    tclient.run(args=['ls', '-lt',
                      'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/'])
    tclient.run(args=['cat',
                      'rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/' + yaml_fname])

    tclient.run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

    tclient.run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/%s '
                '-c rgw-tests/ceph-qe-scripts/rgw/v1/tests/multisite/yamls/%s ' % (script_fname, yaml_fname))])


def copy_file_from(src_node, dest_node, file_name = 'io_info.yaml'):

    # copies to /tmp dir and then puts it in destination machines

    log.info('copy of io_info.yaml from initiated')

    io_info_file = src_node.get_file(file_name, '/tmp')

    dest_node.put_file(io_info_file, file_name)

    log.info('copy of io_info.yaml completed')


def test_exec(ctx, config, data, tclient, mclient):

    assert data is not None, "Got no test in configuration"

    log.info('test name :%s' % config['test-name'])

    script_name = config['test-name']

    log.info('script_name: %s' % script_name)

    test_data(script_name, data, tclient=tclient)

    # copy the io_yaml from from target node to master node.

    time.sleep(60)
    # wait for sync

    # no verification is being done for acls test cases right now.

    if not 'acls' in script_name:

        log.info('no test with acls: %s' % script_name)

    # copy file from the node running tests to all other rgw nodes

    if mclient != tclient:
        mclient.run(args=[run.Raw('sudo mv io_info.yaml io_info_2.yaml')])

    clients = ctx.cluster.only(teuthology.is_type('rgw'))
    for remote, roles_for_host in clients.remotes.iteritems():
        if remote != tclient:
            copy_file_from(tclient, remote)


@contextlib.contextmanager
def userexec(ctx, config):

    # Create user and copy the user_details to target client

    """
    -multisite-test.userexec:
        master_client: source.rgw.0
        master_config:
            cluster_name: source
            user_count: 3
        target_client: target.rgw.1
    """

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    log.info('cloning the repo to client machines')

    remotes = ctx.cluster.only(teuthology.is_type('rgw'))
    for remote, roles_for_host in remotes.remotes.iteritems():

        cleanup = lambda x: remote.run(args=[run.Raw('sudo rm -rf %s' % x)])

        soot = ['venv', 'rgw-tests', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                '*.key.*', 'user_details', 'io_info.yaml', 'io_info_2.yaml']

        map(cleanup, soot)

        remote.run(args=['mkdir', 'rgw-tests'])
        remote.run(
            args=[
                'cd',
                'rgw-tests',
                run.Raw(';'),
                'git',
                'clone',
                'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git',
                ])

        remote.run(args=['virtualenv', 'venv'])
        remote.run(
            args=[
                'source',
                'venv/bin/activate',
                run.Raw(';'),
                run.Raw('pip install boto names PyYaml ConfigParser simplejson'),
                run.Raw(';'),
                'deactivate'])

    master_client = config['master_client']
    (mclient,) = ctx.cluster.only(master_client).remotes.iterkeys()

    target_client = config['target_client']
    (tclient,) = ctx.cluster.only(target_client).remotes.iterkeys()

    user_config = config['master_config']

    user_data = None

    user_data = dict(
        config=dict(
            cluster_name=user_config['cluster_name'],
            user_count=user_config['user_count'],
        )
    )

    user_creation(user_data, mclient, tclient)

    yield


@contextlib.contextmanager
def task(ctx, config):
    """

    tasks:
    - multisite-test-v1:
        test-name: test_Mbuckets
        master_client: source.client.0
        target_client: target.client.1
        target_config:
            bucket_count: 5

    tasks:
    - multisite-test-v1:
        test-name: test_Mbuckets_with_Nobjects
        master_client: source.client.0
        target_client: target.client.1
        target_config:
            user_count: 3
            bucket_count: 5
            objects_count: 5
            min_file_size: 5
            max_file_size: 10

    tasks:
    - multisite-test-v1:
          test-name: test_bucket_with_delete
          master_client: c1.rgw.0
          target_client: c2.rgw.1
          target_config:
              bucket_count: 5
              objects_count: 5
              min_file_size: 5
              max_file_size: 10



    tasks:
    - multisite-test-v1:
          test-name: test_multipart_upload
          master_client: c1.rgw.0
          target_client: c2.rgw.1
          target_config:
              bucket_count: 5
              min_file_size: 5
              max_file_size: 10

    tasks:
    - multisite-test-v1:
          test-name: test_multipart_upload_download
          master_client: c1.rgw.0
          target_client: c2.rgw.1
          target_config:
              bucket_count: 5
              min_file_size: 100
              max_file_size: 200


    tasks:
    - multisite-test-v1:
      test-name: test_multipart_upload_cancel
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          break_at_part_no: 10
          min_file_size: 100
          max_file_size: 200

    tasks:
    - multisite-test-v1:
      test-name: test_basic_versioning
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          version_count: 5
          min_file_size: 10
          max_file_size: 20

    tasks:
    - multisite-test-v1:
      test-name: test_delete_key_versions
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          version_count: 5
          min_file_size: 10
          max_file_size: 20

    tasks:
    - multisite-test:
      test-name: test_suspend_versioning
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          version_count: 5
          min_file_size: 10
          max_file_size: 20

    tasks:
    - multisite-test-v1:
      test-name: test_version_with_revert
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          version_count: 5
          min_file_size: 10
          max_file_size: 20


    tasks:
    - multisite-test-v1:
      test-name: test_acls
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          min_file_size: 10
          max_file_size: 20


    tasks:
    - multisite-test-v1:
      test-name: test_acls_all_usrs
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          min_file_size: 10
          max_file_size: 20

    tasks:
    - multisite-test-v1:
      test-name: test_acls_copy_obj
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          objects_count: 10
          min_file_size: 10
          max_file_size: 20

    tasks:
     multisite-test-v1:
      test-name: test_acls_reset
      master_client: c1.rgw.0
      target_client: c2.rgw.1
      target_config:
          bucket_count: 5
          objects_count: 10
          min_file_size: 10
          max_file_size: 20


    """

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task multisite_test only supports a dictionary for configuration"

    master_client = config['master_client']
    (mclient,) = ctx.cluster.only(master_client).remotes.iterkeys()

    target_client = config['target_client']
    (tclient,) = ctx.cluster.only(target_client).remotes.iterkeys()

    test_config = config['target_config']
    data = None

    if config['test-name'] == 'test_Mbuckets':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count']
            )
        )

    if config['test-name'] == 'test_Mbuckets_with_Nobjects':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_Mbuckets_with_Nobjects_shards':

        # changing the value of config['test-name'] to take test_Mbuckets_with_Nobjects,
        # since this test also takes the following configuration

        config['test-name'] = 'test_Mbuckets_with_Nobjects'

        data = dict(
            config=dict(
                shards=test_config['shards'],
                max_objects=test_config['max_objects'],
                bucket_count=test_config['bucket_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_bucket_with_delete':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    # multipart

    if config['test-name'] == 'test_multipart_upload':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_multipart_upload_download':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_multipart_upload_cancel':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                break_at_part_no=test_config['break_at_part_no'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    # Versioning

    if config['test-name'] == 'test_basic_versioning':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                version_count=test_config['version_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_delete_key_versions':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                version_count=test_config['version_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_suspend_versioning':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                version_count=test_config['version_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_version_with_revert':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                version_count=test_config['version_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    # ACLs

    if config['test-name'] == 'test_acls':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_acls_all_usrs':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_acls_copy_obj':

        data = dict(
            config=dict(
                objects_count=test_config['objects_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    if config['test-name'] == 'test_acls_reset':

        data = dict(
            config=dict(
                objects_count=test_config['objects_count'],
                bucket_count=test_config['bucket_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    test_exec(ctx, config, data, tclient, mclient)

    try:
        yield
    finally:

        remotes = ctx.cluster.only(teuthology.is_type('rgw'))
        for remote, roles_for_host in remotes.remotes.iteritems():

            remote.run(
                args=[
                    'source',
                    'venv/bin/activate',
                    run.Raw(';'),
                    run.Raw('pip uninstall boto names PyYaml -y'),
                    run.Raw(';'),
                    'deactivate'])

            log.info('test completed')

            log.info("Deleting repos")

            cleanup = lambda x: remote.run(args=[run.Raw('sudo rm -rf %s' % x)])

            soot = ['venv', 'rgw-tests', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                    '*.key.*', 'user_details', 'io_info.yaml', 'io_info_2.yaml']

            map(cleanup, soot)
