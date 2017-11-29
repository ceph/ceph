import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
log = logging.getLogger(__name__)
import os
import pwd
import time


class Test(object):

    def __init__(self, script_name, user_configuration, configuration, port_number=None):
        self.script_fname = script_name + ".py"
        self.yaml_fname = script_name + ".yaml"
        self.user_data = user_configuration
        self.data = configuration
        # self.port_number = port_number

    def master_execution(self, mclient):

        log.info('Create user on master client')

        temp_yaml_file = 'user_create_' + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

#        temp_yaml_file = 'user_create.yaml'

        if self.user_data is None:
            assert isinstance(self.user_data, dict), "configuration not given"

        user_data = self.user_data

        log.info('creating yaml from the config: %s' % user_data)
        local_file = '/tmp/' + temp_yaml_file
        with open(local_file,  'w') as outfile:
            outfile.write(yaml.dump(user_data, default_flow_style=False))

        log.info('copying yaml to the client node')
        destination_location = \
            'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/' + temp_yaml_file
        mclient.put_file(local_file,  destination_location)
        mclient.run(args=['ls', '-lt',
                             'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/'])
        mclient.run(args=['cat',
                             'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/' + temp_yaml_file])

#        mclient.run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

        mclient.run(
            args=[
                run.Raw(
                    'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/tests/multisite/%s '
                    '-c rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/%s ' % ('user_create.py', temp_yaml_file))])

        log.info('copy user_details file from source client into local dir')

        self.user_file = mclient.get_file('user_details', '/tmp')

#        os.unlink(local_file)

    def target_execution(self, mclient, tclient):

        log.info('test: %s' % self.script_fname)

        temp_yaml_file = self.yaml_fname + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

        if self.data is None:
            assert isinstance(self.data, dict), "configuration not given"

        data = self.data

        log.info('copy user_file to target client')

        if mclient != tclient:
            tclient.put_file(self.user_file, 'user_details')

        log.info('creating yaml from the config: %s' % data)
        local_file = '/tmp/' + temp_yaml_file
        with open(local_file,  'w') as outfile:
            outfile.write(yaml.dump(data, default_flow_style=False))

        log.info('copying yaml to the client node')
        destination_location = \
            'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/' + self.yaml_fname
        tclient.put_file(local_file,  destination_location)
        tclient.run(args=['ls', '-lt',
                             'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/'])
        tclient.run(args=['cat',
                             'rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/' + self.yaml_fname])

        tclient.run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

        tclient.run(
            args=[
                run.Raw(
                    'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/tests/multisite/%s '
                    '-c rgw-tests/ceph-qe-scripts/rgw/tests/multisite/yamls/%s ' % (self.script_fname, self.yaml_fname))])


def copy_file_from(src_node, dest_node, file_name = 'io_info.yaml'):

    # copies to /tmp dir and then puts it in destination machines

    io_info_file = src_node.get_file(file_name, '/tmp')

    dest_node.put_file(io_info_file, file_name, sudo=True)


def test_exec(ctx, config, user_data, data, tclient, mclient):

    assert data is not None, "Got no test in configuration"

    log.info('test name :%s' % config['test-name'])

    script_name = config['test-name']
    # port_number = config['port_number']

    test = Test(script_name, user_data, data)
    test.master_execution(mclient)
    # wait until users are synced to target
    time.sleep(20)
    test.target_execution(mclient, tclient)

    # copy the io_yaml from from target node to master node.

    time.sleep(60)
    # wait for sync

    if not 'acls' in script_name:

        # no verification is being done for acls test cases right now.

        # copy_file_from(tclient, mclient)

        log.info('copy file io_info.yaml from target node to master node initiated')

        io_info_fname = tclient.get_file('io_info.yaml', '/tmp')
        mclient.put_file(io_info_fname, 'io_info.yaml')

        log.info('copy file io_info.yaml from target to master node completed')

        # start verify of io on master node.

        mclient.run(
            args=[
                run.Raw(
                    'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/lib/read_io_info.py')])


@contextlib.contextmanager
def task(ctx, config):
    """

    tasks:
    - rgw-system-test:
        test-name: test_Mbuckets
        master_client: source.client.0
        master_config:
            cluster_name: source
            user_count: 3
        target_client: target.client.1
        target_config:
            bucket_count: 5

    tasks:
    - rgw-system-test:
        client: client.0
        test-name: test_Mbuckets_with_Nobjects
        config:
            cluster_name: ceph
            user_count: 3
            bucket_count: 5
            objects_count: 5
            min_file_size: 5
            max_file_size: 10

    tasks:
    - rgw-system-test:
        client: client.0
        test-name: test_bucket_with_delete
        config:
            cluster_name: ceph
            user_count: 3
            bucket_count: 5
            objects_count: 5
            min_file_size: 5
            max_file_size: 10


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_multipart_upload
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           min_file_size: 5
           max_file_size: 10

    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_multipart_upload_download
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           min_file_size: 5


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_multipart_upload_cancel
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           break_at_part_no: 90
           min_file_size: 1500
           max_file_size: 1000

    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_basic_versioning
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           objects_count: 90
           version_count: 5
           min_file_size: 1000
           max_file_size: 1500


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_delete_key_versions
       config:
            cluster_name: ceph
            user_count: 3
            bucket_count: 5
            objects_count: 90
            version_count: 5
            min_file_size: 1000
            max_file_size: 1500

    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_suspend_versioning
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           objects_count: 90
           version_count: 5
           min_file_size: 1000
           max_file_size: 1500


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_version_with_revert
       config:
           cluster_name: ceph
           user_count: 3
           bucket_count: 5
           objects_count: 90
           version_count: 5
           min_file_size: 1000
           max_file_size: 1500


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_acls
       config:
           cluster_name: ceph
           bucket_count: 5
           objects_count: 90
           min_file_size: 1000
           max_file_size: 1500


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_acls_all_usrs
       config:
           cluster_name: ceph
           bucket_count: 5
           user_count: 3
           objects_count: 90
           min_file_size: 1000
           max_file_size: 1500

    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_acls_copy_obj
       config:
           cluster_name: ceph
           objects_count: 90
           min_file_size: 1000
           max_file_size: 1500


    tasks:
    - rgw-system-test:
       client: client.0
       test-name: test_acls_reset
       config:
           cluster_name: ceph
           bucket_count: 5
           user_count: 3
           objects_count: 90
           min_file_size: 1000
           max_file_size: 1500


    """

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    log.info('cloning the repo to client machines')

    remotes = ctx.cluster.only(teuthology.is_type('client'))
    for remote, roles_for_host in remotes.remotes.iteritems():

        remote.run(args=['sudo', 'rm', '-rf', 'rgw-tests'], check_status=False)
        remote.run(args=['mkdir', 'rgw-tests'])
        remote.run(
            args=[
                'cd',
                'rgw-tests',
                run.Raw(';'),
                'git',
                'clone',
                'http://gitlab.osas.lab.eng.rdu2.redhat.com/ceph/ceph-qe-scripts.git',
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
#    master_client = 'source.client.0'
#    (mclient,) = ctx.cluster.only(master_client).remotes.iterkeys()

    config['port_number'] = '8080'
    user_config = config['master_config']

    user_data = None

    user_data=dict(
        config=dict(
            cluster_name = user_config['cluster_name'],
            user_count = user_config['user_count'],
        )
    )

    target_client = config['target_client']
    (tclient,) = ctx.cluster.only(target_client).remotes.iterkeys()

    test_config = config['target_config']
    data = None

    if config['test-name'] == 'test_Mbuckets':

        data = dict(
            config=dict(
                bucket_count=test_config['bucket_count'],
                objects_count=0,

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
                bucket_count=test_config['bucket_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    test_exec(ctx, config, user_data, data, tclient, mclient)

    try:
        yield
    finally:

        remotes = ctx.cluster.only(teuthology.is_type('client'))
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
                    '*.key.*', 'user_details', 'io_info.yaml']

            map(cleanup, soot)