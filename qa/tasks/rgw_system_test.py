import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)
import os
import pwd
import cStringIO
import time

def do_auto_calculate_io(clients, config, test_config):
    # re weight osds

    """

    # this code may be come of use in future, so just keep in comment.

    out = cStringIO.StringIO()
    clients[0].run(args=['sudo', 'ceph', 'osd', 'df'], stdout=out)
    var = out.readlines()

    osd_weights = {i.split()[0]:i.split()[1] for i in var[1:len(var)-2]}

    for id, weight in osd_weights.items():
        clients[0].run(args=[run.Raw(
            'sudo ceph osd reweight %s %s' % (id, weight))])

    """

    # get the cluster size

    out = cStringIO.StringIO()
    clients[0].run(args=['sudo', 'ceph', 'df'], stdout=out)
    var = out.readlines()
    cluster_size = dict(zip(var[1].split(), var[2].split()))

    available_cluster_size = int(cluster_size['AVAIL'][:-1]) * 1024  # size in MBs

    log.info('cluster available size from parsing: %s' % available_cluster_size)

    available_cluster_size = available_cluster_size / 3  # assuming the replication size is 3, i.e the default value

    log.info('calculated available size: %s' % available_cluster_size)

    usable_size = int(
        (float(60) / float(100)) * available_cluster_size)  # 60 % of cluster size is being used for automation tests

    osize = usable_size / \
            (int(test_config['user_count']) *
             int(test_config['bucket_count']) *
             int(test_config['objects_count']))

    if osize < 10:
        log.error('test does not support small size objects less than 10 MB ')
        exit(1)

    if 'version' in config['test-name']:
        osize = (osize / int(test_config['version_count']))

    test_config['min_file_size'] = osize - 5
    test_config['max_file_size'] = osize

    log.info('calucated object max size: %s' % test_config['max_file_size'])
    log.info('calucated object min size: %s' % test_config['min_file_size'])

    clients[0].run(args=['sudo', 'ceph', 'osd', 'crush', 'tunables', 'optimal'])

    return test_config


def execute_v1_tests(config, clients):
    log.info('starting rgw_v1_system_tests')

    data = None
    test_config = config['config']

    if config['test-name'] == 'test_Mbuckets':
        data = dict(
            config=dict(
                user_count=test_config['user_count'],
                bucket_count=test_config['bucket_count'],
                objects_count=0,

            )
        )

    if config['test-name'] == 'test_Mbuckets_with_Nobjects':
        data = dict(
            config=dict(
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
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
                user_count=test_config['user_count'],
                objects_size_range=dict(
                    min=test_config['min_file_size'],
                    max=test_config['max_file_size']
                )

            )
        )

    script_fname = config['test-name'] + ".py"
    yaml_fname = script_fname + ".yaml"

    log.info('test: %s' % script_fname)

    temp_yaml_file = yaml_fname + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

    log.info('creating yaml from the config: %s' % data)
    local_file = '/tmp/' + temp_yaml_file
    with open(local_file, 'w') as outfile:
        outfile.write(yaml.dump(data, default_flow_style=False))

    log.info('copying yaml to the client node')

    destination_location = 'rgw-tests/ceph-qe-scripts/rgw/v1/tests/s3/yamls/' + yaml_fname
    clients[0].put_file(local_file, destination_location)

    clients[0].run(args=['ls', '-lt', 'rgw-tests/ceph-qe-scripts/rgw/v1/tests/s3/yamls/'])

    clients[0].run(args=['cat', 'rgw-tests/ceph-qe-scripts/rgw/v1/tests/s3/yamls/' + yaml_fname])

    clients[0].run(args=['sudo', 'rm', '-f', run.Raw('%s' % local_file)], check_status=False)

    clients[0].run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/v1/tests/s3/%s '
                '-c rgw-tests/ceph-qe-scripts/rgw/v1/tests/s3/yamls/%s ' % (script_fname, yaml_fname))])


def execute_v2_tests():
    # will be used if any of the v2 tests needed to be run as part of system tests

    log.info('starting rgw_v2_system_tests')

    pass


@contextlib.contextmanager
def task(ctx, config):
    log.info('starting rgw_system_tests')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    remotes = ctx.cluster.only(teuthology.is_type('client'))
    clients = [
        remote for remote,
                   roles_for_host in remotes.remotes.iteritems()]

    log.info('cloning the repo to client.0 machines')

    clients[0].run(args=['sudo', 'rm', '-rf', 'rgw-tests'], check_status=False)
    clients[0].run(args=['mkdir', 'rgw-tests'])
    clients[0].run(
        args=[
            'cd',
            'rgw-tests',
            run.Raw(';'),
            'git',
            'clone',
            'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git',
        ])

    clients[0].run(args=['virtualenv', 'venv'])
    clients[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install boto names PyYaml ConfigParser'),
            run.Raw(';'),
            'deactivate'])

    time.sleep(60)
    log.info('trying to restart rgw service after sleep 60 secs')

    clients[0].run(args=[run.Raw('sudo systemctl restart ceph-radosgw.target')])

    log.info('starting the tests after sleep of 60 secs')
    time.sleep(60)

    log.info('test_dir_version: %s' % config['test_dir_version'])

    if config['test_dir_version'] == 'v1':
        execute_v1_tests(config, clients)

    if config['test_dir_version'] == 'v2':
        execute_v2_tests()

    try:
        yield
    finally:

        clients[0].run(
            args=[
                'source',
                'venv/bin/activate',
                run.Raw(';'),
                run.Raw('pip uninstall boto names PyYaml -y'),
                run.Raw(';'),
                'deactivate'])

        log.info('Test completed')

        log.info("Deleting repos")

        cleanup = lambda x: clients[0].run(args=[run.Raw('sudo rm -rf %s' % x)])

        soot = ['venv', 'rgw-tests', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                '*.key.*']

        map(cleanup, soot)
