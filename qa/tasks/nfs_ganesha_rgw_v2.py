import contextlib
import logging
import os
from teuthology import misc as teuthology
from teuthology.orchestra import run
import pwd
import yaml
import cStringIO
import json
import psutil
import re
import time

log = logging.getLogger(__name__)

# format in {"test-name": "script-file"}

tests_mapper = {'test_on_nfs_io_create': 'test_on_nfs_io',
                'test_on_nfs_io_delete': 'test_on_nfs_io',
                'test_on_nfs_io_move': 'test_on_nfs_io',
                'test_on_s3_io_create': 'test_on_s3_io',
                'test_on_s3_io_delete': 'test_on_s3_io',
                'test_on_s3_io_move': 'test_on_s3_io',
                }


@contextlib.contextmanager
def task(ctx, config):

    log.info('starting nfs_ganesha_rgw tests')
    # RGW and NFS should be on the same machine

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    test_name = config['test-name'] + ".yaml"
    script_name = tests_mapper.get(config['test-name'], None) + ".py"
    nfs_version = config['nfs-version']
    mount_dir = config['mount-dir']

    log.info('got test_name: %s' % test_name)
    log.info('got nfs version: %s' % nfs_version)
    log.info('got mount dir: %s' % mount_dir)

    remotes = ctx.cluster.only(teuthology.is_type('mon'))
    mon = [remote for remote, roles_for_host in remotes.remotes.iteritems()]

    rgw_remote = ctx.cluster.only(teuthology.is_type('rgw'))
    rgw = [remote for remote, roles_for_host in rgw_remote.remotes.iteritems()]

    # clone the repo

    rgw[0].run(args=['sudo', 'rm', '-rf', 'nfs-ganesha-rgw'], check_status=False)
    rgw[0].run(args=['sudo', 'rm', '-rf', run.Raw('/tmp/nfs-ganesh-rgw_log*')], check_status=False)
    rgw[0].run(args=['mkdir', '-p', 'nfs-ganesha-rgw'])

    # stop native nfs-ganesha service.

    rgw[0].run(args=['sudo', 'systemctl', 'stop', 'nfs-server.service'])  # systemctl stop nfs-server.service
    rgw[0].run(args=['sudo', 'systemctl', 'disable', 'nfs-server.service'])  # systemctl disable nfs-server.service

    out = cStringIO.StringIO()
    mon[0].run(args=['sudo', 'cat', '/etc/ceph/ceph.client.admin.keyring'], stdout=out)
    v_as_out = out.read()
    teuthology.create_file(rgw[0], '/etc/ceph/ceph.client.admin.keyring', data=v_as_out, sudo=True)

    # parsing nfs-ganesha conf file

    out = cStringIO.StringIO()
    rgw[0].run(args=['sudo', 'cat', '/etc/ganesha/ganesha.conf'],
               stdout=out)
    v_as_out = out.readlines()

    clean = lambda x: re.sub('[^A-Za-z0-9]+', '', x)

    for content in v_as_out:

        if 'Access_Key_Id' in content:
            access_key = clean(content.split('=')[1])

        if 'Secret_Access_Key' in content:
            secret_key = clean(content.split('=')[1])

        if 'User_Id' in content:
            rgw_user_id = clean(content.split('=')[1])

        if 'Pseudo' in content:
            pseudo = content.split('=')[1].strip(' ').strip('\n').strip(' ').strip(';').strip('/')

    rgw[0].run(args=['sudo', 'setenforce', '1'])

    log.info('restarting nfs_ganesha service')

    rgw[0].run(args=['sudo', 'systemctl', 'restart', 'nfs-ganesha.service'])

    time.sleep(60)

    rgw[0].run(args=['cd', 'nfs-ganesha-rgw', run.Raw(';'), 'git', 'clone',
                     'https://github.com/red-hat-storage/ceph-qe-scripts.git'])

    rgw[0].run(args=['cd', 'nfs-ganesha-rgw/ceph-qe-scripts', run.Raw(';'), 'git', 'checkout', 'wip-nfs-ganesha-rgw-v2'])

    rgw[0].run(args=['virtualenv', 'venv'])

    rgw[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install --upgrade setuptools'),
            run.Raw(';'),
            'deactivate'])

    rgw[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install boto boto3 names PyYaml psutil ConfigParser'),
            run.Raw(';'),
            'deactivate'])

    # copy rgw user details (yaml format) to nfs node or rgw node

    rgw_user_config = dict(user_id=rgw_user_id,
                           access_key=access_key,
                           secret_key=secret_key,
                           rgw_hostname=rgw[0].shortname,
                           ganesha_config_exists=True,
                           already_mounted=False,
                           nfs_version=nfs_version,
                           nfs_mnt_point=mount_dir,
                           Pseudo=pseudo
                           )

    rgw_user_config_fname = 'rgw_user.yaml'

    temp_yaml_file = rgw_user_config_fname + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

    log.info('creating rgw_user_config_fname: %s' % rgw_user_config)
    local_file = '/tmp/' + temp_yaml_file
    with open(local_file, 'w') as outfile:
        outfile.write(yaml.dump(rgw_user_config, default_flow_style=False))

    log.info('copying rgw_user_config_fname to the client node')
    destination_location = 'nfs-ganesha-rgw/ceph-qe-scripts/rgw/v2/tests/nfs-ganesha/config/' + rgw_user_config_fname
    rgw[0].put_file(local_file, destination_location)

    rgw[0].run(args=[run.Raw('sudo rm -rf %s' % local_file)], check_status=False)

    # run the test

    rgw[0].run(
        args=[run.Raw(
            'sudo venv/bin/python2.7 nfs-ganesha-rgw/ceph-qe-scripts/rgw/v2/tests/nfs-ganesha/%s '
            '-r nfs-ganesha-rgw/ceph-qe-scripts/rgw/v2/tests/nfs-ganesha/config/rgw_user.yaml '
            '-c nfs-ganesha-rgw/ceph-qe-scripts/rgw/v2/tests/nfs-ganesha/config/%s ' % (script_name, test_name))])

    try:
        yield
    finally:
        log.info("Deleting the test soot")

        rgw[0].run(args=['sudo', 'umount', run.Raw('%s' % mount_dir)])

        cleanup = lambda x: rgw[0].run(args=[run.Raw('sudo rm -rf %s' % x)])

        soot = ['venv', 'rgw-tests', 'test_data' '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                '*.key.*']

        map(cleanup, soot)
