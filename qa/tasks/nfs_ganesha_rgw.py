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

log = logging.getLogger(__name__)


def NFS_Ganesh_RGW_config_gen(sys_conf):
    ganesha_conf = '''
                EXPORT
                {
                        Export_ID=77;
                        Path = "/";
                        Pseudo = "/";
                        Access_Type = RW;
                        SecType = "sys";
                        NFS_Protocols = 4;
                        Transport_Protocols = TCP;

                        FSAL {
                                Name = RGW;
                                User_Id = %s;
                                Access_Key_Id ="%s";
                                Secret_Access_Key = "%s";
                        }
                }

                NFSV4 {
                    Allow_Numeric_Owners = true;
                    Only_Numeric_Owners = true;
                }

                Cache_Inode {
                    Dir_Max = 10000;
                }

                RGW {
                    name = "client.rgw.%s";
                    ceph_conf = "/etc/ceph/ceph.conf";
                    init_args = "-d --debug-rgw=16";
                }

        ''' % (sys_conf['user_id'],
               sys_conf['access_key'],
               sys_conf['secret_key'],
               sys_conf['rgw_hostname'])

    return ganesha_conf



@contextlib.contextmanager
def task(ctx, config):
    """
     Install and config NFS-Ganesha and mount

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    test_name = config['test-name'] + ".py"

    log.info('got test_name: %s' % test_name)

    remotes = ctx.cluster.only(teuthology.is_type('mon'))
    mon = [
        remote for remote,
                   roles_for_host in remotes.remotes.iteritems()]

    rgw_remote = ctx.cluster.only(teuthology.is_type('rgw'))
    rgw = [
        remote for remote,
                   roles_for_host in rgw_remote.remotes.iteritems()]

    # clone the repo

    rgw[0].run(args=['sudo', 'rm', '-rf', 'nfs-ganesha-rgw'], check_status=False)
    rgw[0].run(args=['sudo', 'rm', '-rf', run.Raw('/tmp/nfs-ganesh-rgw_log*')], check_status=False)
    rgw[0].run(args=['mkdir', '-p', 'nfs-ganesha-rgw'])

    # stop native nfs-ganesha service.

    rgw[0].run(args=['sudo', 'systemctl', 'stop', 'nfs-server.service'])  # systemctl stop nfs-server.service
    rgw[0].run(args=['sudo', 'systemctl', 'disable', 'nfs-server.service'])  # systemctl disable nfs-server.service

    sys_conf = {}

    sys_conf['rgw_hostname'] = rgw[0].shortname

    # copy ceph.client.admin.keyring from mon to rgw node becasue of bz

    out = cStringIO.StringIO()
    mon[0].run(args=['sudo', 'cat', '/etc/ceph/ceph.client.admin.keyring'], stdout=out)
    v_as_out = out.read()
    teuthology.create_file(rgw[0], '/etc/ceph/ceph.client.admin.keyring', data=v_as_out, sudo=True)

    out = cStringIO.StringIO()
    rgw[0].run(args=['radosgw-admin', 'user', 'create', '--display-name="johnny rotten"', '--uid=johnny'],
               stdout=out)
    v_as_out = out.read()

    v_as_json = json.loads(v_as_out)

    sys_conf['user_id'] = v_as_json['user_id']
    sys_conf['display_name'] = v_as_json['display_name']

    sys_conf['access_key'] = v_as_json['keys'][0]['access_key']

    sys_conf['secret_key'] = v_as_json['keys'][0]['secret_key']

    ganesha_config = NFS_Ganesh_RGW_config_gen(sys_conf)

    log.info('ganesha_config: %s' % ganesha_config)

    # install nfs_ganesha_rgw

    rgw[0].run(args=['sudo', 'yum', 'install', 'nfs-ganesha-rgw', '-y'])

    # backup default ganesha config file

    rgw[0].run(args=['sudo', 'mv', '/etc/ganesha/ganesha.conf', '/etc/ganesha/ganesha.conf.bkp'])

    ganesha_config_path_on_client = '/etc/ganesha/ganesha.conf'

    teuthology.sudo_write_file(rgw[0], ganesha_config_path_on_client, ganesha_config)

    rgw[0].run(args=['cd', 'nfs-ganesha-rgw', run.Raw(';'), 'git', 'clone',
                     'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git'])

    # rgw[0].run(args=['cd', 'nfs-ganesha-rgw/ceph-qe-scripts', run.Raw(';'), 'git', 'checkout', 'sleep_time'])

    rgw[0].run(args=['virtualenv', 'venv'])
    rgw[0].run(
        args=[
            'source',
            'venv/bin/activate',
            run.Raw(';'),
            run.Raw('pip install boto names PyYaml psutil ConfigParser'),
            run.Raw(';'),
            'deactivate'])

    # kill nfs_ganesha process

    """
    
    rgw[0].run(
        args=[
            run.Raw(
                'sudo venv/bin/python2.7 nfs-ganesha-rgw/ceph-qe-scripts/rgw/lib/process_manage.py')])
    
    """

    # rgw[0].run(args=[run.Raw('sudo python nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/process_manage.py')])

    # start the nfs_ganesha service

    rgw[0].run(args=['sudo', '/usr/bin/ganesha.nfsd', '-f', '/etc/ganesha/ganesha.conf'])

    rgw[0].run(args=['mkdir', '-p', run.Raw('~/ganesha-mount')])

    # mount NFS_Ganesha

    rgw[0].run(args=[run.Raw('sudo mount -v -t nfs -o nfsvers=4.1,sync,rw,noauto,soft,proto=tcp %s:/  %s' % (
        rgw[0].shortname, '~/ganesha-mount'))])

    # copy rgw user details (yaml format) to nfs node or ganesha node

    rgw_user_config = dict(user_id=sys_conf['user_id'],
                           access_key=sys_conf['access_key'],
                           secret_key=sys_conf['secret_key'],
                           rgw_hostname=rgw[0].shortname,
                           ganesha_config_exists=True,
                           already_mounted=True
                           )

    yaml_fname = 'rgw_user.yaml'

    temp_yaml_file = yaml_fname + "_" + str(os.getpid()) + pwd.getpwuid(os.getuid()).pw_name

    log.info('creating yaml from the config: %s' % rgw_user_config)
    local_file = '/tmp/' + temp_yaml_file
    with open(local_file, 'w') as outfile:
        outfile.write(yaml.dump(rgw_user_config, default_flow_style=False))

    log.info('copying yaml to the client node')
    destination_location = 'nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/yaml/' + yaml_fname
    rgw[0].put_file(local_file, destination_location)

    rgw[0].run(args=[run.Raw('sudo rm -rf %s' % local_file)], check_status=False)

    # run the test

    rgw[0].run(
        args=[run.Raw(
            'sudo venv/bin/python2.7 nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/%s  -c '
            'nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/yaml/rgw_user.yaml' % test_name)])

    try:
        yield
    finally:
        log.info("Deleting the test soot")

        rgw[0].run(args=['sudo', 'umount', run.Raw('~/ganesha-mount')])

        cleanup = lambda x: rgw[0].run(args=[run.Raw('sudo rm -rf %s' % x)])

        soot = ['venv', 'rgw-tests', '*.json', 'Download.*', 'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*',
                '*.key.*']

        map(cleanup, soot)

