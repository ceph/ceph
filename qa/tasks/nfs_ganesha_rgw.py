import contextlib
import logging
import os
from teuthology import misc as teuthology
from teuthology.orchestra import run

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

    remotes = ctx.cluster.only(teuthology.is_type('client'))
    clients = [
        remote for remote,
                   roles_for_host in remotes.remotes.iteritems()]
    # clone the repo

    clients[0].run(args=['sudo', 'rm', '-rf', 'nfs-ganesha-rgw'], check_status=False)
    clients[0].run(args=['sudo', 'rm', '-rf', run.Raw('/tmp/nfs-ganesh-rgw_log*')], check_status=False)
    clients[0].run(args=['mkdir', '-p', 'nfs-ganesha-rgw'])

    # stop native nfs-ganesha service.

    clients[0].run(args=['sudo', 'systemctl', 'stop', 'nfs-server.service']) # systemctl stop nfs-server.service
    clients[0].run(args=['sudo', 'systemctl', 'disable', 'nfs-server.service']) # systemctl disable nfs-server.service

    # install nfs-ganesha-rgw

    # clients[0].run(args=['sudo', 'yum', 'install', 'nfs-ganesha-rgw', '-y'])

    sys_conf = {}

    sys_conf['rgw_hostname'] = clients[0].shortname


    out = cStringIO.StringIO()
    clients[0].run(args=['radosgw-admin', 'user', 'create', '--display-name="johnny rotten"', '--uid=johnny'],
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

    clients[0].run(args=['sudo', 'yum', 'install', 'nfs-ganesha-rgw', '-y'])

    # backup default ganesha config file

    clients[0].run(args=['sudo', 'mv', '/etc/ganesha/ganesha.conf', '/etc/ganesha/ganesha.conf.bkp'])

    ganesha_config_path_on_client = '/etc/ganesha/ganesha.conf'

    teuthology.sudo_write_file(clients[0],ganesha_config_path_on_client, ganesha_config)

    clients[0].run(args=['cd', 'nfs-ganesha-rgw', run.Raw(';'), 'git', 'clone',
                         'http://gitlab.osas.lab.eng.rdu2.redhat.com/ceph/ceph-qe-scripts.git', '-b', 'wip-nfs-ganesha'])

    clients[0].run(args=['sudo', 'pip', 'install', 'psutil'])

    # kill nfs_ganesha process

    clients[0].run(args=[run.Raw('sudo python nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/process_manage.py')])

    # start the nfs_ganesha service

    clients[0].run(args=['sudo', '/usr/bin/ganesha.nfsd', '-f','/etc/ganesha/ganesha.conf'])

    clients[0].run(args=['mkdir', '-p', run.Raw('~/ganesha-mount')])

    # mount NFS_Ganesha

    clients[0].run(args=[run.Raw('sudo mount -v -t nfs -o nfsvers=4.1,sync,rw,noauto,soft,proto=tcp %s:/  %s' % (
    clients[0].shortname, '~/ganesha-mount'))])

    # run the basic IO

    clients[0].run(
        args=[run.Raw(
            'sudo python ~/nfs-ganesha-rgw/ceph-qe-scripts/rgw/tests/nfs-ganesha/basic_io.py -c 30-4-14 -p ~/ganesha-mount -r 100')])

    try:
        yield
    finally:
        log.info("Deleting repo's")
        if ctx.archive is not None:
            path = os.path.join(ctx.archive, 'remote')
            os.makedirs(path)
            sub = os.path.join(path, clients[0].shortname)
            os.makedirs(sub)
            # teuthology.pull_directory(clients[0], '/tmp/apilog',os.path.join(sub, 'log'))
            clients[0].run(args=['sudo', 'rm', '-rf', run.Raw('~/nfs-ganesha-rgw')])
            clients[0].run(args=['sudo', 'umount', run.Raw('~/ganesha-mount')])
