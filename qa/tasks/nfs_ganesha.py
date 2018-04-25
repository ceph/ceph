import contextlib
import logging
import os
from teuthology import misc as teuthology
from teuthology.orchestra import run
import cStringIO
import json

log = logging.getLogger(__name__)

def NFS_Ganesh_RGW_config_gen(conf):

    ganesha_conf = '''
                EXPORT
                {
                        Export_ID=77;
                        Path = "/";
                        Pseudo = "/";
                        Access_Type = RW;
                        SecType = "sys";
                        NFS_Protocols = %s;
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
                    name = %s";
                    ceph_conf = "/etc/ceph/ceph.conf";
                    init_args = "-d --debug-rgw=16";
                }

        ''' % (conf['nfs_version'],
               conf['user_id'],
               conf['access_key'],
               conf['secret_key'],
               conf['hostname'])

    return ganesha_conf


@contextlib.contextmanager
def task(ctx, config):
    """
     Install and config NFS-Ganesha and mount

     tasks:
        - ssh-keys: null
        - ceph-ansible: null
        - nfs_ganesha:
            cluster_name: ceph
            mount_point: ~/ganesha_mnt  # full path
            nfs_version: 4 # 3 or 4
            type: rgw # rgw or fs

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    nfs_version = config['nfs_version']
    mount_point = config['mount_point']
    cluster_name = config['cluster_name']

    log.info('got nfs version: %s' % nfs_version)

    remotes = ctx.cluster.only(teuthology.is_type('mon'))
    mon = [
        remote for remote,
                   roles_for_host in remotes.remotes.iteritems()]

    rgw_remote = ctx.cluster.only(teuthology.is_type('rgw'))
    rgw = [
        remote for remote,
                   roles_for_host in rgw_remote.remotes.iteritems()]

    client_remote = ctx.cluster.only(teuthology.is_type('client'))
    client = [
        remote for remote,
                   roles_for_host in client_remote.remotes.iteritems()]

    # clone the repo

    client[0].run(args=['sudo', 'rm', '-rf', 'nfs-ganesha-rgw'], check_status=False)
    client[0].run(args=['sudo', 'rm', '-rf', run.Raw('/tmp/nfs-ganesh-rgw_log*')], check_status=False)
    client[0].run(args=['mkdir', '-p', 'nfs-ganesha-rgw'])

    # stop native nfs-ganesha service.

    client[0].run(args=['sudo', 'systemctl', 'stop', 'nfs-server.service'])  # systemctl stop nfs-server.service
    client[0].run(args=['sudo', 'systemctl', 'disable', 'nfs-server.service'])  # systemctl disable nfs-server.service

    # copy admin.keyring from mon to rgw, client node because of bz

    out = cStringIO.StringIO()
    mon[0].run(args=['sudo', 'cat', '/etc/ceph/%s.client.admin.keyring' % cluster_name], stdout=out)
    v_as_out = out.read()
    teuthology.create_file(rgw[0], '/etc/ceph/%s.client.admin.keyring' % cluster_name, data=v_as_out, sudo=True)
    teuthology.create_file(client[0], '/etc/ceph/%s.client.admin.keyring' % cluster_name, data=v_as_out, sudo=True)

    if config['type'] == 'rgw':

        out = cStringIO.StringIO()
        rgw[0].run(args=['radosgw-admin', 'user', 'create', '--display-name="johnny rotten"', '--uid=johnny'],
                   stdout=out)
        v_as_out = out.read()

        v_as_json = json.loads(v_as_out)

        config['user_id'] = v_as_json['user_id']
        config['display_name'] = v_as_json['display_name']

        config['access_key'] = v_as_json['keys'][0]['access_key']

        config['secret_key'] = v_as_json['keys'][0]['secret_key']

        client['hostname'] = "client.rgw.%s" % rgw[0].shortname

    config['nfs_version'] = nfs_version

    # generate nfs_ganesha config

    ganesha_config = NFS_Ganesh_RGW_config_gen(config)

    log.info('ganesha_config: %s' % ganesha_config)

    # install nfs_ganesha_rgw

    client[0].run(args=['sudo', 'yum', 'install', 'nfs-ganesha-rgw', '-y'])

    # backup default ganesha config file and write the generated config file

    client[0].run(args=['sudo', 'mv', '/etc/ganesha/ganesha.conf', '/etc/ganesha/ganesha.conf.bkp'])

    ganesha_config_path_on_client = '/etc/ganesha/ganesha.conf'

    teuthology.sudo_write_file(rgw[0], ganesha_config_path_on_client, ganesha_config)

    # start the nfs_ganesha service

    client[0].run(args=['sudo', '/usr/bin/ganesha.nfsd', '-f', '/etc/ganesha/ganesha.conf'])

    client[0].run(args=['mkdir', '-p', run.Raw(mount_point)])

    # mount NFS_Ganesha

    client[0].run(args=[run.Raw('sudo mount -v -t nfs -o nfsvers=%s,sync,rw,noauto,soft,proto=tcp %s:/  %s' %
                             (nfs_version,
                              rgw[0].shortname,
                              mount_point))])

    try:
        yield
    finally:
        log.info("un-mount ganesha mount_point")

        client[0].run(args=['sudo', 'umount', run.Raw(mount_point)])

        log.info('removing nfs-ganesha-rgw')

        client[0].run(args=['sudo', 'yum', 'remove', 'nfs-ganesha-rgw', '-y'])
