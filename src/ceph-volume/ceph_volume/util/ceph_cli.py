'''
Utility function to call the ceph binary
'''
from ceph_volume import conf


def get_cmd(cmd, user=None, keyring=None):
    base_cmd = [
        'ceph',
        '--cluster', conf.cluster,
    ]
    if user:
        base_cmd.extend(['--name', user,])
    if keyring:
        base_cmd.extend(['--keyring', keyring,])
    else:
        keyring = conf.ceph.get_safe('global', 'keyring')
        if keyring:
            base_cmd.extend(['--keyring', keyring,])

    mon_host =  conf.ceph.get_safe('global', 'mon_host')
    if mon_host:
        base_cmd.extend(['--mon-host', mon_host,])
    mon_dns_serv_name =  conf.ceph.get_safe('global', 'mon_dns_serv_name')
    if mon_dns_serv_name:
        base_cmd.extend(['--mon-dns-serv-name', mon_dns_serv_name,])

    return base_cmd + cmd
