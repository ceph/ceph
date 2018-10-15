"""
ldap_client
"""
import logging
import contextlib
import time
import os

from teuthology.orchestra import run
from teuthology import misc

log = logging.getLogger(__name__)

def get_node_name(ctx, this_task):
    """
    Given a teuthology task site ('client.1' for example), return
    the network hostname for that site.
    """
    for task_list in ctx.cluster.remotes:
        if this_task in ctx.cluster.remotes[task_list]:
            return task_list.name.split('@')[1]
    return ''

def get_task_site(ctx, taskname):
    """
    Given a taskname ('ldap_client' for example), return the associated
    teuthology location that that task is running on ('client.1' for example)
    """
    for tentry in ctx.config['tasks']:
        if taskname in tentry:
            return tentry[taskname][0]
    return ''

def get_dc_path(path):
    dcvals = path.split('.')[1:]
    out_str = []
    for path_part in dcvals:
        out_str.append('dc=%s' % path_part)
    return (','.join(out_str))

@contextlib.contextmanager
def task(ctx, config):
    """
    Install ldap_client in order to test ldap rgw authentication

    Usage
       tasks:
       - ldap_client:
           [client.0]

    """
    log.info('in ldap_client')
    assert isinstance(config, list)
    ldap_admin_key = os.environ.get('LDAP_ADMIN_KEY_VALUE', 't0pSecret')
    (client,) = ctx.cluster.only(config[0]).remotes
    system_type = misc.get_system_type(client)
    if system_type == 'rpm':
        install_cmd = ['sudo', 'yum', '-y', 'install', 'openldap-clients']
        client.run(args=install_cmd)
        client.run(args=['sudo', 'yum-config-manager', '--enable', 'epel'])
        client.run(args=['sudo', 'yum', '-y', 'install', 'python-boto'])
        client.run(args=['sudo', 'yum-config-manager', '--disable', 'epel'])
    else:
        install_cmd = ['sudo', 'apt-get', '-y', 'install', 'ldap-utils']
        client.run(args=install_cmd)
        client.run(args=['sudo', 'apt-get', 'install', 'python-boto', '-y'])
    client.run(args=['sudo', 'pip', 'install', '-U', 'boto'])

    ldap_server_task = get_task_site(ctx, 'ldap_server')
    server_site = get_node_name(ctx, ldap_server_task)
    ldap_client_task = get_task_site(ctx, 'ldap_client')
    client_site = get_node_name(ctx, ldap_client_task)

    dc_splits = get_dc_path(ctx.cluster.remotes.keys()[0].name)
    new_globals = ctx.ceph['ceph'].conf['global']
    new_globals.update({'rgw_ldap_secret': '/etc/bindpass'})
    new_globals.update({'rgw_ldap_uri': 'ldap://%s:389' % server_site})
    new_globals.update({'rgw_ldap_binddn': 'uid=rgw,cn=users,cn=accounts,%s' % dc_splits})
    new_globals.update({'rgw_ldap_searchdn': 'cn=users,cn=accounts,%s' % dc_splits})
    new_globals.update({'rgw_ldap_dnattr': 'uid'})
    new_globals.update({'rgw_s3_auth_use_ldap': 'true'})
    new_globals.update({'debug rgw': '20'})
    with open('/tmp/ceph.conf', 'w+') as cephconf:
        ctx.ceph['ceph'].conf.write(outfile=cephconf)
    with open('/tmp/ceph.conf', 'r') as newcephconf:
        confstr = newcephconf.read()
    tbindpass = ldap_admin_key
    for remote in ctx.cluster.remotes:
        misc.sudo_write_file(remote, '/etc/ceph/ceph.conf', confstr, perms='0644', owner='root:root')
        misc.sudo_write_file(remote, '/etc/bindpass', tbindpass, perms='0600', owner='ceph:ceph')
    iyam = client.name.split('@')[-1].split('.')[0]
    if misc.get_system_type(client) == 'rpm':
        client.run(args=['sudo', 'systemctl', 'restart', 'ceph-radosgw@rgw.%s' % iyam])
    else:
        client.run(args=['sudo', 'service', 'radosgw', 'restart', 'id=rgw.%s' % iyam])
    try:
        yield
    finally:
        pass
