"""
ldap_server
"""
import logging
import time

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
    Given a taskname ('ldap_server' for example), return the associated
    teuthology location that that task is running on ('client.1' for example)
    """
    for tentry in ctx.config['tasks']:
        if taskname in tentry:
            return tentry[taskname][0]
    return ''

def fix_yum_boto(client):
    """
    Set up boto on Rhel/Fedora/Centos.
    """
    client.run(args=['sudo', 'yum-config-manager', '--enable', 'epel'])
    client.run(args=['sudo', 'yum', '-y', 'install', 'python-boto'])
    client.run(args=['sudo', 'yum-config-manager', '--disable', 'epel'])

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
    (client,) = ctx.cluster.only(config[0]).remotes
    system_type = misc.get_system_type(client)
    if system_type == 'rpm':
        install_cmd = ['sudo', 'yum', '-y', 'install', 'openldap-clients']
        client.run(args=install_cmd)
        fix_yum_boto(client)
    else:
        install_cmd = ['sudo', 'apt-get', '-y', 'install', 'ldap-utils']
        client.run(args=install_cmd)
        client.run(args=['sudo', 'apt-get', 'install', 'python-boto', '-y'])    

    ldap_server_task = get_task_site(ctx, 'ldap_server')
    server_site = get_node_name(ctx, ldap_server_task)
    ldap_client_task = get_task_site(ctx, 'ldap_client')
    client_site = get_node_name(ctx, ldap_client_task)

    client.run(args=['sudo', 'useradd', 'rgw'])
    client.run(args=['echo', 't0pSecret\nt0pSecret', run.Raw('|'), 'sudo',
                     'passwd', 'rgw'])
    client.run(args=['sudo', 'useradd', 'newuser'])
    client.run(args=['echo', 't0pSecret\nt0pSecret', run.Raw('|'), 'sudo',
                     'passwd', 'newuser'])


    client_actions = """
#!/bin/python
import json
import os
from subprocess import Popen
import sys
import ConfigParser
site = sys.argv[1]
c2= ConfigParser.SafeConfigParser()
c2.read('/etc/ceph/ceph.conf')
c2.set('global','rgw_ldap_secret','"/etc/bindpass"')
c2.set('global','rgw_ldap_uri','"ldap://%s:389"' % site)
c2.set('global','rgw_ldap_binddn','"uid=rgw,cn=users,cn=accounts,dc=ceph,dc=redhat,dc=com"')
c2.set('global','rgw_ldap_searchdn','"cn=users,cn=accounts,dc=ceph,dc=redhat,dc=com"')
c2.set('global','rgw_ldap_dnattr','"uid"')
c2.set('global','rgw_s3_auth_use_ldap','"true"')
c2.set('global','debug rgw','20')
with open('/tmp/ceph.conf', 'w') as x:
    c2.write(x)
with open('/tmp/nodes.json') as x:
    cnodes = json.loads(x.read())
sites = set()
for mtype in cnodes:
    for isite in cnodes[mtype]:
        noden = isite.split('.')[0]
        sites.add(noden)
slist = list(sites)
with open('/tmp/bindpass', 'w') as x:
    x.write('t0pSecret')
for nnode in slist:
    p = Popen('ssh %s sudo cp /tmp/bindpass /etc/bindpass' % nnode, shell=True) 
    os.waitpid(p.pid,0)
    p = Popen('ssh %s sudo chmod 0600 /etc/bindpass' % nnode, shell=True)
    os.waitpid(p.pid,0)
    p = Popen('ssh %s sudo chown rgw:rgw /etc/bindpass' % nnode, shell=True)
    os.waitpid(p.pid,0)
    p = Popen('ssh %s sudo cp /tmp/ceph.conf /etc/ceph/ceph.conf' % nnode, shell=True)
    os.waitpid(p.pid,0)
"""
    misc.sudo_write_file(
        remote=client,
        path='/tmp/setup_ldap_client.py',
        data=client_actions,
        perms='0744',
        )
    client.run(args=['python', '/tmp/setup_ldap_client.py', server_site])
    if system_type == 'rpm':
        client.run(args=['sudo', 'systemctl', 'restart', 'ceph-radosgw.service'])
    else:
        short_client = client_site.split('.')[0]
        client.run(args=['sudo', 'service', 'radosgw', 'restart', 'id=rgw.%s' % short_client])
