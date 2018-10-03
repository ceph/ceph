"""
ldap_server
"""
import logging
import contextlib
import time
import os

from teuthology.orchestra import run
from teuthology import misc

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Start up an ldap_server in order to test ldap rgw authentication

    Usage
       tasks:
       - ldap_server:
           [client.0]

    Note: the ldap server runs on a teuthology client, so the client
          references in this file are ldap server references.
    """

    log.info('in ldap_server')
    assert isinstance(config, list)
    ldap_admin_key = os.environ.get('LDAP_ADMIN_KEY_VALUE', 't0pSecret')
    ldap_user_key = os.environ.get('LDAP_USER_KEY_VALUE', 't0pSecret')
    try:
        (client,) = ctx.cluster.only(config[0]).remotes
        system_type = misc.get_system_type(client)
        if system_type == 'rpm':
            install_cmd = ['sudo', 'yum', '-y', 'install', 'ipa-server', 'ipa-server-dns']
        else:
            install_cmd = ['sudo', 'apt-get', '-y', 'install', 'freeipa-server', 'freeipa-server-dns']
        client.run(args=install_cmd)
        path_parts = ctx.cluster.remotes.keys()[0].name.split('.')[1:]
        client.run(args=['sudo',
                         'ipa-server-install',
                         '--realm',
                         '.'.join(path_parts),
                         '--ds-password',
                         ldap_admin_key,
                         '--admin-password',
                         ldap_admin_key,
                         '--unattended'])
        time.sleep(120)
        client.run(args=['echo',
                         ldap_admin_key,
                         run.Raw('|'),
                         'kinit',
                         'admin'])
        client.run(args=['ipa', 'user-add', 'rgw',
                         '--first', 'rados', '--last', 'gateway'])
        client.run(args=['ipa', 'user-add', 'testuser',
                         '--first', 'new', '--last', 'user'])
        client.run(args=['echo',
                         '%s\n%s' % (ldap_admin_key,ldap_admin_key),
                         run.Raw('|'),
                         'ipa',
                         'user-mod',
                         'rgw',
                         '--password'])
        client.run(args=['echo',
                         '%s\n%s' % (ldap_user_key,ldap_user_key),
                         run.Raw('|'),
                         'ipa',
                         'user-mod',
                         'testuser',
                         '--password'])
        yield
    finally:
        (client,) = ctx.cluster.only(config[0]).remotes
        client.run(args=[ 'yes',
                          run.Raw('|'),
                          'sudo',
                          'ipa-server-install',
                          '--uninstall'])
        log.info("Finished ldap_server task")
