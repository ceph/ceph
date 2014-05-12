"""
Connection utilities
"""
import base64
import paramiko
import os
import logging

from ..config import config
from ..contextutil import safe_while

log = logging.getLogger(__name__)


def split_user(user_at_host):
    """
    break apart user@host fields into user and host.
    """
    try:
        user, host = user_at_host.rsplit('@', 1)
    except ValueError:
        user, host = None, user_at_host
    assert user != '', \
        "Bad input to split_user: {user_at_host!r}".format(user_at_host=user_at_host)
    return user, host


def create_key(keytype, key):
    """
    Create an ssh-rsa or ssh-dss key.
    """
    if keytype == 'ssh-rsa':
        return paramiko.rsakey.RSAKey(data=base64.decodestring(key))
    elif keytype == 'ssh-dss':
        return paramiko.dsskey.DSSKey(data=base64.decodestring(key))
    else:
        raise ValueError('keytype must be ssh-rsa or ssh-dsa')


def connect(user_at_host, host_key=None, keep_alive=False,
            _SSHClient=None, _create_key=None):
    """
    ssh connection routine.

    :param user_at_host: user@host
    :param host_key: ssh key
    :param keep_alive: keep_alive indicator
    :param _SSHClient: client, default is paramiko ssh client
    :param _create_key: routine to create a key (defaults to local reate_key)
    :return: ssh connection.
    """
    user, host = split_user(user_at_host)
    if _SSHClient is None:
        _SSHClient = paramiko.SSHClient
    ssh = _SSHClient()

    if _create_key is None:
        _create_key = create_key

    if host_key is None:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if config.verify_host_keys is True:
            ssh.load_system_host_keys()

    else:
        keytype, key = host_key.split(' ', 1)
        ssh.get_host_keys().add(
            hostname=host,
            keytype=keytype,
            key=_create_key(keytype, key)
            )

    connect_args = dict(
        hostname=host,
        username=user,
        timeout=60
    )

    ssh_config_path = os.path.expanduser("~/.ssh/config")
    if os.path.exists(ssh_config_path):
        ssh_config = paramiko.SSHConfig()
        ssh_config.parse(open(ssh_config_path))
        opts = ssh_config.lookup(host)
        opts_to_args = {
            'identityfile': 'key_filename',
            'host': 'hostname',
            'user': 'username'
        }
        for opt_name, arg_name in opts_to_args.items():
            if opt_name in opts:
                connect_args[arg_name] = opts[opt_name]

    log.info(connect_args)

    # just let the exceptions bubble up to caller
    with safe_while(sleep=1, action='connect to ' + host) as proceed:
        while proceed():
            try:
                ssh.connect(**connect_args)
                break
            except paramiko.AuthenticationException:
                log.exception("Error connecting to {host}".format(host=host))
    ssh.get_transport().set_keepalive(keep_alive)
    return ssh
