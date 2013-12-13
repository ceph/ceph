import base64
import paramiko
import os
from ..config import config


def split_user(user_at_host):
    try:
        user, host = user_at_host.rsplit('@', 1)
    except ValueError:
        user, host = None, user_at_host
    assert user != '', \
        "Bad input to split_user: {user_at_host!r}".format(user_at_host=user_at_host)
    return user, host


def create_key(keytype, key):
    if keytype == 'ssh-rsa':
        return paramiko.rsakey.RSAKey(data=base64.decodestring(key))
    elif keytype == 'ssh-dss':
        return paramiko.dsskey.DSSKey(data=base64.decodestring(key))
    else:
        raise ValueError('keytype must be ssh-rsa or ssh-dsa')


def connect(user_at_host, host_key=None, keep_alive=False,
            _SSHClient=None, _create_key=None):
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

    # just let the exceptions bubble up to caller
    ssh.connect(**connect_args)
    ssh.get_transport().set_keepalive(keep_alive)
    return ssh
