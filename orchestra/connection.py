import paramiko

def split_user(user_at_host):
    try:
        user, host = user_at_host.rsplit('@', 1)
    except ValueError:
        user, host = None, user_at_host
    assert user != '', \
        "Bad input to split_user: {user_at_host!r}".format(user_at_host=user_at_host)
    return user, host


def connect(user_at_host, _SSHClient=None):
    user, host = split_user(user_at_host)
    if _SSHClient is None:
        _SSHClient = paramiko.SSHClient
    ssh = _SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(
        hostname=host,
        username=user,
        timeout=60,
        )
    return ssh
