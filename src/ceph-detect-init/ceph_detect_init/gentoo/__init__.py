import os
distro = None
release = None
codename = None


# From ceph-disk, but there is no way to access it since it's not in a module
def is_systemd():
    """
    Detect whether systemd is running;
    WARNING: not mutually exclusive with openrc
    """
    with open('/proc/1/comm', 'rb') as i:
        for line in i:
            if 'systemd' in line:
                return True
    return False


def is_openrc():
    """
    Detect whether openrc is running.
    """
    OPENRC_CGROUP = '/sys/fs/cgroup/openrc'
    return os.path.isdir(OPENRC_CGROUP)


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    if is_openrc():
        return 'openrc'
    if is_systemd():
        return 'systemd'
    return 'unknown'
