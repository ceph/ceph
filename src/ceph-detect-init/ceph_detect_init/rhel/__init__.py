distro = None
release = None
codename = None


def choose_init():
    """
    Select a init system on RHEL

    :rtype: str
    :return: name of the init system
    """
    if release:
        version = int(release.split('.')[0])

        if version >= 7:
            return 'systemd'
        elif version == 6:
            return 'upstart'
        else:
            return 'sysvinit'

    return 'sysvinit'
