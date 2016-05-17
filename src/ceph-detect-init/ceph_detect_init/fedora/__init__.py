distro = None
release = None
codename = None


def choose_init():
    """
    Select a init system on Fedora

    :rtype: str
    :return: name of the init system
    """
    if release:
        version = int(release.split('.')[0])

        if version >= 15:
            return 'systemd'
        elif version >= 9:
            return 'upstart'
        else:
            return 'sysvinit'

    return 'sysvinit'
