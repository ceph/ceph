distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
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
