distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    if release and int(release.split('.')[0]) >= 12:
        return 'systemd'
    return 'sysvinit'
