distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    if distro.lower() in ('ubuntu', 'linuxmint'):
        return 'upstart'
    return 'sysvinit'
