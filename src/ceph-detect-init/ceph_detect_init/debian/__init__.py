distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    assert(distro and codename)

    if distro.lower() == 'ubuntu':
        if codename.lower() >= 'vivid':
            return 'systemd'
        elif codename.lower() >= 'edgy':
            return 'upstart'
        else:
            return 'sysvinit'

    elif distro.lower() == 'linuxmint':
        if codename.lower() == 'debian':
            # Linux Mint Debian Edition
            return 'sysvinit'
        else:
            # Ubuntu-based editions
            if float(release) >= 2.0:
                return 'upstart'
            else:
                return 'sysvinit'

    elif distro.lower() == 'debian':
        if codename.lower() in ('jessie', 'stretch'):
            return 'systemd'
        else:
            return 'sysvinit'
