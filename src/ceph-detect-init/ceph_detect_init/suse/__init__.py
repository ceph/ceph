distro = None
release = None
codename = None


def choose_init():
    """
    Select a init system on SUSE

    :rtype: str
    :return: name of the init system
    """
    if release and int(release.split('.')[0]) >= 12:
        return 'systemd'
    return 'sysvinit'
