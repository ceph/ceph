distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    init_mapping = {
        '11': 'sysvinit',   # SLE_11
        '12': 'systemd',    # SLE_12
        '13.1': 'systemd',  # openSUSE_13.1
        '13.2': 'systemd',  # openSUSE_13.2
    }
    return init_mapping.get(release, 'sysvinit')
