import os
import subprocess

distro = None
release = None
codename = None


def choose_init():
    """Select a init system

    Returns the name of a init system (upstart, sysvinit ...).
    """
    # yes, this is heuristics
    if os.path.isdir('/run/systemd/system'):
        return 'systemd'
    if not subprocess.call('. /lib/lsb/init-functions ; init_is_upstart',
                           shell=True):
        return 'upstart'
    if os.path.isfile('/sbin/init') and not os.path.islink('/sbin/init'):
        return 'sysvinit'
