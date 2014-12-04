import logging
from teuthology import misc

log = logging.getLogger(__name__)

'''
Map 'generic' package name to 'flavor-specific' package name.
If entry is None, either the package isn't known here, or
it's known but should not be installed on remotes of this flavor
'''

_PACKAGE_MAP = {
    'sqlite': {'deb': 'sqlite3', 'rpm': None}
}

'''
Map 'generic' service name to 'flavor-specific' service name.
'''
_SERVICE_MAP = {
    'httpd': {'deb': 'apache2', 'rpm': 'httpd'}
}


def get_package_name(pkg, rem):
    """
    Find the remote-specific name of the generic 'pkg'
    """
    flavor = misc.get_system_type(rem)

    try:
        return _PACKAGE_MAP[pkg][flavor]
    except KeyError:
        return None


def get_service_name(service, rem):
    """
    Find the remote-specific name of the generic 'service'
    """
    flavor = misc.get_system_type(rem)
    try:
        return _SERVICE_MAP[service][flavor]
    except KeyError:
        return None


def install_package(package, remote):
    """
    Install 'package' on 'remote'
    Assumes repo has already been set up (perhaps with install_repo)
    """
    log.info('Installing package %s on %s', package, remote)
    flavor = misc.get_system_type(remote)
    if flavor == 'deb':
        pkgcmd = ['DEBIAN_FRONTEND=noninteractive',
                  'sudo',
                  '-E',
                  'apt-get',
                  '-y',
                  'install',
                  '{package}'.format(package=package)]
    elif flavor == 'rpm':
        pkgcmd = ['sudo',
                  'yum',
                  '-y',
                  'install',
                  '{package}'.format(package=package)]
    else:
        log.error('install_package: bad flavor ' + flavor + '\n')
        return False
    return remote.run(args=pkgcmd)


def remove_package(package, remote):
    """
    Remove package from remote
    """
    flavor = misc.get_system_type(remote)
    if flavor == 'deb':
        pkgcmd = ['DEBIAN_FRONTEND=noninteractive',
                  'sudo',
                  '-E',
                  'apt-get',
                  '-y',
                  'purge',
                  '{package}'.format(package=package)]
    elif flavor == 'rpm':
        pkgcmd = ['sudo',
                  'yum',
                  '-y',
                  'erase',
                  '{package}'.format(package=package)]
    else:
        log.error('remove_package: bad flavor ' + flavor + '\n')
        return False
    return remote.run(args=pkgcmd)
