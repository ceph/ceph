from cStringIO import StringIO
from .orchestra import run
import logging
import teuthology.misc as teuthology
import textwrap

log = logging.getLogger(__name__)

'''
Infer things about platform type with this map.
The key comes from processing lsb_release -ics or -irs (see _get_relmap).
'''
_RELEASE_MAP = {
    'Ubuntu precise': dict(flavor='deb', release='ubuntu', version='precise'),
    'Debian wheezy': dict(flavor='deb', release='debian', version='wheezy'),
    'CentOS 6.4': dict(flavor='rpm', release='centos', version='6.4'),
    'RedHatEnterpriseServer 6.4': dict(flavor='rpm', release='rhel',
                                       version='6.4'),
}

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


def _get_relmap(rem):
    """
    Internal worker to get the appropriate dict from RELEASE_MAP
    """
    relmap = getattr(rem, 'relmap', None)
    if relmap is not None:
        return relmap
    lsb_release_out = StringIO()
    rem.run(args=['lsb_release', '-ics'], stdout=lsb_release_out)
    release = lsb_release_out.getvalue().replace('\n', ' ').rstrip()
    if release in _RELEASE_MAP:
        rem.relmap = _RELEASE_MAP[release]
        return rem.relmap
    else:
        lsb_release_out = StringIO()
        rem.run(args=['lsb_release', '-irs'], stdout=lsb_release_out)
        release = lsb_release_out.getvalue().replace('\n', ' ').rstrip()
        if release in _RELEASE_MAP:
            rem.relmap = _RELEASE_MAP[release]
            return rem.relmap
    raise RuntimeError('Can\'t get release info for {}'.format(rem))


def get_package_name(pkg, rem):
    """
    Find the remote-specific name of the generic 'pkg'
    """
    flavor = _get_relmap(rem)['flavor']

    try:
        return _PACKAGE_MAP[pkg][flavor]
    except KeyError:
        return None


def get_service_name(service, rem):
    """
    Find the remote-specific name of the generic 'service'
    """
    flavor = _get_relmap(rem)['flavor']
    try:
        return _SERVICE_MAP[service][flavor]
    except KeyError:
        return None


def install_repo(remote, reposerver, pkgdir, username=None, password=None):
    """
    Install a package repo for reposerver on remote.
    URL will be http if username and password are none, otherwise https.
    pkgdir is the piece path between "reposerver" and "deb" or "rpm"
     (say, 'packages', or 'packages-staging/my-branch', for example).
    so:
        http[s]://[<username>:<password>@]<reposerver>/<pkgdir>/{deb|rpm}
    will be written to deb's apt inktank.list or rpm's inktank.repo
    """

    relmap = _get_relmap(remote)
    log.info('Installing repo on %s', remote)
    if username is None or password is None:
        repo_uri = 'http://{reposerver}/{pkgdir}'
    else:
        repo_uri = 'https://{username}:{password}@{reposerver}/{pkgdir}'

    if relmap['flavor'] == 'deb':
        contents = 'deb ' + repo_uri + '/deb {codename} main'
        contents = contents.format(username=username, password=password,
                                   reposerver=reposerver, pkgdir=pkgdir,
                                   codename=relmap['version'],)
        teuthology.sudo_write_file(remote,
                                   '/etc/apt/sources.list.d/inktank.list',
                                   contents)
        remote.run(args=['sudo',
                         'apt-get',
                         'install',
                         'apt-transport-https',
                         '-y'])
        result = remote.run(args=['sudo', 'apt-get', 'update', '-y'],
                            stdout=StringIO())
        return result

    elif relmap['flavor'] == 'rpm':
        baseurl = repo_uri + '/rpm/{release}{version}'
        contents = textwrap.dedent('''
            [inktank]
            name=Inktank Storage, Inc.
            baseurl={baseurl}
            gpgcheck=1
            enabled=1
            '''.format(baseurl=baseurl))
        contents = contents.format(username=username,
                                   password=password,
                                   pkgdir=pkgdir,
                                   release=relmap['release'],
                                   version=relmap['version'])
        teuthology.sudo_write_file(remote,
                                   '/etc/yum.repos.d/inktank.repo',
                                   contents)
        return remote.run(args=['sudo', 'yum', 'makecache'])

    else:
        return False


def remove_repo(remote):
    log.info('Removing repo on %s', remote)
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        teuthology.delete_file(remote, '/etc/apt/sources.list.d/inktank.list',
                               sudo=True, force=True)
        result = remote.run(args=['sudo', 'apt-get', 'update', '-y'],
                            stdout=StringIO())
        return result

    elif flavor == 'rpm':
        teuthology.delete_file(remote, '/etc/yum.repos.d/inktank.repo',
                               sudo=True, force=True)
        return remote.run(args=['sudo', 'yum', 'makecache'])

    else:
        return False


def install_repokey(remote, keyurl):
    """
    Install a repo key from keyurl on remote.
    Installing keys is assumed to be idempotent.
    Example keyurl: 'http://download.inktank.com/keys/release.asc'
    """
    log.info('Installing repo key on %s', remote)
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        return remote.run(args=['wget',
                                '-q',
                                '-O-',
                                keyurl,
                                run.Raw('|'),
                                'sudo',
                                'apt-key',
                                'add',
                                '-'])
    elif flavor == 'rpm':
        return remote.run(args=['sudo', 'rpm', '--import', keyurl])
    else:
        return False


def install_package(package, remote):
    """
    Install 'package' on 'remote'
    Assumes repo has already been set up (perhaps with install_repo)
    """
    log.info('Installing package %s on %s', package, remote)
    flavor = _get_relmap(remote)['flavor']
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
    flavor = _get_relmap(remote)['flavor']
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
