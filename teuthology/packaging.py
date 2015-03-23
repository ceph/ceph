import logging
import ast
import re

from cStringIO import StringIO

from teuthology import misc
from .config import config

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


def get_koji_build_info(build_id, remote, ctx):
    """
    Queries kojihub and retrieves information about
    the given build_id. The package, koji, must be installed
    on the remote for this command to work.

    We need a remote here because koji can only be installed
    on rpm based machines and teuthology runs on Ubuntu.

    Here is an example of the build info returned:

    {'owner_name': 'kdreyer', 'package_name': 'ceph',
     'task_id': 8534149, 'completion_ts': 1421278726.1171,
     'creation_event_id': 10486804, 'creation_time': '2015-01-14 18:15:17.003134',
     'epoch': None, 'nvr': 'ceph-0.80.5-4.el7ost', 'name': 'ceph',
     'completion_time': '2015-01-14 18:38:46.1171', 'state': 1, 'version': '0.80.5',
     'volume_name': 'DEFAULT', 'release': '4.el7ost', 'creation_ts': 1421277317.00313,
     'package_id': 34590, 'id': 412677, 'volume_id': 0, 'owner_id': 2826
    }

    :param build_id:  The brew build_id we want to retrieve info on.
    :param remote:    The remote to run the koji command on.
    :param ctx:       The ctx from the current run, used to provide a
                      failure_reason and status if the koji command fails.
    :returns:         A python dict containing info about the build.
    """
    py_cmd = ('import koji; '
              'hub = koji.ClientSession("{kojihub_url}"); '
              'print hub.getBuild({build_id})')
    py_cmd = py_cmd.format(
        build_id=build_id,
        kojihub_url=config.kojihub_url
    )
    proc = remote.run(
        args=[
            'python', '-c', py_cmd
        ],
        stdout=StringIO(), stderr=StringIO(), check_status=False
    )
    if proc.exitstatus == 0:
        # returns the __repr__ of a python dict
        stdout = proc.stdout.getvalue().strip()
        # take the __repr__ and makes it a python dict again
        build_info = ast.literal_eval(stdout)
    else:
        msg = "Failed to query koji for build {0}".format(build_id)
        log.error(msg)
        log.error("stdout: {0}".format(proc.stdout.getvalue().strip()))
        log.error("stderr: {0}".format(proc.stderr.getvalue().strip()))
        ctx.summary["failure_reason"] = msg
        ctx.summary["status"] = "dead"
        raise RuntimeError(msg)

    return build_info


def get_kojiroot_base_url(build_info, arch="x86_64"):
    """
    Builds the base download url for kojiroot given the current
    build information.

    :param build_info:  A dict of koji build information, possibly
                        retrieved from get_koji_build_info.
    :param arch:        The arch you want to download rpms for.
    :returns:           The base_url to use when downloading rpms
                        from brew.
    """
    base_url = "{kojiroot}/{package_name}/{ver}/{rel}/{arch}/".format(
        kojiroot=config.kojiroot_url,
        package_name=build_info["package_name"],
        ver=build_info["version"],
        rel=build_info["release"],
        arch=arch,
    )
    return base_url


def get_koji_package_name(package, build_info, arch="x86_64"):
    """
    Builds the package name for a brew rpm.

    :param package:     The name of the package
    :param build_info:  A dict of koji build information, possibly
                        retrieved from get_brew_build_info.
    :param arch:        The arch you want to download rpms for.
    :returns:           A string representing the file name for the
                        requested package in koji.
    """
    pkg_name = "{name}-{ver}-{rel}.{arch}.rpm".format(
        name=package,
        ver=build_info["version"],
        rel=build_info["release"],
        arch=arch,
    )

    return pkg_name


def get_package_version(remote, package):
    installed_ver = None
    if remote.os.package_type == "deb":
        proc = remote.run(
            args=[
                'dpkg-query', '-W', '-f', '${Version}', package
            ],
            stdout=StringIO(),
        )
    else:
        proc = remote.run(
            args=[
                'rpm', '-q', package, '--qf', '%{VERSION}'
            ],
            stdout=StringIO(),
        )
    if proc.exitstatus == 0:
        installed_ver = proc.stdout.getvalue().strip()
        # Does this look like a version string?
        # this assumes a version string starts with non-alpha characters
        if installed_ver and re.match('^[^a-zA-Z]', installed_ver):
            log.info("The installed version of {pkg} is {ver}".format(
                pkg=package,
                ver=installed_ver,
            ))
        else:
            installed_ver = None
    else:
        # should this throw an exception and stop the job?
        log.warning(
            "Unable to determine if {pkg} is installed: {stdout}".format(
                pkg=package,
                stdout=proc.stdout.getvalue().strip(),
            )
        )

    return installed_ver
