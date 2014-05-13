from cStringIO import StringIO

import contextlib
import copy
import logging
import time
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run
from ..orchestra.run import CommandFailedError

log = logging.getLogger(__name__)

# Should the RELEASE value get extracted from somewhere?
RELEASE = "1-0"

# This is intended to be a complete listing of ceph packages. If we're going
# to hardcode this stuff, I don't want to do it in more than once place.
PACKAGES = {}
PACKAGES['ceph'] = {}
PACKAGES['ceph']['deb'] = [
    'ceph',
    'ceph-dbg',
    'ceph-mds',
    'ceph-mds-dbg',
    'ceph-common',
    'ceph-common-dbg',
    'ceph-fuse',
    'ceph-fuse-dbg',
    'ceph-test',
    'ceph-test-dbg',
    'radosgw',
    'radosgw-dbg',
    'python-ceph',
    'libcephfs1',
    'libcephfs1-dbg',
    'libcephfs-java',
    'librados2',
    'librados2-dbg',
    'librbd1',
    'librbd1-dbg',
]
PACKAGES['ceph']['rpm'] = [
    'ceph-debuginfo',
    'ceph-radosgw',
    'ceph-test',
    'ceph-devel',
    'ceph',
    'ceph-fuse',
    'rest-bench',
    'libcephfs_jni1',
    'libcephfs1',
    'python-ceph',
]


def _run_and_log_error_if_fails(remote, args):
    """
    Yet another wrapper around command execution. This one runs a command on
    the given remote, then, if execution fails, logs the error and re-raises.

    :param remote: the teuthology.orchestra.remote.Remote object
    :param args: list of arguments comprising the command the be executed
    :returns: None
    :raises: CommandFailedError
    """
    response = StringIO()
    try:
        remote.run(
            args=args,
            stdout=response,
            stderr=response,
        )
    except CommandFailedError:
        log.error(response.getvalue().strip())
        raise


def _get_config_value_for_remote(ctx, remote, config, key):
    """
    Look through config, and attempt to determine the "best" value to use for a
    given key. For example, given:

        config = {
            'all':
                {'branch': 'master'},
            'branch': 'next'
        }
        _get_config_value_for_remote(ctx, remote, config, 'branch')

    would return 'master'.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param config: the config dict
    :param key: the name of the value to retrieve
    """
    roles = ctx.cluster.remotes[remote]
    if 'all' in config:
        return config['all'].get(key)
    elif roles:
        for role in roles:
            if role in config and key in config[role]:
                return config[role].get(key)
    return config.get(key)


def _get_uri(tag, branch, sha1):
    """
    Set the uri -- common code used by both install and debian upgrade
    """
    uri = None
    if tag:
        uri = 'ref/' + tag
    elif branch:
        uri = 'ref/' + branch
    elif sha1:
        uri = 'sha1/' + sha1
    else:
        # FIXME: Should master be the default?
        log.debug("defaulting to master branch")
        uri = 'ref/master'
    return uri


def _get_baseurlinfo_and_dist(ctx, remote, config):
    """
    Through various commands executed on the remote, determines the
    distribution name and version in use, as well as the portion of the repo
    URI to use to specify which version of the project (normally ceph) to
    install.Example:

        {'arch': 'x86_64',
        'dist': 'raring',
        'dist_release': None,
        'distro': 'Ubuntu',
        'distro_release': None,
        'flavor': 'basic',
        'relval': '13.04',
        'uri': 'ref/master'}

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param config: the config dict
    :returns: dict -- the information you want.
    """
    retval = {}
    relval = None
    r = remote.run(
        args=['arch'],
        stdout=StringIO(),
    )
    retval['arch'] = r.stdout.getvalue().strip()
    r = remote.run(
        args=['lsb_release', '-is'],
        stdout=StringIO(),
    )
    retval['distro'] = r.stdout.getvalue().strip()
    r = remote.run(
        args=[
            'lsb_release', '-rs'], stdout=StringIO())
    retval['relval'] = r.stdout.getvalue().strip()
    dist_name = None
    if ((retval['distro'] == 'CentOS') | (retval['distro'] == 'RedHatEnterpriseServer')):
        relval = retval['relval']
        relval = relval[0:relval.find('.')]
        distri = 'centos'
        retval['distro_release'] = '%s%s' % (distri, relval)
        retval['dist'] = retval['distro_release']
        dist_name = 'el'
        retval['dist_release'] = '%s%s' % (dist_name, relval)
    elif retval['distro'] == 'Fedora':
        distri = retval['distro']
        dist_name = 'fc'
        retval['distro_release'] = '%s%s' % (dist_name, retval['relval'])
        retval['dist'] = retval['dist_release'] = retval['distro_release']
    else:
        r = remote.run(
            args=['lsb_release', '-sc'],
            stdout=StringIO(),
        )
        retval['dist'] = r.stdout.getvalue().strip()
        retval['distro_release'] = None
        retval['dist_release'] = None

    # branch/tag/sha1 flavor
    retval['flavor'] = config.get('flavor', 'basic')

    log.info('config is %s', config)
    tag = _get_config_value_for_remote(ctx, remote, config, 'tag')
    branch = _get_config_value_for_remote(ctx, remote, config, 'branch')
    sha1 = _get_config_value_for_remote(ctx, remote, config, 'sha1')
    uri = _get_uri(tag, branch, sha1)
    retval['uri'] = uri

    return retval


def _get_baseurl(ctx, remote, config):
    """
    Figures out which package repo base URL to use.

    Example:
        'http://gitbuilder.ceph.com/ceph-deb-raring-x86_64-basic/ref/master'
    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param config: the config dict
    :returns: str -- the URL
    """
    # get distro name and arch
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    base_url = 'http://{host}/{proj}-{pkg_type}-{dist}-{arch}-{flavor}/{uri}'.format(
        host=ctx.teuthology_config.get('gitbuilder_host',
                                       'gitbuilder.ceph.com'),
        proj=config.get('project', 'ceph'),
        pkg_type=remote.system_type,
        **baseparms
    )
    return base_url


class VersionNotFoundError(Exception):

    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "Failed to fetch package version from %s" % self.url


def _block_looking_for_package_version(remote, base_url, wait=False):
    """
    Look for, and parse, a file called 'version' in base_url.

    :param remote: the teuthology.orchestra.remote.Remote object
    :param wait: wait forever for the file to show up. (default False)
    :returns: str -- the version e.g. '0.67-240-g67a95b9-1raring'
    :raises: VersionNotFoundError
    """
    while True:
        r = remote.run(
            args=['wget', '-q', '-O-', base_url + '/version'],
            stdout=StringIO(),
            check_status=False,
        )
        if r.exitstatus != 0:
            if wait:
                log.info('Package not there yet, waiting...')
                time.sleep(15)
                continue
            raise VersionNotFoundError(base_url)
        break
    version = r.stdout.getvalue().strip()
    return version

def _get_local_dir(config, remote):
    """
    Extract local directory name from the task lists.
    Copy files over to the remote site.
    """
    ldir = config.get('local', None)
    if ldir:
        remote.run(args=['sudo', 'mkdir', '-p', ldir,])
        for fyle in os.listdir(ldir):
            fname = "%s/%s" % (ldir, fyle)
            teuthology.sudo_write_file(remote, fname, open(fname).read(), '644')
    return ldir

def _update_deb_package_list_and_install(ctx, remote, debs, config):
    """
    Runs ``apt-get update`` first, then runs ``apt-get install``, installing
    the requested packages on the remote system.

    TODO: split this into at least two functions.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param debs: list of packages names to install
    :param config: the config dict
    """

    # check for ceph release key
    r = remote.run(
        args=[
            'sudo', 'apt-key', 'list', run.Raw('|'), 'grep', 'Ceph',
        ],
        stdout=StringIO(),
        check_status=False,
    )
    if r.stdout.getvalue().find('Ceph automated package') == -1:
        # if it doesn't exist, add it
        remote.run(
            args=[
                'wget', '-q', '-O-',
                'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote deb {arch}".format(
        pkglist=", ".join(debs), arch=baseparms['arch'])
    )
    # get baseurl
    base_url = _get_baseurl(ctx, remote, config)
    log.info('Pulling from %s', base_url)

    # get package version string
    # FIXME this is a terrible hack.
    while True:
        r = remote.run(
            args=[
                'wget', '-q', '-O-', base_url + '/version',
            ],
            stdout=StringIO(),
            check_status=False,
        )
        if r.exitstatus != 0:
            if config.get('wait_for_package'):
                log.info('Package not there yet, waiting...')
                time.sleep(15)
                continue
            raise VersionNotFoundError("%s/version" % base_url)
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break

    remote.run(
        args=[
            'echo', 'deb', base_url, baseparms['dist'], 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(
                proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
    )
    ldir = _get_local_dir(config, remote)
    if ldir:
        for fyle in os.listdir(ldir):
            fname = "%s/%s" % (ldir, fyle)
            remote.run(args=['sudo', 'dpkg', '-i', fname],)


def _yum_fix_repo_priority(remote, project, uri):
    """
    On the remote, 'priority=1' lines to each enabled repo in:

        /etc/yum.repos.d/{project}.repo

    :param remote: the teuthology.orchestra.remote.Remote object
    :param project: the project whose repos need modification
    """
    remote.run(
        args=[
            'sudo',
            'sed',
            '-i',
            '-e',
            run.Raw(
                '\':a;N;$!ba;s/enabled=1\\ngpg/enabled=1\\npriority=1\\ngpg/g\''),
            '-e',
            run.Raw("'s;ref/[a-zA-Z0-9_]*/;{uri}/;g'".format(uri=uri)),
            '/etc/yum.repos.d/%s.repo' % project,
        ]
    )


def _update_rpm_package_list_and_install(ctx, remote, rpm, config):
    """
    Installs the ceph-release package for the relevant branch, then installs
    the requested packages on the remote system.

    TODO: split this into at least two functions.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param rpm: list of packages names to install
    :param config: the config dict
    """
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
        pkglist=", ".join(rpm), arch=baseparms['arch']))
    host = ctx.teuthology_config.get('gitbuilder_host',
                                     'gitbuilder.ceph.com')
    dist_release = baseparms['dist_release']
    start_of_url = 'http://{host}/ceph-rpm-{distro_release}-{arch}-{flavor}/{uri}'.format(
        host=host, **baseparms)
    ceph_release = 'ceph-release-{release}.{dist_release}.noarch'.format(
        release=RELEASE, dist_release=dist_release)
    rpm_name = "{rpm_nm}.rpm".format(rpm_nm=ceph_release)
    base_url = "{start_of_url}/noarch/{rpm_name}".format(
        start_of_url=start_of_url, rpm_name=rpm_name)
    err_mess = StringIO()
    try:
        # When this was one command with a pipe, it would sometimes
        # fail with the message 'rpm: no packages given for install'
        remote.run(args=['wget', base_url, ],)
        remote.run(args=['sudo', 'rpm', '-i', rpm_name, ], stderr=err_mess, )
    except Exception:
        cmp_msg = 'package {pkg} is already installed'.format(
            pkg=ceph_release)
        if cmp_msg != err_mess.getvalue().strip():
            raise

    remote.run(args=['rm', '-f', rpm_name])

    # Fix Repo Priority
    uri = baseparms['uri']
    _yum_fix_repo_priority(remote, config.get('project', 'ceph'), uri)

    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'all',
        ])
    version_no = StringIO()
    version_url = "{start_of_url}/version".format(start_of_url=start_of_url)
    while True:
        r = remote.run(args=['wget', '-q', '-O-', version_url, ],
                       stdout=version_no, check_status=False)
        if r.exitstatus != 0:
            if config.get('wait_for_package'):
                log.info('Package not there yet, waiting...')
                time.sleep(15)
                continue
            raise VersionNotFoundError(version_url)
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break

    tmp_vers = version_no.getvalue().strip()[1:]
    if '-' in tmp_vers:
        tmp_vers = tmp_vers.split('-')[0]
    ldir = _get_local_dir(config, remote)
    for cpack in rpm:
        pk_err_mess = StringIO()
        pkg2add = "{cpack}-{version}".format(cpack=cpack, version=tmp_vers)
        pkg = None
        if ldir:
            pkg = "{ldir}/{cpack}-{trailer}".format(ldir=ldir, cpack=cpack, trailer=tmp_vers)
            remote.run(
                args = ['if', 'test', '-e',
                        run.Raw(pkg), run.Raw(';'), 'then',
                        'sudo', 'yum', 'remove', pkg, '-y', run.Raw(';'),
                        'sudo', 'yum', 'install', pkg, '-y',
                        run.Raw(';'), 'fi']
            )
        if pkg is None:
            remote.run(args=['sudo', 'yum', 'install', pkg2add, '-y', ],
                    stderr=pk_err_mess)
        else:
            remote.run(
                args = ['if', 'test', run.Raw('!'), '-e',
                        run.Raw(pkg), run.Raw(';'), 'then',
                        'sudo', 'yum', 'install', pkg2add, '-y',
                        run.Raw(';'), 'fi'])


def purge_data(ctx):
    """
    Purge /var/lib/ceph on every remote in ctx.

    :param ctx: the argparse.Namespace object
    """
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_purge_data, remote)


def _purge_data(remote):
    """
    Purge /var/lib/ceph on remote.

    :param remote: the teuthology.orchestra.remote.Remote object
    """
    log.info('Purging /var/lib/ceph on %s', remote)
    remote.run(args=[
        'sudo',
        'rm', '-rf', '--one-file-system', '--', '/var/lib/ceph',
        run.Raw('||'),
        'true',
        run.Raw(';'),
        'test', '-d', '/var/lib/ceph',
        run.Raw('&&'),
        'sudo',
        'find', '/var/lib/ceph',
        '-mindepth', '1',
        '-maxdepth', '2',
        '-type', 'd',
        '-exec', 'umount', '{}', ';',
        run.Raw(';'),
        'sudo',
        'rm', '-rf', '--one-file-system', '--', '/var/lib/ceph',
    ])


def install_packages(ctx, pkgs, config):
    """
    Installs packages on each remote in ctx.

    :param ctx: the argparse.Namespace object
    :param pkgs: list of packages names to install
    :param config: the config dict
    """
    install_pkgs = {
        "deb": _update_deb_package_list_and_install,
        "rpm": _update_rpm_package_list_and_install,
    }
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            p.spawn(
                install_pkgs[system_type],
                ctx, remote, pkgs[system_type], config)


def _remove_deb(ctx, config, remote, debs):
    """
    Removes Debian packages from remote, rudely

    TODO: be less rude (e.g. using --force-yes)

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param debs: list of packages names to install
    """
    log.info("Removing packages: {pkglist} on Debian system.".format(
        pkglist=", ".join(debs)))
    # first ask nicely
    remote.run(
        args=[
            'for', 'd', 'in',
        ] + debs + [
            run.Raw(';'),
            'do',
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'), 'purge',
            run.Raw('$d'),
            run.Raw('||'),
            'true',
            run.Raw(';'),
            'done',
        ])
    # mop up anything that is broken
    remote.run(
        args=[
            'dpkg', '-l',
            run.Raw('|'),
            'grep', '^.HR',
            run.Raw('|'),
            'awk', '{print $2}',
            run.Raw('|'),
            'sudo',
            'xargs', '--no-run-if-empty',
            'dpkg', '-P', '--force-remove-reinstreq',
        ])
    # then let apt clean up
    remote.run(
        args=[
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'autoremove',
        ],
        stdout=StringIO(),
    )


def _remove_rpm(ctx, config, remote, rpm):
    """
    Removes RPM packages from remote

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param rpm: list of packages names to remove
    """
    log.info("Removing packages: {pkglist} on rpm system.".format(
        pkglist=", ".join(rpm)))
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    dist_release = baseparms['dist_release']
    remote.run(
        args=[
            'for', 'd', 'in',
        ] + rpm + [
            run.Raw(';'),
            'do',
            'sudo', 'yum', 'remove',
            run.Raw('$d'),
            '-y',
            run.Raw('||'),
            'true',
            run.Raw(';'),
            'done',
        ])
    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'all',
        ])
    projRelease = '%s-release-%s.%s.noarch' % (
        config.get('project', 'ceph'), RELEASE, dist_release)
    remote.run(args=['sudo', 'yum', 'erase', projRelease, '-y'])
    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'expire-cache',
        ])


def remove_packages(ctx, config, pkgs):
    """
    Removes packages from each remote in ctx.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param pkgs: list of packages names to remove
    """
    remove_pkgs = {
        "deb": _remove_deb,
        "rpm": _remove_rpm,
    }
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            p.spawn(remove_pkgs[
                    system_type], ctx, config, remote, pkgs[system_type])


def _remove_sources_list_deb(remote, proj):
    """
    Removes /etc/apt/sources.list.d/{proj}.list and then runs ``apt-get
    update``.

    :param remote: the teuthology.orchestra.remote.Remote object
    :param proj: the project whose sources.list needs removing
    """
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/apt/sources.list.d/{proj}.list'.format(
                proj=proj),
            run.Raw('&&'),
            'sudo', 'apt-get', 'update',
            # ignore failure
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )


def _remove_sources_list_rpm(remote, proj):
    """
    Removes /etc/yum.repos.d/{proj}.repo, /var/lib/{proj}, and /var/log/{proj}.

    :param remote: the teuthology.orchestra.remote.Remote object
    :param proj: the project whose sources.list needs removing
    """
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/yum.repos.d/{proj}.repo'.format(
                proj=proj),
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )
    # FIXME
    # There probably should be a way of removing these files that is
    # implemented in the yum/rpm remove procedures for the ceph package.
    # FIXME but why is this function doing these things?
    remote.run(
        args=[
            'sudo', 'rm', '-fr', '/var/lib/{proj}'.format(proj=proj),
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )
    remote.run(
        args=[
            'sudo', 'rm', '-fr', '/var/log/{proj}'.format(proj=proj),
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )


def remove_sources(ctx, config):
    """
    Removes repo source files from each remote in ctx.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    remove_sources_pkgs = {
        'deb': _remove_sources_list_deb,
        'rpm': _remove_sources_list_rpm,
    }
    log.info("Removing {proj} sources lists".format(
        proj=config.get('project', 'ceph')))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            p.spawn(remove_sources_pkgs[
                    system_type], remote, config.get('project', 'ceph'))
            p.spawn(remove_sources_pkgs[
                    system_type], remote, 'calamari')

deb_packages = {'ceph': [
    'ceph',
    'ceph-dbg',
    'ceph-mds',
    'ceph-mds-dbg',
    'ceph-common',
    'ceph-common-dbg',
    'ceph-fuse',
    'ceph-fuse-dbg',
    'ceph-test',
    'ceph-test-dbg',
    'radosgw',
    'radosgw-dbg',
    'python-ceph',
    'libcephfs1',
    'libcephfs1-dbg',
]}

rpm_packages = {'ceph': [
    'ceph-debuginfo',
    'ceph-radosgw',
    'ceph-test',
    'ceph-devel',
    'ceph',
    'ceph-fuse',
    'rest-bench',
    'libcephfs_jni1',
    'libcephfs1',
    'python-ceph',
]}


@contextlib.contextmanager
def install(ctx, config):
    """
    The install task. Installs packages for a given project on all hosts in
    ctx. May work for projects besides ceph, but may not. Patches welcomed!

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    project = config.get('project', 'ceph')

    global deb_packages
    global rpm_packages
    debs = deb_packages.get(project, [])
    rpm = rpm_packages.get(project, [])

    # pull any additional packages out of config
    extra_pkgs = config.get('extra_packages')
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))
    debs += extra_pkgs
    rpm += extra_pkgs

    # the extras option right now is specific to the 'ceph' project
    extras = config.get('extras')
    if extras is not None:
        debs = ['ceph-test', 'ceph-test-dbg', 'ceph-fuse', 'ceph-fuse-dbg',
                'librados2', 'librados2-dbg', 'librbd1', 'librbd1-dbg', 'python-ceph']
        rpm = ['ceph-fuse', 'librbd1', 'librados2', 'ceph-test', 'python-ceph']

    # install lib deps (so we explicitly specify version), but do not
    # uninstall them, as other packages depend on them (e.g., kvm)
    proj_install_debs = {'ceph': [
        'librados2',
        'librados2-dbg',
        'librbd1',
        'librbd1-dbg',
    ]}

    proj_install_rpm = {'ceph': [
        'librbd1',
        'librados2',
    ]}

    install_debs = proj_install_debs.get(project, [])
    install_rpm = proj_install_rpm.get(project, [])

    install_info = {
        "deb": debs + install_debs,
        "rpm": rpm + install_rpm}
    remove_info = {
        "deb": debs,
        "rpm": rpm}
    install_packages(ctx, install_info, config)
    try:
        yield
    finally:
        remove_packages(ctx, config, remove_info)
        remove_sources(ctx, config)
        if project == 'ceph':
            purge_data(ctx)


def _upgrade_deb_packages(ctx, config, remote, debs):
    """
    Upgrade project's packages on remote Debian host
    Before doing so, installs the project's GPG key, writes a sources.list
    file, and runs ``apt-get update``.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param debs: the Debian packages to be installed
    :param branch: the branch of the project to be used
    """
    # check for ceph release key
    r = remote.run(
        args=[
            'sudo', 'apt-key', 'list', run.Raw('|'), 'grep', 'Ceph',
        ],
        stdout=StringIO(),
        check_status=False,
    )
    if r.stdout.getvalue().find('Ceph automated package') == -1:
        # if it doesn't exist, add it
        remote.run(
            args=[
                'wget', '-q', '-O-',
                'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    # get distro name and arch
    r = remote.run(
        args=['lsb_release', '-sc'],
        stdout=StringIO(),
    )
    dist = r.stdout.getvalue().strip()
    r = remote.run(
        args=['arch'],
        stdout=StringIO(),
    )
    arch = r.stdout.getvalue().strip()
    log.info("dist %s arch %s", dist, arch)

    # branch/tag/sha1 flavor
    flavor = 'basic'
    sha1 = config.get('sha1')
    branch = config.get('branch')
    tag = config.get('tag')
    uri = _get_uri(tag, branch, sha1)
    base_url = 'http://{host}/{proj}-deb-{dist}-{arch}-{flavor}/{uri}'.format(
        host=ctx.teuthology_config.get('gitbuilder_host',
                                       'gitbuilder.ceph.com'),
        proj=config.get('project', 'ceph'),
        dist=dist,
        arch=arch,
        flavor=flavor,
        uri=uri,
    )
    log.info('Pulling from %s', base_url)

    # get package version string
    while True:
        r = remote.run(
            args=[
                'wget', '-q', '-O-', base_url + '/version',
            ],
            stdout=StringIO(),
            check_status=False,
        )
        if r.exitstatus != 0:
            if config.get('wait_for_package'):
                log.info('Package not there yet, waiting...')
                time.sleep(15)
                continue
            raise VersionNotFoundError("%s/version" % base_url)
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break
    remote.run(
        args=[
            'echo', 'deb', base_url, dist, 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(
                proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
    )


def _upgrade_rpm_packages(ctx, config, remote, pkgs):
    """
    Upgrade project's packages on remote RPM-based host
    Before doing so, it makes sure the project's -release RPM is installed -
    removing any previous version first.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param pkgs: the RPM packages to be installed
    :param branch: the branch of the project to be used
    """
    distinfo = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info(
        "Host {host} is: {distro} {ver} {arch}".format(
            host=remote.shortname,
            distro=distinfo['distro'],
            ver=distinfo['relval'],
            arch=distinfo['arch'],)
    )

    base_url = _get_baseurl(ctx, remote, config)
    log.info('Repo base URL: %s', base_url)
    version = _block_looking_for_package_version(
        remote,
        base_url,
        config.get('wait_for_package', False))
    # FIXME: 'version' as retreived from the repo is actually the RPM version
    # PLUS *part* of the release. Example:
    # Right now, ceph master is given the following version in the repo file:
    # v0.67-rc3.164.gd5aa3a9 - whereas in reality the RPM version is 0.61.7
    # and the release is 37.g1243c97.el6 (for centos6).
    # Point being, I have to mangle a little here.
    if version[0] == 'v':
        version = version[1:]
    if '-' in version:
        version = version.split('-')[0]
    project = config.get('project', 'ceph')

    # Remove the -release package before upgrading it
    args = ['sudo', 'rpm', '-ev', '%s-release' % project]
    _run_and_log_error_if_fails(remote, args)

    # Build the new -release package path
    release_rpm = "{base}/noarch/{proj}-release-{release}.{dist_release}.noarch.rpm".format(
        base=base_url,
        proj=project,
        release=RELEASE,
        dist_release=distinfo['dist_release'],
    )

    # Upgrade the -release package
    args = ['sudo', 'rpm', '-Uv', release_rpm]
    _run_and_log_error_if_fails(remote, args)
    uri = _get_baseurlinfo_and_dist(ctx, remote, config)['uri']
    _yum_fix_repo_priority(remote, project, uri)

    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'all',
        ])

    # Build a space-separated string consisting of $PKG-$VER for yum
    pkgs_with_vers = ["%s-%s" % (pkg, version) for pkg in pkgs]

    # Actually upgrade the project packages
    # FIXME: This currently outputs nothing until the command is finished
    # executing. That sucks; fix it.
    args = ['sudo', 'yum', '-y', 'install']
    args += pkgs_with_vers
    _run_and_log_error_if_fails(remote, args)


@contextlib.contextmanager
def upgrade(ctx, config):
    """
    Upgrades packages for a given project.

    For example::

        tasks:
        - install.upgrade:
             all:
                branch: end

    or specify specific roles::

        tasks:
        - install.upgrade:
             mon.a:
                branch: end
             osd.0:
                branch: other

    or rely on the overrides for the target version::

        overrides:
          install:
            ceph:
              sha1: ...
        tasks:
        - install.upgrade:
            all:

    (HACK: the overrides will *only* apply the sha1/branch/tag if those
    keys are not present in the config.)

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    assert config is None or isinstance(config, dict), \
        "install.upgrade only supports a dictionary for configuration"

    for i in config.keys():
            assert config.get(i) is None or isinstance(
                config.get(i), dict), 'host supports dictionary'

    project = config.get('project', 'ceph')

    # use 'install' overrides here, in case the upgrade target is left
    # unspecified/implicit.
    install_overrides = ctx.config.get(
        'overrides', {}).get('install', {}).get(project, {})
    log.info('project %s config %s overrides %s', project, config, install_overrides)

    # FIXME: extra_pkgs is not distro-agnostic
    extra_pkgs = config.get('extra_packages', [])
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))

    # build a normalized remote -> config dict
    remotes = {}
    if 'all' in config:
        for remote in ctx.cluster.remotes.iterkeys():
            remotes[remote] = config.get('all')
    else:
        for role in config.keys():
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            if remote in remotes:
                log.warn('remote %s came up twice (role %s)', remote, role)
                continue
            remotes[remote] = config.get(role)

    for remote, node in remotes.iteritems():
        if not node:
            node = {}

        this_overrides = copy.deepcopy(install_overrides)
        if 'sha1' in node or 'tag' in node or 'branch' in node:
            log.info('config contains sha1|tag|branch, removing those keys from override')
            this_overrides.pop('sha1', None)
            this_overrides.pop('tag', None)
            this_overrides.pop('branch', None)
        teuthology.deep_merge(node, this_overrides)
        log.info('remote %s config %s', remote, node)

        system_type = teuthology.get_system_type(remote)
        assert system_type in ('deb', 'rpm')
        pkgs = PACKAGES[project][system_type]
        log.info("Upgrading {proj} {system_type} packages: {pkgs}".format(
            proj=project, system_type=system_type, pkgs=', '.join(pkgs)))
            # FIXME: again, make extra_pkgs distro-agnostic
        pkgs += extra_pkgs
        node['project'] = project
        if system_type == 'deb':
            _upgrade_deb_packages(ctx, node, remote, pkgs)
        elif system_type == 'rpm':
            _upgrade_rpm_packages(ctx, node, remote, pkgs)

    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Install packages for a given project.

    tasks:
    - install:
        project: ceph
        branch: bar
    - install:
        project: samba
        branch: foo
        extra_packages: ['samba']

    Overrides are project specific:

    overrides:
      install:
        ceph:
          sha1: ...

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task install only supports a dictionary for configuration"

    project, = config.get('project', 'ceph'),
    log.debug('project %s' % project)
    overrides = ctx.config.get('overrides')
    if overrides:
        install_overrides = overrides.get('install', {})
        teuthology.deep_merge(config, install_overrides.get(project, {}))
    log.debug('config %s' % config)

    # Flavor tells us what gitbuilder to fetch the prebuilt software
    # from. It's a combination of possible keywords, in a specific
    # order, joined by dashes. It is used as a URL path name. If a
    # match is not found, the teuthology run fails. This is ugly,
    # and should be cleaned up at some point.

    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            log.info(
                'Using notcmalloc flavor and running some daemons under valgrind')
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                log.info('Recording coverage for this run.')
                flavor = 'gcov'

    ctx.summary['flavor'] = flavor

    with contextutil.nested(
        lambda: install(ctx=ctx, config=dict(
            branch=config.get('branch'),
            tag=config.get('tag'),
            sha1=config.get('sha1'),
            flavor=flavor,
            extra_packages=config.get('extra_packages', []),
            extras=config.get('extras', None),
            wait_for_package=ctx.config.get('wait_for_package', False),
            project=project,
        )),
    ):
        yield
