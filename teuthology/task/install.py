from cStringIO import StringIO

import contextlib
import logging
import time

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
    # This function was written to figure out which branch should be used for a
    # given remote. 'all' overrides any applicable roles.

    roles = ctx.cluster.remotes[remote]
    if 'all' in config:
        return config['all'].get(key)
    elif roles:
        for role in roles:
            if role in config and key in config[role]:
                return config[role].get(key)
    return config.get(key)


def _get_baseurlinfo_and_dist(ctx, remote, config):
    # XXX CLEANUP
    # This helper function is in *dire* need of cleaning up What is `relval`?
    # Why so many `dist_` prepending keys?
    # The below list is an unfortunate usage of variables that confuse and make
    # it harder to read/understand/improve anything here:
    # dist, distro, distro_release, dist_release, dist_name, distri
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

    uri = None
    log.info('config is %s', config)
    tag = _get_config_value_for_remote(ctx, remote, config, 'tag')
    branch = _get_config_value_for_remote(ctx, remote, config, 'branch')
    sha1 = _get_config_value_for_remote(ctx, remote, config, 'sha1')
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
    retval['uri'] = uri

    return retval


def _get_baseurl(ctx, remote, config):
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

    wait -- wait forever for the file to show up. (default False)
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


def _update_deb_package_list_and_install(ctx, remote, debs, config):
    """
    updates the package list so that apt-get can
    download the appropriate packages
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
            raise Exception('failed to fetch package version from %s' %
                            base_url + '/version')
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break

    remote.run(
        args=[
            'echo', 'deb', base_url, baseparms['dist'], 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw('Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
    )


def _yum_fix_repo_priority(remote, project):
    remote.run(
        args=[
            'sudo',
            'sed',
            '-i',
            run.Raw('\':a;N;$!ba;s/enabled=1\\ngpg/enabled=1\\npriority=1\\ngpg/g\''),
            '/etc/yum.repos.d/%s.repo' % project,
        ]
    )


def _update_rpm_package_list_and_install(ctx, remote, rpm, config):
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
        pkglist=", ".join(rpm), arch=baseparms['arch']))
    host = ctx.teuthology_config.get('gitbuilder_host',
                                     'gitbuilder.ceph.com')
    dist_release = baseparms['dist_release']
    start_of_url = 'http://{host}/ceph-rpm-{distro_release}-{arch}-{flavor}/{uri}'.format(host=host, **baseparms)
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

    #Fix Repo Priority
    _yum_fix_repo_priority(remote, config.get('project', 'ceph'))

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
            raise Exception('failed to fetch package version from %s' %
                            version_url)
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break

    tmp_vers = version_no.getvalue().strip()[1:]
    dloc = tmp_vers.rfind('-')
    t_vers1 = tmp_vers[0:dloc]
    t_vers2 = tmp_vers[dloc + 1:]
    trailer = "-{tv1}-{tv2}.{dist_release}".format(tv1=t_vers1, tv2=t_vers2, dist_release=dist_release)
    for cpack in rpm:
        pk_err_mess = StringIO()
        pkg2add = "{cpack}{trailer}".format(cpack=cpack, trailer=trailer)
        remote.run(args=['sudo', 'yum', 'install', pkg2add, '-y', ],
                   stderr=pk_err_mess)


def purge_data(ctx):
    """
    Purge /var/lib/ceph
    """
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_purge_data, remote)


def _purge_data(remote):
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
    installs Debian packages.
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
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw('Dpkg::Options::="--force-confold"'), 'purge',
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
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw('Dpkg::Options::="--force-confold"'),
            'autoremove',
        ],
        stdout=StringIO(),
    )


def _remove_rpm(ctx, config, remote, rpm):
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
    projRelease = '%s-release-%s.%s.noarch' % (config.get('project', 'ceph'), RELEASE, dist_release)
    remote.run(args=['sudo', 'yum', 'erase', projRelease, '-y'])
    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'expire-cache',
        ])


def remove_packages(ctx, config, pkgs):
    remove_pkgs = {
        "deb": _remove_deb,
        "rpm": _remove_rpm,
    }
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            p.spawn(remove_pkgs[system_type], ctx, config, remote, pkgs[system_type])


def _remove_sources_list_deb(remote, proj):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/apt/sources.list.d/{proj}.list'.format(proj=proj),
            run.Raw('&&'),
            'sudo', 'apt-get', 'update',
            # ignore failure
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )


def _remove_sources_list_rpm(remote, proj):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/yum.repos.d/{proj}.repo'.format(proj=proj),
            run.Raw('||'),
            'true',
        ],
        stdout=StringIO(),
    )
    # There probably should be a way of removing these files that is implemented in the yum/rpm
    # remove procedures for the ceph package.
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
    remove_sources_pkgs = {
        'deb': _remove_sources_list_deb,
        'rpm': _remove_sources_list_rpm,
    }
    log.info("Removing {proj} sources lists".format(proj=config.get('project', 'ceph')))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            p.spawn(remove_sources_pkgs[system_type], remote, config.get('project', 'ceph'))

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
        debs = ['ceph-test', 'ceph-test-dbg', 'ceph-fuse', 'ceph-fuse-dbg', 'librados2', 'librados2-dbg', 'librbd1', 'librbd1-dbg', 'python-ceph']
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


def _upgrade_deb_packages(ctx, config, remote, debs, branch):
    """
    upgrade all packages
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
    uri = 'ref/' + branch
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
            raise Exception('failed to fetch package version from %s' %
                            base_url + '/version')
        version = r.stdout.getvalue().strip()
        log.info('Package version is %s', version)
        break
    remote.run(
        args=[
            'echo', 'deb', base_url, dist, 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw('Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
    )


def _upgrade_rpm_packages(ctx, config, remote, pkgs, branch):
    """
    Upgrade RPM packages.
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
    version = "{repover}.{therest}".format(
        repover=version,
        therest=distinfo['dist_release'],
    )

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
    _yum_fix_repo_priority(remote, project)

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
    upgrades project debian packages.

    For example::

        tasks:
        - install.upgrade:
             all:
                branch: end
    or
        tasks:
        - install.upgrade:
             mon.a:
                branch: end
             osd.0:
                branch: other
    """
    assert config is None or isinstance(config, dict), \
        "install.upgrade only supports a dictionary for configuration"

    for i in config.keys():
            assert isinstance(config.get(i), dict), 'host supports dictionary'

    branch = None

    project = config.get('project', 'ceph')

    # FIXME: extra_pkgs is not distro-agnostic
    extra_pkgs = config.get('extra_packages', [])
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))

    if config.get('all') is not None:
        node = config.get('all')
        for var, branch_val in node.iteritems():
            if var == 'branch' or var == 'tag' or var == 'sha1':
                branch = branch_val
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = teuthology.get_system_type(remote)
            assert system_type in ('deb', 'rpm')
            pkgs = PACKAGES[project][system_type]
            log.info("Upgrading {proj} {system_type} packages: {pkgs}".format(
                proj=project, system_type=system_type, pkgs=', '.join(pkgs)))
            # FIXME: again, make extra_pkgs distro-agnostic
            pkgs += extra_pkgs
            if system_type == 'deb':
                _upgrade_deb_packages(ctx, config, remote, pkgs, branch)
            elif system_type == 'rpm':
                _upgrade_rpm_packages(ctx, config, remote, pkgs, branch)
    # FIXME: I highly doubt if this needs to be a separate codepath.
    else:
        list_roles = []
        for role in config.keys():
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            kkeys = config.get(role)
            if remote in list_roles:
                continue
            else:
                for var, branch_val in kkeys.iteritems():
                    if var == 'branch' or var == 'tag' or var == 'sha1':
                        branch = branch_val
                        system_type = teuthology.get_system_type(remote)
                        assert system_type in ('deb', 'rpm')
                        pkgs = PACKAGES[project][system_type]
                        if system_type == 'deb':
                            _upgrade_deb_packages(ctx, config, remote, pkgs, branch)
                        elif system_type == 'rpm':
                            _upgrade_rpm_packages(ctx, config, remote, pkgs, branch)
                        list_roles.append(remote)
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Install packages

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
            log.info('Using notcmalloc flavor and running some daemons under valgrind')
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
            wait_for_package=ctx.config.get('wait-for-package', False),
            project=project,
        )),
    ):
        yield
