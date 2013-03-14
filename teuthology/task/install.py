from cStringIO import StringIO

import contextlib
import logging
import time

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

def _get_system_type(remote):
    """
    Return this system type (for example, deb or rpm)
    """
    r = remote.run(
        args= [
            'sudo','lsb_release', '-is',
        ],
    stdout=StringIO(),
    )
    system_value = r.stdout.getvalue().strip()
    log.debug("System to be installed: %s" % system_value)
    if system_value in ['Ubuntu','Debian',]:
	return "deb"
    if system_value in ['CentOS',]:
        return "rpm"
    return system_value

def _get_baseurlinfo_and_dist(ctx, remote, config):
    retval = {}
    r = remote.run(
            args=['lsb_release', '-sc'],
            stdout=StringIO(),
            )
    retval['dist'] = r.stdout.getvalue().strip()
    r = remote.run(
            args=['arch'],
            stdout=StringIO(),
            )
    retval['arch'] = r.stdout.getvalue().strip()

    # branch/tag/sha1 flavor
    retval['flavor'] = config.get('flavor', 'basic')

    uri = None
    log.info('config is %s', config)
    if config.get('sha1') is not None:
        uri = 'sha1/' + config.get('sha1')
    elif config.get('tag') is not None:
        uri = 'ref/' + config.get('tag')
    elif config.get('branch') is not None:
        uri = 'ref/' + config.get('branch')
    else:
        uri = 'ref/master'
    retval['uri'] = uri

    return retval

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
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote deb {arch}".format(
            pkglist=", ".join(debs), arch=baseparms['arch']))
    dist = baseparms['dist']
    base_url = 'http://{host}/ceph-deb-{dist}-{arch}-{flavor}/{uri}'.format(
        host=ctx.teuthology_config.get('gitbuilder_host',
                                       'gitbuilder.ceph.com'),
        **baseparms
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
            'sudo', 'tee', '/etc/apt/sources.list.d/ceph.list'
            ],
        stdout=StringIO(),
        )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'apt-get', '-y', '--force-yes',
            'install',
            ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
        )

def _update_rpm_package_list_and_install(ctx, remote, rpm, config):
    baseparms = _get_baseurlinfo_and_dist(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
            pkglist=", ".join(rpm), arch=baseparms['arch']))
    host=ctx.teuthology_config.get('gitbuilder_host',
                                       'gitbuilder.ceph.com')
    r = remote.run(
        args=[
            'lsb_release', '-rs'], stdout=StringIO())
    relval = r.stdout.getvalue()
    relval = relval[0:relval.find('.')]
    
    start_of_url = 'http://{host}/ceph-rpm-centos{relval}-{arch}-{flavor}/'.format(
            host=host,relval=relval,**baseparms)
    # Should the REALEASE value get extracted from somewhere?
    RELEASE = "1-0"
    end_of_url = '{uri}/noarch/ceph-release-{release}.el{relval}.noarch.rpm'.format(
            uri=baseparms['uri'],release=RELEASE,relval=relval)
    base_url = '%s%s' % (start_of_url, end_of_url)
    remote.run(
        args=['sudo','yum','reinstall',base_url,'-y',], stdout=StringIO())
    for cpack in rpm:
        remote.run(args=['sudo', 'yum', 'install', cpack, '-y',],stdout=StringIO())

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
            system_type = _get_system_type(remote)
            p.spawn(
                install_pkgs[system_type],
                ctx, remote, pkgs[system_type], config)

def _remove_deb(remote, debs):
    log.info("Removing packages: {pkglist} on Debian system.".format(
            pkglist=", ".join(debs)))
    # first ask nicely
    remote.run(
        args=[
            'for', 'd', 'in',
            ] + debs + [
            run.Raw(';'),
            'do',
            'sudo', 'apt-get', '-y', '--force-yes', 'purge',
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
            'sudo', 'apt-get', '-y', '--force-yes',
            'autoremove',
            ],
        stdout=StringIO(),
        )

def _remove_rpm(remote, rpm):
    log.info("Removing packages: {pkglist} on rpm system.".format(
            pkglist=", ".join(rpm)))
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
            'yum', 'clean', 'expire-cache',
            ])

def remove_packages(ctx, pkgs):
    remove_pkgs = {
        "deb": _remove_deb,
        "rpm": _remove_rpm,
        }
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = _get_system_type(remote)
            p.spawn(remove_pkgs[system_type], remote, pkgs[system_type])

def _remove_sources_list_deb(remote):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/apt/sources.list.d/ceph.list',
            run.Raw('&&'),
            'sudo', 'apt-get', 'update',
            # ignore failure
            run.Raw('||'),
            'true',
            ],
        stdout=StringIO(),
       )

def _remove_sources_list_rpm(remote):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/yum.repos.d/ceph.repo',
            run.Raw('||'),
            'true',
            ],
        stdout=StringIO(),
       )
    # There probably should be a way of removing these files that is implemented in the yum/rpm
    # remove procedures for the ceph package.
    remote.run(
        args=[
            'sudo', 'rm', '-fr', '/var/lib/ceph',
            run.Raw('||'),
            'true',
            ],
        stdout=StringIO(),
       )
    remote.run(
        args=[
            'sudo', 'rm', '-fr', '/var/log/ceph',
            run.Raw('||'),
            'true',
            ],
        stdout=StringIO(),
       )

def remove_sources(ctx):
    remove_sources_pkgs = {
        'deb': _remove_sources_list_deb,
        'rpm': _remove_sources_list_rpm,
        }
    log.info("Removing ceph sources lists")
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            system_type = _get_system_type(remote)
            p.spawn(remove_sources_pkgs[system_type], remote)

@contextlib.contextmanager
def install(ctx, config):
    debs = [
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
        ]
    rpm = [
        'ceph-debug',
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

    # pull any additional packages out of config
    extra_pkgs = config.get('extra_packages')
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))
    debs = debs + extra_pkgs
    rpm = rpm + extra_pkgs

    extras = config.get('extras')
    if extras is not None:
        debs = ['ceph-test', 'ceph-test-dbg', 'ceph-fuse', 'ceph-fuse-dbg']
	rpm = ['ceph-fuse',]

    # install lib deps (so we explicitly specify version), but do not
    # uninstall them, as other packages depend on them (e.g., kvm)
    debs_install = debs + [
        'librados2',
        'librados2-dbg',
        'librbd1',
        'librbd1-dbg',
        ]
    rpm_install = rpm + [
        'librbd1',
        'librados2',
        ]
    install_info = {"deb": debs_install, "rpm": rpm_install}
    remove_info = {"deb": debs, "rpm": rpm}
    install_packages(ctx, install_info, config)
    try:
        yield
    finally:
        remove_packages(ctx, remove_info)
        remove_sources(ctx)


@contextlib.contextmanager
def task(ctx, config):
    """
    Install ceph packages
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

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
                extras=config.get('extras',None),
                wait_for_package=ctx.config.get('wait-for-package', False),
                )),
        ):
        yield
