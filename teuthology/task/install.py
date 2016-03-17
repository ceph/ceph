from cStringIO import StringIO

import contextlib
import copy
import logging
import time
import os
import subprocess
import yaml

from teuthology.config import config as teuth_config
from teuthology import misc as teuthology
from teuthology import contextutil, packaging
from teuthology.parallel import parallel
from ..orchestra import run
from . import ansible

log = logging.getLogger(__name__)

# Should the RELEASE value get extracted from somewhere?
RELEASE = "1-0"


def _get_gitbuilder_project(ctx, remote, config):
    return packaging.GitbuilderProject(
        config.get('project', 'ceph'),
        config,
        remote=remote,
        ctx=ctx
    )


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
                'http://git.ceph.com/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote deb {arch}".format(
        pkglist=", ".join(debs), arch=gitbuilder.arch)
    )
    # get baseurl
    log.info('Pulling from %s', gitbuilder.base_url)

    version = gitbuilder.version
    log.info('Package version is %s', version)

    remote.run(
        args=[
            'echo', 'deb', gitbuilder.base_url, gitbuilder.distro, 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(
                proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(args=['sudo', 'apt-get', 'update'], check_status=False)
    remote.run(
        args=[
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y',
            '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
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
    repo_path = '/etc/yum.repos.d/%s.repo' % project
    remote.run(
        args=[
            'if', 'test', '-f', repo_path, run.Raw(';'), 'then',
            'sudo', 'sed', '-i', '-e',
            run.Raw('\':a;N;$!ba;s/enabled=1\\ngpg/enabled=1\\npriority=1\\ngpg/g\''),
            '-e',
            run.Raw("'s;ref/[a-zA-Z0-9_-]*/;{uri}/;g'".format(uri=uri)),
            repo_path, run.Raw(';'), 'fi'
        ]
    )


def _yum_fix_repo_host(remote, project):
    """
    Update the hostname to reflect the gitbuilder_host setting.
    """
    old_host = teuth_config._defaults['gitbuilder_host']
    new_host = teuth_config.gitbuilder_host
    if new_host == old_host:
        return
    repo_path = '/etc/yum.repos.d/%s.repo' % project
    host_sed_expr = "'s/{0}/{1}/'".format(old_host, new_host)
    remote.run(
        args=[
            'if', 'test', '-f', repo_path, run.Raw(';'), 'then',
            'sudo', 'sed', '-i', '-e', run.Raw(host_sed_expr),
            repo_path, run.Raw(';'), 'fi']
    )


def _yum_set_check_obsoletes(remote):
    """
    Set check_obsoletes = 1 in /etc/yum/pluginconf.d/priorities.conf

    Creates a backup at /etc/yum/pluginconf.d/priorities.conf.orig so we can
    restore later.
    """
    conf_path = '/etc/yum/pluginconf.d/priorities.conf'
    conf_path_orig = conf_path + '.orig'
    cmd = [
        'test', '-e', conf_path_orig, run.Raw('||'), 'sudo', 'cp', '-af',
        conf_path, conf_path_orig,
    ]
    remote.run(args=cmd)
    cmd = [
        'grep', 'check_obsoletes', conf_path, run.Raw('&&'), 'sudo', 'sed',
        '-i', 's/check_obsoletes.*0/check_obsoletes = 1/g', conf_path,
        run.Raw('||'), 'echo', 'check_obsoletes = 1', run.Raw('|'), 'sudo',
        'tee', '-a', conf_path,
    ]
    remote.run(args=cmd)


def _yum_unset_check_obsoletes(remote):
    """
    Restore the /etc/yum/pluginconf.d/priorities.conf backup
    """
    conf_path = '/etc/yum/pluginconf.d/priorities.conf'
    conf_path_orig = conf_path + '.orig'
    remote.run(args=['sudo', 'mv', '-f', conf_path_orig, conf_path],
               check_status=False)


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
    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    log.info('Pulling from %s', gitbuilder.base_url)
    log.info('Package version is %s', gitbuilder.version)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
        pkglist=", ".join(rpm), arch=gitbuilder.arch))
    dist_release = gitbuilder.dist_release
    project = gitbuilder.project
    start_of_url = gitbuilder.base_url
    proj_release = '{proj}-release-{release}.{dist_release}.noarch'.format(
        proj=project, release=RELEASE, dist_release=dist_release)
    rpm_name = "{rpm_nm}.rpm".format(rpm_nm=proj_release)
    base_url = "{start_of_url}/noarch/{rpm_name}".format(
        start_of_url=start_of_url, rpm_name=rpm_name)
    # When this was one command with a pipe, it would sometimes
    # fail with the message 'rpm: no packages given for install'
    remote.run(args=['wget', base_url, ],)
    remote.run(args=['sudo', 'yum', '-y', 'localinstall', rpm_name])

    remote.run(args=['rm', '-f', rpm_name])

    uri = gitbuilder.uri_reference
    _yum_fix_repo_priority(remote, project, uri)
    _yum_fix_repo_host(remote, project)
    _yum_set_check_obsoletes(remote)

    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'all',
        ])

    ldir = _get_local_dir(config, remote)
    for cpack in rpm:
        pkg = None
        if ldir:
            pkg = "{ldir}/{cpack}".format(
                ldir=ldir,
                cpack=cpack,
            )
            remote.run(
                args = ['if', 'test', '-e',
                        run.Raw(pkg), run.Raw(';'), 'then',
                        'sudo', 'yum', 'remove', pkg, '-y', run.Raw(';'),
                        'sudo', 'yum', 'install', pkg, '-y',
                        run.Raw(';'), 'fi']
            )
        if pkg is None:
            remote.run(args=['sudo', 'yum', 'install', cpack, '-y'])
        else:
            remote.run(
                args = ['if', 'test', run.Raw('!'), '-e',
                        run.Raw(pkg), run.Raw(';'), 'then',
                        'sudo', 'yum', 'install', cpack, '-y',
                        run.Raw(';'), 'fi'])


def verify_package_version(ctx, config, remote):
    """
    Ensures that the version of package installed is what
    was asked for in the config.

    For most cases this is for ceph, but we also install samba
    for example.
    """
    # Do not verify the version if the ceph-deploy task is being used to
    # install ceph. Verifying the ceph installed by ceph-deploy should work,
    # but the qa suites will need reorganized first to run ceph-deploy
    # before the install task.
    # see: http://tracker.ceph.com/issues/11248
    if config.get("extras"):
        log.info("Skipping version verification...")
        return True
    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    version = gitbuilder.version
    pkg_to_check = gitbuilder.project
    installed_ver = packaging.get_package_version(remote, pkg_to_check)
    if installed_ver and version in installed_ver:
        msg = "The correct {pkg} version {ver} is installed.".format(
            ver=version,
            pkg=pkg_to_check
        )
        log.info(msg)
    else:
        raise RuntimeError(
            "{pkg} version {ver} was not installed, found {installed}.".format(
                ver=version,
                installed=installed_ver,
                pkg=pkg_to_check
            )
        )


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

    for remote in ctx.cluster.remotes.iterkeys():
        # verifies that the install worked as expected
        verify_package_version(ctx, config, remote)


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
            # Any package that is unpacked or half-installed and also requires
            # reinstallation
            'grep', '^.\(U\|H\)R',
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
    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    dist_release = gitbuilder.dist_release
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
        ],
        check_status=False,
    )


def _remove_sources_list_rpm(remote, proj):
    """
    Removes /etc/yum.repos.d/{proj}.repo, /var/lib/{proj}, and /var/log/{proj}

    :param remote: the teuthology.orchestra.remote.Remote object
    :param proj: the project whose .repo needs removing
    """
    remote.run(
        args=['sudo', 'rm', '/etc/yum.repos.d/{proj}.repo'.format(proj=proj)],
        check_status=False,
    )
    # FIXME
    # There probably should be a way of removing these files that is
    # implemented in the yum/rpm remove procedures for the ceph package.
    # FIXME but why is this function doing these things?
    remote.run(
        args=['sudo', 'rm', '-r', '/var/lib/{proj}'.format(proj=proj)],
        check_status=False,
    )
    remote.run(
        args=['sudo', 'rm', '-r', '/var/log/{proj}'.format(proj=proj)],
        check_status=False,
    )
    _yum_unset_check_obsoletes(remote)


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
    with parallel() as p:
        project = config.get('project', 'ceph')
        log.info("Removing {proj} sources lists".format(
            proj=project))
        for remote in ctx.cluster.remotes.iterkeys():
            remove_fn = remove_sources_pkgs[remote.os.package_type]
            p.spawn(remove_fn, remote, project)

    with parallel() as p:
        project = 'calamari'
        log.info("Removing {proj} sources lists".format(
            proj=project))
        for remote in ctx.cluster.remotes.iterkeys():
            remove_fn = remove_sources_pkgs[remote.os.package_type]
            p.spawn(remove_fn, remote, project)


def get_package_list(ctx, config):
    debug = config.get('debuginfo', False)
    project = config.get('project', 'ceph')
    yaml_path = None
    # Look for <suite_path>/packages/packages.yaml
    if hasattr(ctx, 'config') and 'suite_path' in ctx.config:
        suite_packages_path = os.path.join(
            ctx.config['suite_path'],
            'packages',
            'packages.yaml',
        )
        if os.path.exists(suite_packages_path):
            yaml_path = suite_packages_path
    # If packages.yaml isn't found in the suite_path, potentially use
    # teuthology's
    yaml_path = yaml_path or os.path.join(
        os.path.dirname(__file__),
        'packages.yaml',
    )
    default_packages = yaml.safe_load(open(yaml_path))
    default_debs = default_packages.get(project, dict()).get('deb', [])
    default_rpms = default_packages.get(project, dict()).get('rpm', [])
    # If a custom deb and/or rpm list is provided via the task config, use
    # that. Otherwise, use the list from whichever packages.yaml was found
    # first
    debs = config.get('packages', dict()).get('deb', default_debs)
    rpms = config.get('packages', dict()).get('rpm', default_rpms)
    # Optionally include or exclude debug packages
    if not debug:
        debs = filter(lambda p: not p.endswith('-dbg'), debs)
        rpms = filter(lambda p: not p.endswith('-debuginfo'), rpms)
    package_list = dict(deb=debs, rpm=rpms)
    log.debug("Package list is: {}".format(package_list))
    return package_list


@contextlib.contextmanager
def install(ctx, config):
    """
    The install task. Installs packages for a given project on all hosts in
    ctx. May work for projects besides ceph, but may not. Patches welcomed!

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    project = config.get('project', 'ceph')

    package_list = get_package_list(ctx, config)
    debs = package_list['deb']
    rpm = package_list['rpm']

    # pull any additional packages out of config
    extra_pkgs = config.get('extra_packages')
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))
    debs += extra_pkgs
    rpm += extra_pkgs

    # When extras is in the config we want to purposely not install ceph.
    # This is typically used on jobs that use ceph-deploy to install ceph
    # or when we are testing ceph-deploy directly.  The packages being
    # installed are needed to properly test ceph as ceph-deploy won't
    # install these. 'extras' might not be the best name for this.
    extras = config.get('extras')
    if extras is not None:
        debs = ['ceph-test', 'ceph-fuse',
                'librados2', 'librbd1',
                'python-ceph']
        rpm = ['ceph-fuse', 'librbd1', 'librados2', 'ceph-test', 'python-ceph']
    package_list = dict(deb=debs, rpm=rpm)
    install_packages(ctx, package_list, config)
    try:
        yield
    finally:
        remove_packages(ctx, config, package_list)
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
                'http://git.ceph.com/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    # get distro name and arch
    dist = gitbuilder.distro
    base_url = gitbuilder.base_url
    log.info('Pulling from %s', base_url)

    version = gitbuilder.version
    log.info('Package version is %s', version)

    remote.run(
        args=[
            'echo', 'deb', base_url, dist, 'main',
            run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/{proj}.list'.format(
                proj=config.get('project', 'ceph')),
        ],
        stdout=StringIO(),
    )
    remote.run(args=['sudo', 'apt-get', 'update'], check_status=False)
    remote.run(
        args=[
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
    )


@contextlib.contextmanager
def rh_install(ctx, config):
    """
    Installs rh ceph on all hosts in ctx.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    version = config['rhbuild']
    rh_versions = ['1.3.0', '1.3.1', '1.3.2', '2.0']
    if version in rh_versions:
        log.info("%s is a supported version", version)
    else:
        raise RuntimeError("Unsupported RH Ceph version %s", version)

    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            if remote.os.name == 'rhel':
                log.info("Installing on RHEL node: %s", remote.shortname)
                p.spawn(rh_install_pkgs, ctx, remote, version)
            else:
                log.info("Node %s is not RHEL", remote.shortname)
                raise RuntimeError("Test requires RHEL nodes")
    try:
        yield
    finally:
        if config.get('skip_uninstall'):
            log.info("Skipping uninstall of Ceph")
        else:
            rh_uninstall(ctx=ctx, config=config)


def rh_uninstall(ctx, config):
    """
     Uninstalls rh ceph on all hosts.
     It actually spawns rh_uninstall_pkgs() on the remotes for uninstall.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(rh_uninstall_pkgs, ctx, remote)


def rh_install_pkgs(ctx, remote, installed_version):
    """
    Installs RH build using ceph-deploy.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    """
    pkgs = ['ceph-deploy']
    if (installed_version == '1.3.2'):
        pkgs.append('ceph-selinux')
    rh_version_check = {'0.94.1': '1.3.0', '0.94.3': '1.3.1',
                        '0.94.5': '1.3.2', '10.0.4': '2.0'}
    log.info("Remove any epel packages installed on node %s", remote.shortname)
    remote.run(args=['sudo', 'yum', 'remove', run.Raw("leveldb xmlstarlet fcgi"), '-y'],check_status=False)
    for pkg in pkgs:
        log.info("Check if %s is already installed on node %s", pkg, remote.shortname)
        remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
        r = remote.run(
             args=['yum', 'list', 'installed', run.Raw(pkg)],
             stdout=StringIO(),
             check_status=False,
            )
        if r.stdout.getvalue().find(pkg) == -1:
            log.info("Installing %s " % pkg)
            remote.run(args=['sudo', 'yum', 'install', pkg, '-y'])
        else:
            log.info("Removing and reinstalling %s on %s", pkg, remote.shortname)
            remote.run(args=['sudo', 'yum', 'remove', pkg, '-y'])
            remote.run(args=['sudo', 'yum', 'install', pkg, '-y'])

    log.info("Check if ceph is already installed on %s", remote.shortname)
    r = remote.run(
          args=['yum', 'list', 'installed','ceph'],
          stdout=StringIO(),
          check_status=False,
        )
    host = r.hostname
    if r.stdout.getvalue().find('ceph') == -1:
        log.info("Install ceph using ceph-deploy on %s", remote.shortname)
        remote.run(args=['sudo', 'ceph-deploy', 'install', run.Raw('--no-adjust-repos'), host])
        remote.run(args=['sudo', 'yum', 'install', 'ceph-test', '-y'])
    else:
        log.info("Removing and reinstalling Ceph on %s", remote.shortname)
        remote.run(args=['sudo', 'ceph-deploy', 'uninstall', host])
        remote.run(args=['sudo', 'ceph-deploy', 'purgedata', host])
        remote.run(args=['sudo', 'ceph-deploy', 'install', host])
        remote.run(args=['sudo', 'yum', 'remove', 'ceph-test', '-y'])
        remote.run(args=['sudo', 'yum', 'install', 'ceph-test', '-y'])

    # check package version
    version = packaging.get_package_version(remote, 'ceph')
    log.info("Node: {n} Ceph version installed is {v}".format(n=remote.shortname,v=version))
    if rh_version_check[version] == installed_version:
        log.info("Installed version matches on %s", remote.shortname)
    else:
        raise RuntimeError("Version check failed on node %s", remote.shortname)


def rh_uninstall_pkgs(ctx, remote):
    """
    Removes Ceph from all RH hosts

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    """
    log.info("uninstalling packages using ceph-deploy on node %s", remote.shortname)
    r = remote.run(args=['date'], check_status=False)
    host = r.hostname
    remote.run(args=['sudo', 'ceph-deploy', 'uninstall', host])
    time.sleep(4)
    remote.run(args=['sudo', 'ceph-deploy', 'purgedata', host])
    log.info("Uninstalling ceph-deploy")
    remote.run(args=['sudo', 'yum', 'remove', 'ceph-deploy', '-y'], check_status=False)
    remote.run(args=['sudo', 'yum', 'remove', 'ceph-test', '-y'], check_status=False)


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
    gitbuilder = _get_gitbuilder_project(ctx, remote, config)
    log.info(
        "Host {host} is: {distro} {ver} {arch}".format(
            host=remote.shortname,
            distro=gitbuilder.os_type,
            ver=gitbuilder.os_version,
            arch=gitbuilder.arch,)
    )

    base_url = gitbuilder.base_url
    log.info('Repo base URL: %s', base_url)
    project = gitbuilder.project

    # Remove the -release package before upgrading it
    args = ['sudo', 'rpm', '-ev', '%s-release' % project]
    remote.run(args=args)

    # Build the new -release package path
    release_rpm = "{base}/noarch/{proj}-release-{release}.{dist_release}.noarch.rpm".format(
        base=base_url,
        proj=project,
        release=RELEASE,
        dist_release=gitbuilder.dist_release,
    )

    # Upgrade the -release package
    args = ['sudo', 'rpm', '-Uv', release_rpm]
    remote.run(args=args)
    uri = gitbuilder.uri_reference
    _yum_fix_repo_priority(remote, project, uri)
    _yum_fix_repo_host(remote, project)
    _yum_set_check_obsoletes(remote)

    remote.run(
        args=[
            'sudo', 'yum', 'clean', 'all',
        ])

    # Actually upgrade the project packages
    args = ['sudo', 'yum', '-y', 'install']
    args += pkgs
    remote.run(args=args)


def upgrade_old_style(ctx, node, remote, pkgs, system_type):
    """
    Handle the upgrade using methods in use prior to ceph-deploy.
    """
    if system_type == 'deb':
        _upgrade_deb_packages(ctx, node, remote, pkgs)
    elif system_type == 'rpm':
        _upgrade_rpm_packages(ctx, node, remote, pkgs)

def upgrade_with_ceph_deploy(ctx, node, remote, pkgs, sys_type):
    """
    Upgrade using ceph-deploy
    """
    dev_table = ['branch', 'tag', 'dev']
    ceph_dev_parm = ''
    ceph_rel_parm = ''
    for entry in node.keys():
        if entry in dev_table:
            ceph_dev_parm = node[entry]
        if entry == 'release':
            ceph_rel_parm = node[entry]
    params = []
    if ceph_dev_parm:
        params += ['--dev', ceph_dev_parm]
    if ceph_rel_parm:
        params += ['--release', ceph_rel_parm]
    params.append(remote.name)
    subprocess.call(['ceph-deploy', 'install'] + params)
    remote.run(args=['sudo', 'restart', 'ceph-all'])

def upgrade_remote_to_config(ctx, config):
    assert config is None or isinstance(config, dict), \
        "install.upgrade only supports a dictionary for configuration"

    project = config.get('project', 'ceph')

    # use 'install' overrides here, in case the upgrade target is left
    # unspecified/implicit.
    install_overrides = ctx.config.get(
        'overrides', {}).get('install', {}).get(project, {})
    log.info('project %s config %s overrides %s', project, config,
             install_overrides)

    # build a normalized remote -> config dict
    remotes = {}
    if 'all' in config:
        for remote in ctx.cluster.remotes.iterkeys():
            remotes[remote] = config.get('all')
    else:
        for role in config.keys():
            remotes_dict = ctx.cluster.only(role).remotes
            if not remotes_dict:
                # This is a regular config argument, not a role
                continue
            remote = remotes_dict.keys()[0]
            if remote in remotes:
                log.warn('remote %s came up twice (role %s)', remote, role)
                continue
            remotes[remote] = config.get(role)

    result = {}
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
        node['project'] = project

        result[remote] = node

    return result


def upgrade_common(ctx, config, deploy_style):
    """
    Common code for upgrading
    """
    remotes = upgrade_remote_to_config(ctx, config)
    project = config.get('project', 'ceph')

    # FIXME: extra_pkgs is not distro-agnostic
    extra_pkgs = config.get('extra_packages', [])
    log.info('extra packages: {packages}'.format(packages=extra_pkgs))

    for remote, node in remotes.iteritems():

        system_type = teuthology.get_system_type(remote)
        assert system_type in ('deb', 'rpm')
        pkgs = get_package_list(ctx, config)[system_type]
        excluded_packages = config.get('exclude_packages', list())
        pkgs = list(set(pkgs).difference(set(excluded_packages)))
        log.info("Upgrading {proj} {system_type} packages: {pkgs}".format(
            proj=project, system_type=system_type, pkgs=', '.join(pkgs)))
            # FIXME: again, make extra_pkgs distro-agnostic
        pkgs += extra_pkgs

        deploy_style(ctx, node, remote, pkgs, system_type)
        verify_package_version(ctx, node, remote)
    return len(remotes)

docstring_for_upgrade = """"
    Upgrades packages for a given project.

    For example::

        tasks:
        - install.{cmd_parameter}:
             all:
                branch: end

    or specify specific roles::

        tasks:
        - install.{cmd_parameter}:
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
        - install.{cmd_parameter}:
            all:

    (HACK: the overrides will *only* apply the sha1/branch/tag if those
    keys are not present in the config.)

    It is also possible to attempt to exclude packages from the upgrade set:

        tasks:
        - install.{cmd_parameter}:
            exclude_packages: ['ceph-test', 'ceph-test-dbg']

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

#
# __doc__ strings for upgrade and ceph_deploy_upgrade are set from
# the same string so that help(upgrade) and help(ceph_deploy_upgrade)
# look the same.
#

@contextlib.contextmanager
def upgrade(ctx, config):
    upgrade_common(ctx, config, upgrade_old_style)
    yield

upgrade.__doc__ = docstring_for_upgrade.format(cmd_parameter='upgrade')

@contextlib.contextmanager
def ceph_deploy_upgrade(ctx, config):
    upgrade_common(ctx, config, upgrade_with_ceph_deploy)
    yield

ceph_deploy_upgrade.__doc__ = docstring_for_upgrade.format(
        cmd_parameter='ceph_deploy_upgrade')

@contextlib.contextmanager
def ship_utilities(ctx, config):
    """
    Write a copy of valgrind.supp to each of the remote sites.  Set executables used
    by Ceph in /usr/local/bin.  When finished (upon exit of the teuthology run), remove
    these files.

    :param ctx: Context
    :param config: Configuration
    """
    assert config is None
    testdir = teuthology.get_testdir(ctx)
    filenames = []

    log.info('Shipping valgrind.supp...')
    with file(os.path.join(os.path.dirname(__file__), 'valgrind.supp'), 'rb') as f:
        fn = os.path.join(testdir, 'valgrind.supp')
        filenames.append(fn)
        for rem in ctx.cluster.remotes.iterkeys():
            teuthology.sudo_write_file(
                remote=rem,
                path=fn,
                data=f,
                )
            f.seek(0)

    FILES = ['daemon-helper', 'adjust-ulimits']
    destdir = '/usr/bin'
    for filename in FILES:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join(destdir, filename)
        filenames.append(dst)
        with file(src, 'rb') as f:
            for rem in ctx.cluster.remotes.iterkeys():
                teuthology.sudo_write_file(
                    remote=rem,
                    path=dst,
                    data=f,
                )
                f.seek(0)
                rem.run(
                    args=[
                        'sudo',
                        'chmod',
                        'a=rx',
                        '--',
                        dst,
                    ],
                )

    try:
        yield
    finally:
        log.info('Removing shipped files: %s...', ' '.join(filenames))
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo',
                    'rm',
                    '-f',
                    '--',
                ] + list(filenames),
                wait=False,
            ),
        )


def get_flavor(config):
    """
    Determine the flavor to use.

    Flavor tells us what gitbuilder to fetch the prebuilt software
    from. It's a combination of possible keywords, in a specific
    order, joined by dashes. It is used as a URL path name. If a
    match is not found, the teuthology run fails. This is ugly,
    and should be cleaned up at some point.
    """
    config = config or dict()
    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                flavor = 'gcov'
    return flavor


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
    - install:
        rhbuild: 1.3.0
        playbook: downstream_setup.yml
        vars:
           yum_repos:
             - url: "http://location.repo"
               name: "ceph_repo"

    Overrides are project specific:

    overrides:
      install:
        ceph:
          sha1: ...


    Debug packages may optionally be installed:

    overrides:
      install:
        ceph:
          debuginfo: true


    Default package lists (which come from packages.yaml) may be overridden:

    overrides:
      install:
        ceph:
          packages:
            deb:
            - ceph-osd
            - ceph-mon
            rpm:
            - ceph-devel
            - rbd-fuse

    When tag, branch and sha1 do not reference the same commit hash, the
    tag takes precedence over the branch and the branch takes precedence
    over the sha1.

    When the overrides have a sha1 that is different from the sha1 of
    the project to be installed, it will be a noop if the project has
    a branch or tag, because they take precedence over the sha1. For
    instance:

    overrides:
      install:
        ceph:
          sha1: 1234

    tasks:
    - install:
        project: ceph
          sha1: 4567
          branch: foobar # which has sha1 4567

    The override will transform the tasks as follows:

    tasks:
    - install:
        project: ceph
          sha1: 1234
          branch: foobar # which has sha1 4567

    But the branch takes precedence over the sha1 and foobar
    will be installed. The override of the sha1 has no effect.

    When passed 'rhbuild' as a key, it will attempt to install an rh ceph build using ceph-deploy

    Reminder regarding teuthology-suite side effects:

    The teuthology-suite command always adds the following:

    overrides:
      install:
        ceph:
          sha1: 1234

    where sha1 matches the --ceph argument. For instance if
    teuthology-suite is called with --ceph master, the sha1 will be
    the tip of master. If called with --ceph v0.94.1, the sha1 will be
    the v0.94.1 (as returned by git rev-parse v0.94.1 which is not to
    be confused with git rev-parse v0.94.1^{commit})

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

    rhbuild = None
    if config.get('rhbuild'):
        rhbuild = config.get('rhbuild')
        log.info("Build is %s " % rhbuild)

    flavor = get_flavor(config)
    log.info("Using flavor: %s", flavor)

    ctx.summary['flavor'] = flavor
    nested_tasks = [lambda: rh_install(ctx=ctx, config=config),
                    lambda: ship_utilities(ctx=ctx, config=None)]

    if config.get('rhbuild'):
        if config.get('playbook'):
            ansible_config=dict(config)
            # remove key not required by ansible task
            del ansible_config['rhbuild']
            nested_tasks.insert(0, lambda: ansible.CephLab(ctx,config=ansible_config))
        with contextutil.nested(*nested_tasks):
                yield
    else:
        with contextutil.nested(
            lambda: install(ctx=ctx, config=dict(
                branch=config.get('branch'),
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                debuginfo=config.get('debuginfo'),
                flavor=flavor,
                extra_packages=config.get('extra_packages', []),
                extras=config.get('extras', None),
                wait_for_package=config.get('wait_for_package', False),
                project=project,
                packages=config.get('packages', dict()),
            )),
            lambda: ship_utilities(ctx=ctx, config=None),
        ):
            yield
