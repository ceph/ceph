import contextlib
import logging
import time
import yaml
import os

from cStringIO import StringIO

from teuthology import packaging
from teuthology.orchestra import run
from teuthology.parallel import parallel

log = logging.getLogger(__name__)


@contextlib.contextmanager
def install(ctx, config):
    """
    Installs rh ceph on all hosts in ctx.

    :param ctx: the argparse.Namespace object
    :param config: the config dict

    uses yaml defined in qa suite or in users
    home dir to check for supported versions and
    packages to install.

    the format of yaml is:
    versions:
        supported:
           - '1.3.0'
        rpm:
            mapped:
               '1.3.0' : '0.94.1'
        deb:
            mapped:
               '1.3.0' : '0.94.1'
        pkgs:
            rpm:
             - ceph-mon
             - ceph-osd
            deb:
             - ceph-osd
             - ceph-mds
    """
    yaml_path = None
    # Look for rh specific packages in <suite_path>/rh/downstream.yaml
    if 'suite_path' in ctx.config:
        ds_yaml = os.path.join(
            ctx.config['suite_path'],
            'rh',
            'downstream.yaml',
        )
        if os.path.exists(ds_yaml):
            yaml_path = ds_yaml
    # default to user home dir if one exists
    default_yaml = os.path.expanduser('~/downstream.yaml')
    if os.path.exists(default_yaml):
        yaml_path = default_yaml
    log.info("using yaml path %s", yaml_path)
    downstream_config = yaml.safe_load(open(yaml_path))
    rh_versions = downstream_config.get('versions', dict()).get('supported', [])
    version = config['rhbuild']
    if version in rh_versions:
        log.info("%s is a supported version", version)
    else:
        raise RuntimeError("Unsupported RH Ceph version %s", version)
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            if remote.os.name == 'rhel':
                log.info("Installing on RHEL node: %s", remote.shortname)
                p.spawn(rh_install_pkgs, ctx, remote, version, downstream_config)
            else:
                log.info("Node %s is not RHEL", remote.shortname)
                raise RuntimeError("Test requires RHEL nodes")
    try:
        yield
    finally:
        if config.get('skip_uninstall'):
            log.info("Skipping uninstall of Ceph")
        else:
            uninstall(ctx=ctx, config=config)


def uninstall(ctx, config):
    """
     Uninstalls rh ceph on all hosts.
     It actually spawns uninstall_pkgs() on the remotes for uninstall.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(uninstall_pkgs, ctx, remote)


def install_pkgs(ctx, remote, installed_version):
    """
    Installs RH build using ceph-deploy.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    """
    pkgs = ['ceph-deploy']
    # install ceph-selinux for 1.3.2 as its not dependency of any core packages
    if (installed_version == '1.3.2'):
        pkgs.append('ceph-selinux')
    # install ceph-fuse for 2.0 as its not dependency of any core packages
    if (installed_version == '2.0'):
        pkgs.append('ceph-fuse')
    rh_version_check = {'0.94.1': '1.3.0', '0.94.3': '1.3.1',
                        '0.94.5': '1.3.2', '10.1.0': '2.0'}
    log.info("Remove any epel packages installed on node %s", remote.shortname)
    remote.run(
        args=['sudo', 'yum', 'remove',
              run.Raw("leveldb xmlstarlet fcgi"), '-y'],
        check_status=False
    )
    for pkg in pkgs:
        log.info("Check if %s is already installed on node %s",
                 pkg, remote.shortname)
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
            log.info("Removing and reinstalling %s on %s",
                     pkg, remote.shortname)
            remote.run(args=['sudo', 'yum', 'remove', pkg, '-y'])
            remote.run(args=['sudo', 'yum', 'install', pkg, '-y'])

    log.info("Check if ceph is already installed on %s", remote.shortname)
    r = remote.run(
        args=['yum', 'list', 'installed', 'ceph'],
        stdout=StringIO(),
        check_status=False,
    )
    host = r.hostname
    if r.stdout.getvalue().find('ceph') == -1:
        log.info("Install ceph using ceph-deploy on %s", remote.shortname)
        remote.run(args=[
            'sudo', 'ceph-deploy', 'install',
            run.Raw('--no-adjust-repos'), host]
        )
        remote.run(args=['sudo', 'yum', 'install', 'ceph-test', '-y'])
    else:
        log.info("Removing and reinstalling Ceph on %s", remote.shortname)
        remote.run(args=['sudo', 'ceph-deploy', 'uninstall', host])
        remote.run(args=['sudo', 'ceph-deploy', 'purgedata', host])
        remote.run(args=['sudo', 'ceph-deploy', 'install', host])
        remote.run(args=['sudo', 'yum', 'remove', 'ceph-test', '-y'])
        remote.run(args=['sudo', 'yum', 'install', 'ceph-test', '-y'])

    # check package version
    version = packaging.get_package_version(remote, 'ceph-common')
    log.info(
        "Node: {n} Ceph version installed is {v}".format(
            n=remote.shortname, v=version)
    )
    if rh_version_check[version] == installed_version:
        log.info("Installed version matches on %s", remote.shortname)
    else:
        raise RuntimeError("Version check failed on node %s", remote.shortname)


def uninstall_pkgs(ctx, remote):
    """
    Removes Ceph from all RH hosts

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    """
    log.info(
        "uninstalling packages using ceph-deploy on node %s",
        remote.shortname
    )
    r = remote.run(args=['date'], check_status=False)
    host = r.hostname
    remote.run(args=['sudo', 'ceph-deploy', 'uninstall', host])
    time.sleep(4)
    remote.run(args=['sudo', 'ceph-deploy', 'purgedata', host])
    log.info("Uninstalling ceph-deploy")
    remote.run(
        args=['sudo', 'yum', 'remove', 'ceph-deploy', '-y'],
        check_status=False
    )
    remote.run(
        args=['sudo', 'yum', 'remove', 'ceph-test', '-y'],
        check_status=False
    )
