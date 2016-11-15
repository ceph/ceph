import contextlib
import logging
import yaml
import os

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
                log.info("Install on Ubuntu node: %s", remote.shortname)
                p.spawn(rh_install_deb_pkgs, ctx, remote, version,
                        downstream_config, config['deb-repo'],
                        config['deb-gpg-key'])
    try:
        yield
    finally:
        if config.get('skip_uninstall'):
            log.info("Skipping uninstall of Ceph")
        else:
            with parallel() as p:
                for remote in ctx.cluster.remotes.iterkeys():
                    p.spawn(rh_uninstall_pkgs, ctx, remote, downstream_config)


def rh_install_pkgs(ctx, remote, version, downstream_config):
    """
    Installs RH build using ceph-deploy.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param downstream_config the dict object that has downstream pkg info
    """
    rh_version_check = downstream_config.get('versions').get('rpm').get('mapped')
    rh_rpm_pkgs = downstream_config.get('pkgs').get('rpm')
    pkgs = str.join(' ', rh_rpm_pkgs)
    log.info("Remove any epel packages installed on node %s", remote.shortname)
    remote.run(
        args=[
            'sudo',
            'yum',
            'remove',
            run.Raw("leveldb xmlstarlet fcgi"),
            '-y'],
        check_status=False)
    log.info("Installing redhat ceph packages")
    remote.run(args=['sudo', 'yum', '-y', 'install',
                     run.Raw(pkgs)])
    # check package version
    installed_version = packaging.get_package_version(remote, 'ceph-common')
    log.info(
        "Node: {n} Ceph version installed is {v}".format(
            n=remote.shortname,
            v=version))
    req_ver = rh_version_check[version]
    if installed_version.startswith(req_ver):
        log.info("Installed version matches on %s", remote.shortname)
    else:
        raise RuntimeError("Version check failed on node %s", remote.shortname)


def set_rh_deb_repo(remote, deb_repo, deb_gpg_key=None):
    """
    Sets up debian repo and gpg key for package verification
    :param remote - remote node object
    :param deb_repo - debian repo root path
    :param deb_gpg_key - gpg key for the package
    """
    repos = ['MON', 'OSD', 'Tools']
    log.info("deb repo: %s", deb_repo)
    log.info("gpg key url: %s", deb_gpg_key)
    # remove any additional repo so that upstream packages are not used
    # all required packages come from downstream repo
    remote.run(args=['sudo', 'rm', '-f', run.Raw('/etc/apt/sources.list.d/*')],
               check_status=False)
    for repo in repos:
        cmd = 'echo deb {root}/{repo} $(lsb_release -sc) main'.format(
            root=deb_repo, repo=repo)
        remote.run(args=['sudo', run.Raw(cmd), run.Raw('>'),
                         "/tmp/{0}.list".format(repo)])
        remote.run(args=['sudo', 'cp', "/tmp/{0}.list".format(repo),
                         '/etc/apt/sources.list.d/'])
    # add ds gpgkey
    ds_keys = ['https://www.redhat.com/security/897da07a.txt',
               'https://www.redhat.com/security/f21541eb.txt']
    if deb_gpg_key is not None:
        ds_keys.append(deb_gpg_key)
    for key in ds_keys:
        wget_cmd = 'wget -O - ' + key
        remote.run(args=['sudo', run.Raw(wget_cmd),
                         run.Raw('|'), 'sudo', 'apt-key', 'add', run.Raw('-')])
    remote.run(args=['sudo', 'apt-get', 'update'])


def rh_install_deb_pkgs(
        ctx,
        remote,
        version,
        downstream_config,
        deb_repo,
        deb_gpg_key):
    """
    Setup debian repo, Install gpg key
    and Install on debian packages
    : param ctx
    : param remote
    : param downstream_config the dict object that has downstream pkg info
    : deb_repo - http path of downstream ubuntu repo
    : deb_gpg_key - gpg key for the ubuntu pkg
    """
    set_rh_deb_repo(remote, deb_repo, deb_gpg_key)
    rh_version_check = downstream_config.get('versions').get('deb').get('mapped')
    rh_deb_pkgs = downstream_config.get('pkgs').get('deb')
    pkgs = str.join(' ', rh_deb_pkgs)
    log.info("Installing redhat ceph packages")
    remote.run(args=['sudo', 'apt-get', '-y', 'install',
                     run.Raw(pkgs)])
    # check package version
    installed_version = packaging.get_package_version(remote, 'ceph-common')
    log.info(
        "Node: {n} Ceph version installed is {v}".format(
            n=remote.shortname,
            v=version))
    req_ver = rh_version_check[version]
    if installed_version.startswith(req_ver):
        log.info("Installed version matches on %s", remote.shortname)
    else:
        raise RuntimeError("Version check failed on node %s", remote.shortname)


def rh_uninstall_pkgs(ctx, remote, downstream_config):
    """
    Removes Ceph from all RH hosts

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param downstream_config the dict object that has downstream pkg info
    """
    rh_rpm_pkgs = downstream_config.get('pkgs').get('rpm')
    rpm_pkgs = str.join(' ', rh_rpm_pkgs)

    rh_deb_pkgs = downstream_config.get('pkgs').get('deb')
    deb_pkgs = str.join(' ', rh_deb_pkgs)

    if remote.os.name == 'rhel':
        remote.run(args=['sudo', 'yum', 'remove', run.Raw(rpm_pkgs), '-y'])
    else:
        remote.run(args=['sudo', 'apt-get', 'remove', run.Raw(deb_pkgs), '-y'])
    remote.run(args=['sudo', 'rm', '-rf', '/var/lib/ceph'])
