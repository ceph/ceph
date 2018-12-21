import logging
import os.path

from distutils.version import LooseVersion

from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology import packaging

from .util import _get_builder_project, _get_local_dir

log = logging.getLogger(__name__)


def _remove(ctx, config, remote, rpm):
    """
    Removes RPM packages from remote

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param rpm: list of packages names to remove
    """
    remote_os = remote.os
    dist_release = remote_os.name
    rpm = _package_overrides(rpm, remote_os)
    log.info("Removing packages: {pkglist} on rpm system.".format(
        pkglist=", ".join(rpm)))
    repos = config.get('repos')

    if dist_release in ['opensuse', 'sle']:
        remote.run(args='''
            for d in {rpms} ; do
                sudo zypper -n --no-gpg-checks remove --capability $d || true
            done'''.format(rpms=' '.join(rpm)))
        remote.run(args='sudo zypper clean -a')
    else:
        remote.run(args='''
            for d in {rpms} ; do
                sudo yum -y remove $d || true
            done'''.format(rpms=' '.join(rpm)))
        remote.run(args='sudo yum clean all')

    if repos:
        if dist_release in ['opensuse', 'sle']:
            _zypper_removerepo(remote, repos)
        else:
            raise Exception('Custom repos were specified for %s ' % remote_os +
                            'but these are currently not supported')
    else:
        builder = _get_builder_project(ctx, remote, config)
        builder.remove_repo()

    if dist_release in ['opensuse', 'sle']:
        remote.run(args='sudo zypper clean -a')
    else:
        remote.run(args='sudo yum clean expire-cache')


def _package_overrides(pkgs, os):
    """
    Replaces some package names with their distro-specific equivalents
    (currently "python3-*" -> "python34-*" for CentOS)

    :param pkgs: list of RPM package names
    :param os: the teuthology.orchestra.opsys.OS object
    """
    is_rhel = os.name in ['centos', 'rhel']
    result = []
    for pkg in pkgs:
        if is_rhel:
            if pkg.startswith('python3-') or pkg == 'python3':
                pkg = pkg.replace('3', '34', 1)
        result.append(pkg)
    return result

def _zypper_addrepo(remote, repo_list):
    """
    Add zypper repos to the remote system.

    :param remote: remote node where to add packages
    :param repo_list: list of dictionaries with keys 'name', 'url'
    :return:
    """
    for repo in repo_list:
        remote.run(args=[
            'sudo', 'zypper', '-n', 'addrepo', '--refresh', '--no-gpgcheck',
            '-p', '1', repo['url'], repo['name'],
        ])

def _zypper_removerepo(remote, repo_list):
    """
    Remove zypper repos on the remote system.

    :param remote: remote node where to remove packages from
    :param repo_list: list of dictionaries with keys 'name', 'url'
    :return:
    """
    for repo in repo_list:
        remote.run(args=[
            'sudo', 'zypper', '-n', 'removerepo', repo['name'],
        ])


def _downgrade_packages(ctx, remote, pkgs, pkg_version, config):
    """
    Downgrade packages listed by 'downgrade_packages'

    Downgrade specified packages to given version. The list of packages
    downgrade is provided by 'downgrade_packages' as a property of "install"
    task.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param pkgs: list of package names to install
    :param pkg_version: the version to which all packages will be downgraded
    :param config: the config dict
    :return: list of package names from 'pkgs' which are not yet
             installed/downgraded
    """
    downgrade_pkgs = config.get('downgrade_packages', [])
    if not downgrade_pkgs:
        return pkgs
    log.info('Downgrading packages: {pkglist}'.format(
        pkglist=', '.join(downgrade_pkgs)))
    # assuming we are going to downgrade packages with the same version
    first_pkg = downgrade_pkgs[0]
    installed_version = packaging.get_package_version(remote, first_pkg)
    assert installed_version, "failed to get version of {}".format(first_pkg)
    assert LooseVersion(installed_version) < LooseVersion(pkg_version)
    # to compose package name like "librados2-0.94.10-87.g116a558.el7"
    pkgs_opt = ['-'.join([pkg, pkg_version]) for pkg in downgrade_pkgs]
    remote.run(args='sudo yum -y downgrade {}'.format(' '.join(pkgs_opt)))
    return [pkg for pkg in pkgs if pkg not in downgrade_pkgs]


def _update_package_list_and_install(ctx, remote, rpm, config):
    """
    Installs the repository for the relevant branch, then installs
    the requested packages on the remote system.

    TODO: split this into at least two functions.

    :param ctx: the argparse.Namespace object
    :param remote: the teuthology.orchestra.remote.Remote object
    :param rpm: list of packages names to install
    :param config: the config dict
    """
    # rpm does not force installation of a particular version of the project
    # packages, so we can put extra_system_packages together with the rest
    system_pkglist = config.get('extra_system_packages')
    if system_pkglist:
        if isinstance(system_pkglist, dict):
            rpm += system_pkglist.get('rpm')
        else:
            rpm += system_pkglist
    remote_os = remote.os
    rpm = _package_overrides(rpm, remote_os)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
        pkglist=", ".join(rpm), arch=remote.arch))

    dist_release = remote_os.name
    repos = config.get('repos')
    if repos:
        log.debug("Adding repos: %s" % repos)
        if dist_release in ['opensuse', 'sle']:
            _zypper_addrepo(remote, repos)
        else:
            raise Exception('Custom repos were specified for %s ' % remote_os +
                            'but these are currently not supported')
    else:
        builder = _get_builder_project(ctx, remote, config)
        log.info('Pulling from %s', builder.base_url)
        log.info('Package version is %s', builder.version)
        builder.install_repo()

    if dist_release not in ['opensuse', 'sle']:
        project = builder.project
        uri = builder.uri_reference
        _yum_fix_repo_priority(remote, project, uri)
        _yum_fix_repo_host(remote, project)
        _yum_set_check_obsoletes(remote)

    if dist_release in ['opensuse', 'sle']:
        remote.run(args='sudo zypper clean -a')
    else:
        remote.run(args='sudo yum clean all')

    ldir = _get_local_dir(config, remote)

    if dist_release in ['opensuse', 'sle']:
        remove_cmd = 'sudo zypper -n remove --capability'
        install_cmd = 'sudo zypper -n --no-gpg-checks install --capability --no-recommends'
    else:
        remove_cmd = 'sudo yum -y remove'
        install_cmd = 'sudo yum -y install'
        # to compose version string like "0.94.10-87.g116a558.el7"
        pkg_version = '.'.join([builder.version, builder.dist_release])
        rpm = _downgrade_packages(ctx, remote, rpm, pkg_version, config)

    for cpack in rpm:
        if ldir:
            remote.run(args='''
              if test -e {pkg} ; then
                {remove_cmd} {pkg} ;
                {install_cmd} {pkg} ;
              else
                {install_cmd} {cpack} ;
              fi
            '''.format(remove_cmd=remove_cmd,
                       install_cmd=install_cmd,
                       pkg=os.path.join(ldir, cpack),
                       cpack=cpack))
        else:
            remote.run(args='''
              {install_cmd} {cpack}
            '''.format(install_cmd=install_cmd,
                       cpack=cpack))


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
    # Skip this bit if we're not using gitbuilder
    if not isinstance(packaging.get_builder_project(),
                      packaging.GitbuilderProject):
        return
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


def _remove_sources_list(ctx, config, remote):
    """
    Removes /etc/yum.repos.d/{proj}.repo, /var/lib/{proj}, and /var/log/{proj}

    :param remote: the teuthology.orchestra.remote.Remote object
    :param proj: the project whose .repo needs removing
    """
    builder = _get_builder_project(ctx, remote, config)
    builder.remove_repo()
    proj = builder.project
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
    if remote.os.name not in ['opensuse', 'sle']:
        _yum_unset_check_obsoletes(remote)


def _upgrade_packages(ctx, config, remote, pkgs):
    """
    Upgrade project's packages on remote RPM-based host
    Before doing so, it makes sure the project's repository is installed -
    removing any previous version first.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    :param remote: the teuthology.orchestra.remote.Remote object
    :param pkgs: the RPM packages to be installed
    :param branch: the branch of the project to be used
    """
    builder = _get_builder_project(ctx, remote, config)
    log.info(
        "Host {host} is: {distro} {ver} {arch}".format(
            host=remote.shortname,
            distro=builder.os_type,
            ver=builder.os_version,
            arch=builder.arch,)
    )

    base_url = builder.base_url
    log.info('Repo base URL: %s', base_url)
    project = builder.project

    # Remove the repository before re-adding it
    builder.remove_repo()
    builder.install_repo()

    if builder.dist_release not in ['opensuse', 'sle']:
        uri = builder.uri_reference
        _yum_fix_repo_priority(remote, project, uri)
        _yum_fix_repo_host(remote, project)
        _yum_set_check_obsoletes(remote)

    if builder.dist_release in ['opensuse', 'sle']:
        pkg_mng_cmd = 'zypper'
        pkg_mng_opts = '-a'
    else:
        pkg_mng_cmd = 'yum'
        pkg_mng_opts = 'all'

    remote.run(
        args=[
            'sudo', pkg_mng_cmd, 'clean', pkg_mng_opts,
        ])

    # Actually upgrade the project packages
    if builder.dist_release in ['opensuse', 'sle']:
        pkg_mng_opts = '-n'
        pkg_mng_subcommand_opts = '--capability'
        pkg_mng_install_opts = '--no-recommends'
    else:
        pkg_mng_opts = '-y'
        pkg_mng_subcommand_opts = ''
        pkg_mng_install_opts = ''
    args = ['sudo', pkg_mng_cmd, pkg_mng_opts,
            'install', pkg_mng_subcommand_opts,
            pkg_mng_install_opts]
    args += pkgs
    remote.run(args=args)
