import logging

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
    rpm = _package_overrides(rpm, remote.os)
    log.info("Removing packages: {pkglist} on rpm system.".format(
        pkglist=", ".join(rpm)))
    builder = _get_builder_project(ctx, remote, config)
    dist_release = builder.dist_release

    if dist_release == 'opensuse':
        pkg_mng_cmd = 'zypper'
        pkg_mng_opts = '-n'
        pkg_mng_subcommand_opts = '--capability'
    else:
        pkg_mng_cmd = 'yum'
        pkg_mng_opts = '-y'
        pkg_mng_subcommand_opts = ''

    remote.run(
        args=[
            'for', 'd', 'in',
        ] + rpm + [
            run.Raw(';'),
            'do',
            'sudo',
            pkg_mng_cmd, pkg_mng_opts, 'remove', pkg_mng_subcommand_opts,
            run.Raw('$d'), run.Raw('||'), 'true', run.Raw(';'),
            'done',
        ])
    if dist_release == 'opensuse':
        pkg_mng_opts = '-a'
    else:
        pkg_mng_opts = 'all'
    remote.run(
        args=[
            'sudo', pkg_mng_cmd, 'clean', pkg_mng_opts,
        ])

    builder.remove_repo()

    if dist_release != 'opensuse':
        pkg_mng_opts = 'expire-cache'
    remote.run(
        args=[
            'sudo', pkg_mng_cmd, 'clean', pkg_mng_opts,
        ])


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
                pkg = pkg.replace('3', '34', count=1)
        result.append(pkg)
    return result


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
    rpm = _package_overrides(rpm, remote.os)
    builder = _get_builder_project(ctx, remote, config)
    log.info('Pulling from %s', builder.base_url)
    log.info('Package version is %s', builder.version)
    log.info("Installing packages: {pkglist} on remote rpm {arch}".format(
        pkglist=", ".join(rpm), arch=builder.arch))
    builder.install_repo()

    dist_release = builder.dist_release
    project = builder.project
    if dist_release != 'opensuse':
        uri = builder.uri_reference
        _yum_fix_repo_priority(remote, project, uri)
        _yum_fix_repo_host(remote, project)
        _yum_set_check_obsoletes(remote)

    if dist_release == 'opensuse':
        remote.run(
            args=[
                'sudo', 'zypper', 'clean', '-a',
            ])
    else:
        remote.run(
            args=[
                'sudo', 'yum', 'clean', 'all',
            ])

    ldir = _get_local_dir(config, remote)

    if dist_release == 'opensuse':
        pkg_mng_cmd = 'zypper'
        pkg_mng_opts = '-n'
        pkg_mng_subcommand_opts = '--capability'
    else:
        pkg_mng_cmd = 'yum'
        pkg_mng_opts = '-y'
        pkg_mng_subcommand_opts = ''

    for cpack in rpm:
        pkg = None
        if ldir:
            pkg = "{ldir}/{cpack}".format(
                ldir=ldir,
                cpack=cpack,
            )
            remote.run(
                args=['if', 'test', '-e',
                      run.Raw(pkg), run.Raw(';'), 'then',
                      'sudo', pkg_mng_cmd, pkg_mng_opts, 'remove',
                      pkg_mng_subcommand_opts, pkg, run.Raw(';'),
                      'sudo', pkg_mng_cmd, pkg_mng_opts, 'install',
                      pkg_mng_subcommand_opts, pkg, run.Raw(';'),
                      'fi']
            )
        if pkg is None:
            remote.run(args=[
                'sudo', pkg_mng_cmd, pkg_mng_opts, 'install',
                pkg_mng_subcommand_opts, cpack
            ])
        else:
            remote.run(
                args=['if', 'test', run.Raw('!'), '-e',
                      run.Raw(pkg), run.Raw(';'), 'then',
                      'sudo', pkg_mng_cmd, pkg_mng_opts, 'install',
                      pkg_mng_subcommand_opts, cpack, run.Raw(';'),
                      'fi'])


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
    if remote.os.name != 'opensuse':
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

    if builder.dist_release != 'opensuse':
        uri = builder.uri_reference
        _yum_fix_repo_priority(remote, project, uri)
        _yum_fix_repo_host(remote, project)
        _yum_set_check_obsoletes(remote)

    if builder.dist_release == 'opensuse':
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
    if builder.dist_release == 'opensuse':
        pkg_mng_opts = '-n'
        pkg_mng_subcommand_opts = '--capability'
    else:
        pkg_mng_opts = '-y'
        pkg_mng_subcommand_opts = ''
    args = ['sudo', pkg_mng_cmd, pkg_mng_opts,
            'install', pkg_mng_subcommand_opts]
    args += pkgs
    remote.run(args=args)
