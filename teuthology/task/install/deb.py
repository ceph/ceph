import logging
import os

from io import StringIO

from teuthology.orchestra import run
from teuthology.contextutil import safe_while

from teuthology.task.install.util import _get_builder_project, _get_local_dir


log = logging.getLogger(__name__)

def _retry_if_eagain_in_output(remote, args):
    # wait at most 5 minutes
    with safe_while(sleep=10, tries=30) as proceed:
        while proceed():
            stderr = StringIO()
            try:
                return remote.run(args=args, stderr=stderr)
            except run.CommandFailedError:
                if "could not get lock" in stderr.getvalue().lower():
                    stdout = StringIO()
                    args = ['sudo', 'fuser', '-v', '/var/lib/dpkg/lock-frontend']
                    remote.run(args=args, stdout=stdout)
                    log.info("The processes holding 'lock-frontend':\n{}".format(stdout.getvalue()))
                    continue
                else:
                    raise

def install_dep_packages(remote, args):
    _retry_if_eagain_in_output(remote, args)

def _update_package_list_and_install(ctx, remote, debs, config):
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
                'http://git.ceph.com/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',  # noqa
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    builder = _get_builder_project(ctx, remote, config)
    log.info("Installing packages: {pkglist} on remote deb {arch}".format(
        pkglist=", ".join(debs), arch=builder.arch)
    )
    system_pkglist = config.get('extra_system_packages')
    if system_pkglist:
        if isinstance(system_pkglist, dict):
            system_pkglist = system_pkglist.get('deb')
        log.info("Installing system (non-project) packages: {pkglist} on remote deb {arch}".format(
            pkglist=", ".join(system_pkglist), arch=builder.arch)
        )
    # get baseurl
    log.info('Pulling from %s', builder.base_url)

    version = builder.version
    log.info('Package version is %s', version)

    builder.install_repo()

    remote.run(args=['sudo', 'apt-get', 'update'], check_status=False)
    install_cmd = [
            'sudo', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y',
            '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ]
    install_dep_packages(remote,
        args=install_cmd + ['%s=%s' % (d, version) for d in debs],
    )
    if system_pkglist:
        install_dep_packages(remote,
            args=install_cmd + system_pkglist,
        )
    ldir = _get_local_dir(config, remote)
    if ldir:
        for fyle in os.listdir(ldir):
            fname = "%s/%s" % (ldir, fyle)
            remote.run(args=['sudo', 'dpkg', '-i', fname],)


def _remove(ctx, config, remote, debs):
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
            'sudo',
            'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
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
            'sudo',
            'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'autoremove',
        ],
    )


def _remove_sources_list(ctx, config, remote):
    builder = _get_builder_project(ctx, remote, config)
    builder.remove_repo()
    remote.run(
        args=[
            'sudo', 'apt-get', 'update',
        ],
        check_status=False,
    )


def _upgrade_packages(ctx, config, remote, debs):
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
                'http://git.ceph.com/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',  # noqa
                run.Raw('|'),
                'sudo', 'apt-key', 'add', '-',
            ],
            stdout=StringIO(),
        )

    builder = _get_builder_project(ctx, remote, config)
    base_url = builder.base_url
    log.info('Pulling from %s', base_url)

    version = builder.version
    log.info('Package version is %s', version)

    builder.install_repo()

    remote.run(args=['sudo', 'apt-get', 'update'], check_status=False)
    install_dep_packages(remote,
        args=[
            'sudo',
            'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-y', '--force-yes',
            '-o', run.Raw('Dpkg::Options::="--force-confdef"'), '-o', run.Raw(
                'Dpkg::Options::="--force-confold"'),
            'install',
        ] + ['%s=%s' % (d, version) for d in debs],
    )
