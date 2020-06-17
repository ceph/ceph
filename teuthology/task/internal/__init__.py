"""
Internal tasks are tasks that are started from the teuthology infrastructure.
Note that there is no corresponding task defined for this module.  All of
the calls are made from other modules, most notably teuthology/run.py
"""
import contextlib
import logging
import os
import time
import yaml
import subprocess

import teuthology.lock.ops
from teuthology import misc
from teuthology.packaging import get_builder_project
from teuthology import report
from teuthology.config import config as teuth_config
from teuthology.exceptions import VersionNotFoundError
from teuthology.job_status import get_status, set_status
from teuthology.orchestra import cluster, remote, run
# the below import with noqa is to workaround run.py which does not support multilevel submodule import
from teuthology.task.internal.redhat import setup_cdn_repo, setup_base_repo, setup_additional_repo, setup_stage_cdn  # noqa

log = logging.getLogger(__name__)


@contextlib.contextmanager
def base(ctx, config):
    """
    Create the test directory that we will be using on the remote system
    """
    log.info('Creating test directory...')
    testdir = misc.get_testdir(ctx)
    run.wait(
        ctx.cluster.run(
            args=['mkdir', '-p', '-m0755', '--', testdir],
            wait=False,
        )
    )
    try:
        yield
    finally:
        log.info('Tidying up after the test...')
        # if this fails, one of the earlier cleanups is flawed; don't
        # just cram an rm -rf here
        run.wait(
            ctx.cluster.run(
                args=['find', testdir, '-ls',
                      run.Raw(';'),
                      'rmdir', '--', testdir],
                wait=False,
            ),
        )


def save_config(ctx, config):
    """
    Store the config in a yaml file
    """
    log.info('Saving configuration')
    if ctx.archive is not None:
        with open(os.path.join(ctx.archive, 'config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)


def check_packages(ctx, config):
    """
    Checks gitbuilder to determine if there are missing packages for this job.

    If there are missing packages, fail the job.
    """
    for task in ctx.config['tasks']:
        if list(task.keys())[0] == 'buildpackages':
            log.info("Checking packages skipped because "
                     "the task buildpackages was found.")
            return

    log.info("Checking packages...")
    os_type = ctx.config.get("os_type")
    sha1 = ctx.config.get("sha1")
    # We can only do this check if there are a defined sha1 and os_type
    # in the job config.
    if os_type and sha1:
        package = get_builder_project()("ceph", ctx.config)
        template = "Checking packages for os_type '{os}', " \
            "flavor '{flav}' and ceph hash '{ver}'"
        log.info(
            template.format(
                os=package.os_type,
                flav=package.flavor,
                ver=package.sha1,
            )
        )
        if package.version:
            log.info("Found packages for ceph version {ver}".format(
                ver=package.version
            ))
        else:
            msg = "Packages for distro '{d}' and ceph hash '{ver}' not found"
            msg = msg.format(
                d=package.distro,
                ver=package.sha1,
            )
            log.error(msg)
            # set the failure message and update paddles with the status
            ctx.summary["failure_reason"] = msg
            set_status(ctx.summary, "dead")
            report.try_push_job_info(ctx.config, dict(status='dead'))
            raise VersionNotFoundError(package.base_url)
    else:
        log.info(
            "Checking packages skipped, missing os_type '{os}' or ceph hash '{ver}'".format(
                os=os_type,
                ver=sha1,
            )
        )


@contextlib.contextmanager
def timer(ctx, config):
    """
    Start the timer used by teuthology
    """
    log.info('Starting timer...')
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        log.info('Duration was %f seconds', duration)
        ctx.summary['duration'] = duration


def add_remotes(ctx, config):
    """
    Create a ctx.cluster object populated with remotes mapped to roles
    """
    ctx.cluster = cluster.Cluster()
    # Allow jobs to run without using nodes, for self-testing
    if 'roles' not in ctx.config and 'targets' not in ctx.config:
        return
    remotes = []
    machs = []
    for name in ctx.config['targets'].keys():
        machs.append(name)
    for t, key in ctx.config['targets'].items():
        t = misc.canonicalize_hostname(t)
        try:
            if ctx.config['sshkeys'] == 'ignore':
                key = None
        except (AttributeError, KeyError):
            pass
        rem = remote.Remote(name=t, host_key=key, keep_alive=True)
        remotes.append(rem)
    if 'roles' in ctx.config:
        for rem, roles in zip(remotes, ctx.config['roles']):
            assert all(isinstance(role, str) for role in roles), \
                "Roles in config must be strings: %r" % roles
            ctx.cluster.add(rem, roles)
            log.info('roles: %s - %s' % (rem, roles))
    else:
        for rem in remotes:
            ctx.cluster.add(rem, rem.name)


def connect(ctx, config):
    """
    Connect to all remotes in ctx.cluster
    """
    log.info('Opening connections...')
    for rem in ctx.cluster.remotes.keys():
        log.debug('connecting to %s', rem.name)
        rem.connect()


def push_inventory(ctx, config):
    if not teuth_config.lock_server:
        return

    def push():
        for rem in ctx.cluster.remotes.keys():
            info = rem.inventory_info
            teuthology.lock.ops.update_inventory(info)
    try:
        push()
    except Exception:
        log.exception("Error pushing inventory")

BUILDPACKAGES_FIRST = 0
BUILDPACKAGES_OK = 1
BUILDPACKAGES_REMOVED = 2
BUILDPACKAGES_NOTHING = 3

def buildpackages_prep(ctx, config):
    """
    Make sure the 'buildpackages' task happens before
    the 'install' task.

    Return:

    BUILDPACKAGES_NOTHING if there is no buildpackages task
    BUILDPACKAGES_REMOVED if there is a buildpackages task but no install task
    BUILDPACKAGES_FIRST if a buildpackages task was moved at the beginning
    BUILDPACKAGES_OK if a buildpackages task already at the beginning
    """
    index = 0
    install_index = None
    buildpackages_index = None
    buildpackages_prep_index = None
    for task in ctx.config['tasks']:
        t = list(task)[0]
        if t == 'install':
            install_index = index
        if t == 'buildpackages':
            buildpackages_index = index
        if t == 'internal.buildpackages_prep':
            buildpackages_prep_index = index
        index += 1
    if (buildpackages_index is not None and
        install_index is not None):
        if buildpackages_index > buildpackages_prep_index + 1:
            log.info('buildpackages moved to be the first task')
            buildpackages = ctx.config['tasks'].pop(buildpackages_index)
            ctx.config['tasks'].insert(buildpackages_prep_index + 1,
                                       buildpackages)
            return BUILDPACKAGES_FIRST
        else:
            log.info('buildpackages is already the first task')
            return BUILDPACKAGES_OK
    elif buildpackages_index is not None and install_index is None:
        ctx.config['tasks'].pop(buildpackages_index)
        all_tasks = [list(x.keys())[0] for x in ctx.config['tasks']]
        log.info('buildpackages removed because no install task found in ' +
                 str(all_tasks))
        return BUILDPACKAGES_REMOVED
    elif buildpackages_index is None:
        log.info('no buildpackages task found')
        return BUILDPACKAGES_NOTHING


def serialize_remote_roles(ctx, config):
    """
    Provides an explicit mapping for which remotes have been assigned what roles
    So that other software can be loosely coupled to teuthology
    """
    if ctx.archive is not None:
        with open(os.path.join(ctx.archive, 'info.yaml'), 'r+') as info_file:
            info_yaml = yaml.safe_load(info_file)
            info_file.seek(0)
            info_yaml['cluster'] = dict([(rem.name, {'roles': roles}) for rem, roles in ctx.cluster.remotes.items()])
            yaml.safe_dump(info_yaml, info_file, default_flow_style=False)


def check_ceph_data(ctx, config):
    """
    Check for old /var/lib/ceph subdirectories and detect staleness.
    """
    log.info('Checking for non-empty /var/lib/ceph...')
    processes = ctx.cluster.run(
        args='test -z $(ls -A /var/lib/ceph)',
        wait=False,
    )
    failed = False
    for proc in processes:
        try:
            proc.wait()
        except run.CommandFailedError:
            log.error('Host %s has stale /var/lib/ceph, check lock and nuke/cleanup.', proc.remote.shortname)
            failed = True
    if failed:
        raise RuntimeError('Stale /var/lib/ceph detected, aborting.')


def check_conflict(ctx, config):
    """
    Note directory use conflicts and stale directories.
    """
    log.info('Checking for old test directory...')
    testdir = misc.get_testdir(ctx)
    processes = ctx.cluster.run(
        args=['test', '!', '-e', testdir],
        wait=False,
    )
    failed = False
    for proc in processes:
        try:
            proc.wait()
        except run.CommandFailedError:
            log.error('Host %s has stale test directory %s, check lock and cleanup.', proc.remote.shortname, testdir)
            failed = True
    if failed:
        raise RuntimeError('Stale jobs detected, aborting.')


def fetch_binaries_for_coredumps(path, remote):
    """
    Pul ELFs (debug and stripped) for each coredump found
    """
    # Check for Coredumps:
    coredump_path = os.path.join(path, 'coredump')
    if os.path.isdir(coredump_path):
        log.info('Transferring binaries for coredumps...')
        for dump in os.listdir(coredump_path):
            # Pull program from core file
            dump_path = os.path.join(coredump_path, dump)
            dump_info = subprocess.Popen(['file', dump_path],
                                         stdout=subprocess.PIPE)
            dump_out = dump_info.communicate()[0].decode()

            # Parse file output to get program, Example output:
            # 1422917770.7450.core: ELF 64-bit LSB core file x86-64, version 1 (SYSV), SVR4-style, \
            # from 'radosgw --rgw-socket-path /home/ubuntu/cephtest/apache/tmp.client.0/fastcgi_soc'
            dump_program = dump_out.split("from '")[1].split(' ')[0]

            # Find path on remote server:
            remote_path = remote.sh(['which', dump_program]).rstrip()

            # Pull remote program into coredump folder:
            local_path = os.path.join(coredump_path,
                                      dump_program.lstrip(os.path.sep))
            local_dir = os.path.dirname(local_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            remote._sftp_get_file(remote_path, local_path)

            # Pull Debug symbols:
            debug_path = os.path.join('/usr/lib/debug', remote_path)

            # RPM distro's append their non-stripped ELF's with .debug
            # When deb based distro's do not.
            if remote.system_type == 'rpm':
                debug_path = '{debug_path}.debug'.format(debug_path=debug_path)

            remote.get_file(debug_path, coredump_path)


@contextlib.contextmanager
def archive(ctx, config):
    """
    Handle the creation and deletion of the archive directory.
    """
    log.info('Creating archive directory...')
    archive_dir = misc.get_archive_dir(ctx)
    run.wait(
        ctx.cluster.run(
            args=['install', '-d', '-m0755', '--', archive_dir],
            wait=False,
        )
    )

    try:
        yield
    except Exception:
        # we need to know this below
        set_status(ctx.summary, 'fail')
        raise
    finally:
        passed = get_status(ctx.summary) == 'pass'
        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and passed):
            log.info('Transferring archived files...')
            logdir = os.path.join(ctx.archive, 'remote')
            if (not os.path.exists(logdir)):
                os.mkdir(logdir)
            for rem in ctx.cluster.remotes.keys():
                path = os.path.join(logdir, rem.shortname)
                misc.pull_directory(rem, archive_dir, path)
                # Check for coredumps and pull binaries
                fetch_binaries_for_coredumps(path, rem)

        log.info('Removing archive directory...')
        run.wait(
            ctx.cluster.run(
                args=['rm', '-rf', '--', archive_dir],
                wait=False,
            ),
        )


@contextlib.contextmanager
def sudo(ctx, config):
    """
    Enable use of sudo
    """
    log.info('Configuring sudo...')
    sudoers_file = '/etc/sudoers'
    backup_ext = '.orig.teuthology'
    tty_expr = r's/^\([^#]*\) \(requiretty\)/\1 !\2/g'
    pw_expr = r's/^\([^#]*\) !\(visiblepw\)/\1 \2/g'

    run.wait(
        ctx.cluster.run(
            args="sudo sed -i{ext} -e '{tty}' -e '{pw}' {path}".format(
                ext=backup_ext, tty=tty_expr, pw=pw_expr,
                path=sudoers_file
            ),
            wait=False,
        )
    )
    try:
        yield
    finally:
        log.info('Restoring {0}...'.format(sudoers_file))
        ctx.cluster.run(
            args="sudo mv -f {path}{ext} {path}".format(
                path=sudoers_file, ext=backup_ext
            )
        )


@contextlib.contextmanager
def coredump(ctx, config):
    """
    Stash a coredump of this system if an error occurs.
    """
    log.info('Enabling coredump saving...')
    archive_dir = misc.get_archive_dir(ctx)
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '{adir}/coredump'.format(adir=archive_dir),
                run.Raw('&&'),
                'sudo', 'sysctl', '-w', 'kernel.core_pattern={adir}/coredump/%t.%p.core'.format(adir=archive_dir),
		run.Raw('&&'),
		'echo',
		'kernel.core_pattern={adir}/coredump/%t.%p.core'.format(adir=archive_dir),
		run.Raw('|'),
		'sudo', 'tee', '-a', '/etc/sysctl.conf',
            ],
            wait=False,
        )
    )

    try:
        yield
    finally:
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo', 'sysctl', '-w', 'kernel.core_pattern=core',
                    run.Raw('&&'),
                    # don't litter the archive dir if there were no cores dumped
                    'rmdir',
                    '--ignore-fail-on-non-empty',
                    '--',
                    '{adir}/coredump'.format(adir=archive_dir),
                ],
                wait=False,
            )
        )

        # set status = 'fail' if the dir is still there = coredumps were
        # seen
        for rem in ctx.cluster.remotes.keys():
            try:
                rem.sh("test -e " + archive_dir + "/coredump")
            except run.CommandFailedError:
                continue
            log.warning('Found coredumps on %s, flagging run as failed', rem)
            set_status(ctx.summary, 'fail')
            if 'failure_reason' not in ctx.summary:
                ctx.summary['failure_reason'] = \
                    'Found coredumps on {rem}'.format(rem=rem)


@contextlib.contextmanager
def archive_upload(ctx, config):
    """
    Upload the archive directory to a designated location
    """
    try:
        yield
    finally:
        upload = ctx.config.get('archive_upload')
        archive_path = ctx.config.get('archive_path')
        if upload and archive_path:
            log.info('Uploading archives ...')
            upload_key = ctx.config.get('archive_upload_key')
            if upload_key:
                ssh = "RSYNC_RSH='ssh -i " + upload_key + "'"
            else:
                ssh = ''
            split_path = archive_path.split('/')
            split_path.insert(-2, '.')
            misc.sh(ssh + " rsync -avz --relative /" +
                    os.path.join(*split_path) + " " +
                    upload)
        else:
            log.info('Not uploading archives.')
