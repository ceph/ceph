"""
Workunit task -- Run ceph on sets of specific clients
"""
import logging
import pipes
import os
import re

from tasks.util import get_remote_for_role
from tasks.util.workunit import get_refspec_after_overrides

from teuthology import misc
from teuthology.config import config as teuth_config
from teuthology.orchestra.run import CommandFailedError
from teuthology.parallel import parallel
from teuthology.orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run ceph on all workunits found under the specified path.

    For example::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - workunit:
            clients:
              client.0: [direct_io, xattrs.sh]
              client.1: [snaps]
            branch: foo

    You can also run a list of workunits on all clients:
        tasks:
        - ceph:
        - ceph-fuse:
        - workunit:
            tag: v0.47
            clients:
              all: [direct_io, xattrs.sh, snaps]

    If you have an "all" section it will run all the workunits
    on each client simultaneously, AFTER running any workunits specified
    for individual clients. (This prevents unintended simultaneous runs.)

    To customize tests, you can specify environment variables as a dict. You
    can also specify a time limit for each work unit (defaults to 3h):

        tasks:
        - ceph:
        - ceph-fuse:
        - workunit:
            sha1: 9b28948635b17165d17c1cf83d4a870bd138ddf6
            clients:
              all: [snaps]
            env:
              FOO: bar
              BAZ: quux
            timeout: 3h

    This task supports roles that include a ceph cluster, e.g.::

        tasks:
        - ceph:
        - workunit:
            clients:
              backup.client.0: [foo]
              client.1: [bar] # cluster is implicitly 'ceph'

    You can also specify an alternative top-level dir to 'qa/workunits', like
    'qa/standalone', with::

        tasks:
        - install:
        - workunit:
            basedir: qa/standalone
            clients:
              client.0:
                - test-ceph-helpers.sh

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict)
    assert isinstance(config.get('clients'), dict), \
        'configuration must contain a dictionary of clients'

    overrides = ctx.config.get('overrides', {})
    refspec = get_refspec_after_overrides(config, overrides)
    timeout = config.get('timeout', '3h')
    cleanup = config.get('cleanup', True)

    log.info('Pulling workunits from ref %s', refspec)

    created_mountpoint = {}

    if config.get('env') is not None:
        assert isinstance(config['env'], dict), 'env must be a dictionary'
    clients = config['clients']

    # Create scratch dirs for any non-all workunits
    log.info('Making a separate scratch dir for every client...')
    for role in clients.keys():
        assert isinstance(role, str)
        if role == "all":
            continue

        assert 'client' in role
        created_mnt_dir = _make_scratch_dir(ctx, role, config.get('subdir'))
        created_mountpoint[role] = created_mnt_dir

    # Execute any non-all workunits
    log.info("timeout={}".format(timeout))
    log.info("cleanup={}".format(cleanup))
    with parallel() as p:
        for role, tests in clients.items():
            if role != "all":
                p.spawn(_run_tests, ctx, refspec, role, tests,
                        config.get('env'),
                        basedir=config.get('basedir','qa/workunits'),
                        timeout=timeout,
                        cleanup=cleanup,
                        coverage_and_limits=not config.get('no_coverage_and_limits', None))

    if cleanup:
        # Clean up dirs from any non-all workunits
        for role, created in created_mountpoint.items():
            _delete_dir(ctx, role, created)

    # Execute any 'all' workunits
    if 'all' in clients:
        all_tasks = clients["all"]
        _spawn_on_all_clients(ctx, refspec, all_tasks, config.get('env'),
                              config.get('basedir', 'qa/workunits'),
                              config.get('subdir'), timeout=timeout,
                              cleanup=cleanup)


def _client_mountpoint(ctx, cluster, id_):
    """
    Returns the path to the expected mountpoint for workunits running
    on some kind of filesystem.
    """
    # for compatibility with tasks like ceph-fuse that aren't cluster-aware yet,
    # only include the cluster name in the dir if the cluster is not 'ceph'
    if cluster == 'ceph':
        dir_ = 'mnt.{0}'.format(id_)
    else:
        dir_ = 'mnt.{0}.{1}'.format(cluster, id_)
    return os.path.join(misc.get_testdir(ctx), dir_)


def _delete_dir(ctx, role, created_mountpoint):
    """
    Delete file used by this role, and delete the directory that this
    role appeared in.

    :param ctx: Context
    :param role: "role.#" where # is used for the role id.
    """
    cluster, _, id_ = misc.split_role(role)
    remote = get_remote_for_role(ctx, role)
    mnt = _client_mountpoint(ctx, cluster, id_)
    client = os.path.join(mnt, 'client.{id}'.format(id=id_))

    # Remove the directory inside the mount where the workunit ran
    remote.run(
        args=[
            'sudo',
            'rm',
            '-rf',
            '--',
            client,
        ],
    )
    log.info("Deleted dir {dir}".format(dir=client))

    # If the mount was an artificially created dir, delete that too
    if created_mountpoint:
        remote.run(
            args=[
                'rmdir',
                '--',
                mnt,
            ],
        )
        log.info("Deleted artificial mount point {dir}".format(dir=client))


def _make_scratch_dir(ctx, role, subdir):
    """
    Make scratch directories for this role.  This also makes the mount
    point if that directory does not exist.

    :param ctx: Context
    :param role: "role.#" where # is used for the role id.
    :param subdir: use this subdir (False if not used)
    """
    created_mountpoint = False
    cluster, _, id_ = misc.split_role(role)
    remote = get_remote_for_role(ctx, role)
    dir_owner = remote.user
    mnt = _client_mountpoint(ctx, cluster, id_)
    # if neither kclient nor ceph-fuse are required for a workunit,
    # mnt may not exist. Stat and create the directory if it doesn't.
    try:
        remote.run(
            args=[
                'stat',
                '--',
                mnt,
            ],
        )
        log.info('Did not need to create dir {dir}'.format(dir=mnt))
    except CommandFailedError:
        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
            ],
        )
        log.info('Created dir {dir}'.format(dir=mnt))
        created_mountpoint = True

    if not subdir:
        subdir = 'client.{id}'.format(id=id_)

    if created_mountpoint:
        remote.run(
            args=[
                'cd',
                '--',
                mnt,
                run.Raw('&&'),
                'mkdir',
                '--',
                subdir,
            ],
        )
    else:
        remote.run(
            args=[
                # cd first so this will fail if the mount point does
                # not exist; pure install -d will silently do the
                # wrong thing
                'cd',
                '--',
                mnt,
                run.Raw('&&'),
                'sudo',
                'install',
                '-d',
                '-m', '0755',
                '--owner={user}'.format(user=dir_owner),
                '--',
                subdir,
            ],
        )

    return created_mountpoint


def _spawn_on_all_clients(ctx, refspec, tests, env, basedir, subdir, timeout=None, cleanup=True):
    """
    Make a scratch directory for each client in the cluster, and then for each
    test spawn _run_tests() for each role.

    See run_tests() for parameter documentation.
    """
    is_client = misc.is_type('client')
    client_remotes = {}
    created_mountpoint = {}
    for remote, roles_for_host in ctx.cluster.remotes.items():
        for role in roles_for_host:
            if is_client(role):
                client_remotes[role] = remote
                created_mountpoint[role] = _make_scratch_dir(ctx, role, subdir)

    for unit in tests:
        with parallel() as p:
            for role, remote in client_remotes.items():
                p.spawn(_run_tests, ctx, refspec, role, [unit], env,
                        basedir,
                        subdir,
                        timeout=timeout)

    # cleanup the generated client directories
    if cleanup:
        for role, _ in client_remotes.items():
            _delete_dir(ctx, role, created_mountpoint[role])


def _run_tests(ctx, refspec, role, tests, env, basedir,
               subdir=None, timeout=None, cleanup=True,
               coverage_and_limits=True):
    """
    Run the individual test. Create a scratch directory and then extract the
    workunits from git. Make the executables, and then run the tests.
    Clean up (remove files created) after the tests are finished.

    :param ctx:     Context
    :param refspec: branch, sha1, or version tag used to identify this
                    build
    :param tests:   specific tests specified.
    :param env:     environment set in yaml file.  Could be None.
    :param subdir:  subdirectory set in yaml file.  Could be None
    :param timeout: If present, use the 'timeout' command on the remote host
                    to limit execution time. Must be specified by a number
                    followed by 's' for seconds, 'm' for minutes, 'h' for
                    hours, or 'd' for days. If '0' or anything that evaluates
                    to False is passed, the 'timeout' command is not used.
    """
    testdir = misc.get_testdir(ctx)
    assert isinstance(role, str)
    cluster, type_, id_ = misc.split_role(role)
    assert type_ == 'client'
    remote = get_remote_for_role(ctx, role)
    mnt = _client_mountpoint(ctx, cluster, id_)
    # subdir so we can remove and recreate this a lot without sudo
    if subdir is None:
        scratch_tmp = os.path.join(mnt, 'client.{id}'.format(id=id_), 'tmp')
    else:
        scratch_tmp = os.path.join(mnt, subdir)
    clonedir = '{tdir}/clone.{role}'.format(tdir=testdir, role=role)
    srcdir = '{cdir}/{basedir}'.format(cdir=clonedir,
                                       basedir=basedir)

    git_url = teuth_config.get_ceph_qa_suite_git_url()
    # if we are running an upgrade test, and ceph-ci does not have branches like
    # `jewel`, so should use ceph.git as an alternative.
    try:
        remote.run(logger=log.getChild(role),
                   args=refspec.clone(git_url, clonedir))
    except CommandFailedError:
        if git_url.endswith('/ceph-ci.git'):
            alt_git_url = git_url.replace('/ceph-ci.git', '/ceph.git')
        elif git_url.endswith('/ceph-ci'):
            alt_git_url = re.sub(r'/ceph-ci$', '/ceph.git', git_url)
        else:
            raise
        log.info(
            "failed to check out '%s' from %s; will also try in %s",
            refspec,
            git_url,
            alt_git_url,
        )
        remote.run(logger=log.getChild(role),
                   args=refspec.clone(alt_git_url, clonedir))
    remote.run(
        logger=log.getChild(role),
        args=[
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0',
            run.Raw('>{tdir}/workunits.list.{role}'.format(tdir=testdir, role=role)),
        ],
    )

    workunits_file = '{tdir}/workunits.list.{role}'.format(tdir=testdir, role=role)
    workunits = sorted(misc.get_file(remote, workunits_file).decode().split('\0'))
    assert workunits

    try:
        assert isinstance(tests, list)
        for spec in tests:
            log.info('Running workunits matching %s on %s...', spec, role)
            prefix = '{spec}/'.format(spec=spec)
            to_run = [w for w in workunits if w == spec or w.startswith(prefix)]
            if not to_run:
                raise RuntimeError('Spec did not match any workunits: {spec!r}'.format(spec=spec))
            for workunit in to_run:
                log.info('Running workunit %s...', workunit)
                args = [
                    'mkdir', '-p', '--', scratch_tmp,
                    run.Raw('&&'),
                    'cd', '--', scratch_tmp,
                    run.Raw('&&'),
                    run.Raw('CEPH_CLI_TEST_DUP_COMMAND=1'),
                    run.Raw('CEPH_REF={ref}'.format(ref=refspec)),
                    run.Raw('TESTDIR="{tdir}"'.format(tdir=testdir)),
                    run.Raw('CEPH_ARGS="--cluster {0}"'.format(cluster)),
                    run.Raw('CEPH_ID="{id}"'.format(id=id_)),
                    run.Raw('PATH=$PATH:/usr/sbin'),
                    run.Raw('CEPH_BASE={dir}'.format(dir=clonedir)),
                    run.Raw('CEPH_ROOT={dir}'.format(dir=clonedir)),
                ]
                if env is not None:
                    for var, val in env.items():
                        quoted_val = pipes.quote(val)
                        env_arg = '{var}={val}'.format(var=var, val=quoted_val)
                        args.append(run.Raw(env_arg))
                if coverage_and_limits:
                    args.extend([
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir)])
                if timeout and timeout != '0':
                    args.extend(['timeout', timeout])
                args.extend([
                    '{srcdir}/{workunit}'.format(
                        srcdir=srcdir,
                        workunit=workunit,
                    ),
                ])
                remote.run(
                    logger=log.getChild(role),
                    args=args,
                    label="workunit test {workunit}".format(workunit=workunit)
                )
                if cleanup:
                    args=['sudo', 'rm', '-rf', '--', scratch_tmp]
                    remote.run(logger=log.getChild(role), args=args, timeout=(60*60))
    finally:
        log.info('Stopping %s on %s...', tests, role)
        args=['sudo', 'rm', '-rf', '--', workunits_file, clonedir]
        # N.B. don't cleanup scratch_tmp! If the mount is broken then rm will hang.
        remote.run(
            logger=log.getChild(role),
            args=args,
        )
