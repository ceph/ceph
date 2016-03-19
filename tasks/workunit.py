"""
Workunit task -- Run ceph on sets of specific clients
"""
import logging
import pipes
import os

from teuthology import misc
from teuthology.config import config as teuth_config
from teuthology.orchestra.run import CommandFailedError
from teuthology.parallel import parallel
from teuthology.orchestra import run

log = logging.getLogger(__name__)

CLIENT_PREFIX = 'client.'


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

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict)
    assert isinstance(config.get('clients'), dict), \
        'configuration must contain a dictionary of clients'

    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('workunit', {}))

    refspec = config.get('branch')
    if refspec is None:
        refspec = config.get('tag')
    if refspec is None:
        refspec = config.get('sha1')
    if refspec is None:
        refspec = 'HEAD'

    timeout = config.get('timeout', '3h')

    log.info('Pulling workunits from ref %s', refspec)

    created_mountpoint = {}

    if config.get('env') is not None:
        assert isinstance(config['env'], dict), 'env must be a dictionary'
    clients = config['clients']

    # Create scratch dirs for any non-all workunits
    log.info('Making a separate scratch dir for every client...')
    for role in clients.iterkeys():
        assert isinstance(role, basestring)
        if role == "all":
            continue

        assert role.startswith(CLIENT_PREFIX)
        created_mnt_dir = _make_scratch_dir(ctx, role, config.get('subdir'))
        created_mountpoint[role] = created_mnt_dir

    # Execute any non-all workunits
    with parallel() as p:
        for role, tests in clients.iteritems():
            if role != "all":
                p.spawn(_run_tests, ctx, refspec, role, tests,
                        config.get('env'), timeout=timeout)

    # Clean up dirs from any non-all workunits
    for role, created in created_mountpoint.items():
        _delete_dir(ctx, role, created)

    # Execute any 'all' workunits
    if 'all' in clients:
        all_tasks = clients["all"]
        _spawn_on_all_clients(ctx, refspec, all_tasks, config.get('env'),
                              config.get('subdir'), timeout=timeout)


def _delete_dir(ctx, role, created_mountpoint):
    """
    Delete file used by this role, and delete the directory that this
    role appeared in.

    :param ctx: Context
    :param role: "role.#" where # is used for the role id.
    """
    testdir = misc.get_testdir(ctx)
    id_ = role[len(CLIENT_PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
    # Is there any reason why this is not: join(mnt, role) ?
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
    id_ = role[len(CLIENT_PREFIX):]
    log.debug("getting remote for {id} role {role_}".format(id=id_, role_=role))
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    dir_owner = remote.user
    mnt = os.path.join(misc.get_testdir(ctx), 'mnt.{id}'.format(id=id_))
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


def _spawn_on_all_clients(ctx, refspec, tests, env, subdir, timeout=None):
    """
    Make a scratch directory for each client in the cluster, and then for each
    test spawn _run_tests() for each role.

    See run_tests() for parameter documentation.
    """
    client_generator = misc.all_roles_of_type(ctx.cluster, 'client')
    client_remotes = list()

    created_mountpoint = {}
    for client in client_generator:
        (client_remote,) = ctx.cluster.only('client.{id}'.format(id=client)).remotes.iterkeys()
        client_remotes.append((client_remote, 'client.{id}'.format(id=client)))
        created_mountpoint[client] = _make_scratch_dir(ctx, "client.{id}".format(id=client), subdir)

    for unit in tests:
        with parallel() as p:
            for remote, role in client_remotes:
                p.spawn(_run_tests, ctx, refspec, role, [unit], env, subdir,
                        timeout=timeout)

    # cleanup the generated client directories
    client_generator = misc.all_roles_of_type(ctx.cluster, 'client')
    for client in client_generator:
        _delete_dir(ctx, 'client.{id}'.format(id=client), created_mountpoint[client])


def _run_tests(ctx, refspec, role, tests, env, subdir=None, timeout=None):
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
    assert isinstance(role, basestring)
    assert role.startswith(CLIENT_PREFIX)
    id_ = role[len(CLIENT_PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
    # subdir so we can remove and recreate this a lot without sudo
    if subdir is None:
        scratch_tmp = os.path.join(mnt, 'client.{id}'.format(id=id_), 'tmp')
    else:
        scratch_tmp = os.path.join(mnt, subdir)
    srcdir = '{tdir}/workunit.{role}'.format(tdir=testdir, role=role)
    clonedir = '{tdir}/clone'.format(tdir=testdir)

    git_url = teuth_config.get_ceph_git_url()
    if 'github.com/ceph/ceph' in git_url:
        remote.run(
            logger=log.getChild(role),
            args=[
                'mkdir', '--', srcdir,
                run.Raw('&&'),
                'git',
                'archive',
                '--remote=git://git.ceph.com/ceph.git',
                '%s:qa/workunits' % refspec,
                run.Raw('|'),
                'tar',
                '-C', srcdir,
                '-x',
                '-f-',
            ],
        )
    else:
        remote.run(
            logger=log.getChild(role),
            args=[
                'git',
                'clone',
                git_url,
                clonedir,
                run.Raw(';'),
                'cd', '--', clonedir,
                run.Raw('&&'),
                'git', 'checkout', refspec,
                run.Raw('&&'),
                'mv', 'qa/workunits', srcdir,
            ],
        )

    remote.run(
        logger=log.getChild(role),
        args=[
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0'.format(srcdir=srcdir),
            run.Raw('>{tdir}/workunits.list.{role}'.format(tdir=testdir, role=role)),
        ],
    )

    workunits_file = '{tdir}/workunits.list.{role}'.format(tdir=testdir, role=role)
    workunits = sorted(misc.get_file(remote, workunits_file).split('\0'))
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
                    run.Raw('CEPH_ID="{id}"'.format(id=id_)),
                    run.Raw('PATH=$PATH:/usr/sbin')
                ]
                if env is not None:
                    for var, val in env.iteritems():
                        quoted_val = pipes.quote(val)
                        env_arg = '{var}={val}'.format(var=var, val=quoted_val)
                        args.append(run.Raw(env_arg))
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
                remote.run(
                    logger=log.getChild(role),
                    args=['sudo', 'rm', '-rf', '--', scratch_tmp],
                )
    finally:
        log.info('Stopping %s on %s...', tests, role)
        remote.run(
            logger=log.getChild(role),
            args=[
                'rm', '-rf', '--', workunits_file, srcdir, clonedir,
            ],
        )
