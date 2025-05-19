"""
Rados benchmarking
"""
import contextlib
import logging
import shlex

from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology.exceptions import CommandFailedError


log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        extra_args: ...
        auth_exit_on_failure: <int>
        clients: [client list]
        expected_rc: <int>
        time: <seconds to run>
        pool: <pool to use>
        size: write size to use
        concurrency: max number of outstanding writes (16)
        objectsize: object size to use
        unique_pool: use a unique pool, defaults to False
        ec_pool: create an ec pool, defaults to False
        create_pool: create pool, defaults to True
        erasure_code_profile:
          name: teuthologyprofile
          k: 2
          m: 1
          crush-failure-domain: osd
        cleanup: false (defaults to true)
        type: <write|seq|rand> (defaults to write)
    example:

    tasks:
    - ceph:
    - radosbench:
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning radosbench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    radosbench = {}

    testdir = teuthology.get_testdir(ctx)
    manager = ctx.managers['ceph']
    runtype = config.get('type', 'write')

    config_extra_args = shlex.split(config.setdefault('extra_args', ''))
    expected_rc = config.setdefault('expected_rc', 0)

    create_pool = config.get('create_pool', True)
    for role in config.get(
            'clients',
            list(map(lambda x: 'client.' + x,
                     teuthology.all_roles_of_type(ctx.cluster, 'client')))):
        assert isinstance(role, str)
        (_, id_) = role.split('.', 1)
        (remote,) = ctx.cluster.only(role).remotes.keys()

        if config.get('ec_pool', False):
            profile = config.get('erasure_code_profile', {})
            profile_name = profile.get('name', 'teuthologyprofile')
            manager.create_erasure_code_profile(profile_name, profile)
        else:
            profile_name = None

        cleanup = []
        if not config.get('cleanup', True):
            cleanup = ['--no-cleanup']
        write_to_omap = []
        if config.get('write-omap', False):
            write_to_omap = ['--write-omap']
            log.info('omap writes')

        pool = config.get('pool', 'data')
        if create_pool:
            if pool != 'data':
                manager.create_pool(pool, erasure_code_profile_name=profile_name)
            else:
                pool = manager.create_pool_with_unique_name(erasure_code_profile_name=profile_name)

        concurrency = config.get('concurrency', 16)

        cmd = [
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage',
            'rados',
        ]
        extra_args = [
            *config_extra_args,
            '--no-log-to-stderr',
            f'--name={role}',
            f'--pool={pool}',
        ]
        auth_exit_on_failure = config.get('auth_exit_on_failure', None)
        if auth_exit_on_failure is not None:
            extra_args.append(f'--auth_exit_on_failure={auth_exit_on_failure}')

        bench_args = [
            f'bench',
            f'--concurrent-ios={concurrency}',
        ]
        osize = config.get('objectsize', 65536)
        if osize > 0:
            bench_args.append(f'--object-size={osize}')

        size = ['-b', str(config.get('size', 65536))]
        # If doing a reading run then populate data
        if runtype != "write":
            proc = remote.run(
                args=[
                    "/bin/sh", "-c",
                    " ".join([*cmd,
                              *extra_args,
                              *bench_args,
                              str(60),
                              "write",
                              "--no-cleanup"
                          ]).format(tdir=testdir),
                ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            wait=True
            )
            size = []
            objectsize = []

        proc = remote.run(
            args=[
                "/bin/sh", "-c",
                " ".join([*cmd,
                          *extra_args,
                          *bench_args,
                          *size,
                          str(config.get('time', 360)),
                          runtype,
                          *write_to_omap,
                          *cleanup,
                          ]).format(tdir=testdir),
                ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        radosbench[id_] = proc

    try:
        yield
    finally:
        timeout = config.get('time', 360) * 30 + 300
        log.info('joining radosbench (timing out after %ss)', timeout)
        try:
            run.wait(radosbench.values(), timeout=timeout)
        except CommandFailedError as e:
            for p in radosbench.values():
                if p.exitstatus == expected_rc:
                    pass
                else:
                    raise
        else:
            if expected_rc != 0:
                raise RuntimeError("expected radosbench failure")

        if pool != 'data' and create_pool:
            manager.remove_pool(pool)
