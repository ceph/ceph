"""
Rados benchmarking
"""
import contextlib
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        clients: [client list]
        time: <seconds to run>
        pool: <pool to use>
        size: write size to use
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

    create_pool = config.get('create_pool', True)
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()

        if config.get('ec_pool', False):
            profile = config.get('erasure_code_profile', {})
            profile_name = profile.get('name', 'teuthologyprofile')
            manager.create_erasure_code_profile(profile_name, profile)
        else:
            profile_name = None

        cleanup = []
        if not config.get('cleanup', True):
            cleanup = ['--no-cleanup']

        pool = config.get('pool', 'data')
        if create_pool:
            if pool != 'data':
                manager.create_pool(pool, erasure_code_profile_name=profile_name)
            else:
                pool = manager.create_pool_with_unique_name(erasure_code_profile_name=profile_name)

        osize = config.get('objectsize', 65536)
        if osize is 0:
            objectsize = []
        else:
            objectsize = ['-o', str(osize)]
        size = ['-b', str(config.get('size', 65536))]
        # If doing a reading run then populate data
        if runtype != "write":
            proc = remote.run(
                args=[
                    "/bin/sh", "-c",
                    " ".join(['adjust-ulimits',
                              'ceph-coverage',
                              '{tdir}/archive/coverage',
                              'rados',
                              '--no-log-to-stderr',
                              '--name', role]
                              + size + objectsize +
                              ['-p' , pool,
                          'bench', str(60), "write", "--no-cleanup"
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
                " ".join(['adjust-ulimits',
                          'ceph-coverage',
                          '{tdir}/archive/coverage',
                          'rados',
			  '--no-log-to-stderr',
                          '--name', role]
                          + size + objectsize +
                          ['-p' , pool,
                          'bench', str(config.get('time', 360)), runtype,
                          ] + cleanup).format(tdir=testdir),
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
        run.wait(radosbench.itervalues(), timeout=timeout)

        if pool is not 'data' and create_pool:
            manager.remove_pool(pool)
