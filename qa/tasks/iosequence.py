"""
ceph_test_rados_io_sequence I/O exerciser

Generate sequences of I/O to a single object with different offsets
and lengths with a focus on testing boundary conditions such as
chunk size, stripe size, etc. Good for testing Erasure Coding and
Object Stores.
"""

import contextlib
import logging
import threading
from typing import Any, Dict

import gevent

from teuthology import misc as teuthology
from teuthology.orchestra import run

from .watched_process import WatchedProcess

log = logging.getLogger(__name__)
_lock = threading.Lock()
_uid = 0

@contextlib.contextmanager
def task(ctx, config):
    """
    Run ceph_test_rados_io_sequence based integration tests.

    The config should be as follows::

        iosequence:
          clients: [client list]
          ec_pool: use an ec pool
          erasure_code_profile: profile to use with the erasure coded pool
          min_size: set the min_size of created pool
          runs: <number of times to run> - the pool is remade between runs
          objectsize: <min>,<max> - see ceph_test_radios_io_sequence help
          sequence: <min>,<max> - see ceph_test_radios_io_sequence help
          blocksize: <bytes> - see ceph_test_radios_io_sequence help
          threads: <numthreads> - see ceph_test_radios_io_sequence help
          seed: <int> - see ceph_test_radios_io_sequence help
          seqseed: <int> - see ceph_test_radios_io_sequence help
    For example::

        tasks:
        - ceph:
        - iosequence:
            clients: [client.0]
            ec_pool: create an ec pool, defaults to False
            erasure_code_profile:
              name: teuthologyprofile
              k: 2
              m: 1
              crush-failure-domain: osd
            runs: 10
            blocksize: 2048
            objectsize: 2,10
            sequence: 1,8
            threads: 5
            seed: 12345678
        - interactive:

    Optionally, you can provide the pool name to run against:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo
        - iosequence:
            clients: [client.0]
            pools: [foo]
            ...

    Alternatively, you can provide a pool prefix:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo.client.0
        - iosequence:
            clients: [client.0]
            pool_prefix: foo
            ...

    The tests are run asynchronously, they are not complete when the task
    returns. For instance:

        - iosequence:
            clients: [client.0]
            pools: [ecbase]
        - print: "**** done rados ec-cache-agent (part 2)"

     will run the print task immediately after the ceph_test_rados_iosequence
     tasks begins but not after it completes. To make the iosequence task a
     blocking / sequential task, use:

        - sequential:
          - iosequence:
              clients: [client.0]
              pools: [ecbase]
        - print: "**** done rados ec-cache-agent (part 2)"

    """
    log.info('Beginning iosequence...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    log.info("config is {config}".format(config=str(config)))
    overrides = ctx.config.get('overrides', {})
    log.info("overrides is {overrides}".format(overrides=str(overrides)))
    teuthology.deep_merge(config, overrides.get('iosequence', {}))
    log.info("config is {config}".format(config=str(config)))

    testdir = teuthology.get_testdir(ctx)
    pct_update_delay = None
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'stdin-killer',
        '--',
        'ceph_test_rados_io_sequence']

    def thread():
        """Thread spawned by gevent"""
        global _lock
        global _uid
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        manager = ctx.managers['ceph']
        if config.get('ec_pool', False):
            profile = config.get('erasure_code_profile', {})
            profile_name = profile.get('name', 'teuthologyprofile')
            manager.create_erasure_code_profile(profile_name, profile)
            crush_prof = config.get('erasure_code_crush', {})
            crush_name = None
            if crush_prof:
                crush_name = crush_prof.get('name', 'teuthologycrush')
                manager.create_erasure_code_crush_rule(crush_name, crush_prof)

        else:
            profile_name = None
            crush_name = None

        cluster = config.get("cluster", "ceph")

        for i in range(int(config.get('runs', '1'))):
            log.info("starting run %s out of %s", str(i), config.get('runs', '1'))
            tests = {}
            existing_pools = config.get('pools', [])
            created_pools = []
            for role in config.get('clients', clients):
                assert isinstance(role, str)
                PREFIX = 'client.'
                assert role.startswith(PREFIX)
                id_ = role[len(PREFIX):]

                pool = config.get('pool', None)
                if not pool and existing_pools:
                    pool = existing_pools.pop()
                else:
                    pool = manager.create_pool_with_unique_name(
                        erasure_code_profile_name=profile_name,
                        erasure_code_crush_rule_name=crush_name,
                        erasure_code_use_overwrites=True,
                    )
                    created_pools.append(pool)
                    min_size = config.get('min_size', None)
                    if min_size is not None:
                        manager.raw_cluster_cmd(
                            'osd', 'pool', 'set', pool, 'min_size', str(min_size))

                (remote,) = ctx.cluster.only(role).remotes.keys()
                extraargs = []
                if config.get('objectsize', False):
                    extraargs.extend(['--objectsize', str(config.get('objectsize'))])
                if config.get('sequence', False):
                    extraargs.extend(['--sequence', str(config.get('sequence'))])
                if config.get('blocksize', False):
                    extraargs.extend(['--blocksize', str(config.get('blocksize'))])
                if config.get('threads', False):
                    extraargs.extend(['--threads', str(config.get('threads'))])
                if config.get('seed', False):
                    extraargs.extend(['--seed', str(config.get('seed'))])
                if config.get('seqseed', False):
                    extraargs.extend(['--seqseed', str(config.get('seqseed'))])
                with _lock:
                    uid = "iosequence.{id}".format(id=_uid)
                    _uid += 1
                proc = remote.run(
                    args=["CEPH_CLIENT_ID={id_}".format(id_=id_)] + args +
                    extraargs +
                    ["--pool", pool] +
                    ["--object", uid] +
                    ["--verbose"],
                    logger=log.getChild(uid),
                    stdin=run.PIPE,
                    wait=False
                    )
                tests[id_] = proc

            watched_process: WatchedProcess = WatchedProcess(ctx, log, "iosequence", cluster, tests)
            try:
                run.wait(tests.values())
            except Exception as e:
                watched_process.try_set_exception(e)

            # If test has failed then don't try to clean up
            if watched_process.exception:
                raise watched_process.exception

            wait_for_all_active_clean_pgs = config.get("wait_for_all_active_clean_pgs", False)
            # usually set when we do min_size testing.
            if wait_for_all_active_clean_pgs:
                # Make sure we finish the test first before deleting the pool.
                # Mainly used for test_pool_min_size
                manager.wait_for_clean()
                manager.wait_for_all_osds_up(timeout=1800)

            for pool in created_pools:
                manager.wait_snap_trimming_complete(pool)
                manager.remove_pool(pool)

    running = gevent.spawn(thread)

    try:
        yield
    finally:
        log.info('joining iosequence')
        running.get()
