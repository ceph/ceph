"""
Rados modle-based integration tests
"""
import contextlib
import logging
import gevent
from teuthology import misc as teuthology


from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        rados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          max_in_flight: <max number of operations in flight>
          object_size: <size of objects in bytes>
          min_stride_size: <minimum write stride size in bytes>
          max_stride_size: <maximum write stride size in bytes>
          op_weights: <dictionary mapping operation type to integer weight>
          runs: <number of times to run> - the pool is remade between runs
          ec_pool: use an ec pool
          erasure_code_profile: profile to use with the erasure coded pool
          fast_read: enable ec_pool's fast_read
          min_size: set the min_size of created pool
          pool_snaps: use pool snapshots instead of selfmanaged snapshots
	  write_fadvise_dontneed: write behavior like with LIBRADOS_OP_FLAG_FADVISE_DONTNEED.
	                          This mean data don't access in the near future.
				  Let osd backend don't keep data in cache.

    For example::

        tasks:
        - ceph:
        - rados:
            clients: [client.0]
            ops: 1000
            max_seconds: 0   # 0 for no limit
            objects: 25
            max_in_flight: 16
            object_size: 4000000
            min_stride_size: 1024
            max_stride_size: 4096
            op_weights:
              read: 20
              write: 10
              delete: 2
              snap_create: 3
              rollback: 2
              snap_remove: 0
            ec_pool: create an ec pool, defaults to False
            erasure_code_use_overwrites: test overwrites, default false
            erasure_code_profile:
              name: teuthologyprofile
              k: 2
              m: 1
              crush-failure-domain: osd
            pool_snaps: true
	    write_fadvise_dontneed: true
            runs: 10
        - interactive:

    Optionally, you can provide the pool name to run against:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo
        - rados:
            clients: [client.0]
            pools: [foo]
            ...

    Alternatively, you can provide a pool prefix:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo.client.0
        - rados:
            clients: [client.0]
            pool_prefix: foo
            ...

    The tests are run asynchronously, they are not complete when the task
    returns. For instance:

        - rados:
            clients: [client.0]
            pools: [ecbase]
            ops: 4000
            objects: 500
            op_weights:
              read: 100
              write: 100
              delete: 50
              copy_from: 50
        - print: "**** done rados ec-cache-agent (part 2)"

     will run the print task immediately after the rados tasks begins but
     not after it completes. To make the rados task a blocking / sequential
     task, use:

        - sequential:
          - rados:
              clients: [client.0]
              pools: [ecbase]
              ops: 4000
              objects: 500
              op_weights:
                read: 100
                write: 100
                delete: 50
                copy_from: 50
        - print: "**** done rados ec-cache-agent (part 2)"

    """
    log.info('Beginning rados...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    log.info("config is {config}".format(config=str(config)))
    overrides = ctx.config.get('overrides', {})
    log.info("overrides is {overrides}".format(overrides=str(overrides)))
    teuthology.deep_merge(config, overrides.get('rados', {}))
    log.info("config is {config}".format(config=str(config)))

    object_size = int(config.get('object_size', 4000000))
    op_weights = config.get('op_weights', {})
    testdir = teuthology.get_testdir(ctx)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'ceph_test_rados']
    if config.get('ec_pool', False):
        args.extend(['--no-omap'])
        if not config.get('erasure_code_use_overwrites', False):
            args.extend(['--ec-pool'])
    if config.get('write_fadvise_dontneed', False):
        args.extend(['--write-fadvise-dontneed'])
    if config.get('set_redirect', False):
        args.extend(['--set_redirect'])
    if config.get('set_chunk', False):
        args.extend(['--set_chunk'])
    if config.get('enable_dedup', False):
        args.extend(['--enable_dedup'])
    if config.get('low_tier_pool', None):
        args.extend(['--low_tier_pool', config.get('low_tier_pool', None)])
    if config.get('dedup_chunk_size', False):
        args.extend(['--dedup_chunk_size', config.get('dedup_chunk_size', None)] )
    if config.get('dedup_chunk_algo', False):
        args.extend(['--dedup_chunk_algo', config.get('dedup_chunk_algo', None)])
    if config.get('pool_snaps', False):
        args.extend(['--pool-snaps'])
    if config.get('balance_reads', False):
        args.extend(['--balance-reads'])
    if config.get('localize_reads', False):
        args.extend(['--localize-reads'])
    args.extend([
        '--max-ops', str(config.get('ops', 10000)),
        '--objects', str(config.get('objects', 500)),
        '--max-in-flight', str(config.get('max_in_flight', 16)),
        '--size', str(object_size),
        '--min-stride-size', str(config.get('min_stride_size', object_size // 10)),
        '--max-stride-size', str(config.get('max_stride_size', object_size // 5)),
        '--max-seconds', str(config.get('max_seconds', 0)),
        '--max-attr-len', str(config.get('max_attr_len', 20000))
        ])

    weights = {}
    weights['read'] = 100
    weights['write'] = 100
    weights['delete'] = 10
    # Parallel of the op_types in test/osd/TestRados.cc
    for field in [
        # read handled above
        # write handled above
        # delete handled above
        "snap_create",
        "snap_remove",
        "rollback",
        "setattr",
        "rmattr",
        "watch",
        "copy_from",
        "hit_set_list",
        "is_dirty",
        "undirty",
        "cache_flush",
        "cache_try_flush",
        "cache_evict",
        "append",
        "write",
        "read",
        "delete",
        "set_chunk",
        "tier_promote",
        "tier_evict",
        "tier_promote",
        "tier_flush"
        ]:
        if field in op_weights:
            weights[field] = op_weights[field]

    if config.get('write_append_excl', True):
        if 'write' in weights:
            weights['write'] = weights['write'] // 2
            weights['write_excl'] = weights['write']

        if 'append' in weights:
            weights['append'] = weights['append'] // 2
            weights['append_excl'] = weights['append']

    for op, weight in weights.items():
        args.extend([
            '--op', op, str(weight)
        ])
                

    def thread():
        """Thread spawned by gevent"""
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
                        erasure_code_use_overwrites=
                          config.get('erasure_code_use_overwrites', False)
                    )
                    created_pools.append(pool)
                    if config.get('fast_read', False):
                        manager.raw_cluster_cmd(
                            'osd', 'pool', 'set', pool, 'fast_read', 'true')
                    min_size = config.get('min_size', None);
                    if min_size is not None:
                        manager.raw_cluster_cmd(
                            'osd', 'pool', 'set', pool, 'min_size', str(min_size))

                (remote,) = ctx.cluster.only(role).remotes.keys()
                proc = remote.run(
                    args=["CEPH_CLIENT_ID={id_}".format(id_=id_)] + args +
                    ["--pool", pool],
                    logger=log.getChild("rados.{id}".format(id=id_)),
                    stdin=run.PIPE,
                    wait=False
                    )
                tests[id_] = proc
            run.wait(tests.values())
            wait_for_all_active_clean_pgs = config.get("wait_for_all_active_clean_pgs", False)
            # usually set when we do min_size testing.
            if  wait_for_all_active_clean_pgs:
                # Make sure we finish the test first before deleting the pool.
                # Mainly used for test_pool_min_size
                manager.wait_for_clean()
                manager.wait_for_all_osds_up(timeout=1800)

            for pool in created_pools:
                manager.wait_snap_trimming_complete(pool);
                manager.remove_pool(pool)

    running = gevent.spawn(thread)

    try:
        yield
    finally:
        log.info('joining rados')
        running.get()
