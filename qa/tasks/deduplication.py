"""
Run ceph-dedup-tool
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
    Run ceph-dedup-tool.

    The config should be as follows::

        ceph-dedup-tool:
          clients: [client list]
          op: <operation name>
          pool: <pool name>
          chunk_pool: <chunk pool name>
          chunk_size: <chunk size>
          chunk_algorithm: <chunk algorithm, fixed|fastcdc>
          fingerprint_algorithm: <fingerprint algorithm, sha1|sha256|sha512>
          object_dedup_threashold: <#% of duplicate chunks within an object to trigger object dedup>
          chunk_dedup_threashold: <the number of duplicate chunks to trigger chunk dedup>
          max_thread: <the number of threads>
          wakeup_period: <duration>

    For example::

        tasks:
        - exec:
            client.0:
              - sudo ceph osd pool create low_tier 4
        - deduplication:
            clients: [client.0]
            op: 'sample-dedup'
            pool: 'default.rgw.buckets.data'
            chunk_pool: 'low_tier'
            chunk_size: 8192
            chunk_algorithm: 'fastcdc'
            fingerprint_algorithm: 'sha1'
            object_dedup_threshold: 15
            chunk_dedup_threshold: 5
            max_thread: 2
            wakeup_period: 20

    """
    log.info('Beginning deduplication...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    #assert hasattr(ctx, 'rgw')
    testdir = teuthology.get_testdir(ctx)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'ceph-dedup-tool']
    if config.get('op', None):
        args.extend(['--op', config.get('op', None)])
    if config.get('chunk_pool', None):
        args.extend(['--chunk-pool', config.get('chunk_pool', None)])
    if config.get('chunk_size', False):
        args.extend(['--chunk-size', str(config.get('chunk_size', 8192))])
    if config.get('chunk_algorithm', False):
        args.extend(['--chunk-algorithm', config.get('chunk_algorithm', None)] )
    if config.get('fingerprint_algorithm', False):
        args.extend(['--fingerprint-algorithm', config.get('fingerprint_algorithm', None)] )
    if config.get('object_dedup_threshold', False):
        args.extend(['--object-dedup-threshold', str(config.get('object_dedup_threshold', 50))])
    if config.get('chunk_dedup_threshold', False):
        args.extend(['--chunk-dedup-threshold', str(config.get('chunk_dedup_threshold', 5))])
    if config.get('max_thread', False):
        args.extend(['--max-thread', str(config.get('max_thread', 2))])
    if config.get('wakeup_period', False):
        args.extend(['"--wakeup-period"', str(config.get('wakeup_period', 30))])
    if config.get('pool', False):
        args.extend(['--pool', config.get('pool', None)])

    args.extend([
        '--debug',
        '--deamon',
        '--iterative'])

    def thread():
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        manager = ctx.managers['ceph']
        tests = {}
        log.info("args %s", args)
        for role in config.get('clients', clients):
            assert isinstance(role, str)
            PREFIX = 'client.'
            assert role.startswith(PREFIX)
            id_ = role[len(PREFIX):]
            (remote,) = ctx.cluster.only(role).remotes.keys()
            proc = remote.run(
                args=args,
                stdin=run.PIPE,
                wait=False
                )
            tests[id_] = proc

    running = gevent.spawn(thread)

    try:
        yield
    finally:
        log.info('joining ceph-dedup-tool')
        running.get()
