from cStringIO import StringIO

import contextlib
import gevent
import json
import logging
import random
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Benchmark the recovery system.

    Generates objects with smalliobench, runs it normally to get a
    baseline performance measurement, then marks an OSD out and reruns
    to measure performance during recovery.

    The config should be as follows:

    recovery_bench:
        duration: <seconds for each measurement run>
        num_objects: <number of objects>
        io_size: <io size in bytes>

    example:

    tasks:
    - ceph:
    - recovery_bench:
        duration: 60
        num_objects: 500
        io_size: 4096
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'recovery_bench task only accepts a dict for configuration'

    log.info('Beginning recovery bench...')

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    while len(manager.get_osd_status()['up']) < num_osds:
        manager.sleep(10)

    bench_proc = RecoveryBencher(
        manager,
        config,
        )
    try:
        yield
    finally:
        log.info('joining recovery bencher')
        bench_proc.do_join()

class RecoveryBencher:
    def __init__(self, manager, config):
        self.ceph_manager = manager
        self.ceph_manager.wait_for_clean()

        osd_status = self.ceph_manager.get_osd_status()
        self.osds = osd_status['up']

        self.config = config
        if self.config is None:
            self.config = dict()

        else:
            def tmp(x):
                print x
            self.log = tmp

        log.info("spawning thread")

        self.thread = gevent.spawn(self.do_bench)

    def do_join(self):
        self.thread.get()

    def do_bench(self):
        duration = self.config.get("duration", 60)
        num_objects = self.config.get("num_objects", 500)
        io_size = self.config.get("io_size", 4096)

        osd = str(random.choice(self.osds))
        (osd_remote,) = self.ceph_manager.ctx.cluster.only('osd.%s' % osd).remotes.iterkeys()

        testdir = teuthology.get_testdir(self.ceph_manager.ctx)

        # create the objects
        osd_remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'smalliobench'.format(tdir=testdir),
                '--use-prefix', 'recovery_bench',
                '--init-only', '1',
                '--num-objects', str(num_objects),
                '--io-size', str(io_size),
                ],
            wait=True,
        )

        # baseline bench
        log.info('non-recovery (baseline)')
        p = osd_remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'smalliobench',
                '--use-prefix', 'recovery_bench',
                '--do-not-init', '1',
                '--duration', str(duration),
                '--io-size', str(io_size),
                ],
            stdout=StringIO(),
            stderr=StringIO(),
            wait=True,
        )
        self.process_samples(p.stderr.getvalue())

        self.ceph_manager.raw_cluster_cmd('osd', 'out', osd)
        time.sleep(5)

        # recovery bench
        log.info('recovery active')
        p = osd_remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'smalliobench',
                '--use-prefix', 'recovery_bench',
                '--do-not-init', '1',
                '--duration', str(duration),
                '--io-size', str(io_size),
                ],
            stdout=StringIO(),
            stderr=StringIO(),
            wait=True,
        )
        self.process_samples(p.stderr.getvalue())

        self.ceph_manager.raw_cluster_cmd('osd', 'in', osd)

    def process_samples(self, input):
        lat = {}
        for line in input.split('\n'):
            try:
                sample = json.loads(line)
                samples = lat.setdefault(sample['type'], [])
                samples.append(float(sample['latency']))
            except Exception:
              pass

        for type in lat:
            samples = lat[type]
            samples.sort()

            num = len(samples)

            # median
            if num & 1 == 1: # odd number of samples
                median = samples[num / 2]
            else:
                median = (samples[num / 2] + samples[num / 2 - 1]) / 2

            # 99%
            ninety_nine = samples[int(num * 0.99)]

            log.info("%s: median %f, 99%% %f" % (type, median, ninety_nine))
