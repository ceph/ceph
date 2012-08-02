from cStringIO import StringIO

import logging
import os
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def rados_start(remote, cmd):
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
        '/tmp/cephtest/enable-coredump',
        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
        '/tmp/cephtest/archive/coverage',
        '/tmp/cephtest/binary/usr/local/bin/rados',
        '-c', '/tmp/cephtest/ceph.conf',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        wait=True,
        )
    return proc

def task(ctx, config):
    """
    Test [deep] scrub
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'scrub_test task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    
    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    log.info('num_osds is %s' % num_osds)

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < num_osds:
        time.sleep(10)

    for i in range(num_osds):
        manager.raw_cluster_cmd('tell', 'osd.%d' % i, 'flush_pg_stats')
    manager.wait_for_clean()

    # write some data
    p = rados_start(mon, ['-p', 'rbd', 'bench', '1', 'write', '-b', '4096'])
    err = p.exitstatus
    log.info('err is %d' % err)

    # wait for some PG to have data that we can mess with
    victim = None
    osd = None
    while victim is None:
        stats = manager.get_pg_stats()
        for pg in stats:
            size = pg['stat_sum']['num_bytes']
            if size > 0:
                victim = pg['pgid']
                osd = pg['acting'][0]
                break

        if victim is None:
            time.sleep(3)

    log.info('messing with PG %s on osd %d' % (victim, osd))


    (osd_remote,) = ctx.cluster.only('osd.%d' % osd).remotes.iterkeys()
    data_path = os.path.join('/tmp/cephtest/data',
                             'osd.{id}.data'.format(id=osd),
                             'current',
                             '{pg}_head'.format(pg=victim)
                            )


    # fuzz time
    ls_fp = StringIO()
    osd_remote.run(
        args=[ 'ls', data_path ],
        stdout=ls_fp,
    )
    ls_out = ls_fp.getvalue()
    ls_fp.close()

    # find an object file we can mess with
    file = None
    for line in ls_out.split('\n'):
        if line.find('object'):
            file = line
            break
    assert file is not None

    log.info('fuzzing %s' % file)

    # put a single \0 at the beginning of the file
    osd_remote.run(
        args=[ 'dd',
               'if=/dev/zero',
               'of=%s' % os.path.join(data_path, file),
               'bs=1', 'count=1', 'conv=notrunc'
             ]
    )

    # scrub, verify inconsistent
    manager.raw_cluster_cmd('pg', 'deep-scrub', victim)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if state.find('scrubbing'):
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert inconsistent
        break


    # repair, verify no longer inconsistent
    manager.raw_cluster_cmd('pg', 'repair', victim)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if state.find('scrubbing'):
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert not inconsistent
        break

    log.info('test successful!')
