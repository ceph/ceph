"""Scrub testing"""
from cStringIO import StringIO

import logging
import os
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test [deep] scrub

    tasks:
    - chef:
    - install:
    - ceph:
        log-whitelist:
        - '!= known digest'
        - '!= known omap_digest'
        - deep-scrub 0 missing, 1 inconsistent objects
        - deep-scrub 1 errors
        - repair 0 missing, 1 inconsistent objects
        - repair 1 errors, 1 fixed
    - scrub_test: 
    
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
    p = manager.do_rados(mon, ['-p', 'rbd', 'bench', '--no-cleanup', '1', 'write', '-b', '4096'])
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
    data_path = os.path.join(
        '/var/lib/ceph/osd',
        'ceph-{id}'.format(id=osd),
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
    osdfilename = None
    for line in ls_out.split('\n'):
        if 'object' in line:
            osdfilename = line
            break
    assert osdfilename is not None

    # Get actual object name from osd stored filename
    tmp=osdfilename.split('__')
    objname=tmp[0]
    objname=objname.replace('\u', '_')
    log.info('fuzzing %s' % objname)

    # put a single \0 at the beginning of the file
    osd_remote.run(
        args=[ 'sudo', 'dd',
               'if=/dev/zero',
               'of=%s' % os.path.join(data_path, osdfilename),
               'bs=1', 'count=1', 'conv=notrunc'
             ]
    )

    # scrub, verify inconsistent
    manager.raw_cluster_cmd('pg', 'deep-scrub', victim)
    # Give deep-scrub a chance to start
    time.sleep(60)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if 'scrubbing' in state:
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert inconsistent
        break


    # repair, verify no longer inconsistent
    manager.raw_cluster_cmd('pg', 'repair', victim)
    # Give repair a chance to start
    time.sleep(60)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if 'scrubbing' in state:
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert not inconsistent
        break

    # Test deep-scrub with various omap modifications
    manager.do_rados(mon, ['-p', 'rbd', 'setomapval', objname, 'key', 'val'])
    manager.do_rados(mon, ['-p', 'rbd', 'setomapheader', objname, 'hdr'])

    # Modify omap on specific osd
    log.info('fuzzing omap of %s' % objname)
    manager.osd_admin_socket(osd, ['rmomapkey', 'rbd', objname, 'key']);
    manager.osd_admin_socket(osd, ['setomapval', 'rbd', objname, 'badkey', 'badval']);
    manager.osd_admin_socket(osd, ['setomapheader', 'rbd', objname, 'badhdr']);

    # scrub, verify inconsistent
    manager.raw_cluster_cmd('pg', 'deep-scrub', victim)
    # Give deep-scrub a chance to start
    time.sleep(60)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if 'scrubbing' in state:
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert inconsistent
        break

    # repair, verify no longer inconsistent
    manager.raw_cluster_cmd('pg', 'repair', victim)
    # Give repair a chance to start
    time.sleep(60)

    while True:
        stats = manager.get_single_pg_stats(victim)
        state = stats['state']

        # wait for the scrub to finish
        if 'scrubbing' in state:
            time.sleep(3)
            continue

        inconsistent = stats['state'].find('+inconsistent') != -1
        assert not inconsistent
        break

    log.info('test successful!')
