"""Scrub testing"""
from cStringIO import StringIO

import contextlib
import logging
import os
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def wait_for_victim_pg(manager):
    """Return a PG with some data and its acting set"""
    # wait for some PG to have data that we can mess with
    victim = None
    while victim is None:
        stats = manager.get_pg_stats()
        for pg in stats:
            size = pg['stat_sum']['num_bytes']
            if size > 0:
                victim = pg['pgid']
                acting = pg['acting']
                return victim, acting
        time.sleep(3)


def find_victim_object(ctx, pg, osd):
    """Return a file to be fuzzed"""
    (osd_remote,) = ctx.cluster.only('osd.%d' % osd).remotes.iterkeys()
    data_path = os.path.join(
        '/var/lib/ceph/osd',
        'ceph-{id}'.format(id=osd),
        'current',
        '{pg}_head'.format(pg=pg)
        )

    # fuzz time
    with contextlib.closing(StringIO()) as ls_fp:
        osd_remote.run(
            args=['sudo', 'ls', data_path],
            stdout=ls_fp,
        )
        ls_out = ls_fp.getvalue()

    # find an object file we can mess with
    osdfilename = next(line for line in ls_out.split('\n')
                       if not line.startswith('__'))
    assert osdfilename is not None

    # Get actual object name from osd stored filename
    objname, _ = osdfilename.split('__', 1)
    objname = objname.replace(r'\u', '_')
    return osd_remote, os.path.join(data_path, osdfilename), objname


def corrupt_file(osd_remote, path):
    # put a single \0 at the beginning of the file
    osd_remote.run(
        args=['sudo', 'dd',
              'if=/dev/zero',
              'of=%s' % path,
              'bs=1', 'count=1', 'conv=notrunc']
    )


def deep_scrub(manager, victim):
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


def repair(manager, victim):
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


def test_repair_corrupted_obj(ctx, manager, pg, osd_remote, obj_path):
    corrupt_file(osd_remote, obj_path)
    deep_scrub(manager, pg)
    repair(manager, pg)


def test_repair_bad_omap(ctx, manager, pg, osd, objname):
    mon = manager.controller
    # Test deep-scrub with various omap modifications
    manager.do_rados(mon, ['-p', 'rbd', 'setomapval', objname, 'key', 'val'])
    manager.do_rados(mon, ['-p', 'rbd', 'setomapheader', objname, 'hdr'])

    # Modify omap on specific osd
    log.info('fuzzing omap of %s' % objname)
    manager.osd_admin_socket(osd, ['rmomapkey', 'rbd', objname, 'key'])
    manager.osd_admin_socket(osd, ['setomapval', 'rbd', objname,
                                   'badkey', 'badval'])
    manager.osd_admin_socket(osd, ['setomapheader', 'rbd', objname, 'badhdr'])

    deep_scrub(manager, pg)
    repair(manager, pg)


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
    p = manager.do_rados(mon, ['-p', 'rbd', 'bench', '--no-cleanup', '1',
                               'write', '-b', '4096'])
    err = p.exitstatus
    log.info('err is %d' % err)

    # wait for some PG to have data that we can mess with
    pg, acting = wait_for_victim_pg(manager)
    osd = acting[0]

    log.info('messing with PG %s on osd %d' % (pg, osd))
    osd_remote, obj_path, obj_name = find_victim_object(ctx, pg, osd)

    test_repair_corrupted_obj(ctx, manager, pg, osd_remote, obj_path)
    test_repair_bad_omap(ctx, manager, pg, osd, obj_name)

    log.info('test successful!')
