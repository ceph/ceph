"""Scrub testing for Crimson OSD (SeaStore)

Unlike scrub_test.py this task does NOT use the BlueStore FUSE objectstore
mount.  All corruptions are performed via OSD admin-socket commands that are
now registered in src/crimson/admin/osd_admin.cc:

  setomapval / rmomapkey / setomapheader  — omap corruption
  truncobj                                — change object size on one replica
  writeobj                                — overwrite bytes on one replica
  removeobj                               — remove object from one replica

All seven corruption scenarios from scrub_test.py are covered.
"""

import contextlib
import json
import logging
import time
from io import StringIO

from tasks import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers shared with scrub_test.py
# ---------------------------------------------------------------------------

def wait_for_victim_pg(manager, poolid):
    """Return a PG with some data and its acting set."""
    victim = None
    while victim is None:
        stats = manager.get_pg_stats()
        for pg in stats:
            pgid = str(pg['pgid'])
            pgpool = int(pgid.split('.')[0])
            if poolid != pgpool:
                continue
            size = pg['stat_sum']['num_bytes']
            if size > 0:
                victim = pg['pgid']
                acting = pg['acting']
                return victim, acting
        time.sleep(3)


def get_pgnum(pgid):
    pos = pgid.find('.')
    assert pos != -1
    return pgid[pos+1:]


def get_obj_name(manager, pool):
    """Return the name of any object in pool."""
    out = manager.do_rados(['ls'], pool=pool, stdout=StringIO())
    for line in out.stdout.getvalue().splitlines():
        name = line.strip()
        if name:
            return name
    raise RuntimeError('No objects found in pool %s' % pool)


def deep_scrub(manager, victim, pool):
    """Trigger a deep-scrub and assert the PG becomes inconsistent."""
    pgnum = get_pgnum(victim)
    manager.do_pg_scrub(pool, pgnum, 'deep-scrub')
    stats = manager.get_single_pg_stats(victim)
    inconsistent = stats['state'].find('+inconsistent') != -1
    assert inconsistent, \
        'Expected PG %s to be inconsistent after deep-scrub' % victim


def repair(manager, victim, pool):
    """Trigger a repair and assert the PG is no longer inconsistent."""
    pgnum = get_pgnum(victim)
    manager.do_pg_scrub(pool, pgnum, 'repair')
    stats = manager.get_single_pg_stats(victim)
    inconsistent = stats['state'].find('+inconsistent') != -1
    assert not inconsistent, \
        'Expected PG %s to be clean after repair' % victim


# ---------------------------------------------------------------------------
# Sub-tests
# ---------------------------------------------------------------------------

def test_repair_corrupted_obj(manager, pg, osd, obj_name, pool):
    """Corrupt object data on one OSD via truncobj, verify detection and repair.

    truncobj on a single OSD changes the object size on that replica only,
    causing a size_mismatch / data_digest_mismatch that scrub can detect.
    """
    log.info('test_repair_corrupted_obj: truncating %s on osd.%d', obj_name, osd)
    manager.osd_admin_socket(osd, ['truncobj', pool, obj_name, '1'])
    deep_scrub(manager, pg, pool)
    repair(manager, pg, pool)


def test_repair_bad_omap(manager, pg, osd, obj_name):
    """Corrupt omap on one OSD via admin socket, verify scrub detects and repairs."""
    pool = 'rbd'
    log.info('test_repair_bad_omap: fuzzing omap of %s on osd.%d', obj_name, osd)
    manager.osd_admin_socket(osd, ['rmomapkey', pool, obj_name, 'key'])
    manager.osd_admin_socket(osd, ['setomapval', pool, obj_name, 'badkey', 'badval'])
    manager.osd_admin_socket(osd, ['setomapheader', pool, obj_name, 'badhdr'])

    deep_scrub(manager, pg, pool)

    # Undo the omap corruption before repair (same rationale as scrub_test.py)
    manager.osd_admin_socket(osd, ['setomapheader', pool, obj_name, 'hdr'])
    manager.osd_admin_socket(osd, ['rmomapkey', pool, obj_name, 'badkey'])
    manager.osd_admin_socket(osd, ['setomapval', pool, obj_name, 'key', 'val'])
    repair(manager, pg, pool)


# ---------------------------------------------------------------------------
# MessUp — per-replica corruption via admin socket (mirrors classic MessUp)
# ---------------------------------------------------------------------------

class MessUp:
    """Corrupt a single OSD's copy of an object via admin-socket commands.

    Mirrors the classic scrub_test.MessUp but uses Crimson admin-socket
    commands instead of FUSE filesystem operations.
    """

    def __init__(self, manager, pool, osd_id, obj_name, omap_key, omap_val):
        self.manager = manager
        self.pool = pool
        self.osd_id = osd_id
        self.obj = obj_name
        self.omap_key = omap_key
        self.omap_val = omap_val

    # ---- file-like corruptions (via truncobj / writeobj / removeobj) -------

    @contextlib.contextmanager
    def remove(self):
        """Remove the object from this OSD only → 'missing' shard error."""
        self.manager.osd_admin_socket(
            self.osd_id, ['removeobj', self.pool, self.obj])
        yield ('missing',)
        # repair will restore the object from the other replicas; nothing to undo

    @contextlib.contextmanager
    def append(self):
        """Append 1 byte on this OSD → data_digest_mismatch + size_mismatch."""
        size = self.manager.do_rados(['stat', self.obj], pool=self.pool,
                                     stdout=StringIO())
        obj_size = int(size.stdout.getvalue().split(',')[1].strip().split()[0])
        self.manager.osd_admin_socket(
            self.osd_id, ['writeobj', self.pool, self.obj,
                          str(obj_size), '1'])
        yield ('data_digest_mismatch', 'size_mismatch')
        # repair restores from other replicas; nothing to undo

    @contextlib.contextmanager
    def truncate(self):
        """Truncate to 0 on this OSD → data_digest_mismatch + size_mismatch."""
        self.manager.osd_admin_socket(
            self.osd_id, ['truncobj', self.pool, self.obj, '0'])
        yield ('data_digest_mismatch', 'size_mismatch')

    @contextlib.contextmanager
    def change_obj(self):
        """Overwrite byte 0 on this OSD → data_digest_mismatch."""
        self.manager.osd_admin_socket(
            self.osd_id, ['writeobj', self.pool, self.obj, '0', '1'])
        yield ('data_digest_mismatch',)

    # ---- omap corruptions (via setomapval / rmomapkey) ---------------------

    @contextlib.contextmanager
    def rm_omap(self):
        self.manager.osd_admin_socket(
            self.osd_id, ['rmomapkey', self.pool, self.obj, self.omap_key])
        yield ('omap_digest_mismatch',)
        self.manager.osd_admin_socket(
            self.osd_id, ['setomapval', self.pool, self.obj,
                          self.omap_key, self.omap_val])

    @contextlib.contextmanager
    def add_omap(self):
        self.manager.osd_admin_socket(
            self.osd_id, ['setomapval', self.pool, self.obj, 'badkey', 'badval'])
        yield ('omap_digest_mismatch',)
        self.manager.osd_admin_socket(
            self.osd_id, ['rmomapkey', self.pool, self.obj, 'badkey'])

    @contextlib.contextmanager
    def change_omap(self):
        self.manager.osd_admin_socket(
            self.osd_id, ['setomapval', self.pool, self.obj,
                          self.omap_key, 'badval'])
        yield ('omap_digest_mismatch',)
        self.manager.osd_admin_socket(
            self.osd_id, ['setomapval', self.pool, self.obj,
                          self.omap_key, self.omap_val])


# ---------------------------------------------------------------------------
# InconsistentObjChecker — full parity with scrub_test.py
# ---------------------------------------------------------------------------

class InconsistentObjChecker:
    """Check the returned inconsistents / inconsistent info."""

    def __init__(self, osd, acting, obj_name):
        self.osd = osd
        self.acting = acting
        self.obj = obj_name
        assert self.osd in self.acting

    def basic_checks(self, inc):
        assert inc['object']['name'] == self.obj
        assert inc['object']['snap'] == 'head'
        assert len(inc['shards']) == len(self.acting), \
            'number of returned shards does not match acting set'

    def run(self, check, inc):
        getattr(self, check)(inc)

    def _check_errors(self, inc, err_name):
        bad_found = False
        good_found = False
        for shard in inc['shards']:
            log.info('shard = %r', shard)
            log.info('err = %s', err_name)
            assert 'osd' in shard
            osd = shard['osd']
            err = err_name in shard['errors']
            if osd == self.osd:
                assert not bad_found, 'multiple entries for the given OSD'
                assert err, "Didn't find '%s' in errors" % err_name
                bad_found = True
            else:
                assert osd in self.acting, 'shard not in acting set'
                assert not err, "Unexpected '%s' in errors" % err_name
                good_found = True
        assert bad_found, 'Shard for osd.%d not found' % self.osd
        assert good_found, 'No other acting shards found'

    def _check_attrs(self, inc, attr_name):
        bad_attr = None
        good_attr = None
        for shard in inc['shards']:
            log.info('shard = %r', shard)
            log.info('attr = %s', attr_name)
            assert 'osd' in shard
            osd = shard['osd']
            attr = shard.get(attr_name, False)
            if osd == self.osd:
                assert bad_attr is None, 'multiple entries for the given OSD'
                bad_attr = attr
            else:
                assert osd in self.acting, 'shard not in acting set'
                assert good_attr is None or good_attr == attr, \
                    'multiple good attrs found'
                good_attr = attr
        assert bad_attr is not None, 'bad %s not found' % attr_name
        assert good_attr is not None, 'good %s not found' % attr_name
        assert good_attr != bad_attr, \
            'bad attr identical to good: %s == %s' % (good_attr, bad_attr)

    def data_digest_mismatch(self, inc):
        assert 'data_digest_mismatch' in inc['errors']
        self._check_attrs(inc, 'data_digest')

    def missing(self, inc):
        assert 'missing' in inc['union_shard_errors']
        self._check_errors(inc, 'missing')

    def size_mismatch(self, inc):
        assert 'size_mismatch' in inc['errors']
        self._check_attrs(inc, 'size')

    def omap_digest_mismatch(self, inc):
        assert 'omap_digest_mismatch' in inc['errors']
        self._check_attrs(inc, 'omap_digest')


# ---------------------------------------------------------------------------
# test_list_inconsistent_obj — all 7 scenarios, full parity with scrub_test.py
# ---------------------------------------------------------------------------

def test_list_inconsistent_obj(manager, pg, acting, osd_id, obj_name):
    mon = manager.controller
    pool = 'rbd'
    omap_key = 'key'
    omap_val = 'val'
    manager.do_rados(['setomapval', obj_name, omap_key, omap_val], pool=pool)
    # Establish baseline digests
    pgnum = get_pgnum(pg)
    manager.do_pg_scrub(pool, pgnum, 'deep-scrub')

    messup = MessUp(manager, pool, osd_id, obj_name, omap_key, omap_val)
    for test in [messup.rm_omap, messup.add_omap, messup.change_omap,
                 messup.append, messup.truncate, messup.change_obj,
                 messup.remove]:
        with test() as checks:
            deep_scrub(manager, pg, pool)

            cmd = 'rados list-inconsistent-pg {pool} --format=json'.format(pool=pool)
            pgs = json.loads(mon.sh(cmd))
            assert pgs == [pg], 'Expected [%s], got %r' % (pg, pgs)

            cmd = 'rados list-inconsistent-obj {pg} --format=json'.format(pg=pg)
            objs = json.loads(mon.sh(cmd))
            assert len(objs['inconsistents']) == 1

            checker = InconsistentObjChecker(osd_id, acting, obj_name)
            inc_obj = objs['inconsistents'][0]
            log.info('inc = %r', inc_obj)
            checker.basic_checks(inc_obj)
            for check in checks:
                checker.run(check, inc_obj)


# ---------------------------------------------------------------------------
# Task entry point
# ---------------------------------------------------------------------------

def task(ctx, config):
    """
    Test [deep] scrub on Crimson OSD (SeaStore)

    All corruption scenarios from scrub_test.py are supported via
    OSD admin-socket commands (no FUSE mount required).

    tasks:
    - crimson_scrub_test:

    Requires in overrides:
      ceph:
        conf:
          osd:
            osd deep scrub update digest min age: 0
            osd skip data digest: false
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'crimson_scrub_test task only accepts a dict for configuration'

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    log.info('num_osds is %s', num_osds)

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
    )

    while len(manager.get_osd_status()['up']) < num_osds:
        time.sleep(10)

    manager.flush_pg_stats(range(num_osds))
    manager.wait_for_clean()

    osd_dump = manager.get_osd_dump_json()
    poolid = -1
    for p in osd_dump['pools']:
        if p['pool_name'] == 'rbd':
            poolid = p['pool']
            break
    assert poolid != -1, 'rbd pool not found'

    # Write some data
    p = manager.do_rados(['bench', '--no-cleanup', '1', 'write', '-b', '4096'],
                         pool='rbd')
    log.info('bench exitstatus: %d', p.exitstatus)

    pg, acting = wait_for_victim_pg(manager, poolid)
    osd = acting[0]
    log.info('messing with PG %s on osd.%d', pg, osd)

    obj_name = get_obj_name(manager, 'rbd')

    manager.do_rados(['setomapval', obj_name, 'key', 'val'], pool='rbd')
    manager.do_rados(['setomapheader', obj_name, 'hdr'], pool='rbd')

    # Establish baseline digests
    pgnum = get_pgnum(pg)
    manager.do_pg_scrub('rbd', pgnum, 'deep-scrub')

    test_repair_corrupted_obj(manager, pg, osd, obj_name, 'rbd')
    test_repair_bad_omap(manager, pg, osd, obj_name)
    test_list_inconsistent_obj(manager, pg, acting, osd, obj_name)

    log.info('crimson_scrub_test: all tests passed')
