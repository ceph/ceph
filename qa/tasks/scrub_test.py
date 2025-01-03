"""Scrub testing"""

import contextlib
import json
import logging
import os
import time
import tempfile

from tasks import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def wait_for_victim_pg(manager, poolid):
    """Return a PG with some data and its acting set"""
    # wait for some PG to have data that we can mess with
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


def find_victim_object(ctx, pg, osd):
    """Return a file to be fuzzed"""
    (osd_remote,) = ctx.cluster.only('osd.%d' % osd).remotes.keys()
    data_path = os.path.join(
        '/var/lib/ceph/osd',
        'ceph-{id}'.format(id=osd),
        'fuse',
        '{pg}_head'.format(pg=pg),
        'all',
        )

    # fuzz time
    ls_out = osd_remote.sh('sudo ls %s' % data_path)

    # find an object file we can mess with (and not the pg info object)
    osdfilename = next(line for line in ls_out.split('\n')
                       if not line.endswith('::::head#'))
    assert osdfilename is not None

    # Get actual object name from osd stored filename
    objname = osdfilename.split(':')[4]
    return osd_remote, os.path.join(data_path, osdfilename), objname


def corrupt_file(osd_remote, path):
    # put a single \0 at the beginning of the file
    osd_remote.run(
        args=['sudo', 'dd',
              'if=/dev/zero',
              'of=%s/data' % path,
              'bs=1', 'count=1', 'conv=notrunc']
    )


def get_pgnum(pgid):
    pos = pgid.find('.')
    assert pos != -1
    return pgid[pos+1:]


def deep_scrub(manager, victim, pool):
    # scrub, verify inconsistent
    pgnum = get_pgnum(victim)
    manager.do_pg_scrub(pool, pgnum, 'deep-scrub')

    stats = manager.get_single_pg_stats(victim)
    inconsistent = stats['state'].find('+inconsistent') != -1
    assert inconsistent


def repair(manager, victim, pool):
    # repair, verify no longer inconsistent
    pgnum = get_pgnum(victim)
    manager.do_pg_scrub(pool, pgnum, 'repair')

    stats = manager.get_single_pg_stats(victim)
    inconsistent = stats['state'].find('+inconsistent') != -1
    assert not inconsistent


def test_repair_corrupted_obj(ctx, manager, pg, osd_remote, obj_path, pool):
    corrupt_file(osd_remote, obj_path)
    deep_scrub(manager, pg, pool)
    repair(manager, pg, pool)


def test_repair_bad_omap(ctx, manager, pg, osd, objname):
    # Test deep-scrub with various omap modifications
    # Modify omap on specific osd
    log.info('fuzzing omap of %s' % objname)
    manager.osd_admin_socket(osd, ['rmomapkey', 'rbd', objname, 'key'])
    manager.osd_admin_socket(osd, ['setomapval', 'rbd', objname,
                                   'badkey', 'badval'])
    manager.osd_admin_socket(osd, ['setomapheader', 'rbd', objname, 'badhdr'])

    deep_scrub(manager, pg, 'rbd')
    # please note, the repair here is errnomous, it rewrites the correct omap
    # digest and data digest on the replicas with the corresponding digests
    # from the primary osd which is hosting the victim object, see
    # find_victim_object().
    # so we need to either put this test and the end of this task or
    # undo the mess-up manually before the "repair()" that just ensures
    # the cleanup is sane, otherwise the succeeding tests will fail. if they
    # try set "badkey" in hope to get an "inconsistent" pg with a deep-scrub.
    manager.osd_admin_socket(osd, ['setomapheader', 'rbd', objname, 'hdr'])
    manager.osd_admin_socket(osd, ['rmomapkey', 'rbd', objname, 'badkey'])
    manager.osd_admin_socket(osd, ['setomapval', 'rbd', objname,
                                   'key', 'val'])
    repair(manager, pg, 'rbd')


class MessUp:
    def __init__(self, manager, osd_remote, pool, osd_id,
                 obj_name, obj_path, omap_key, omap_val):
        self.manager = manager
        self.osd = osd_remote
        self.pool = pool
        self.osd_id = osd_id
        self.obj = obj_name
        self.path = obj_path
        self.omap_key = omap_key
        self.omap_val = omap_val

    @contextlib.contextmanager
    def _test_with_file(self, messup_cmd, *checks):
        temp = tempfile.mktemp()
        backup_cmd = ['sudo', 'cp', os.path.join(self.path, 'data'), temp]
        self.osd.run(args=backup_cmd)
        self.osd.run(args=messup_cmd.split())
        yield checks
        create_cmd = ['sudo', 'mkdir', self.path]
        self.osd.run(args=create_cmd, check_status=False)
        restore_cmd = ['sudo', 'cp', temp, os.path.join(self.path, 'data')]
        self.osd.run(args=restore_cmd)

    def remove(self):
        cmd = 'sudo rmdir {path}'.format(path=self.path)
        return self._test_with_file(cmd, 'missing')

    def append(self):
        cmd = 'sudo dd if=/dev/zero of={path}/data bs=1 count=1 ' \
              'conv=notrunc oflag=append'.format(path=self.path)
        return self._test_with_file(cmd,
                                    'data_digest_mismatch',
                                    'size_mismatch')

    def truncate(self):
        cmd = 'sudo dd if=/dev/null of={path}/data'.format(path=self.path)
        return self._test_with_file(cmd,
                                    'data_digest_mismatch',
                                    'size_mismatch')

    def change_obj(self):
        cmd = 'sudo dd if=/dev/zero of={path}/data bs=1 count=1 ' \
              'conv=notrunc'.format(path=self.path)
        return self._test_with_file(cmd,
                                    'data_digest_mismatch')

    @contextlib.contextmanager
    def rm_omap(self):
        cmd = ['rmomapkey', self.pool, self.obj, self.omap_key]
        self.manager.osd_admin_socket(self.osd_id, cmd)
        yield ('omap_digest_mismatch',)
        cmd = ['setomapval', self.pool, self.obj,
               self.omap_key, self.omap_val]
        self.manager.osd_admin_socket(self.osd_id, cmd)

    @contextlib.contextmanager
    def add_omap(self):
        cmd = ['setomapval', self.pool, self.obj, 'badkey', 'badval']
        self.manager.osd_admin_socket(self.osd_id, cmd)
        yield ('omap_digest_mismatch',)
        cmd = ['rmomapkey', self.pool, self.obj, 'badkey']
        self.manager.osd_admin_socket(self.osd_id, cmd)

    @contextlib.contextmanager
    def change_omap(self):
        cmd = ['setomapval', self.pool, self.obj, self.omap_key, 'badval']
        self.manager.osd_admin_socket(self.osd_id, cmd)
        yield ('omap_digest_mismatch',)
        cmd = ['setomapval', self.pool, self.obj, self.omap_key, self.omap_val]
        self.manager.osd_admin_socket(self.osd_id, cmd)


class InconsistentObjChecker:
    """Check the returned inconsistents/inconsistent info"""

    def __init__(self, osd, acting, obj_name):
        self.osd = osd
        self.acting = acting
        self.obj = obj_name
        assert self.osd in self.acting

    def basic_checks(self, inc):
        assert inc['object']['name'] == self.obj
        assert inc['object']['snap'] == "head"
        assert len(inc['shards']) == len(self.acting), \
            "the number of returned shard does not match with the acting set"

    def run(self, check, inc):
        func = getattr(self, check)
        func(inc)

    def _check_errors(self, inc, err_name):
        bad_found = False
        good_found = False
        for shard in inc['shards']:
            log.info('shard = %r' % shard)
            log.info('err = %s' % err_name)
            assert 'osd' in shard
            osd = shard['osd']
            err = err_name in shard['errors']
            if osd == self.osd:
                assert bad_found is False, \
                    "multiple entries found for the given OSD"
                assert err is True, \
                    "Didn't find '{err}' in errors".format(err=err_name)
                bad_found = True
            else:
                assert osd in self.acting, "shard not in acting set"
                assert err is False, \
                    "Expected '{err}' in errors".format(err=err_name)
                good_found = True
        assert bad_found is True, \
            "Shard for osd.{osd} not found".format(osd=self.osd)
        assert good_found is True, \
            "No other acting shards found"

    def _check_attrs(self, inc, attr_name):
        bad_attr = None
        good_attr = None
        for shard in inc['shards']:
            log.info('shard = %r' % shard)
            log.info('attr = %s' % attr_name)
            assert 'osd' in shard
            osd = shard['osd']
            attr = shard.get(attr_name, False)
            if osd == self.osd:
                assert bad_attr is None, \
                    "multiple entries found for the given OSD"
                bad_attr = attr
            else:
                assert osd in self.acting, "shard not in acting set"
                assert good_attr is None or good_attr == attr, \
                    "multiple good attrs found"
                good_attr = attr
        assert bad_attr is not None, \
            "bad {attr} not found".format(attr=attr_name)
        assert good_attr is not None, \
            "good {attr} not found".format(attr=attr_name)
        assert good_attr != bad_attr, \
            "bad attr is identical to the good ones: " \
            "{0} == {1}".format(good_attr, bad_attr)

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


def test_list_inconsistent_obj(ctx, manager, osd_remote, pg, acting, osd_id,
                               obj_name, obj_path):
    mon = manager.controller
    pool = 'rbd'
    omap_key = 'key'
    omap_val = 'val'
    manager.do_rados(['setomapval', obj_name, omap_key, omap_val], pool=pool)
    # Update missing digests, requires "osd deep scrub update digest min age: 0"
    pgnum = get_pgnum(pg)
    manager.do_pg_scrub(pool, pgnum, 'deep-scrub')

    messup = MessUp(manager, osd_remote, pool, osd_id, obj_name, obj_path,
                    omap_key, omap_val)
    for test in [messup.rm_omap, messup.add_omap, messup.change_omap,
                 messup.append, messup.truncate, messup.change_obj,
                 messup.remove]:
        with test() as checks:
            deep_scrub(manager, pg, pool)
            cmd = 'rados list-inconsistent-pg {pool} ' \
                  '--format=json'.format(pool=pool)
            pgs = json.loads(mon.sh(cmd))
            assert pgs == [pg]

            cmd = 'rados list-inconsistent-obj {pg} ' \
                  '--format=json'.format(pg=pg)
            objs = json.loads(mon.sh(cmd))
            assert len(objs['inconsistents']) == 1

            checker = InconsistentObjChecker(osd_id, acting, obj_name)
            inc_obj = objs['inconsistents'][0]
            log.info('inc = %r', inc_obj)
            checker.basic_checks(inc_obj)
            for check in checks:
                checker.run(check, inc_obj)


def task(ctx, config):
    """
    Test [deep] scrub

    tasks:
    - chef:
    - install:
    - ceph:
        log-ignorelist:
        - '!= data_digest'
        - '!= omap_digest'
        - '!= size'
        - deep-scrub 0 missing, 1 inconsistent objects
        - deep-scrub [0-9]+ errors
        - repair 0 missing, 1 inconsistent objects
        - repair [0-9]+ errors, [0-9]+ fixed
        - shard [0-9]+ .* : missing
        - deep-scrub 1 missing, 1 inconsistent objects
        - does not match object info size
        - attr name mistmatch
        - deep-scrub 1 missing, 0 inconsistent objects
        - failed to pick suitable auth object
        - candidate size [0-9]+ info size [0-9]+ mismatch
      conf:
        osd:
          osd deep scrub update digest min age: 0
    - scrub_test:
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'scrub_test task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

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
        manager.raw_cluster_cmd('tell', 'osd.%d' % i, 'injectargs',
                                '--', '--osd-objectstore-fuse')
    manager.flush_pg_stats(range(num_osds))
    manager.wait_for_clean()

    osd_dump = manager.get_osd_dump_json()
    poolid = -1
    for p in osd_dump['pools']:
        if p['pool_name'] == 'rbd':
            poolid = p['pool']
            break
    assert poolid != -1

    # write some data
    p = manager.do_rados(['bench', '--no-cleanup', '1', 'write', '-b', '4096'], pool='rbd')
    log.info('err is %d' % p.exitstatus)

    # wait for some PG to have data that we can mess with
    pg, acting = wait_for_victim_pg(manager, poolid)
    osd = acting[0]

    osd_remote, obj_path, obj_name = find_victim_object(ctx, pg, osd)
    manager.do_rados(['setomapval', obj_name, 'key', 'val'], pool='rbd')
    log.info('err is %d' % p.exitstatus)
    manager.do_rados(['setomapheader', obj_name, 'hdr'], pool='rbd')
    log.info('err is %d' % p.exitstatus)

    # Update missing digests, requires "osd deep scrub update digest min age: 0"
    pgnum = get_pgnum(pg)
    manager.do_pg_scrub('rbd', pgnum, 'deep-scrub')

    log.info('messing with PG %s on osd %d' % (pg, osd))
    test_repair_corrupted_obj(ctx, manager, pg, osd_remote, obj_path, 'rbd')
    test_repair_bad_omap(ctx, manager, pg, osd, obj_name)
    test_list_inconsistent_obj(ctx, manager, osd_remote, pg, acting, osd,
                               obj_name, obj_path)
    log.info('test successful!')

    # shut down fuse mount
    for i in range(num_osds):
        manager.raw_cluster_cmd('tell', 'osd.%d' % i, 'injectargs',
                                '--', '--no-osd-objectstore-fuse')
    time.sleep(5)
    log.info('done')
