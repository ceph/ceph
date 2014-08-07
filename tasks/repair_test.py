"""
Test pool repairing after objects are damaged.
"""
import logging
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def setup(ctx, config):
    """
    Create the repair test pool.
    """
    ctx.manager.wait_for_clean()
    ctx.manager.create_pool("repair_test_pool", 1)
    return "repair_test_pool"

def teardown(ctx, config, pool):
    """
    Remove the repair test pool.
    """
    ctx.manager.remove_pool(pool)
    ctx.manager.wait_for_clean()

def run_test(ctx, config, test):
    """
    Setup a test pool, run the test, and clean up afterwards.
   
    :param test: function passed in, called to run the test.
    """
    s = setup(ctx, config)
    test(ctx, config, s)
    teardown(ctx, config, s)

def choose_primary(ctx):
    """
    Select a primary for the next test.  This routine is typically passed to
    as a 'chooser function'
    """
    def ret(pool, num):
        """
        Return primary to test on.
        """
        log.info("Choosing primary")
        return ctx.manager.get_pg_primary(pool, num)
    return ret

def choose_replica(ctx):
    """
    Select a replica for the next test.  This routine is typically passed to
    as a 'chooser function'
    """
    def ret(pool, num):
        """
        Return replica to test on.
        """
        log.info("Choosing replica")
        return ctx.manager.get_pg_replica(pool, num)
    return ret

def trunc(ctx):
    """
    Truncate an object in the pool. This function is typically passed as a
    'corrupter function'
    """
    def ret(osd, pool, obj):
        """
        truncate an object
        """
        log.info("truncating object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['truncobj', pool, obj, '1'])
    return ret

def dataerr(ctx):
    """
    Generate an error on an object in the pool. This function is typically
    passed as a 'corrupter function'
    """
    def ret(osd, pool, obj):
        """
        cause an error in the data 
        """
        log.info("injecting data err on object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['injectdataerr', pool, obj])
    return ret

def mdataerr(ctx):
    """
    Generate an mdata error on an object in the pool. This function is
    typically passed as a 'corrupter function'
    """
    def ret(osd, pool, obj):
        """
        cause an error in the mdata
        """
        log.info("injecting mdata err on object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['injectmdataerr', pool, obj])
    return ret

def omaperr(ctx):
    """
    Cause data corruption by injecting omap errors into a pool.
    """
    def ret(osd, pool, obj):
        """
        Cause an omap error.
        """
        log.info("injecting omap err on object")
        return ctx.manager.osd_admin_socket(osd, ['setomapval', pool, obj, 'badkey', 'badval']);
    return ret

def gen_repair_test_1(corrupter, chooser, scrub_type):
    """
    Repair test.  Wrapper for the internal ret function.

    The internal ret function creates an object in the pool, corrupts it,
    scrubs it, and verifies that the pool is inconsistent.  It then repairs
    the pool, rescrubs it, and verifies that the pool is consistent

    :param corrupter: error generating function (truncate, data-error, or
     meta-data error, for example).
    :param chooser: osd type chooser (primary or replica)
    :param scrub_type: regular scrub or deep-scrub
    """
    def ret(ctx, config, pool):
        """
        :param pool: repair test pool
        """
        log.info("starting repair test type 1")
        victim_osd = chooser(pool, 0)

        # create object
        log.info("doing put")
        ctx.manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        corrupter(victim_osd, pool, 'repair_test_obj')

        # verify inconsistent
        log.info("scrubbing")
        ctx.manager.do_pg_scrub(pool, 0, scrub_type)

        assert ctx.manager.pg_inconsistent(pool, 0)

        # repair
        log.info("repairing")
        ctx.manager.do_pg_scrub(pool, 0, "repair")

        log.info("re-scrubbing")
        ctx.manager.do_pg_scrub(pool, 0, scrub_type)

        # verify consistent
        assert not ctx.manager.pg_inconsistent(pool, 0)
        log.info("done")
    return ret

def gen_repair_test_2(chooser):
    """
    Repair test.  Wrapper for the internal ret function.

    The internal ret function first creates a set of objects and
    sets the omap value.  It then corrupts an object, does both a scrub
    and a deep-scrub, and then corrupts more objects.  After that, it
    repairs the pool and makes sure that the pool is consistent some
    time after a deep-scrub. 

    :param chooser: primary or replica selection routine.
    """
    def ret(ctx, config, pool):
        """
        :param pool: repair test pool. 
        """
        log.info("starting repair test type 2")
        victim_osd = chooser(pool, 0)
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

        # create object
        log.info("doing put and setomapval")
        ctx.manager.do_put(pool, 'file1', '/etc/hosts')
        ctx.manager.do_rados(mon, ['-p', pool, 'setomapval', 'file1', 'key', 'val'])
        ctx.manager.do_put(pool, 'file2', '/etc/hosts')
        ctx.manager.do_put(pool, 'file3', '/etc/hosts')
        ctx.manager.do_put(pool, 'file4', '/etc/hosts')
        ctx.manager.do_put(pool, 'file5', '/etc/hosts')
        ctx.manager.do_rados(mon, ['-p', pool, 'setomapval', 'file5', 'key', 'val'])
        ctx.manager.do_put(pool, 'file6', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        omaperr(ctx)(victim_osd, pool, 'file1')

        # verify inconsistent
        log.info("scrubbing")
        ctx.manager.do_pg_scrub(pool, 0, 'deep-scrub')

        assert ctx.manager.pg_inconsistent(pool, 0)

        # Regression test for bug #4778, should still
        # be inconsistent after scrub
        ctx.manager.do_pg_scrub(pool, 0, 'scrub')

        assert ctx.manager.pg_inconsistent(pool, 0)

        # Additional corruptions including 2 types for file1
        log.info("corrupting more objects")
        dataerr(ctx)(victim_osd, pool, 'file1')
        mdataerr(ctx)(victim_osd, pool, 'file2')
        trunc(ctx)(victim_osd, pool, 'file3')
        omaperr(ctx)(victim_osd, pool, 'file6')

        # see still inconsistent
        log.info("scrubbing")
        ctx.manager.do_pg_scrub(pool, 0, 'deep-scrub')

        assert ctx.manager.pg_inconsistent(pool, 0)

        # repair
        log.info("repairing")
        ctx.manager.do_pg_scrub(pool, 0, "repair")

        # Let repair clear inconsistent flag
        time.sleep(10)

        # verify consistent
        assert not ctx.manager.pg_inconsistent(pool, 0)

        # In the future repair might determine state of
        # inconsistency itself, verify with a deep-scrub
        log.info("scrubbing")
        ctx.manager.do_pg_scrub(pool, 0, 'deep-scrub')

        # verify consistent
        assert not ctx.manager.pg_inconsistent(pool, 0)

        log.info("done")
    return ret

def task(ctx, config):
    """
    Test [deep] repair in several situations:
      Repair [Truncate, Data EIO, MData EIO] on [Primary|Replica]

    The config should be as follows:

      Must include the log-whitelist below
      Must enable filestore_debug_inject_read_err config

    example:

    tasks:
    - chef:
    - install:
    - ceph:
        log-whitelist: ['candidate had a read error', 'deep-scrub 0 missing, 1 inconsistent objects', 'deep-scrub 0 missing, 4 inconsistent objects', 'deep-scrub 1 errors', 'deep-scrub 4 errors', '!= known omap_digest', 'repair 0 missing, 1 inconsistent objects', 'repair 0 missing, 4 inconsistent objects', 'repair 1 errors, 1 fixed', 'repair 4 errors, 4 fixed', 'scrub 0 missing, 1 inconsistent', 'scrub 1 errors', 'size 1 != known size']
        conf:
          osd:
            filestore debug inject read err: true
    - repair_test:

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'repair_test task only accepts a dict for config'

    if not hasattr(ctx, 'manager'):
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
        ctx.manager = ceph_manager.CephManager(
            mon,
            ctx=ctx,
            logger=log.getChild('ceph_manager')
            )

    tests = [
        gen_repair_test_1(mdataerr(ctx), choose_primary(ctx), "scrub"),
        gen_repair_test_1(mdataerr(ctx), choose_replica(ctx), "scrub"),
        gen_repair_test_1(dataerr(ctx), choose_primary(ctx), "deep-scrub"),
        gen_repair_test_1(dataerr(ctx), choose_replica(ctx), "deep-scrub"),
        gen_repair_test_1(trunc(ctx), choose_primary(ctx), "scrub"),
        gen_repair_test_1(trunc(ctx), choose_replica(ctx), "scrub"),
        gen_repair_test_2(choose_primary(ctx)),
        gen_repair_test_2(choose_replica(ctx))
        ]

    for test in tests:
        run_test(ctx, config, test)
