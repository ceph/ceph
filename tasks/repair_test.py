"""
Test pool repairing after objects are damaged.
"""
import logging
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def choose_primary(ctx, pool, num):
    """
    Return primary to test on.
    """
    log.info("Choosing primary")
    return ctx.manager.get_pg_primary(pool, num)


def choose_replica(ctx, pool, num):
    """
    Return replica to test on.
    """
    log.info("Choosing replica")
    return ctx.manager.get_pg_replica(pool, num)


def trunc(ctx, osd, pool, obj):
    """
    truncate an object
    """
    log.info("truncating object")
    return ctx.manager.osd_admin_socket(
        osd,
        ['truncobj', pool, obj, '1'])


def dataerr(ctx, osd, pool, obj):
    """
    cause an error in the data
    """
    log.info("injecting data err on object")
    return ctx.manager.osd_admin_socket(
        osd,
        ['injectdataerr', pool, obj])


def mdataerr(ctx, osd, pool, obj):
    """
    cause an error in the mdata
    """
    log.info("injecting mdata err on object")
    return ctx.manager.osd_admin_socket(
        osd,
        ['injectmdataerr', pool, obj])


def omaperr(ctx, osd, pool, obj):
    """
    Cause an omap error.
    """
    log.info("injecting omap err on object")
    return ctx.manager.osd_admin_socket(osd, ['setomapval', pool, obj,
                                              'badkey', 'badval'])


def repair_test_1(ctx, corrupter, chooser, scrub_type):
    """
    Creates an object in the pool, corrupts it,
    scrubs it, and verifies that the pool is inconsistent.  It then repairs
    the pool, rescrubs it, and verifies that the pool is consistent

    :param corrupter: error generating function (truncate, data-error, or
     meta-data error, for example).
    :param chooser: osd type chooser (primary or replica)
    :param scrub_type: regular scrub or deep-scrub
    """
    pool = "repair_pool_1"
    ctx.manager.wait_for_clean()
    with ctx.manager.pool(pool, 1):

        log.info("starting repair test type 1")
        victim_osd = chooser(ctx, pool, 0)

        # create object
        log.info("doing put")
        ctx.manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        corrupter(ctx, victim_osd, pool, 'repair_test_obj')

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


def repair_test_2(ctx, config, chooser):
    """
    First creates a set of objects and
    sets the omap value.  It then corrupts an object, does both a scrub
    and a deep-scrub, and then corrupts more objects.  After that, it
    repairs the pool and makes sure that the pool is consistent some
    time after a deep-scrub.

    :param chooser: primary or replica selection routine.
    """
    pool = "repair_pool_2"
    ctx.manager.wait_for_clean()
    with ctx.manager.pool(pool, 1):
        log.info("starting repair test type 2")
        victim_osd = chooser(ctx, pool, 0)
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

        # create object
        log.info("doing put and setomapval")
        ctx.manager.do_put(pool, 'file1', '/etc/hosts')
        ctx.manager.do_rados(mon, ['-p', pool, 'setomapval', 'file1',
                                   'key', 'val'])
        ctx.manager.do_put(pool, 'file2', '/etc/hosts')
        ctx.manager.do_put(pool, 'file3', '/etc/hosts')
        ctx.manager.do_put(pool, 'file4', '/etc/hosts')
        ctx.manager.do_put(pool, 'file5', '/etc/hosts')
        ctx.manager.do_rados(mon, ['-p', pool, 'setomapval', 'file5',
                                   'key', 'val'])
        ctx.manager.do_put(pool, 'file6', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        omaperr(ctx, victim_osd, pool, 'file1')

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
        dataerr(ctx, victim_osd, pool, 'file1')
        mdataerr(ctx, victim_osd, pool, 'file2')
        trunc(ctx, victim_osd, pool, 'file3')
        omaperr(ctx, victim_osd, pool, 'file6')

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


def hinfoerr(ctx, victim, pool, obj):
    """
    cause an error in the hinfo_key
    """
    log.info("remove the hinfo_key")
    ctx.manager.objectstore_tool(pool,
                                 options='',
                                 args='rm-attr hinfo_key',
                                 object_name=obj,
                                 osd=victim)


def repair_test_erasure_code(ctx, corrupter, victim, scrub_type):
    """
    Creates an object in the pool, corrupts it,
    scrubs it, and verifies that the pool is inconsistent.  It then repairs
    the pool, rescrubs it, and verifies that the pool is consistent

    :param corrupter: error generating function.
    :param chooser: osd type chooser (primary or replica)
    :param scrub_type: regular scrub or deep-scrub
    """
    pool = "repair_pool_3"
    ctx.manager.wait_for_clean()
    with ctx.manager.pool(pool_name=pool, pg_num=1,
                          erasure_code_profile_name='default'):

        log.info("starting repair test for erasure code")

        # create object
        log.info("doing put")
        ctx.manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        corrupter(ctx, victim, pool, 'repair_test_obj')

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
        log-whitelist:
          - 'candidate had a stat error'
          - 'candidate had a read error'
          - 'deep-scrub 0 missing, 1 inconsistent objects'
          - 'deep-scrub 0 missing, 4 inconsistent objects'
          - 'deep-scrub 1 errors'
          - 'deep-scrub 4 errors'
          - '!= known omap_digest'
          - 'repair 0 missing, 1 inconsistent objects'
          - 'repair 0 missing, 4 inconsistent objects'
          - 'repair 1 errors, 1 fixed'
          - 'repair 4 errors, 4 fixed'
          - 'scrub 0 missing, 1 inconsistent'
          - 'scrub 1 errors'
          - 'size 1 != known size'
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

    ctx.manager.wait_for_all_up()

    ctx.manager.raw_cluster_cmd('osd', 'set', 'noscrub')
    ctx.manager.raw_cluster_cmd('osd', 'set', 'nodeep-scrub')

    repair_test_1(ctx, mdataerr, choose_primary, "scrub")
    repair_test_1(ctx, mdataerr, choose_replica, "scrub")
    repair_test_1(ctx, dataerr, choose_primary, "deep-scrub")
    repair_test_1(ctx, dataerr, choose_replica, "deep-scrub")
    repair_test_1(ctx, trunc, choose_primary, "scrub")
    repair_test_1(ctx, trunc, choose_replica, "scrub")
    repair_test_2(ctx, config, choose_primary)
    repair_test_2(ctx, config, choose_replica)

    repair_test_erasure_code(ctx, hinfoerr, 'primary', "deep-scrub")
