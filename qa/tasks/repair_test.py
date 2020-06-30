"""
Test pool repairing after objects are damaged.
"""
import logging
import time

from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def choose_primary(manager, pool, num):
    """
    Return primary to test on.
    """
    log.info("Choosing primary")
    return manager.get_pg_primary(pool, num)


def choose_replica(manager, pool, num):
    """
    Return replica to test on.
    """
    log.info("Choosing replica")
    return manager.get_pg_replica(pool, num)


def trunc(manager, osd, pool, obj):
    """
    truncate an object
    """
    log.info("truncating object")
    return manager.osd_admin_socket(
        osd,
        ['truncobj', pool, obj, '1'])


def dataerr(manager, osd, pool, obj):
    """
    cause an error in the data
    """
    log.info("injecting data err on object")
    return manager.osd_admin_socket(
        osd,
        ['injectdataerr', pool, obj])


def mdataerr(manager, osd, pool, obj):
    """
    cause an error in the mdata
    """
    log.info("injecting mdata err on object")
    return manager.osd_admin_socket(
        osd,
        ['injectmdataerr', pool, obj])


def omaperr(manager, osd, pool, obj):
    """
    Cause an omap error.
    """
    log.info("injecting omap err on object")
    return manager.osd_admin_socket(osd, ['setomapval', pool, obj,
                                              'badkey', 'badval'])


def repair_test_1(manager, corrupter, chooser, scrub_type):
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
    manager.wait_for_clean()
    with manager.pool(pool, 1):

        log.info("starting repair test type 1")
        victim_osd = chooser(manager, pool, 0)

        # create object
        log.info("doing put")
        manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        corrupter(manager, victim_osd, pool, 'repair_test_obj')

        # verify inconsistent
        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)

        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # repair
        log.info("repairing")
        manager.do_pg_scrub(pool, 0, "repair")

        log.info("re-scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)

        # verify consistent
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)
        log.info("done")


def repair_test_2(ctx, manager, config, chooser):
    """
    First creates a set of objects and
    sets the omap value.  It then corrupts an object, does both a scrub
    and a deep-scrub, and then corrupts more objects.  After that, it
    repairs the pool and makes sure that the pool is consistent some
    time after a deep-scrub.

    :param chooser: primary or replica selection routine.
    """
    pool = "repair_pool_2"
    manager.wait_for_clean()
    with manager.pool(pool, 1):
        log.info("starting repair test type 2")
        victim_osd = chooser(manager, pool, 0)
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.keys()

        # create object
        log.info("doing put and setomapval")
        manager.do_put(pool, 'file1', '/etc/hosts')
        manager.do_rados(mon, ['-p', pool, 'setomapval', 'file1',
                                   'key', 'val'])
        manager.do_put(pool, 'file2', '/etc/hosts')
        manager.do_put(pool, 'file3', '/etc/hosts')
        manager.do_put(pool, 'file4', '/etc/hosts')
        manager.do_put(pool, 'file5', '/etc/hosts')
        manager.do_rados(mon, ['-p', pool, 'setomapval', 'file5',
                                   'key', 'val'])
        manager.do_put(pool, 'file6', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        omaperr(manager, victim_osd, pool, 'file1')

        # verify inconsistent
        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')

        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # Regression test for bug #4778, should still
        # be inconsistent after scrub
        manager.do_pg_scrub(pool, 0, 'scrub')

        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # Additional corruptions including 2 types for file1
        log.info("corrupting more objects")
        dataerr(manager, victim_osd, pool, 'file1')
        mdataerr(manager, victim_osd, pool, 'file2')
        trunc(manager, victim_osd, pool, 'file3')
        omaperr(manager, victim_osd, pool, 'file6')

        # see still inconsistent
        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')

        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # repair
        log.info("repairing")
        manager.do_pg_scrub(pool, 0, "repair")

        # Let repair clear inconsistent flag
        time.sleep(10)

        # verify consistent
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)

        # In the future repair might determine state of
        # inconsistency itself, verify with a deep-scrub
        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')

        # verify consistent
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)

        log.info("done")


def hinfoerr(manager, victim, pool, obj):
    """
    cause an error in the hinfo_key
    """
    log.info("remove the hinfo_key")
    manager.objectstore_tool(pool,
                             options='',
                             args='rm-attr hinfo_key',
                             object_name=obj,
                             osd=victim)


def repair_test_erasure_code(manager, corrupter, victim, scrub_type):
    """
    Creates an object in the pool, corrupts it,
    scrubs it, and verifies that the pool is inconsistent.  It then repairs
    the pool, rescrubs it, and verifies that the pool is consistent

    :param corrupter: error generating function.
    :param chooser: osd type chooser (primary or replica)
    :param scrub_type: regular scrub or deep-scrub
    """
    pool = "repair_pool_3"
    manager.wait_for_clean()
    with manager.pool(pool_name=pool, pg_num=1,
                          erasure_code_profile_name='default'):

        log.info("starting repair test for erasure code")

        # create object
        log.info("doing put")
        manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        # corrupt object
        log.info("corrupting object")
        corrupter(manager, victim, pool, 'repair_test_obj')

        # verify inconsistent
        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)

        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # repair
        log.info("repairing")
        manager.do_pg_scrub(pool, 0, "repair")

        log.info("re-scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)

        # verify consistent
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)
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
          - 'deep-scrub [0-9]+ errors'
          - '!= omap_digest'
          - '!= data_digest'
          - 'repair 0 missing, 1 inconsistent objects'
          - 'repair 0 missing, 4 inconsistent objects'
          - 'repair [0-9]+ errors, [0-9]+ fixed'
          - 'scrub 0 missing, 1 inconsistent objects'
          - 'scrub [0-9]+ errors'
          - 'size 1 != size'
          - 'attr name mismatch'
          - 'Regular scrub request, deep-scrub details will be lost'
          - 'candidate size [0-9]+ info size [0-9]+ mismatch'
        conf:
          osd:
            filestore debug inject read err: true
    - repair_test:

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'repair_test task only accepts a dict for config'

    manager = ctx.managers['ceph']
    manager.wait_for_all_osds_up()

    manager.raw_cluster_cmd('osd', 'set', 'noscrub')
    manager.raw_cluster_cmd('osd', 'set', 'nodeep-scrub')

    repair_test_1(manager, mdataerr, choose_primary, "scrub")
    repair_test_1(manager, mdataerr, choose_replica, "scrub")
    repair_test_1(manager, dataerr, choose_primary, "deep-scrub")
    repair_test_1(manager, dataerr, choose_replica, "deep-scrub")
    repair_test_1(manager, trunc, choose_primary, "scrub")
    repair_test_1(manager, trunc, choose_replica, "scrub")
    repair_test_2(ctx, manager, config, choose_primary)
    repair_test_2(ctx, manager, config, choose_replica)

    repair_test_erasure_code(manager, hinfoerr, 'primary', "deep-scrub")

    manager.raw_cluster_cmd('osd', 'unset', 'noscrub')
    manager.raw_cluster_cmd('osd', 'unset', 'nodeep-scrub')
