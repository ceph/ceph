"""
Test pool repairing after objects are damaged — Crimson OSD (SeaStore) variant.

Differences from repair_test.py:
  - No erasure-code repair test (EC pool support in Crimson is still
    in progress; hinfoerr / objectstore_tool are BlueStore-only).
  - All corruption commands (truncobj, setomapval, injectdataerr,
    injectmdataerr) are issued via the OSD admin socket, which is now
    fully supported in Crimson (src/crimson/admin/osd_admin.cc).
"""
import logging
import time

log = logging.getLogger(__name__)


def choose_primary(manager, pool, num):
    log.info("Choosing primary")
    return manager.get_pg_primary(pool, num)


def choose_replica(manager, pool, num):
    log.info("Choosing replica")
    return manager.get_pg_replica(pool, num)


def dataerr(manager, osd, pool, obj):
    """Inject a data read error via admin socket (supported on Crimson)"""
    log.info("injecting data err on object")
    return manager.osd_admin_socket(osd, ['injectdataerr', pool, obj])


def mdataerr(manager, osd, pool, obj):
    """Inject a metadata read error via admin socket (supported on Crimson)"""
    log.info("injecting mdata err on object")
    return manager.osd_admin_socket(osd, ['injectmdataerr', pool, obj])


def trunc(manager, osd, pool, obj):
    """Truncate an object to 1 byte via the 'truncobj' admin-socket command."""
    log.info("truncating object via admin socket")
    return manager.osd_admin_socket(osd, ['truncobj', pool, obj, '1'])


def omaperr(manager, osd, pool, obj):
    """Cause an omap error via the 'setomapval' admin-socket command."""
    log.info("injecting omap err on object via admin socket")
    return manager.osd_admin_socket(osd, ['setomapval', pool, obj,
                                          'badkey', 'badval'])


def repair_test_1(manager, corrupter, chooser, scrub_type):
    """
    Creates an object, corrupts it, scrubs to verify inconsistency,
    repairs, re-scrubs to verify consistency.
    """
    pool = "repair_pool_1"
    manager.wait_for_clean()
    with manager.pool(pool, 1):
        log.info("starting repair test type 1")
        victim_osd = chooser(manager, pool, 0)

        log.info("doing put")
        manager.do_put(pool, 'repair_test_obj', '/etc/hosts')

        log.info("corrupting object")
        corrupter(manager, victim_osd, pool, 'repair_test_obj')

        log.info("scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        log.info("repairing")
        manager.do_pg_scrub(pool, 0, "repair")

        log.info("re-scrubbing")
        manager.do_pg_scrub(pool, 0, scrub_type)
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)
        log.info("done")


def repair_test_2(ctx, manager, config, chooser):
    """
    Creates multiple objects, corrupts them in several ways (omap,
    data, metadata), scrubs repeatedly, then repairs and verifies
    consistency.
    """
    pool = "repair_pool_2"
    manager.wait_for_clean()
    with manager.pool(pool, 1):
        log.info("starting repair test type 2")
        victim_osd = chooser(manager, pool, 0)

        log.info("doing put and setomapval")
        manager.do_put(pool, 'file1', '/etc/hosts')
        manager.do_rados(['setomapval', 'file1', 'key', 'val'], pool=pool)
        manager.do_put(pool, 'file2', '/etc/hosts')
        manager.do_put(pool, 'file3', '/etc/hosts')
        manager.do_put(pool, 'file4', '/etc/hosts')
        manager.do_put(pool, 'file5', '/etc/hosts')
        manager.do_rados(['setomapval', 'file5', 'key', 'val'], pool=pool)
        manager.do_put(pool, 'file6', '/etc/hosts')

        log.info("corrupting object (omap)")
        omaperr(manager, victim_osd, pool, 'file1')

        log.info("scrubbing (deep)")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        # Regression test for bug #4778 — should still be inconsistent after scrub
        manager.do_pg_scrub(pool, 0, 'scrub')
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        log.info("corrupting more objects")
        dataerr(manager, victim_osd, pool, 'file1')
        mdataerr(manager, victim_osd, pool, 'file2')
        trunc(manager, victim_osd, pool, 'file3')
        omaperr(manager, victim_osd, pool, 'file6')

        log.info("scrubbing (deep)")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' in s)

        log.info("repairing")
        manager.do_pg_scrub(pool, 0, "repair")

        # Let repair clear inconsistent flag
        time.sleep(10)
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)

        log.info("scrubbing (deep) after repair")
        manager.do_pg_scrub(pool, 0, 'deep-scrub')
        manager.with_pg_state(pool, 0, lambda s: 'inconsistent' not in s)
        log.info("done")


def task(ctx, config):
    """
    Test [deep] repair on Crimson OSD (SeaStore).

    Supports dataerr / mdataerr (via admin socket) and trunc / omaperr
    (via rados CLI).  Erasure-code repair is not tested here.

    tasks:
    - crimson_repair_test:

    Requires in overrides:
      ceph:
        conf:
          osd:
            osd objectstore: seastore
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'crimson_repair_test task only accepts a dict for config'

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

    manager.raw_cluster_cmd('osd', 'unset', 'noscrub')
    manager.raw_cluster_cmd('osd', 'unset', 'nodeep-scrub')
