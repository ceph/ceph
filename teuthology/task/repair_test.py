import logging

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def setup(ctx, config):
    ctx.manager.wait_for_clean()
    ctx.manager.create_pool("repair_test_pool", 1)
    return "repair_test_pool"

def teardown(ctx, config, pool):
    ctx.manager.remove_pool(pool)
    ctx.manager.wait_for_clean()

def run_test(ctx, config, test):
    s = setup(ctx, config)
    test(ctx, config, s)
    teardown(ctx, config, s)

def choose_primary(ctx):
    def ret(pool, num):
        log.info("Choosing primary")
        return ctx.manager.get_pg_primary(pool, num)
    return ret

def choose_replica(ctx):
    def ret(pool, num):
        log.info("Choosing replica")
        return ctx.manager.get_pg_replica(pool, num)
    return ret

def trunc(ctx):
    def ret(osd, pool, obj):
        log.info("truncating object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['truncobj', pool, obj, '1'])
    return ret

def dataerr(ctx):
    def ret(osd, pool, obj):
        log.info("injecting data err on object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['injectdataerr', pool, obj])
    return ret

def mdataerr(ctx):
    def ret(osd, pool, obj):
        log.info("injecting mdata err on object")
        return ctx.manager.osd_admin_socket(
            osd,
            ['injectmdataerr', pool, obj])
    return ret

def gen_repair_test(corrupter, chooser, scrub_type):
    def ret(ctx, config, pool):
        log.info("starting repair test")
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

def task(ctx, config):
    """
    Test [deep] repair in several situations:
      Repair [Truncate, Data EIO, MData EIO] on [Primary|Replica]
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
        gen_repair_test(mdataerr(ctx), choose_primary(ctx), "scrub"),
        gen_repair_test(mdataerr(ctx), choose_replica(ctx), "scrub"),
        gen_repair_test(dataerr(ctx), choose_primary(ctx), "deep-scrub"),
        gen_repair_test(dataerr(ctx), choose_replica(ctx), "deep-scrub"),
        gen_repair_test(trunc(ctx), choose_primary(ctx), "scrub"),
        gen_repair_test(trunc(ctx), choose_replica(ctx), "scrub")
        ]

    for test in tests:
        run_test(ctx, config, test)
