"""
Special case divergence test with ceph-objectstore-tool export/remove/import
"""
import logging
import time
from cStringIO import StringIO

from teuthology import misc as teuthology
from util.rados import rados
import os


log = logging.getLogger(__name__)


def task(ctx, config):
    """
    Test handling of divergent entries with prior_version
    prior to log_tail and a ceph-objectstore-tool export/import

    overrides:
      ceph:
        conf:
          osd:
            debug osd: 5

    Requires 3 osds on a single test node.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'divergent_priors task only accepts a dict for configuration'

    manager = ctx.managers['ceph']

    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)
    manager.flush_pg_stats([0, 1, 2])
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'noin')
    manager.raw_cluster_cmd('osd', 'set', 'nodown')
    manager.wait_for_clean()

    # something that is always there
    dummyfile = '/etc/fstab'
    dummyfile2 = '/etc/resolv.conf'
    testdir = teuthology.get_testdir(ctx)

    # create 1 pg pool
    log.info('creating foo')
    manager.raw_cluster_cmd('osd', 'pool', 'create', 'foo', '1')

    osds = [0, 1, 2]
    for i in osds:
        manager.set_config(i, osd_min_pg_log_entries=10)
        manager.set_config(i, osd_max_pg_log_entries=10)
        manager.set_config(i, osd_pg_log_trim_min=5)

    # determine primary
    divergent = manager.get_pg_primary('foo', 0)
    log.info("primary and soon to be divergent is %d", divergent)
    non_divergent = list(osds)
    non_divergent.remove(divergent)

    log.info('writing initial objects')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    # write 100 objects
    for i in range(100):
        rados(ctx, mon, ['-p', 'foo', 'put', 'existing_%d' % i, dummyfile])

    manager.wait_for_clean()

    # blackhole non_divergent
    log.info("blackholing osds %s", str(non_divergent))
    for i in non_divergent:
        manager.set_config(i, objectstore_blackhole=1)

    DIVERGENT_WRITE = 5
    DIVERGENT_REMOVE = 5
    # Write some soon to be divergent
    log.info('writing divergent objects')
    for i in range(DIVERGENT_WRITE):
        rados(ctx, mon, ['-p', 'foo', 'put', 'existing_%d' % i,
                         dummyfile2], wait=False)
    # Remove some soon to be divergent
    log.info('remove divergent objects')
    for i in range(DIVERGENT_REMOVE):
        rados(ctx, mon, ['-p', 'foo', 'rm',
                         'existing_%d' % (i + DIVERGENT_WRITE)], wait=False)
    time.sleep(10)
    mon.run(
        args=['killall', '-9', 'rados'],
        wait=True,
        check_status=False)

    # kill all the osds but leave divergent in
    log.info('killing all the osds')
    for i in osds:
        manager.kill_osd(i)
    for i in osds:
        manager.mark_down_osd(i)
    for i in non_divergent:
        manager.mark_out_osd(i)

    # bring up non-divergent
    log.info("bringing up non_divergent %s", str(non_divergent))
    for i in non_divergent:
        manager.revive_osd(i)
    for i in non_divergent:
        manager.mark_in_osd(i)

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    objname = "existing_%d" % (DIVERGENT_WRITE + DIVERGENT_REMOVE)
    log.info('writing non-divergent object ' + objname)
    rados(ctx, mon, ['-p', 'foo', 'put', objname, dummyfile2])

    manager.wait_for_recovery()

    # ensure no recovery of up osds first
    log.info('delay recovery')
    for i in non_divergent:
        manager.wait_run_admin_socket(
            'osd', i, ['set_recovery_delay', '100000'])

    # bring in our divergent friend
    log.info("revive divergent %d", divergent)
    manager.raw_cluster_cmd('osd', 'set', 'noup')
    manager.revive_osd(divergent)

    log.info('delay recovery divergent')
    manager.wait_run_admin_socket(
        'osd', divergent, ['set_recovery_delay', '100000'])

    manager.raw_cluster_cmd('osd', 'unset', 'noup')
    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)

    log.info('wait for peering')
    rados(ctx, mon, ['-p', 'foo', 'put', 'foo', dummyfile])

    # At this point the divergent_priors should have been detected

    log.info("killing divergent %d", divergent)
    manager.kill_osd(divergent)

    # Export a pg
    (exp_remote,) = ctx.\
        cluster.only('osd.{o}'.format(o=divergent)).remotes.keys()
    FSPATH = manager.get_filepath()
    JPATH = os.path.join(FSPATH, "journal")
    prefix = ("sudo adjust-ulimits ceph-objectstore-tool "
              "--data-path {fpath} --journal-path {jpath} "
              "--log-file="
              "/var/log/ceph/objectstore_tool.$$.log ".
              format(fpath=FSPATH, jpath=JPATH))
    pid = os.getpid()
    expfile = os.path.join(testdir, "exp.{pid}.out".format(pid=pid))
    cmd = ((prefix + "--op export-remove --pgid 2.0 --file {file}").
           format(id=divergent, file=expfile))
    proc = exp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0

    cmd = ((prefix + "--op import --file {file}").
           format(id=divergent, file=expfile))
    proc = exp_remote.run(args=cmd, wait=True,
                          check_status=False, stdout=StringIO())
    assert proc.exitstatus == 0

    log.info("reviving divergent %d", divergent)
    manager.revive_osd(divergent)
    manager.wait_run_admin_socket('osd', divergent, ['dump_ops_in_flight'])
    time.sleep(20);

    log.info('allowing recovery')
    # Set osd_recovery_delay_start back to 0 and kick the queue
    for i in osds:
        manager.raw_cluster_cmd('tell', 'osd.%d' % i, 'debug',
                                    'kick_recovery_wq', ' 0')

    log.info('reading divergent objects')
    for i in range(DIVERGENT_WRITE + DIVERGENT_REMOVE):
        exit_status = rados(ctx, mon, ['-p', 'foo', 'get', 'existing_%d' % i,
                                       '/tmp/existing'])
        assert exit_status is 0

    cmd = 'rm {file}'.format(file=expfile)
    exp_remote.run(args=cmd, wait=True)
    log.info("success")
