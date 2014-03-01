"""
Raise exceptions on osd coredumps or test err directories
"""
import contextlib
import logging
import time
from ..orchestra import run

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Die if {testdir}/err exists or if an OSD dumps core
    """
    if config is None:
        config = {}

    first_mon = teuthology.get_first_mon(ctx, config)
    mon = teuthology.get_single_remote_value(ctx, first_mon)

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    log.info('num_osds is %s' % num_osds)

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < num_osds:
        time.sleep(10)

    testdir = teuthology.get_testdir(ctx)

    while True:
        for i in range(num_osds):
            osd_remote = teuthology.get_single_remote_value(ctx, 'osd.%d' % i)
            p = osd_remote.run(
                args = [ 'test', '-e', '{tdir}/err'.format(tdir=testdir) ],
                wait=True,
                check_status=False,
            )
            exit_status = p.exitstatus

            if exit_status == 0:
                log.info("osd %d has an error" % i)
                raise Exception("osd %d error" % i)

            log_path = '/var/log/ceph/osd.%d.log' % (i)

            p = osd_remote.run(
                args = [
                         'tail', '-1', log_path,
                         run.Raw('|'),
                         'grep', '-q', 'end dump'
                       ],
                wait=True,
                check_status=False,
            )
            exit_status = p.exitstatus

            if exit_status == 0:
                log.info("osd %d dumped core" % i)
                raise Exception("osd %d dumped core" % i)

        time.sleep(5)
