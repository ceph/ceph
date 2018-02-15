import logging
import ceph_manager
import time

from teuthology.task import Task

log = logging.getLogger(__name__)


class CrushErrors(Task):

    def begin(self):
        cluster_name = self.ctx.managers.keys()[0]
        self.manager = self.ctx.managers[cluster_name]
        self.manager.mark_out_osd(0)
        self.manager.raw_cluster_cmd('osd', 'pool', 'create', 'crushtestpool', '128')
        log.info("waiting for crush errors warning")
        grace = 30

        # wait a bit
        log.info('sleeping for {s} seconds'.format(
            s=grace))
        time.sleep(grace)
        health = self.manager.get_mon_health(True)
        log.info('got health %s' % health)
        if 'CRUSH_ERRORS' not in health['checks']:
            raise RuntimeError('expected CRUSH_ERRORS but got none')

        self.manager.mark_in_osd(0)

task = CrushErrors
