import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestFSTop(CephFSTestCase):
    def test_fstop_non_existent_cluster(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", "stats")
        try:
            self.mount_a.run_shell(['cephfs-top',
                                    '--cluster=hpec',
                                    '--id=admin',
                                    '--selftest'])
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected cephfs-top command to fail.')
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", "stats")

    def test_fstop(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", "stats")
        self.mount_a.run_shell(['cephfs-top',
                                '--id=admin',
                                '--selftest'])
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", "stats")
