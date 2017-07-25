import logging
import time
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestExports(CephFSTestCase):
    MDSS_REQUIRED = 2

    def _wait_subtrees(self, status, rank, test):
        timeout = 30
        pause = 2
        test = sorted(test)
        for i in range(timeout/pause):
            subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, rank)['name'])
            subtrees = filter(lambda s: s['dir']['path'].startswith('/'), subtrees)
            filtered = sorted([(s['dir']['path'], s['auth_first']) for s in subtrees])
            log.info("%s =?= %s", filtered, test)
            if filtered == test:
                return subtrees
            time.sleep(pause)
        raise RuntimeError("rank {0} failed to reach desired subtree state", rank)

    def test_export_pin(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "-p", "1/2/3"])
        self._wait_subtrees(status, 0, [])

        # NOP
        self.mount_a.setfattr("1", "ceph.dir.pin", "-1")
        self._wait_subtrees(status, 0, [])

        # NOP (rank < -1)
        self.mount_a.setfattr("1", "ceph.dir.pin", "-2341")
        self._wait_subtrees(status, 0, [])

        # pin /1 to rank 1
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1)])

        # Check export_targets is set properly
        status = self.fs.status()
        log.info(status)
        r0 = status.get_rank(self.fs.id, 0)
        self.assertTrue(sorted(r0['export_targets']) == [1])

        # redundant pin /1/2 to rank 1
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1)])

        # change pin /1/2 to rank 0
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 0)])
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])

        # change pin /1/2/3 to (presently) non-existent rank 2
        self.mount_a.setfattr("1/2/3", "ceph.dir.pin", "2")
        self._wait_subtrees(status, 0, [('/1', 1), ('/1/2', 0)])
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 0)])

        # change pin /1/2 back to rank 1
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1)])

        # add another directory pinned to 1
        self.mount_a.run_shell(["mkdir", "-p", "1/4/5"])
        self.mount_a.setfattr("1/4/5", "ceph.dir.pin", "1")
        self._wait_subtrees(status, 1, [('/1', 1), ('/1/2', 1), ('/1/4/5', 1)])

        # change pin /1 to 0
        self.mount_a.setfattr("1", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/2', 1), ('/1/4/5', 1)])

        # change pin /1/2 to default (-1); does the subtree root properly respect it's parent pin?
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "-1")
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1)])

        if len(list(status.get_standbys())):
            self.fs.set_max_mds(3)
            self.fs.wait_for_state('up:active', rank=2)
            self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2)])

            # Check export_targets is set properly
            status = self.fs.status()
            log.info(status)
            r0 = status.get_rank(self.fs.id, 0)
            self.assertTrue(sorted(r0['export_targets']) == [1,2])
            r1 = status.get_rank(self.fs.id, 1)
            self.assertTrue(sorted(r1['export_targets']) == [0])
            r2 = status.get_rank(self.fs.id, 2)
            self.assertTrue(sorted(r2['export_targets']) == [])

        # Test rename
        self.mount_a.run_shell(["mkdir", "-p", "a/b", "aa/bb"])
        self.mount_a.setfattr("a", "ceph.dir.pin", "1")
        self.mount_a.setfattr("aa/bb", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2), ('/a', 1), ('/aa/bb', 0)])
        self.mount_a.run_shell(["mv", "aa", "a/b/"])
        self._wait_subtrees(status, 0, [('/1', 0), ('/1/4/5', 1), ('/1/2/3', 2), ('/a', 1), ('/a/b/aa/bb', 0)])
