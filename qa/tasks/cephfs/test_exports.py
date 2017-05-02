import logging
import time
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestExports(CephFSTestCase):
    def test_export_pin(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("FUSE needed for measuring op counts")

        self.fs.set_allow_multimds(True)
        self.fs.set_max_mds(2)

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "-p", "1/2/3"])
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 0)

        # NOP
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "-1", "1"])
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 0)

        # NOP (rank < -1)
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "-2341", "1"])
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 0)

        # pin /1 to rank 1
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "1", "1"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 1)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        log.info(subtrees)
        self.assertTrue(len(subtrees) == 1 and subtrees[0]['auth_first'] == 1)

        # Check export_targets is set properly
        status = self.fs.status()
        log.info(status)
        r0 = status.get_rank(self.fs.id, 0)
        self.assertTrue(sorted(r0['export_targets']) == [1])

        # redundant pin /1/2 to rank 1
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "1", "1/2"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 1)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 2 and subtrees[0]['auth_first'] == 1 and subtrees[1]['auth_first'] == 1)

        # change pin /1/2 to rank 0
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "0", "1/2"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 2)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1/2'), subtrees)
        self.assertTrue(len(subtrees) == 1 and subtrees[0]['auth_first'] == 0)

        # change pin /1/2/3 to (presently) non-existent rank 2
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "2", "1/2/3"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 2)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1/2'), subtrees)
        self.assertTrue(len(subtrees) == 1 and subtrees[0]['auth_first'] == 0)

        # change pin /1/2 back to rank 1
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "1", "1/2"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 1)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 2 and subtrees[0]['auth_first'] == 1 and subtrees[1]['auth_first'] == 1)

        # add another directory pinned to 1
        self.mount_a.run_shell(["mkdir", "-p", "1/4/5"])
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "1", "1/4/5"])

        # change pin /1 to 0
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "0", "1"])
        time.sleep(20)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'] == '/1', subtrees)
        self.assertTrue(len(subtrees) == 1)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 1)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1/'), subtrees)
        self.assertTrue(len(subtrees) == 2 and subtrees[0]['auth_first'] == 1 and subtrees[1]['auth_first'] == 1) # /1/2 and /1/4/5

        # change pin /1/2 to default (-1); does the subtree root properly respect it's parent pin?
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "-1", "1/2"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 1)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/1'), subtrees)
        self.assertTrue(len(subtrees) == 1) # /1/4 still here!

        if len(list(status.get_standbys())):
            self.fs.set_max_mds(3)
            time.sleep(10)
            status = self.fs.status()
            subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 2)['name'])
            log.info(subtrees)
            subtrees = filter(lambda s: s['dir']['path'].startswith('/1/2/3'), subtrees)
            self.assertTrue(len(subtrees) == 1)

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
        self.mount_a.run_shell(["setfattr", "-n", "ceph.dir.pin", "-v", "1", "a"])
        time.sleep(10);
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/a'), subtrees)
        self.assertTrue(len(subtrees) == 1 and subtrees[0]['auth_first'] == 1)
        self.mount_a.run_shell(["mv", "aa", "a/b/"])
        time.sleep(10)
        subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, 0)['name'])
        log.info(subtrees)
        subtrees = filter(lambda s: s['dir']['path'].startswith('/a'), subtrees)
        self.assertTrue(len(subtrees) == 1 and subtrees[0]['auth_first'] == 1)
