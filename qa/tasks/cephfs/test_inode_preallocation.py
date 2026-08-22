import logging
import random
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

INO_PREALLOC_KILLPOINT = [
    "INO_PREALLOC_NULL",
    "REPLY_EXTRA_BL",
    "HANDLE_CLIENT_REQUEST",
    "DISPATCH_CLIENT_REQUEST",
    "PREPARE_NEW_INODE",
]

class TestInodePreallocationKillpoints(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def _run_workload(self, killpoint):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        rinfo = self.fs.get_rank(rank=1, status=status)

        self.fs.set_config("mds_kill_ino_prealloc_at", str(killpoint), rank=1, status=status)
        self.fs.set_config("mds_client_prealloc_inos", str(random.randint(1000, 10000)), rank=1, status=status)
        self.fs.set_config("mds_client_delegate_inos_pct", str(random.randint(50, 100)), rank=1, status=status)
        self.config_set('mds', 'mds_allow_async_dirops', True)

        self.mount_a.run_shell_payload("mkdir top && touch top/file")
        self.mount_a.setfattr("top", "ceph.dir.pin", "1")
        self._wait_subtrees([('/top', 1)], status=status, rank=0)
        p = self.mount_a.open_n_background("top", 1000)

        # Expecting inode preallocation MDS crash
        self.fs.wait_for_death(timeout=240, status=status, rank=1)
        self.delete_mds_coredump(rinfo['name'])
        self.fs.mds_restart(rinfo['name'])
        status = self.fs.wait_for_daemons()
        p.stdin.close()
        p.wait()

    @staticmethod
    def make_test_killpoint(killpoint, name):
        """
        returns test_ functions like test_ino_prealloc_killpoint_REPLY_EXTRA_BL,
        test_ino_prealloc_killpoint_INODE_CREATED, and so on.
        """
        def test(self):
            log.info(f"Starting workload with killpoint {name}={killpoint}")
            self._run_workload(killpoint)
            log.info(f"Test passed for killpoint {name}={killpoint}")
        return test

for killpoint, name in enumerate(INO_PREALLOC_KILLPOINT):
    if killpoint == 0: # skip no-op
        continue
    test_function = TestInodePreallocationKillpoints.make_test_killpoint(killpoint, name)
    setattr(TestInodePreallocationKillpoints, f"test_ino_prealloc_killpoint_{name}", test_function)
