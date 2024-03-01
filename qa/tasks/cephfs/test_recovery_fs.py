import logging
from os.path import join as os_path_join

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestFSRecovery(CephFSTestCase):
    """
    Tests for recovering FS after loss of FSMap
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def test_recover_fs_after_fsmap_removal(self):
        data_pool = self.fs.get_data_pool_name()
        metadata_pool = self.fs.get_metadata_pool_name()
        # write data in mount, and fsync
        self.mount_a.create_n_files('file_on_fs', 1, sync=True)
        # faild MDSs to allow removing the file system in the next step
        self.fs.fail()
        # Remove file system to lose FSMap and keep the pools intact.
        # This mimics the scenario where the monitor store is rebuilt
        # using  OSDs to recover a cluster with corrupt monitor store.
        # The FSMap is permanently lost, but the FS pools are
        # recovered/intact
        self.fs.rm()
        # Recreate file system with pool and previous fscid
        self.run_ceph_cmd(
            'fs', 'new', self.fs.name, metadata_pool, data_pool,
            '--recover', '--force', '--fscid', f'{self.fs.id}')
        self.fs.set_joinable()
        # Check status of file system
        self.fs.wait_for_daemons()
        # check data in file sytem is intact
        filepath = os_path_join(self.mount_a.hostfs_mntpt, 'file_on_fs_0')
        self.assertEqual(self.mount_a.read_file(filepath), "0")
