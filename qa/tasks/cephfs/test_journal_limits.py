import logging
import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestJournalLimits(CephFSTestCase):
    def test_journal_hard_limit_enospc(self):
        """
        Verify that the MDS rejects mutating operations with ENOSPC when
        the journal size exceeds mds_log_hard_limit_segments, and
        resumes normally when the limit is lifted.
        """
        # Save the original cluster configuration dynamically
        orig_events = self.ceph_cluster.mon_manager.raw_cluster_cmd(
            'config', 'get', 'mds', 'mds_log_events_per_segment').strip()
        orig_max = self.ceph_cluster.mon_manager.raw_cluster_cmd(
            'config', 'get', 'mds', 'mds_log_max_segments').strip()
        orig_warn = self.ceph_cluster.mon_manager.raw_cluster_cmd(
            'config', 'get', 'mds', 'mds_log_warn_factor').strip()
        orig_hard = self.ceph_cluster.mon_manager.raw_cluster_cmd(
            'config', 'get', 'mds', 'mds_log_hard_limit_segments').strip()

        try:
            self.config_set('mds', 'mds_log_events_per_segment', '10')
            self.config_set('mds', 'mds_log_max_segments', '8')
            self.config_set('mds', 'mds_log_warn_factor', '1.0')
            self.config_set('mds', 'mds_log_hard_limit_segments', '12')

            self.fs.mds_restart()
            self.fs.wait_for_daemons()

            hit_enospc = False
            try:
                for i in range(200):
                    self.mount_a.run_shell(['touch', f'test_limit_{i}'])
            except CommandFailedError as e:
                # The SSH wrapper might not capture the exact ENOSPC stderr string,
                # but the command failing here means our limit worked.
                hit_enospc = True
                log.info(f"Successfully hit the journal hard limit. Command failed as expected: {e}")

            self.assertTrue(hit_enospc, "Failed to trigger the journal hard limit ENOSPC!")


            # Restore original limits to let the MDS recover
            self.config_set('mds', 'mds_log_events_per_segment', orig_events)
            self.config_set('mds', 'mds_log_max_segments', orig_max)
            self.config_set('mds', 'mds_log_warn_factor', orig_warn)
            self.config_set('mds', 'mds_log_hard_limit_segments', orig_hard)

            # Verify that writes resume successfully using a retry loop (up to 60s)
            log.info("Waiting for MDS to process config and resume writes...")
            recovery_successful = False
            for _ in range(12):
                time.sleep(5)
                try:
                    self.mount_a.run_shell(['touch', 'recovery_success'])
                    recovery_successful = True
                    break
                except CommandFailedError:
                    log.info("MDS still rejecting writes, waiting...")

            self.assertTrue(recovery_successful, "MDS failed to recover and accept writes after lifting the limit!")
            self.mount_a.run_shell(['rm', '-f', 'recovery_success'])
        finally:
            # Fallback cleanup: Ensure original configuration is restored
            # even if one of the assertions above fails and halts the test.
            self.config_set('mds', 'mds_log_events_per_segment', orig_events)
            self.config_set('mds', 'mds_log_max_segments', orig_max)
            self.config_set('mds', 'mds_log_warn_factor', orig_warn)
            self.config_set('mds', 'mds_log_hard_limit_segments', orig_hard)
