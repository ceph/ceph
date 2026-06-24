import logging
import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestStandbyAffinity(CephFSTestCase):
    MDSS_REQUIRED = 3

    def test_standby_affinity_promotion(self):
        """
        Verify that the MDSMonitor honors the mds_join_fs configuration
        during a failover event.
        1. The test configures mds_join_fs for a dynamically discovered standby.
        2. The standby daemon restarts, reads the config, and reports it to the Monitor.
        3. The active Rank 0 MDS is forcefully stopped.
        4. The Monitor invokes get_available_standby() to evaluate candidates.
        5. The Monitor successfully promotes the preferred standby over vanilla alternatives.
        """
        fs_name = self.fs.name
        mds_ids = list(self.mds_cluster.mds_ids)

        # Guarantee we target the true Active Rank 0 MDS by querying the runtime map
        active_mds_names = self.fs.get_active_names()
        if not active_mds_names:
            self.fail("No active MDS found in the filesystem map.")

        active_mds = active_mds_names[0]

        preferred_standby = None
        vanilla_standby = None

        # Safely assign standby roles from the remaining IDs (skipping active primary)
        for mds_id in mds_ids:
            if mds_id == active_mds:
                continue
            if not preferred_standby:
                preferred_standby = mds_id
            elif not vanilla_standby:
                vanilla_standby = mds_id

        log.info(f"Roles assigned - Active Rank 0: {active_mds}, "
                 f"Preferred Standby: {preferred_standby}, "
                 f"Vanilla Standby: {vanilla_standby}")

        # Explicitly assign one standby to prefer the filesystem.
        # When a daemon starts up, it reads this mds_join_fs string and
        # sends it to the Monitor. The Monitor converts this into the
        # info.join_fscid integer inside the MDSMap::mds_info_t struct.
        # This feeds data into the scoring matrix.
        self.config_set(f'mds.{preferred_standby}', 'mds_join_fs', fs_name)
        # Ensure the other is strictly vanilla
        self.config_set(f'mds.{vanilla_standby}', 'mds_join_fs', "")

        # Restart daemons to apply configuration changes safely
        self.mds_cluster.mds_restart(preferred_standby)
        self.mds_cluster.mds_restart(vanilla_standby)
        self.fs.wait_for_daemons()

        # Trigger the Failover
        log.info("Evicting active rank 0 to evaluate promotion standby affinity...")
        self.mds_cluster.mon_manager.raw_cluster_cmd("mds", "fail", "0")

        # The Monitor evaluates both standbys. Because preferred_standby has
        # a matching mds_join_fs configuration, it receives Score SCORE_PREF_MATCH (6)
        # and vanilla_standby receives Score SCORE_PREF_VANILLA (5).
        # Because 6 > 5, the loop returns the preferred_standby pointer to the Monitor,
        # which then promotes it.
        time.sleep(10)
        self.fs.wait_for_daemons()

        # Verify the outcome
        new_status = self.fs.status()
        new_active_name = new_status.get_rank(self.fs.id, 0)['name']

        log.info(f"Monitor promoted MDS {new_active_name} to Active.")

        # Assertion: The monitor MUST have picked the preferred standby over the vanilla one.
        self.assertEqual(
            new_active_name,
            preferred_standby,
            f"Failover failure! MDSMonitor promoted {new_active_name} "
            f"instead of {preferred_standby}."
        )
