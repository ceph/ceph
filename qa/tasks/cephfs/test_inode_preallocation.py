import logging
import signal
import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

# Must match enum ino_prealloc_killpoint in src/mds/mdstypes.h
INO_PREALLOC_KILLPOINTS = [
    "NONE",                                 # 0
    "INO_PREALLOC_DELEGATE_BEFORE",         # 1
    "INO_PREALLOC_DELEGATE_AFTER",          # 2
    "INO_PREALLOC_PREPARE_NEW_INODE",       # 3
    "INO_PREALLOC_APPLY_ALLOCATED_BEFORE",  # 4
    "INO_PREALLOC_APPLY_ALLOCATED_AFTER",   # 5
    "INO_PREALLOC_SESSION_SAVE_BEFORE",     # 6
    "INO_PREALLOC_REPLAY_ERASE_BEFORE",     # 7
]

class TestInodePreallocationKillpoints(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def _run_workload(self, killpoint_name):
        killpoint_val = INO_PREALLOC_KILLPOINTS.index(killpoint_name)

        self.fs.set_max_mds(1)
        status = self.fs.wait_for_daemons()

        # Target Rank 0 for the killpoint test
        rinfo = self.fs.get_rank(rank=0, status=status)

        # Force aggressive inode delegation and preallocation churn
        self.fs.set_config("mds_client_prealloc_inos", "100", rank=0, status=status)
        self.fs.set_config("mds_client_delegate_inos_pct", "100", rank=0, status=status)
        self.fs.set_config("mds_allow_async_dirops", "true", rank=0, status=status)

        is_replay_test = (killpoint_name == "INO_PREALLOC_REPLAY_ERASE_BEFORE")

        # For active-phase killpoints (1-6), set the fault via the local admin socket
        if not is_replay_test:
            self.fs.set_config("mds_kill_ino_prealloc_at", str(killpoint_val), rank=0, status=status)

        # Setup directory
        self.mount_a.run_shell_payload("mkdir -p top")

        log.info(f"Triggering workload for killpoint {killpoint_name} ({killpoint_val}) on Rank 0...")
        try:
            self.mount_a.run_shell_payload(
                "for i in $(seq 1 500); do "
                "  touch top/file_$i && rm -f top/file_$i; "
                "done"
            )
        except CommandFailedError as e:
            log.info(f"Workload interrupted by expected MDS crash: {e}")

        if killpoint_name == "INO_PREALLOC_SESSION_SAVE_BEFORE":
            # Explicitly force a journal flush to ensure SessionMap::save() is hit
            log.info("Forcing MDS journal flush to trigger SessionMap::save()...")
            try:
                self.fs.rank_asok(['flush', 'journal'], rank=0, status=status)
            except CommandFailedError as e:
                log.info(f"MDS crashed during journal flush (expected): {e}")

        if is_replay_test:
            # The workload completed normally and populated the dirty journal.
            # Now set the killpoint globally via the monitor so the recovering standby hits it.
            log.info("Workload complete. Setting killpoint globally for journal replay...")
            self.fs.mon_manager.raw_cluster_cmd("config", "set", "mds", "mds_kill_ino_prealloc_at", str(killpoint_val))

            # Send SIGKILL to leave the journal dirty and force a failover
            log.info(f"Sending SIGKILL to active MDS {rinfo['name']} to force dirty journal recovery...")
            self.fs.rank_signal(rank=0, signal=signal.SIGKILL)

            # Wait for the monitor to detect the kill and promote a standby
            time.sleep(10)

            # Wait for the standby daemon to abort during the EMetaBlob::replay phase
            log.info("Waiting for the promoted standby daemon to abort during journal replay...")
            self.fs.wait_for_death(timeout=120, status=status, rank=0)

            # Clean up the global config so the NEXT standby can successfully recover the cluster
            log.info("Standby crashed successfully during replay. Removing global config...")
            self.fs.mon_manager.raw_cluster_cmd("config", "rm", "mds", "mds_kill_ino_prealloc_at")

            # The SIGKILLed daemon has no core dump, but the crashed standby does.
            # Sweep all daemons, delete any existing core dumps, and restart everyone.
            log.info("Cleaning up core dumps and restarting daemons...")
            for mds_id in self.mds_cluster.mds_ids:
                try:
                    self.delete_mds_coredump(mds_id)
                except (AssertionError, CommandFailedError) as e:
                    log.info(f"No core dump to clean up for {mds_id}. "
                             f"This is normal behavior for the active daemon that received SIGKILL. "
                             f"(Error: {e})")
                self.fs.mds_restart(mds_id)

        else:
            # Active-phase wait logic
            log.info(f"Waiting for Rank 0 ({rinfo['name']}) to abort at killpoint {killpoint_name}...")
            self.fs.wait_for_death(timeout=120, status=status, rank=0)

            # Cleanup coredumps and restart the originally killed daemon
            self.delete_mds_coredump(rinfo['name'])
            log.info("Restarting daemon to restore cluster health...")
            self.fs.mds_restart(rinfo['name'])

        # Verify active cluster recovery and journal/sessionmap replay
        status = self.fs.wait_for_daemons()
        log.info("Cluster successfully recovered and replayed preallocation state.")

        # Verification check: Ensure client can perform I/O post-recovery
        self.mount_a.run_shell_payload("touch top/recovery_check_file")
        self.mount_a.run_shell_payload("rm -f top/recovery_check_file")

    @staticmethod
    def make_test_killpoint(killpoint_name):
        def test(self):
            log.info(f"=== Starting test for {killpoint_name} ===")
            self._run_workload(killpoint_name)
            log.info(f"=== Completed test for {killpoint_name} ===")
        return test

# Dynamically attach test_ino_prealloc_killpoint_<NAME> for each enum value (1..7)
for val, name in enumerate(INO_PREALLOC_KILLPOINTS):
    if val == 0:  # Skip NONE
        continue
    test_func = TestInodePreallocationKillpoints.make_test_killpoint(name)
    setattr(TestInodePreallocationKillpoints, f"test_ino_prealloc_killpoint_{name}", test_func)
