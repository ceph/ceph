"""
Test to exercise killpoints available during directory export code path
After hitting a killpoint, MDS crashes and cluster must find a standby MDS
to become active. Exported data must be available after mds replacement.

"""
import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase
import time

log = logging.getLogger(__name__)


class TestKillPoints(CephFSTestCase):
    # Two active MDS
    MDSS_REQUIRED = 2

    init = False

    def setup_cluster(self):
        # Set Multi-MDS cluster
        self.fs.set_flag("allow_multimds", 1, "--yes-i-really-mean-it")
        self.fs.set_max_mds(2)

        self.fs.wait_for_daemons()

        num_active_mds = len(self.fs.get_active_names())
        if num_active_mds != 2:
            log.error("Incorrect number of MDS active: %d" %num_active_mds)
            return False

        # Get a clean mount
        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # Create test data
        if self.init is False:
            self.populate_data(8)
            self.init = True

        return True

    def populate_data(self, nfiles):
            size_mb = 8
            dname = "abc"
            self.mount_a.run_shell(["mkdir", dname])
            for i in range(nfiles):
                fname =dname.join("/").join(str(i)).join(".txt")
                self.mount_a.write_n_mb(fname, size_mb)

            # Get the list of file to confirm the count of files
            self.org_files = self.mount_a.ls()

    def verify_data(self):
        s1 = set(self.mount_a.ls())
        s2 = set(self.org_files)

        if s1.isdisjoint(s2):
            log.error("Directory contents org: %s" %str(org_files))
            log.error("Directory contents new: %s" %str(out))
            return False

        log.info("Directory contents matches")
        return True

    def run_export_dir(self, importv, exportv):
        # Wait till all MDS becomes active
        self.fs.wait_for_daemons()

        # Get all active ranks
        ranks = self.fs.get_all_mds_rank()

        original_active = self.fs.get_active_names()

        if len(ranks) != 2:
            log.error("Incorrect number of MDS ranks, exiting the test")
            return False

        rank_0_id = original_active[0]
        rank_1_id = original_active[1]

        killpoint = {}
        killpoint[rank_0_id] = ('export', exportv)
        killpoint[rank_1_id] = ('import', importv)

        command = ["config", "set", "mds_kill_export_at", str(exportv)]
        result = self.fs.mds_asok(command, rank_0_id)
        assert(result["success"])

        command = ["config", "set", "mds_kill_import_at", str(importv)]
        result = self.fs.mds_asok(command, rank_1_id)
        assert(result["success"])

        # This should kill either or both MDS process
        command = ["export", "dir", "/abc", "1"]
        try:
            result = self.fs.mds_asok(command, rank_0_id)
        except Exception as e:
            log.error(e.__str__())

        def log_crashed_mds():
            crashed_mds = self.fs.get_crashed_mds()
            for k in crashed_mds:
                name = crashed_mds[k]
                log.info("MDS %s crashed at type %s killpoint %d"
                        %(name, killpoint[name][0], killpoint[name][1]))

        def log_active_mds():
            active_mds = self.fs.get_up_and_active_mds()
            for k in active_mds:
                name = active_mds[k]
                log.info("MDS %s active at type %s killpoint %d"
                        %(name, killpoint[name][0], killpoint[name][1]))

        # Waiting time for monitor to promote replacement for dead MDS
        grace = int(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        #log_active_mds()
        #log_crashed_mds()

        time.sleep(grace * 2)

        log_active_mds()
        log_crashed_mds()

        # Restart the crashed MDS daemon.
        crashed_mds = self.fs.get_crashed_mds()
        for k in crashed_mds:
            print "Restarting rank %d" %k
            self.fs.mds_fail(crashed_mds[k])
            self.fs.mds_restart(crashed_mds[k])

        # After the restarted, the other MDS can fail on a killpoint.
        # Wait for another grace period to let monitor notice the failure
        # and update MDSMap.

        log_active_mds()
        log_crashed_mds()

        time.sleep(grace * 2)

        log_active_mds()
        log_crashed_mds()

        # Restart the killed daemon.
        for k in crashed_mds:
            print "Restarting rank %d" %k
            self.fs.mds_fail(crashed_mds[k])
            self.fs.mds_restart(crashed_mds[k])

        time.sleep(grace * 2)

        active_mds = self.fs.get_up_and_active_mds()
        if len(active_mds) != 2:
            "One or more MDS did not come up"
            return False

        if not self.verify_data():
            return False;

        return True


    def run_export(self, importv, exportv):
        success_count = 0

        for v in range(1, importv + 1):
            for w in range(1, exportv + 1):
                if self.run_export_dir(v, w):
                    success_count = success_count + 1
                else:
                    log.error("Error for killpoint %d:%d" %(v, w))

        for v in range(1, exportv + 1):
            for w in range(1, importv + 1):
                if self.run_export_dir(v, w):
                    success_count = success_count + 1
                else:
                    log.error("Error for killpoint %d:%d" %(v, w))

        return success_count

    def test_export(self):
        self.init = False
        self.setup_cluster()
        success_count = 0

        import_killpoint = 13
        export_killpoint = 13

        killpoints_count = 2 * import_killpoint * export_killpoint

        success_count = self.run_export(import_killpoint, export_killpoint)
        assert(success_count == killpoints_count)

        log.info("All %d scenarios passed" %killpoints_count)
