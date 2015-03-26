
import json
import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestSessionMap(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    def test_version_splitting(self):
        """
        That when many sessions are updated, they are correctly
        split into multiple versions to obey mds_sessionmap_keys_per_op
        """

        # Start umounted
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # Configure MDS to write one OMAP key at once
        self.set_conf('mds', 'mds_sessionmap_keys_per_op', 1)
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        # I would like two MDSs, so that I can do an export dir later
        self.fs.mon_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "2")
        self.fs.wait_for_daemons()

        active_mds_names = self.fs.get_active_names()
        rank_0_id = active_mds_names[0]
        rank_1_id = active_mds_names[1]
        log.info("Ranks 0 and 1 are {0} and {1}".format(
            rank_0_id, rank_1_id))

        # Bring the clients back
        self.mount_a.mount()
        self.mount_b.mount()
        self.mount_a.create_files()  # Kick the client into opening sessions
        self.mount_b.create_files()

        # See that they've got sessions
        self.assert_session_count(2, mds_id=rank_0_id)

        # See that we persist their sessions
        self.fs.mds_asok(["flush", "journal"], rank_0_id)
        table_json = json.loads(self.fs.table_tool(["0", "show", "session"]))
        log.info("SessionMap: {0}".format(json.dumps(table_json, indent=2)))
        self.assertEqual(table_json['0']['result'], 0)
        self.assertEqual(len(table_json['0']['data']['Sessions']), 2)

        # Now, induce a "force_open_sessions" event by exporting a dir
        self.mount_a.run_shell(["mkdir", "bravo"])
        self.mount_a.run_shell(["touch", "bravo/file"])
        self.mount_b.run_shell(["ls", "-l", "bravo/file"])

        def get_omap_wrs():
            return self.fs.mds_asok(['perf', 'dump', 'objecter'], rank_1_id)['objecter']['omap_wr']

        # Flush so that there are no dirty sessions on rank 1
        self.fs.mds_asok(["flush", "journal"], rank_1_id)

        # Export so that we get a force_open to rank 1 for the two sessions from rank 0
        initial_omap_wrs = get_omap_wrs()
        self.fs.mds_asok(['export', 'dir', '/bravo', '1'], rank_0_id)

        # This is the critical (if rather subtle) check: that in the process of doing an export dir,
        # we hit force_open_sessions, and as a result we end up writing out the sessionmap.  There
        # will be two sessions dirtied here, and because we have set keys_per_op to 1, we should see
        # a single session get written out (the first of the two, triggered by the second getting marked
        # dirty)
        # The number of writes is two per session, because the header (sessionmap version) update and
        # KV write both count.
        self.assertEqual(get_omap_wrs() - initial_omap_wrs, 2)

        # Now end our sessions and check the backing sessionmap is updated correctly
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # In-memory sessionmap check
        self.assert_session_count(0, mds_id=rank_0_id)

        # On-disk sessionmap check
        self.fs.mds_asok(["flush", "journal"], rank_0_id)
        table_json = json.loads(self.fs.table_tool(["0", "show", "session"]))
        log.info("SessionMap: {0}".format(json.dumps(table_json, indent=2)))
        self.assertEqual(table_json['0']['result'], 0)
        self.assertEqual(len(table_json['0']['data']['Sessions']), 0)
