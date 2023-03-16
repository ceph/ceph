import time
import json
import logging

from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestSessionMap(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    def test_tell_session_drop(self):
        """
        That when a `tell` command is sent using the python CLI,
        its MDS session is gone after it terminates
        """
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        status = self.fs.status()
        self.fs.rank_tell(["session", "ls"], status=status)

        ls_data = self.fs.rank_asok(['session', 'ls'], status=status)
        self.assertEqual(len(ls_data), 0)

    def _get_connection_count(self, status=None):
        perf = self.fs.rank_asok(["perf", "dump"], status=status)
        conn = 0
        for module, dump in perf.items():
            if "AsyncMessenger::Worker" in module:
                conn += dump['msgr_active_connections']
        return conn

    def test_tell_conn_close(self):
        """
        That when a `tell` command is sent using the python CLI,
        the conn count goes back to where it started (i.e. we aren't
        leaving connections open)
        """
        self.config_set('mds', 'ms_async_reap_threshold', '1')

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        status = self.fs.status()
        s = self._get_connection_count(status=status)
        self.fs.rank_tell(["session", "ls"], status=status)
        self.wait_until_true(
            lambda: self._get_connection_count(status=status) == s,
            timeout=30
        )

    def test_mount_conn_close(self):
        """
        That when a client unmounts, the thread count on the MDS goes back
        to what it was before the client mounted
        """
        self.config_set('mds', 'ms_async_reap_threshold', '1')

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        status = self.fs.status()
        s = self._get_connection_count(status=status)
        self.mount_a.mount_wait()
        self.assertGreater(self._get_connection_count(status=status), s)
        self.mount_a.umount_wait()
        self.wait_until_true(
            lambda: self._get_connection_count(status=status) == s,
            timeout=30
        )

    def test_version_splitting(self):
        """
        That when many sessions are updated, they are correctly
        split into multiple versions to obey mds_sessionmap_keys_per_op
        """

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # Configure MDS to write one OMAP key at once
        self.set_conf('mds', 'mds_sessionmap_keys_per_op', 1)
        self.fs.mds_fail_restart()
        status = self.fs.wait_for_daemons()

        # Bring the clients back
        self.mount_a.mount_wait()
        self.mount_b.mount_wait()

        # See that they've got sessions
        self.assert_session_count(2, mds_id=self.fs.get_rank(status=status)['name'])

        # See that we persist their sessions
        self.fs.rank_asok(["flush", "journal"], rank=0, status=status)
        table_json = json.loads(self.fs.table_tool(["0", "show", "session"]))
        log.info("SessionMap: {0}".format(json.dumps(table_json, indent=2)))
        self.assertEqual(table_json['0']['result'], 0)
        self.assertEqual(len(table_json['0']['data']['sessions']), 2)

        # Now, induce a "force_open_sessions" event by exporting a dir
        self.mount_a.run_shell(["mkdir", "bravo"])
        self.mount_a.run_shell(["touch", "bravo/file_a"])
        self.mount_b.run_shell(["touch", "bravo/file_b"])

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        def get_omap_wrs():
            return self.fs.rank_asok(['perf', 'dump', 'objecter'], rank=1, status=status)['objecter']['omap_wr']

        # Flush so that there are no dirty sessions on rank 1
        self.fs.rank_asok(["flush", "journal"], rank=1, status=status)

        # Export so that we get a force_open to rank 1 for the two sessions from rank 0
        initial_omap_wrs = get_omap_wrs()
        self.fs.rank_asok(['export', 'dir', '/bravo', '1'], rank=0, status=status)

        # This is the critical (if rather subtle) check: that in the process of doing an export dir,
        # we hit force_open_sessions, and as a result we end up writing out the sessionmap.  There
        # will be two sessions dirtied here, and because we have set keys_per_op to 1, we should see
        # a single session get written out (the first of the two, triggered by the second getting marked
        # dirty)
        # The number of writes is two per session, because the header (sessionmap version) update and
        # KV write both count. Also, multiply by 2 for each openfile table update.
        self.wait_until_true(
            lambda: get_omap_wrs() - initial_omap_wrs == 2*2,
            timeout=30  # Long enough for an export to get acked
        )

        # Now end our sessions and check the backing sessionmap is updated correctly
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # In-memory sessionmap check
        self.assert_session_count(0, mds_id=self.fs.get_rank(status=status)['name'])

        # On-disk sessionmap check
        self.fs.rank_asok(["flush", "journal"], rank=0, status=status)
        table_json = json.loads(self.fs.table_tool(["0", "show", "session"]))
        log.info("SessionMap: {0}".format(json.dumps(table_json, indent=2)))
        self.assertEqual(table_json['0']['result'], 0)
        self.assertEqual(len(table_json['0']['data']['sessions']), 0)

    def _configure_auth(self, mount, id_name, mds_caps, osd_caps=None, mon_caps=None):
        """
        Set up auth credentials for a client mount, and write out the keyring
        for the client to use.
        """

        if osd_caps is None:
            osd_caps = "allow rw"

        if mon_caps is None:
            mon_caps = "allow r"

        out = self.get_ceph_cmd_stdout(
            "auth", "get-or-create", "client.{name}".format(name=id_name),
            "mds", mds_caps,
            "osd", osd_caps,
            "mon", mon_caps
        )
        mount.client_id = id_name
        mount.client_remote.write_file(mount.get_keyring_path(), out, sudo=True)
        self.set_conf("client.{name}".format(name=id_name), "keyring", mount.get_keyring_path())

    def test_session_reject(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires FUSE client to inject client metadata")

        self.mount_a.run_shell(["mkdir", "foo"])
        self.mount_a.run_shell(["mkdir", "foo/bar"])
        self.mount_a.umount_wait()

        # Mount B will be my rejected client
        self.mount_b.umount_wait()

        # Configure a client that is limited to /foo/bar
        self._configure_auth(self.mount_b, "badguy", "allow rw path=/foo/bar")
        # Check he can mount that dir and do IO
        self.mount_b.mount_wait(cephfs_mntpt="/foo/bar")
        self.mount_b.create_destroy()
        self.mount_b.umount_wait()

        # Configure the client to claim that its mount point metadata is /baz
        self.set_conf("client.badguy", "client_metadata", "root=/baz")
        # Try to mount the client, see that it fails
        with self.assert_cluster_log("client session with non-allowable root '/baz' denied"):
            with self.assertRaises(CommandFailedError):
                self.mount_b.mount_wait(cephfs_mntpt="/foo/bar")

    def test_session_evict_blocklisted(self):
        """
        Check that mds evicts blocklisted client
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires FUSE client to use "
                          "mds_cluster.is_addr_blocklisted()")

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload("mkdir {d0,d1} && touch {d0,d1}/file")
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self._wait_subtrees([('/d0', 0), ('/d1', 1)], status=status)

        self.mount_a.run_shell(["touch", "d0/f0"])
        self.mount_a.run_shell(["touch", "d1/f0"])
        self.mount_b.run_shell(["touch", "d0/f1"])
        self.mount_b.run_shell(["touch", "d1/f1"])

        self.assert_session_count(2, mds_id=self.fs.get_rank(rank=0, status=status)['name'])
        self.assert_session_count(2, mds_id=self.fs.get_rank(rank=1, status=status)['name'])

        mount_a_client_id = self.mount_a.get_global_id()
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id],
                         mds_id=self.fs.get_rank(rank=0, status=status)['name'])
        self.wait_until_true(lambda: self.mds_cluster.is_addr_blocklisted(
            self.mount_a.get_global_addr()), timeout=30)

        # 10 seconds should be enough for evicting client
        time.sleep(10)
        self.assert_session_count(1, mds_id=self.fs.get_rank(rank=0, status=status)['name'])
        self.assert_session_count(1, mds_id=self.fs.get_rank(rank=1, status=status)['name'])

        self.mount_a.kill_cleanup()
        self.mount_a.mount_wait()
