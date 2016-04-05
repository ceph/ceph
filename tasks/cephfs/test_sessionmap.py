from StringIO import StringIO
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

        mds_id = self.fs.get_lone_mds_id()
        self.fs.mon_manager.raw_cluster_cmd("tell", "mds.{0}".format(mds_id), "session", "ls")

        ls_data = self.fs.mds_asok(['session', 'ls'])
        self.assertEqual(len(ls_data), 0)

    def _get_thread_count(self, mds_id):
        remote = self.fs.mds_daemons[mds_id].remote

        ps_txt = remote.run(
            args=["ps", "axo", "cmd,nlwp"],
            stdout=StringIO()
        ).stdout.getvalue().strip()
        lines = ps_txt.split("\n")[1:]

        for line in lines:
            if line.find("ceph-mds") != -1:
                if line.find("-i {0}".format(mds_id)) != -1:
                    log.info("Found ps line for daemon: {0}".format(line))
                    return int(line.split()[-1])

        raise RuntimeError("No process found in ps output for MDS {0}: {1}".format(
            mds_id, ps_txt
        ))

    def test_tell_conn_close(self):
        """
        That when a `tell` command is sent using the python CLI,
        the thread count goes back to where it started (i.e. we aren't
        leaving connections open)
        """
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        mds_id = self.fs.get_lone_mds_id()

        initial_thread_count = self._get_thread_count(mds_id)
        self.fs.mon_manager.raw_cluster_cmd("tell", "mds.{0}".format(mds_id), "session", "ls")
        final_thread_count = self._get_thread_count(mds_id)

        self.assertEqual(initial_thread_count, final_thread_count)

    def test_mount_conn_close(self):
        """
        That when a client unmounts, the thread count on the MDS goes back
        to what it was before the client mounted
        """
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        mds_id = self.fs.get_lone_mds_id()

        initial_thread_count = self._get_thread_count(mds_id)
        self.mount_a.mount()
        self.assertGreater(self._get_thread_count(mds_id), initial_thread_count)
        self.mount_a.umount_wait()
        final_thread_count = self._get_thread_count(mds_id)

        self.assertEqual(initial_thread_count, final_thread_count)

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
        self.fs.mon_manager.raw_cluster_cmd_result('mds', 'set', "allow_multimds",
                                                   "true", "--yes-i-really-mean-it")
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
        self.wait_until_true(
            lambda: get_omap_wrs() - initial_omap_wrs == 2,
            timeout=10  # Long enough for an export to get acked
        )

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

    def _sudo_write_file(self, remote, path, data):
        """
        Write data to a remote file as super user

        :param remote: Remote site.
        :param path: Path on the remote being written to.
        :param data: Data to be written.

        Both perms and owner are passed directly to chmod.
        """
        remote.run(
            args=[
                'sudo',
                'python',
                '-c',
                'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                path,
            ],
            stdin=data,
        )

    def _configure_auth(self, mount, id_name, mds_caps, osd_caps=None, mon_caps=None):
        """
        Set up auth credentials for a client mount, and write out the keyring
        for the client to use.
        """

        # This keyring stuff won't work for kclient
        assert(isinstance(mount, FuseMount))

        if osd_caps is None:
            osd_caps = "allow rw"

        if mon_caps is None:
            mon_caps = "allow r"

        out = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.{name}".format(name=id_name),
            "mds", mds_caps,
            "osd", osd_caps,
            "mon", mon_caps
        )
        mount.client_id = id_name
        self._sudo_write_file(mount.client_remote, mount.get_keyring_path(), out)
        self.set_conf("client.{name}".format(name=id_name), "keyring", mount.get_keyring_path())

    def test_session_reject(self):
        self.mount_a.run_shell(["mkdir", "foo"])
        self.mount_a.run_shell(["mkdir", "foo/bar"])
        self.mount_a.umount_wait()

        # Mount B will be my rejected client
        self.mount_b.umount_wait()

        # Configure a client that is limited to /foo/bar
        self._configure_auth(self.mount_b, "badguy", "allow rw path=/foo/bar")
        # Check he can mount that dir and do IO
        self.mount_b.mount(mount_path="/foo/bar")
        self.mount_b.wait_until_mounted()
        self.mount_b.create_destroy()
        self.mount_b.umount_wait()

        # Configure the client to claim that its mount point metadata is /baz
        self.set_conf("client.badguy", "client_metadata", "root=/baz")
        # Try to mount the client, see that it fails
        with self.assert_cluster_log("client session with invalid root '/baz' denied"):
            with self.assertRaises(CommandFailedError):
                self.mount_b.mount(mount_path="/foo/bar")
