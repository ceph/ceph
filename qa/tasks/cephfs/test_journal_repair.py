
"""
Test our tools for recovering the content of damaged journals
"""

import json
import logging
from textwrap import dedent
import time

from teuthology.exceptions import CommandFailedError, ConnectionLostError
from tasks.cephfs.filesystem import ObjectNotFound, ROOT_INO
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology
from tasks.workunit import task as workunit

log = logging.getLogger(__name__)


class TestJournalRepair(CephFSTestCase):
    MDSS_REQUIRED = 2

    def test_inject_to_empty(self):
        """
        That when some dentries in the journal but nothing is in
        the backing store, we correctly populate the backing store
        from the journalled dentries.
        """

        # Inject metadata operations
        self.mount_a.run_shell(["touch", "rootfile"])
        self.mount_a.run_shell(["mkdir", "subdir"])
        self.mount_a.run_shell(["touch", "subdir/subdirfile"])
        # There are several different paths for handling hardlinks, depending
        # on whether an existing dentry (being overwritten) is also a hardlink
        self.mount_a.run_shell(["mkdir", "linkdir"])

        # Test inode -> remote transition for a dentry
        self.mount_a.run_shell(["touch", "linkdir/link0"])
        self.mount_a.run_shell(["rm", "-f", "linkdir/link0"])
        self.mount_a.run_shell(["ln", "subdir/subdirfile", "linkdir/link0"])

        # Test nothing -> remote transition
        self.mount_a.run_shell(["ln", "subdir/subdirfile", "linkdir/link1"])

        # Test remote -> inode transition
        self.mount_a.run_shell(["ln", "subdir/subdirfile", "linkdir/link2"])
        self.mount_a.run_shell(["rm", "-f", "linkdir/link2"])
        self.mount_a.run_shell(["touch", "linkdir/link2"])

        # Test remote -> diff remote transition
        self.mount_a.run_shell(["ln", "subdir/subdirfile", "linkdir/link3"])
        self.mount_a.run_shell(["rm", "-f", "linkdir/link3"])
        self.mount_a.run_shell(["ln", "rootfile", "linkdir/link3"])

        # Test an empty directory
        self.mount_a.run_shell(["mkdir", "subdir/subsubdir"])
        self.mount_a.run_shell(["sync"])

        # Before we unmount, make a note of the inode numbers, later we will
        # check that they match what we recover from the journal
        rootfile_ino = self.mount_a.path_to_ino("rootfile")
        subdir_ino = self.mount_a.path_to_ino("subdir")
        linkdir_ino = self.mount_a.path_to_ino("linkdir")
        subdirfile_ino = self.mount_a.path_to_ino("subdir/subdirfile")
        subsubdir_ino = self.mount_a.path_to_ino("subdir/subsubdir")

        self.mount_a.umount_wait()

        # Stop the MDS
        self.fs.fail()

        # Now, the journal should contain the operations, but the backing
        # store shouldn't
        with self.assertRaises(ObjectNotFound):
            self.fs.list_dirfrag(subdir_ino)
        self.assertEqual(self.fs.list_dirfrag(ROOT_INO), [])

        # Execute the dentry recovery, this should populate the backing store
        self.fs.journal_tool(['event', 'recover_dentries', 'list'], 0)

        # Dentries in ROOT_INO are present
        self.assertEqual(sorted(self.fs.list_dirfrag(ROOT_INO)), sorted(['rootfile_head', 'subdir_head', 'linkdir_head']))
        self.assertEqual(self.fs.list_dirfrag(subdir_ino), ['subdirfile_head', 'subsubdir_head'])
        self.assertEqual(sorted(self.fs.list_dirfrag(linkdir_ino)),
                         sorted(['link0_head', 'link1_head', 'link2_head', 'link3_head']))

        # Now check the MDS can read what we wrote: truncate the journal
        # and start the mds.
        self.fs.journal_tool(['journal', 'reset'], 0)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        # List files
        self.mount_a.mount_wait()

        # First ls -R to populate MDCache, such that hardlinks will
        # resolve properly (recover_dentries does not create backtraces,
        # so ordinarily hardlinks to inodes that happen not to have backtraces
        # will be invisible in readdir).
        # FIXME: hook in forward scrub here to regenerate backtraces
        proc = self.mount_a.run_shell(['ls', '-R'])
        self.mount_a.umount_wait()  # remount to clear client cache before our second ls
        self.mount_a.mount_wait()

        proc = self.mount_a.run_shell(['ls', '-R'])
        self.assertEqual(proc.stdout.getvalue().strip(),
                         dedent("""
                         .:
                         linkdir
                         rootfile
                         subdir

                         ./linkdir:
                         link0
                         link1
                         link2
                         link3

                         ./subdir:
                         subdirfile
                         subsubdir

                         ./subdir/subsubdir:
                         """).strip())

        # Check the correct inos were preserved by path
        self.assertEqual(rootfile_ino, self.mount_a.path_to_ino("rootfile"))
        self.assertEqual(subdir_ino, self.mount_a.path_to_ino("subdir"))
        self.assertEqual(subdirfile_ino, self.mount_a.path_to_ino("subdir/subdirfile"))
        self.assertEqual(subsubdir_ino, self.mount_a.path_to_ino("subdir/subsubdir"))

        # Check that the hard link handling came out correctly
        self.assertEqual(self.mount_a.path_to_ino("linkdir/link0"), subdirfile_ino)
        self.assertEqual(self.mount_a.path_to_ino("linkdir/link1"), subdirfile_ino)
        self.assertNotEqual(self.mount_a.path_to_ino("linkdir/link2"), subdirfile_ino)
        self.assertEqual(self.mount_a.path_to_ino("linkdir/link3"), rootfile_ino)

        # Create a new file, ensure it is not issued the same ino as one of the
        # recovered ones
        self.mount_a.run_shell(["touch", "afterwards"])
        new_ino = self.mount_a.path_to_ino("afterwards")
        self.assertNotIn(new_ino, [rootfile_ino, subdir_ino, subdirfile_ino])

        # Check that we can do metadata ops in the recovered directory
        self.mount_a.run_shell(["touch", "subdir/subsubdir/subsubdirfile"])

    @for_teuthology # 308s
    def test_reset(self):
        """
        That after forcibly modifying the backing store, we can get back into
        a good state by resetting the MDSMap.

        The scenario is that we have two active MDSs, and we lose the journals.  Once
        we have completely lost confidence in the integrity of the metadata, we want to
        return the system to a single-MDS state to go into a scrub to recover what we
        can.
        """

        # Set max_mds to 2
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        rank0_gid = self.fs.get_rank(rank=0, status=status)['gid']
        self.fs.set_joinable(False) # no unintended failover

        # Create a dir on each rank
        self.mount_a.run_shell_payload("mkdir {alpha,bravo} && touch {alpha,bravo}/file")
        self.mount_a.setfattr("alpha/", "ceph.dir.pin", "0")
        self.mount_a.setfattr("bravo/", "ceph.dir.pin", "1")

        # Ensure the pinning has taken effect and the /bravo dir is now
        # migrated to rank 1.
        self._wait_subtrees([('/bravo', 1), ('/alpha', 0)], rank=0, status=status)

        # Do some IO (this should be split across ranks according to
        # the rank-pinned dirs)
        self.mount_a.create_n_files("alpha/file", 1000)
        self.mount_a.create_n_files("bravo/file", 1000)

        # Flush the journals so that we have some backing store data
        # belonging to one MDS, and some to the other MDS.
        self.fs.rank_asok(["flush", "journal"], rank=0)
        self.fs.rank_asok(["flush", "journal"], rank=1)

        # Stop (hard) the second MDS daemon
        self.fs.rank_fail(rank=1)

        # Wipe out the tables for MDS rank 1 so that it is broken and can't start
        # (this is the simulated failure that we will demonstrate that the disaster
        #  recovery tools can get us back from)
        self.fs.erase_metadata_objects(prefix="mds1_")

        # Try to access files from the client
        blocked_ls = self.mount_a.run_shell(["ls", "-R"], wait=False)

        # Check that this "ls -R" blocked rather than completing: indicates
        # it got stuck trying to access subtrees which were on the now-dead MDS.
        log.info("Sleeping to check ls is blocked...")
        time.sleep(60)
        self.assertFalse(blocked_ls.finished)

        # This mount is now useless because it will depend on MDS rank 1, and MDS rank 1
        # is not coming back.  Kill it.
        log.info("Killing mount, it's blocked on the MDS we killed")
        self.mount_a.kill()
        self.mount_a.kill_cleanup()
        try:
            # Now that the mount is dead, the ls -R should error out.
            blocked_ls.wait()
        except (CommandFailedError, ConnectionLostError):
            # The ConnectionLostError case is for kernel client, where
            # killing the mount also means killing the node.
            pass

        # See that the second MDS will crash when it starts and tries to
        # acquire rank 1
        self.fs.set_joinable(True)

        # The daemon taking the damaged rank should start starting, then
        # restart back into standby after asking the mon to mark the rank
        # damaged.
        def is_marked_damaged():
            mds_map = self.fs.get_mds_map()
            return 1 in mds_map['damaged']

        self.wait_until_true(is_marked_damaged, 60)
        self.assertEqual(rank0_gid, self.fs.get_rank(rank=0)['gid'])

        # Now give up and go through a disaster recovery procedure
        self.fs.fail()
        # Invoke recover_dentries quietly, because otherwise log spews millions of lines
        self.fs.journal_tool(["event", "recover_dentries", "summary"], 0, quiet=True)
        self.fs.journal_tool(["event", "recover_dentries", "summary"], 1, quiet=True)
        self.fs.table_tool(["0", "reset", "session"])
        self.fs.journal_tool(["journal", "reset"], 0)
        self.fs.erase_mds_objects(1)
        self.run_ceph_cmd('fs', 'reset', self.fs.name,
                          '--yes-i-really-mean-it')

        # Bring an MDS back online, mount a client, and see that we can walk the full
        # filesystem tree again
        self.fs.set_joinable(True) # redundant with `fs reset`
        status = self.fs.wait_for_daemons()
        self.assertEqual(len(list(self.fs.get_ranks(status=status))), 1)
        self.mount_a.mount_wait()
        self.mount_a.run_shell(["ls", "-R"], wait=True)

    def test_table_tool(self):
        self.mount_a.run_shell(["touch", "foo"])
        self.fs.rank_asok(["flush", "journal"])

        log.info(self.fs.table_tool(["all", "show", "inode"]))
        log.info(self.fs.table_tool(["all", "show", "snap"]))
        log.info(self.fs.table_tool(["all", "show", "session"]))

        # Inode table should always be the same because initial state
        # and choice of inode are deterministic.
        # Should see one inode consumed
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "show", "inode"])),
            {"0": {
                "data": {
                    "version": 2,
                    "inotable": {
                        "projected_free": [
                            {"start": 1099511628777,
                             "len": 1099511626775}],
                        "free": [
                            {"start": 1099511628777,
                             "len": 1099511626775}]}},
                "result": 0}}

        )

        # Should see one session
        session_data = json.loads(self.fs.table_tool(
            ["all", "show", "session"]))
        self.assertEqual(len(session_data["0"]["data"]["sessions"]), 1)
        self.assertEqual(session_data["0"]["result"], 0)

        # Should see no snaps
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "show", "snap"])),
            {"version": 1,
             "snapserver": {"last_snap": 1,
                            "last_created": 1,
                            "last_destroyed": 1,
                            "pending_noop": [],
                            "snaps": [],
                            "need_to_purge": {},
                            "pending_update": [],
                            "pending_destroy": []},
             "result": 0}
        )

        # Reset everything
        for table in ["session", "inode", "snap"]:
            self.fs.table_tool(["all", "reset", table])

        log.info(self.fs.table_tool(["all", "show", "inode"]))
        log.info(self.fs.table_tool(["all", "show", "snap"]))
        log.info(self.fs.table_tool(["all", "show", "session"]))

        # Should see 0 sessions
        session_data = json.loads(self.fs.table_tool(
            ["all", "show", "session"]))
        self.assertEqual(len(session_data["0"]["data"]["sessions"]), 0)
        self.assertEqual(session_data["0"]["result"], 0)

        # Should see entire inode range now marked free
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "show", "inode"])),
            {"0": {"data": {"version": 1,
                            "inotable": {"projected_free": [
                                {"start": 1099511627776,
                                 "len": 1099511627776}],
                                 "free": [
                                    {"start": 1099511627776,
                                    "len": 1099511627776}]}},
                   "result": 0}}
        )

        # Should see no snaps
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "show", "snap"])),
            {"version": 1,
             "snapserver": {"last_snap": 1,
                            "last_created": 1,
                            "last_destroyed": 1,
                            "pending_noop": [],
                            "snaps": [],
                            "need_to_purge": {},
                            "pending_update": [],
                            "pending_destroy": []},
             "result": 0}
        )

    def test_table_tool_take_inos(self):
        initial_range_start = 1099511627776
        initial_range_len = 1099511627776
        # Initially a completely clear range
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "show", "inode"])),
            {"0": {"data": {"version": 0,
                            "inotable": {"projected_free": [
                                {"start": initial_range_start,
                                 "len": initial_range_len}],
                                "free": [
                                    {"start": initial_range_start,
                                     "len": initial_range_len}]}},
                   "result": 0}}
        )

        # Remove some
        self.assertEqual(
            json.loads(self.fs.table_tool(["all", "take_inos", "{0}".format(initial_range_start + 100)])),
            {"0": {"data": {"version": 1,
                            "inotable": {"projected_free": [
                                {"start": initial_range_start + 101,
                                 "len": initial_range_len - 101}],
                                "free": [
                                    {"start": initial_range_start + 101,
                                     "len": initial_range_len - 101}]}},
                   "result": 0}}
        )

    @for_teuthology  # Hack: "for_teuthology" because .sh doesn't work outside teuth
    def test_journal_smoke(self):
        workunit(self.ctx, {
            'clients': {
                "client.{0}".format(self.mount_a.client_id): [
                    "fs/misc/trivial_sync.sh"],
            },
            "timeout": "1h"
        })

        for mount in self.mounts:
            mount.umount_wait()

        self.fs.fail()

        # journal tool smoke
        workunit(self.ctx, {
            'clients': {
                "client.{0}".format(self.mount_a.client_id): [
                    "suites/cephfs_journal_tool_smoke.sh"],
            },
            "timeout": "1h"
        })



        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        self.mount_a.mount_wait()

        # trivial sync moutn a
        workunit(self.ctx, {
            'clients': {
                "client.{0}".format(self.mount_a.client_id): [
                    "fs/misc/trivial_sync.sh"],
            },
            "timeout": "1h"
        })

