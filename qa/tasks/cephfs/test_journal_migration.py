
from StringIO import StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.workunit import task as workunit

JOURNAL_FORMAT_LEGACY = 0
JOURNAL_FORMAT_RESILIENT = 1


class TestJournalMigration(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_journal_migration(self):
        old_journal_version = JOURNAL_FORMAT_LEGACY
        new_journal_version = JOURNAL_FORMAT_RESILIENT

        self.mount_a.umount_wait()
        self.fs.mds_stop()

        # Create a filesystem using the older journal format.
        self.fs.set_ceph_conf('mds', 'mds journal format', old_journal_version)
        self.fs.mds_restart()
        self.fs.recreate()

        # Enable standby replay, to cover the bug case #8811 where
        # a standby replay might mistakenly end up trying to rewrite
        # the journal at the same time as an active daemon.
        self.fs.set_allow_standby_replay(True)

        status = self.fs.wait_for_daemons()

        self.assertTrue(self.fs.get_replay(status=status) is not None)

        # Do some client work so that the log is populated with something.
        with self.mount_a.mounted():
            self.mount_a.create_files()
            self.mount_a.check_files()  # sanity, this should always pass

            # Run a more substantial workunit so that the length of the log to be
            # coverted is going span at least a few segments
            workunit(self.ctx, {
                'clients': {
                    "client.{0}".format(self.mount_a.client_id): ["suites/fsstress.sh"],
                },
                "timeout": "3h"
            })

        # Modify the ceph.conf to ask the MDS to use the new journal format.
        self.fs.set_ceph_conf('mds', 'mds journal format', new_journal_version)

        # Restart the MDS.
        self.fs.mds_fail_restart()

        # This ensures that all daemons come up into a valid state
        status = self.fs.wait_for_daemons()

        # Check that files created in the initial client workload are still visible
        # in a client mount.
        with self.mount_a.mounted():
            self.mount_a.check_files()

        # Verify that the journal really has been rewritten.
        journal_version = self.fs.get_journal_version()
        if journal_version != new_journal_version:
            raise RuntimeError("Journal was not upgraded, version should be {0} but is {1}".format(
                new_journal_version, journal_version()
            ))

        # Verify that cephfs-journal-tool can now read the rewritten journal
        inspect_out = self.fs.journal_tool(["journal", "inspect"], 0)
        if not inspect_out.endswith(": OK"):
            raise RuntimeError("Unexpected journal-tool result: '{0}'".format(
                inspect_out
            ))

        self.fs.journal_tool(["event", "get", "json",
                              "--path", "/tmp/journal.json"], 0)
        p = self.fs.tool_remote.run(
            args=[
                "python",
                "-c",
                "import json; print len(json.load(open('/tmp/journal.json')))"
            ],
            stdout=StringIO())
        event_count = int(p.stdout.getvalue().strip())
        if event_count < 1000:
            # Approximate value of "lots", expected from having run fsstress
            raise RuntimeError("Unexpectedly few journal events: {0}".format(event_count))

        # Do some client work to check that writing the log is still working
        with self.mount_a.mounted():
            workunit(self.ctx, {
                'clients': {
                    "client.{0}".format(self.mount_a.client_id): ["fs/misc/trivial_sync.sh"],
                },
                "timeout": "3h"
            })

        # Check that both an active and a standby replay are still up
        status = self.fs.status()
        self.assertEqual(len(list(self.fs.get_replays(status=status))), 1)
        self.assertEqual(len(list(self.fs.get_ranks(status=status))), 1)
