
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

        # Pick out two daemons to use
        mds_a, mds_b = sorted(self.mds_cluster.mds_ids[0:2]) 

        self.mount_a.umount_wait()
        self.fs.mds_stop()

        # Enable standby replay, to cover the bug case #8811 where
        # a standby replay might mistakenly end up trying to rewrite
        # the journal at the same time as an active daemon.
        self.fs.set_ceph_conf('mds', 'mds standby replay', "true")
        self.fs.set_ceph_conf('mds', 'mds standby for rank', "0")

        # Create a filesystem using the older journal format.
        self.fs.set_ceph_conf('mds', 'mds journal format', old_journal_version)
        self.fs.recreate()
        self.fs.mds_restart(mds_id=mds_a)
        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_active_names(), [mds_a])

        def replay_names():
            return [s['name']
                    for s in self.fs.status().get_replays(fscid = self.fs.id)]

        # Start the standby and wait for it to come up
        self.fs.mds_restart(mds_id=mds_b)
        self.wait_until_equal(
                replay_names,
                [mds_b],
                timeout = 30)

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
        self.fs.mds_fail_restart(mds_id=mds_a)
        self.fs.mds_fail_restart(mds_id=mds_b)

        # This ensures that all daemons come up into a valid state
        self.fs.wait_for_daemons()

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
        inspect_out = self.fs.journal_tool(["journal", "inspect"])
        if not inspect_out.endswith(": OK"):
            raise RuntimeError("Unexpected journal-tool result: '{0}'".format(
                inspect_out
            ))

        self.fs.journal_tool(["event", "get", "json", "--path", "/tmp/journal.json"])
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
        self.assertEqual(len(replay_names()), 1)
        self.assertEqual(len(self.fs.get_active_names()), 1)
        self.assertTrue(self.mds_cluster.mds_daemons[mds_a].running())
        self.assertTrue(self.mds_cluster.mds_daemons[mds_b].running())

