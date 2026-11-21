import logging

from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

"""
End-to-end tests for snapshot copy-on-write.

The scenarios are chosen to cover the cases where the CoW decision and the
purge decision consult different snaprealms:

  * a rename inside one snaprealm, where the old name has to survive in the
    snapshot but the new name must not appear in it,
  * a rename across snaprealms, where the inode has to keep the source realm's
    snapshots and must _not_ retroactively join the destination realm's older
    ones - for a DIRECTORY as well as a file, because a directory is the only
    thing pre_cow_old_inode() ever touches and so the only shape where the CoW
    guard meets the srnode gate in Server::_rename_prepare(); and for a
    directory that owns a snaprealm, which short-circuits that gate and takes
    the other branch of record_snaprealm_past_parent(),
  * what a snapshot preserves beyond file content: mode, xattrs and, for a
    directory, mtime - a directory's snapshot value is entirely metadata,
  * a realm that goes empty because its snapshots were deleted, which is the
    state in which the MDS may advance an inode's @first without copying.

The subvolume shape - where a subvolume's snapids never enter its ancestors'
realm - is checked the same way in
test_old_inodes.py::TestOldInodeCoW, which builds a real subvolume
through TestVolumesHelper rather than an ordinary snapshotted directory.
"""

class TestSnapCoW(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def setUp(self):
        super(TestSnapCoW, self).setUp()
        # rstat propagation up the tree is throttled by mds_dirstat_min_interval
        # (default 1s).  Turn it off so that one snapshot means one
        # propagation attempt, which keeps what these tests observe
        # deterministic rather than a race.
        self.config_set('mds', 'mds_dirstat_min_interval', '0')

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _write(self, path, content):
        self.mount_a.write_file(path, content)

    def _read(self, path):
        return self.mount_a.read_file(path)

    def _snap(self, dirpath, name):
        # Flush first.  write_file() opens with O_TRUNC, so the MDS sees
        # setattr size=0 and the real size arrives later with the client's cap
        # flush; a snapshot taken in between CoWs a size-0 version and then
        # relies on the client's snapflush to fill it in, which is a race.
        self.mount_a.run_shell(["sync"])
        self.mount_a.run_shell(["mkdir", "-p", "%s/.snap/%s" % (dirpath, name)])

    def _rmsnap(self, dirpath, name):
        self.mount_a.run_shell(["rmdir", "%s/.snap/%s" % (dirpath, name)])

    def _snap_path(self, dirpath, snapname, rel):
        return "%s/.snap/%s/%s" % (dirpath, snapname, rel)

    def _read_snap(self, dirpath, snapname, rel):
        return self._read(self._snap_path(dirpath, snapname, rel))

    def _exists(self, path):
        try:
            self.mount_a.run_shell(["test", "-e", path])
            return True
        except CommandFailedError:
            return False

    def _assert_snapshot_contents(self, expected):
        """
        expected: list of (dirpath, snapname, relpath, content).

        The check every test here comes down to: each snapshot must still serve
        the content that was visible when it was taken.
        """
        for dirpath, snapname, rel, want in expected:
            got = self._read_snap(dirpath, snapname, rel)
            self.assertEqual(
                got, want,
                "%s should read %r but read %r - a snapshot lost data"
                % (self._snap_path(dirpath, snapname, rel), want, got))

    def test_rename_within_snaprealm(self):
        """
        Renaming inside a realm must leave the OLD name readable in snapshots
        that covered it (journal_cow_dentry leaves a snapped dentry behind) and
        must _not_ make the NEW name appear in them (the new dentry's @first is
        past the current seq).
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/a" % d, "payload-A")
        self._snap(d, "s1")

        self.mount_a.run_shell(["mv", "%s/a" % d, "%s/b" % d])

        # old name survives in the snapshot, with its content
        self._assert_snapshot_contents([(d, "s1", "a", "payload-A")])
        # new name must not have existed at s1
        self.assertFalse(
            self._exists(self._snap_path(d, "s1", "b")),
            "the post-rename name appeared in a snapshot taken before it")
        # and the live tree is as renamed
        self.assertEqual(self._read("%s/b" % d), "payload-A")
        self.assertFalse(self._exists("%s/a" % d))

    def test_rename_within_snaprealm_then_overwrite(self):
        """
        Same, but the inode is modified after the rename, so the snapshot has
        to be served from a CoW'd version rather than from the head.
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/a" % d, "before")
        self._snap(d, "s1")
        self.mount_a.run_shell(["mv", "%s/a" % d, "%s/b" % d])
        self._write("%s/b" % d, "after")

        self._assert_snapshot_contents([(d, "s1", "a", "before")])
        self.assertEqual(self._read("%s/b" % d), "after")

    def test_rename_across_snaprealms(self):
        """
        Moving an inode to a different realm must:
          a) keep it in the SOURCE realm's snapshots it was already part of -
             record_snaprealm_past_parent() copies them into past_parent_snaps.
          b) not put it into the destination realm's older snapshots -
             current_parent_since jumps past the current global seq.

        Both are asserted through real reads, so this does not depend on how
        the MDS chose to represent it.
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])

        # give x and y their own snaprealms
        self._snap(x, "x0")
        self._snap(y, "y0")

        self._write("%s/f" % x, "in-x")
        self._snap(x, "x1")          # covers x/f
        self._snap(y, "y1")          # must NOT come to cover f

        self.mount_a.run_shell(["mv", "%s/f" % x, "%s/f" % y])

        # (a) the source realm's snapshot still serves it
        self._assert_snapshot_contents([(x, "x1", "f", "in-x")])
        # (b) the destination realm's older snapshot must not
        self.assertFalse(
            self._exists(self._snap_path(y, "y1", "f")),
            "a renamed inode appeared in a destination-realm snapshot taken "
            "before the rename")
        self.assertEqual(self._read("%s/f" % y), "in-x")

        # and a destination snapshot taken AFTER the move does cover it
        self._snap(y, "y2")
        self._assert_snapshot_contents([(y, "y2", "f", "in-x")])

    def test_rename_across_snaprealms_then_overwrite(self):
        """
        As above, with a modification after the move so the source realm's
        snapshot must be served from a preserved version.
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])
        self._snap(x, "x0")
        self._snap(y, "y0")
        self._write("%s/f" % x, "original")
        self._snap(x, "x1")

        self.mount_a.run_shell(["mv", "%s/f" % x, "%s/f" % y])
        self._write("%s/f" % y, "rewritten")

        self._assert_snapshot_contents([(x, "x1", "f", "original")])
        self.assertEqual(self._read("%s/f" % y), "rewritten")

    def test_rename_dir_across_snaprealms(self):
        """
        A DIRECTORY moved across snaprealms - the only shape where the CoW
        guard meets the srnode gate in Server::_rename_prepare():

            if (src_realm != dest_realm &&
                (srci->snaprealm || follows + 1 > srci->get_oldest_snap()))

        pre_cow_old_inode() is only ever called on directories, and skipping a
        CoW leaves oldest_snap untouched while @first advances - so
        get_oldest_snap() rises, and that is the value the gate reads.  If it
        rose far enough the gate would decline, no srnode would be created,
        and the moved directory would stop being covered by the source realm's
        snapshots.

        The renames in test_rename_across_snaprealms move a FILE, whose CoW
        goes through journal_cow_dentry() and is therefore untouched by the
        guard, so they cannot exercise this.

        The churn phase matters: with /x's realm holding no live snapshot the
        writes under /x/d are skipped rather than CoW'd, which is what drives
        get_oldest_snap() up before the gate is consulted.
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])

        # give x and y their own snaprealms, then empty x's again
        self._snap(x, "x0")
        self._snap(y, "y0")
        self._rmsnap(x, "x0")

        d = "x/d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/f" % d, "in-x")

        # churn while x's realm has no live snapshot, so these writes advance
        # @first on d without keeping a copy
        for i in range(6):
            self._write("%s/churn%d" % (d, i), "c%d" % i)
        self.mount_a.run_shell(["sync"])

        self._snap(x, "x1")          # now covers d and d/f
        self._snap(y, "y1")          # must NOT come to cover them

        self.mount_a.run_shell(["mv", d, "%s/d" % y])

        # the source realm's snapshot must still serve the whole subtree
        self._assert_snapshot_contents([(x, "x1", "d/f", "in-x")])
        # ... and the destination's older snapshot must not have gained it
        self.assertFalse(
            self._exists(self._snap_path(y, "y1", "d")),
            "a directory renamed into y appeared in y's snapshot y1, which "
            "was taken before the rename")
        self.assertEqual(self._read("%s/d/f" % y), "in-x")

        # a write after the move must not disturb the source realm's view
        self._write("%s/d/f" % y, "in-y")
        self._assert_snapshot_contents([(x, "x1", "d/f", "in-x")])

        # and a destination snapshot taken after the move does cover it
        self._snap(y, "y2")
        self._assert_snapshot_contents([(y, "y2", "d/f", "in-y")])

    def test_rename_snapshotted_dir_across_snaprealms(self):
        """
        The variant where the moved directory owns a snaprealm of its own,
        which the MDS permits - handle_client_rename() refuses only on a
        subvolume boundary, and it compares the realms of the PARENT
        directories, never the inode being moved.

        That takes a different path through the same gate.  With
        srci->snaprealm non-null,

            if (src_realm != dest_realm &&
                (srci->snaprealm || follows + 1 > srci->get_oldest_snap()))

        short-circuits on the first term and never evaluates
        get_oldest_snap(), and record_snaprealm_past_parent() then takes its
        else branch - oldparent comes from snaprealm->parent rather than
        find_snaprealm().  test_rename_dir_across_snaprealms covers the
        realm-less directory, which is the other half.

        Three things have to survive the move: the directory's OWN snapshot
        travels with it (the snapids live in its srnode), the source realm's
        snapshot keeps serving it (past_parent_snaps), and the destination's
        older snapshot must not start covering it (current_parent_since).
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])
        self._snap(x, "x0")
        self._snap(y, "y0")

        d = "x/d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/f" % d, "in-x")

        self._snap(d, "ds1")         # d gets a snaprealm of its own
        self._snap(x, "x1")          # ... and is also covered by x's
        self._snap(y, "y1")          # must NOT come to cover it

        self.mount_a.run_shell(["mv", d, "%s/d" % y])

        # d's own snapshot moved with it
        self._assert_snapshot_contents([("y/d", "ds1", "f", "in-x")])
        # the source realm's snapshot still serves it.  NOTE: this traverses a
        # remote snap dentry that journal_cow_dentry() left behind in x, whose
        # target is a directory - if anything here is going to surprise us it
        # is this, and it behaves identically before and after the CoW change.
        self._assert_snapshot_contents([(x, "x1", "d/f", "in-x")])
        # ... and the destination's older snapshot must not have gained it
        self.assertFalse(
            self._exists(self._snap_path(y, "y1", "d")),
            "a directory renamed into y appeared in y's snapshot y1, taken "
            "before the rename")
        self.assertEqual(self._read("%s/d/f" % y), "in-x")

        # a write after the move must disturb neither view
        self._write("%s/d/f" % y, "in-y")
        self._assert_snapshot_contents([("y/d", "ds1", "f", "in-x"),
                                        (x, "x1", "d/f", "in-x")])

        self._snap(y, "y2")
        self._assert_snapshot_contents([(y, "y2", "d/f", "in-y")])

    def test_snapshot_preserves_metadata(self):
        """
        cow_old_inode() copies the whole inode_t plus the xattrs, not just the
        data:

            old.inode = *pi;
            if (px) { old.xattrs = *px; }

        Every other test here compares file CONTENT, so a change that
        preserved data but mangled the rest would pass all of them.  It
        matters most for a directory, which has no content at all: a
        directory's snapshot value IS its metadata, and the only other
        assertion made about one anywhere in this series is a len(old_inodes)
        count read out of the admin socket.
        """
        d = "d"
        child = "d/child"
        self.mount_a.run_shell(["mkdir", "-p", child])
        self._write("%s/f" % child, "payload-1")
        self.mount_a.run_shell(["chmod", "0707", "%s/f" % child])
        self.mount_a.setfattr("%s/f" % child, "user.k", "v1")
        self.mount_a.run_shell(["chmod", "0705", child])
        self.mount_a.setfattr(child, "user.dk", "dv1")
        self.mount_a.run_shell(["sync"])

        before_file = self.mount_a.stat("%s/f" % child)
        before_dir = self.mount_a.stat(child)

        self._snap(d, "s1")

        # change every one of them
        self._write("%s/f" % child, "payload-2")
        self.mount_a.run_shell(["chmod", "0700", "%s/f" % child])
        self.mount_a.setfattr("%s/f" % child, "user.k", "v2")
        self.mount_a.run_shell(["chmod", "0711", child])
        self.mount_a.setfattr(child, "user.dk", "dv2")
        self.mount_a.run_shell(["mkdir", "%s/newentry" % child])  # bumps mtime
        self.mount_a.run_shell(["sync"])

        snap_file = self._snap_path(d, "s1", "child/f")
        snap_dir = self._snap_path(d, "s1", "child")

        # the file: content, mode and xattr as of s1
        self._assert_snapshot_contents([(d, "s1", "child/f", "payload-1")])
        self.assertEqual(
            self.mount_a.stat(snap_file)["st_mode"], before_file["st_mode"],
            "%s lost the mode it had when s1 was taken" % snap_file)
        self.assertEqual(
            self.mount_a.getfattr(snap_file, "user.k").strip(), "v1",
            "%s lost the xattr it had when s1 was taken" % snap_file)

        # the directory: mode, xattr and mtime as of s1.  It has no content,
        # so this is the whole of what its snapshot preserves.
        snapped = self.mount_a.stat(snap_dir)
        self.assertEqual(
            snapped["st_mode"], before_dir["st_mode"],
            "%s lost the mode it had when s1 was taken" % snap_dir)
        self.assertEqual(
            self.mount_a.getfattr(snap_dir, "user.dk").strip(), "dv1",
            "%s lost the xattr it had when s1 was taken" % snap_dir)
        self.assertAlmostEqual(
            snapped["st_mtime"], before_dir["st_mtime"], delta=0.01,
            msg="%s reports mtime %s but had %s when s1 was taken; adding an "
                "entry after the snapshot must not move it"
                % (snap_dir, snapped["st_mtime"], before_dir["st_mtime"]))

        self._rmsnap(d, "s1")
        self.mount_a.run_shell(["rm", "-rf", d])

    def test_reads_survive_a_realm_going_empty(self):
        """
        Deleting every snapshot in a realm leaves it with an empty snap set,
        which is the state in which the MDS may advance an inode's @first
        past the current seq iwthout keeping a copy - there is nothing that
        could reference the old version.

        Churn writes in that state, then snapshot again, and assert the new
        snapshots are intact.  If advancing @first ever skips a copy that was
        needed, this is where it shows up.
        """
        d = "d/sub"
        self.mount_a.run_shell(["mkdir", "-p", d])

        self._write("%s/f" % d, "era1")
        self._snap("d", "g1")
        self._rmsnap("d", "g1")            # realm now has no snaps

        for i in range(10):                # churn with an empty realm
            self._write("%s/f" % d, "churn-%d" % i)
            self._write("%s/other" % d, "x%d" % i)

        expected = []
        for i in range(6):
            content = "era2-%d" % i
            self._write("%s/f" % d, content)
            name = "g2_%d" % i
            self._snap("d", name)
            expected.append(("d", name, "sub/f", content))

        self._assert_snapshot_contents(expected)

    def test_reads_survive_interleaved_create_and_delete(self):
        """
        Snapshot rotation: each removal allocates a fresh snapid and moves the
        filesystem-wide last_destroyed, so the sequence the MDS CoWs against
        keeps moving while the realm's own contents churn.  Surviving
        snapshots must still read back.
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])

        live = []
        for i in range(14):
            content = "rot-%d" % i
            self._write("%s/f" % d, content)
            name = "r%d" % i
            self._snap(d, name)
            live.append((name, content))
            if len(live) > 4:
                gone, _ = live.pop(0)
                self._rmsnap(d, gone)
            # everything still live must read back, every round
            self._assert_snapshot_contents([(d, n, "f", c) for n, c in live])
