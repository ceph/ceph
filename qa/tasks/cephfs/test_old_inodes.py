import os
import json
import logging
import collections
from pathlib import Path

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.test_volumes import TestVolumesHelper

log = logging.getLogger(__name__)

"""
A subvolume owns its snaprealm, so its snapids never reach /volumes, the
subvolume group or /.  Snaprealms inherit downward, not upward.  An old_inode
CoW'd onto one of those ancestors for a subvolume snapid can therefore never
be referenced by anything, and nothing in the normal write path is guaranteed
to reclaim it - so it accumulates until the ancestor's dentry grows past
mds_dir_max_commit_size and can no longer be written at all.

CInode::pre_cow_old_inode() must therefore advance @first past the global
snaprealm seq - which is what keeps @first monotonic when an inode moves
between realms - without minting an old_inode, unless a snapshot in the
inode's OWN realm can reference the version being replaced.

Two things are checked here, and both matter:

  1. an ancestor whose snaprealm has no snapshot covering the range is not
     CoW'd at all, and every subvolume snapshot still reads back;
  2. an inode whose realm DOES have a covering snapshot is still CoW'd.

Without the second, the first could be satisfied by a change that simply
stopped preserving data.
"""

class TestOldInodeCoW(TestVolumesHelper):
    """
    Ancestors of a subvolume must not be CoW'd for snapids their snaprealm
    can never reference - and the subvolume's snapshots must still read back.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    SNAPS = 30

    def setUp(self):
        super(TestOldInodeCoW, self).setUp()
        # rstat propagation up the tree is throttled by
        # mds_dirstat_min_interval (default 1s).  With the throttle on an
        # ancestor may not be reached at all and its old_inodes would read 0
        # for the wrong reason, making the assertion below vacuous.
        self.config_set('mds', 'mds_dirstat_min_interval', '0')

    def _ino(self, path):
        rel = str(path).lstrip("/") or "."
        return self.mount_a.path_to_ino(rel)

    def _rel(self, path):
        """
        A path from `subvolume getpath` is filesystem-absolute.  Mount.read_file
        and Mount.write_file prepend the mount point with os.path.join(), which
        is a no-op for an absolute second argument, so strip the leading slash
        before handing them a path.
        """
        return str(path).lstrip("/")

    def _sync(self):
        """
        Flush before snapshotting.  write_file() opens with O_TRUNC, so the
        MDS sees setattr size=0 first and the real size arrives later with the
        client's cap flush.  A snapshot taken in between CoWs a size-0 version
        and then depends on the client's snapflush to fill it in - which is a
        race, and one that has been observed to lose.
        """
        self.mount_a.run_shell(["sync"])

    def _write(self, path, content):
        self.mount_a.write_file(self._rel(path), content)

    def _read(self, path):
        return self.mount_a.read_file(self._rel(path))

    def _old_inodes(self, path):
        """len(old_inodes) for `path`, or None if it is not in the cache."""
        try:
            dump = self.fs.mds_asok(['dump', 'inode', hex(self._ino(path))])
        except CommandFailedError:
            return None
        return len(dump["old_inodes"]) if dump else None

    def _subvol_paths(self, group, subvol):
        sv = Path(self._fs_cmd("subvolume", "getpath", self.volname,
                               subvol, group).strip())
        # sv == /volumes/<group>/<subvol>/<uuid>
        return collections.OrderedDict([
            ("subvol", sv.parent),
            ("group", sv.parent.parent),
            ("volumes", sv.parent.parent.parent),
            ("root", Path("/")),
        ])

    def _make_subvolume(self):
        group = self._gen_subvol_grp_name()
        subvol = self._gen_subvol_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolume", "create", self.volname, subvol, group,
                     "--mode=777")
        return group, subvol

    def _cleanup_subvolume(self, group, subvol, snapnames):
        for name in snapnames:
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             subvol, name, group, "--force")
            except CommandFailedError:
                pass
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group, "--force")
        self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        self._wait_for_trash_empty()

    def _restart_mds(self):
        """
        Re-fetch the dirfrags from RADOS so @snap_purged_thru advances to
        @last_destroyed and the purge gate is SHUT - the state a long lived
        cluster is in, and the one the fix has to hold in.
        """
        # Unmount cleanly BEFORE failing the rank.  Forcing the unmount
        # afterwards leaves the client's session behind in the sessionmap,
        # so the restarted MDS sits in up:reconnect for the whole of
        # mds_reconnect_timeout waiting for a client that is already gone,
        # and then logs "evicting unresponsive client" - which fails the job
        # on the cluster log check.
        self.mount_a.umount_wait()
        self.fs.mds_asok(["flush", "journal"])
        self.fs.mds_asok(["flush", "journal"])
        self.fs.fail()
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

    def _subvolume_snapshot_arm(self, use_global_seq):
        """
        Take SNAPS snapshots of a subvolume, then assert:

          * /volumes and the subvolume group keep NO old_inodes - their
            snaprealm can never reference a subvolume snapid, because
            snaprealms inherit downward, not upward;
          * the subvolume itself KEEPS its old_inodes - if it does not, the
            guard is over-suppressing and the assertion above is vacuous;
          * .snap lists every snapshot - handle_client_lssnap() filters on
            diri->get_oldest_snap(), which rises when a CoW is skipped, so a
            raised watermark must not hide a live snapshot;
          * every snapshot reads back the content that was current when it was
            taken - without this the whole test is passable by a change that
            simply stopped preserving data;
          * removing every snapshot does not mint anything on the ancestors
            either.  rmsnap allocates a FRESH snapid as the realm's new seq,
            so it advances last_destroyed and with it the global snaprealm
            seq - pre-fix that cowed the ancestors once per removal, which was
            half of the observed growth.

        Runs with the purge gate SHUT (via the MDS restart) because that is
        the state a long lived cluster is in, and the one where nothing
        downstream would reclaim a mistake.
        """
        self.config_set('mds', 'mds_use_global_snaprealm_seq_for_subvol',
                        use_global_seq)
        self.assertEqual(
            self.config_get('mds', 'mds_use_global_snaprealm_seq_for_subvol'),
            'true' if use_global_seq else 'false')
        # suppress trimming so nothing can commit a dirfrag and quietly purge
        # behind us - anything we observe here has to be absence of CoW, not
        # reclaim after the fact
        self.config_set('mds', 'mds_log_max_segments', '1024')

        group, subvol = self._make_subvolume()
        paths = self._subvol_paths(group, subvol)
        # `getpath` returns /volumes/<group>/<subvol>/<uuid>; a v2 subvolume
        # snapshot lives at /volumes/<group>/<subvol>/.snap/<name>, so the data
        # written at <uuid>/f reads back at .snap/<name>/<uuid>/f
        data_dir = Path(self._fs_cmd("subvolume", "getpath", self.volname,
                                     subvol, group).strip())
        uuid = data_dir.name
        snapdir = paths["subvol"] / ".snap"
        snapnames = []
        expected = []

        try:
            self._restart_mds()

            for i in range(self.SNAPS):
                content = "gen-%d" % i
                self._write(data_dir / "f", content)
                self._sync()
                name = "s_%d" % i
                self._fs_cmd("subvolume", "snapshot", "create", self.volname,
                             subvol, name, group)
                snapnames.append(name)
                expected.append((name, content))

            counts = collections.OrderedDict(
                (label, self._old_inodes(path))
                for label, path in paths.items())
            log.info("OLDINO global_seq=%s after %d subvolume snapshots: %s",
                     use_global_seq, self.SNAPS, json.dumps(counts))

            for label in ("volumes", "group"):
                self.assertIsNotNone(
                    counts[label],
                    "%s was not in the MDS cache, cannot measure old_inodes "
                    "(counts=%s)" % (label, counts))
                self.assertEqual(
                    counts[label], 0,
                    "%s accumulated %d old_inodes over %d subvolume snapshots "
                    "(counts=%s). Its snaprealm has no snapshot that can "
                    "reference them, so pre_cow_old_inode() should have "
                    "advanced first and skipped the CoW."
                    % (label, counts[label], self.SNAPS, counts))

            # the subvolume itself owns the snapshots, so it MUST keep its
            # old_inodes - if this is 0 the guard is over-suppressing and
            # snapshots are broken, which would make the assertions above
            # meaningless
            self.assertGreaterEqual(
                counts["subvol"], self.SNAPS // 2,
                "the subvolume itself lost its old_inodes (counts=%s); the "
                "CoW guard is suppressing versions that its own snapshots "
                "reference" % counts)

            # skipping a CoW raises get_oldest_snap(), which is what
            # handle_client_lssnap() filters .snap on - no snapshot may vanish
            listed = sorted(self.mount_a.ls(self._rel(snapdir)))
            self.assertEqual(
                listed, sorted(snapnames),
                "%s lists %s but %d snapshots exist; a raised "
                "get_oldest_snap() must not hide one"
                % (snapdir, listed, len(snapnames)))

            # ... and the point of all of it: every snapshot still serves what
            # was visible when it was taken.  Asserting the ancestors are empty
            # without this would be passable by a change that simply stopped
            # preserving data.
            for name, want in expected:
                snap_file = snapdir / name / uuid / "f"
                got = self._read(snap_file)
                self.assertEqual(
                    got, want,
                    "%s should read %r but read %r - a subvolume snapshot lost "
                    "data" % (snap_file, want, got))

            # removal advances the global seq too; the ancestors must stay empty
            for name in snapnames:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             subvol, name, group)
            snapnames = []

            after = collections.OrderedDict(
                (label, self._old_inodes(path)) for label, path in paths.items())
            log.info("OLDINO after removing %d snapshots: %s",
                     self.SNAPS, json.dumps(after))
            for label in ("volumes", "group"):
                self.assertEqual(
                    after[label], 0,
                    "%s accumulated %s old_inodes while the %d snapshots were "
                    "being REMOVED (after=%s). rmsnap allocates a fresh snapid "
                    "as the realm's seq, so it moves the global snaprealm seq "
                    "just as creation does."
                    % (label, after[label], self.SNAPS, after))
        finally:
            self._cleanup_subvolume(group, subvol, snapnames)

    def test_ancestors_are_not_cowed_for_subvolume_snapshots(self):
        """Default configuration: follows comes from the global snaprealm."""
        self._subvolume_snapshot_arm(True)

    def test_ancestors_are_not_cowed_with_global_seq_config_disabled(self):
        """
        mds_use_global_snaprealm_seq_for_subvol=false takes a different branch
        of pre_cow_old_inode() for `follows`, and it is the only branch that
        consults SnapRealm::get_subvolume_ino().  The guard has to hold there
        too - and this is a configuration that ships in fs suites.
        """
        self._subvolume_snapshot_arm(False)

    def test_snapshot_on_volumes_does_cow_the_ancestors(self):
        """
        The mirror image, on the real subvolume tree: when /volumes ITSELF is
        snapshotted its snaprealm does hold a snapid that covers everything
        below it, so /volumes and the subvolume group must be CoW'd.

        test_cow_still_happens_under_a_snapshotted_directory checks the same
        property on a plain directory; this checks it on the two inodes the
        change actually targets, where an over-suppressing guard would do real
        damage.  / stays empty: the snapshot is in /volumes' realm, not root's.
        """
        self.config_set('mds', 'mds_log_max_segments', '1024')

        group, subvol = self._make_subvolume()
        paths = self._subvol_paths(group, subvol)
        data_dir = Path(self._fs_cmd("subvolume", "getpath", self.volname,
                                     subvol, group).strip())
        rel_from_volumes = data_dir.relative_to(paths["volumes"])

        try:
            self._write(data_dir / "f", "before-v1")
            self._sync()
            # /volumes is created mode 0755 owned by root, so an unprivileged
            # mkdir under its .snap is refused with EACCES
            self.mount_a.run_shell(
                ["sudo", "mkdir", self._rel(paths["volumes"] / ".snap" / "v1")])

            # dirty the subvolume so the ancestors are CoW'd against v1
            self._write(data_dir / "f", "after-v1")
            self.mount_a.run_shell(["sync"])

            counts = collections.OrderedDict(
                (label, self._old_inodes(path)) for label, path in paths.items())
            log.info("OLDINO with a snapshot on /volumes: %s",
                     json.dumps(counts))

            for label in ("volumes", "group"):
                self.assertGreaterEqual(
                    counts[label], 1,
                    "%s was not CoW'd although /volumes' snaprealm holds v1 "
                    "(counts=%s); the guard in pre_cow_old_inode() is "
                    "suppressing a version a snapshot references"
                    % (label, counts))

            self.assertEqual(
                counts["root"], 0,
                "/ was CoW'd for v1 (counts=%s), but v1 lives in /volumes' "
                "snaprealm and realms inherit downward, not upward" % counts)

            snap_file = paths["volumes"] / ".snap" / "v1" / rel_from_volumes / "f"
            got = self._read(snap_file)
            self.assertEqual(
                got, "before-v1",
                "%s should read 'before-v1' but read %r" % (snap_file, got))

            self.mount_a.run_shell(
                ["sudo", "rmdir", self._rel(paths["volumes"] / ".snap" / "v1")])
        finally:
            self._cleanup_subvolume(group, subvol, [])

    def test_cow_still_happens_under_a_snapshotted_directory(self):
        """
        Guard against the fix being too aggressive, and pin down where the
        CoW stops.

        One write to /parent/child walks the ancestors with a single @follows,
        and the three inodes it touches must be treated differently:

          /parent/child  inherits /parent's realm, which holds s1  -> CoW
          /parent        OWNS that realm, so find_snaprealm() returns it
                         without walking up at all                 -> CoW
          /              root's realm; s1 is NOT in it, because realms
                         inherit downward, not upward               -> no CoW

        /parent has to be CoW'd for the same reason as the child: writing
        under it moves its rstat, and /parent/.snap/s1 must still report the
        stat as of s1.  / does not, because nothing can ask for root's stat
        at s1 - there is no snapshot in root's realm.

        If pre_cow_old_inode() skips either of the first two, snapshots
        silently stop preserving data.  If it does NOT skip the third, the fix
        is not doing anything.
        """
        parent = "parent"
        child = "parent/child"

        self.mount_a.run_shell(["mkdir", "-p", child])
        self.mount_a.write_n_mb(os.path.join(child, "f"), 1)
        self.mount_a.run_shell(["mkdir", os.path.join(parent, ".snap", "s1")])

        # dirty the child so that it is CoW'd against s1
        self.mount_a.write_n_mb(os.path.join(child, "g"), 1)
        self.mount_a.run_shell(["sync"])

        counts = collections.OrderedDict(
            (label, self._old_inodes(path))
            for label, path in (("child", child), ("parent", parent),
                                ("root", "/")))
        log.info("OLDINO under a snapshotted parent: %s", json.dumps(counts))
        for label in counts:
            self.assertIsNotNone(
                counts[label], "%s not in the MDS cache (counts=%s)"
                % (label, counts))

        # both inodes inside the snapshotted realm must keep a version
        for label, path in (("child", child), ("parent", parent)):
            self.assertGreaterEqual(
                counts[label], 1,
                "%s was not CoW'd although its snaprealm holds s1 "
                "(counts=%s); the guard in pre_cow_old_inode() is suppressing "
                "a version that a snapshot references" % (path, counts))

        # ... and the inode ABOVE the realm must not.  This is the only place
        # that pins down where the CoW stops: without it, a regression that
        # CoW'd every ancestor up to root would still pass.
        self.assertEqual(
            counts["root"], 0,
            "/ was CoW'd for s1 (counts=%s), but s1 lives in /parent's "
            "snaprealm and realms inherit downward, not upward - nothing can "
            "ask for root's stat at s1, so pre_cow_old_inode() should have "
            "advanced first and skipped the CoW" % counts)

        self.mount_a.run_shell(["rmdir", os.path.join(parent, ".snap", "s1")])
        self.mount_a.run_shell(["rm", "-rf", parent])
