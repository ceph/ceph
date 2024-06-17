from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra import run

import logging
import os
import time
log = logging.getLogger(__name__)

DIRFRAG_KILLPOINTS = [
  # (killpoint, rank), assuming rank=1 is auth
  (1, 1),
  (2, 0),
  (3, 0),
  (4, 1),
  (5, 1),
  (6, 1),
  (7, 1),
  (8, 1),
  (9, 1),
  (10, 1),
]

class TestFragmentation(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def get_splits(self, rank=0):
        return self.fs.rank_asok(['perf', 'dump', 'mds'], rank=rank)['mds']['dir_split']

    def get_merges(self, rank=0):
        return self.fs.rank_asok(['perf', 'dump', 'mds'], rank=rank)['mds']['dir_merge']

    def get_dir_ino(self, path, rank=0):
        dir_cache = self.fs.read_cache(path, 0, rank=rank)
        dir_ino = None
        dir_inono = self.mount_a.path_to_ino(path.strip("/"))
        for ino in dir_cache:
            if ino['ino'] == dir_inono:
                dir_ino = ino
                break
        self.assertIsNotNone(dir_ino)
        return dir_ino

    def _configure(self, **kwargs):
        """
        Apply kwargs as MDS configuration settings.
        """

        for k, v in kwargs.items():
            self.config_set('mds', k.__str__(), v.__str__())

    def _test_oversize(self, killpoint=None):
        """
        That a directory is split when it becomes too large.
        """

        split_size = 20
        merge_size = 5

        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        confs = {
            'mds_bal_split_size': split_size,
            'mds_bal_merge_size': merge_size,
            'mds_bal_split_bits': 1,
        }
        self._configure(**confs)
        if killpoint is not None:
            log.info(f"testing killpoint {killpoint}")
            kill_rank = next(filter(lambda k: k[0] == killpoint, DIRFRAG_KILLPOINTS))[1]
            self.fs.set_config('mds_kill_dirfrag_at', str(killpoint), rank=kill_rank)

        # In order to exercise MMDSFragmentNotify, we need 3 MDS with 3 nested
        # subtrees. Also, all MDS need to have splitdir replicated, use
        # subtrees for bottom{0,2} below to effect that.
        subtrees = []
        self.mount_a.run_shell_payload("mkdir -p top/splitdir/bottom{0,2}/placeholder")
        self.mount_a.setfattr("top", "ceph.dir.pin", 2)
        subtrees.append(('/top', 2))
        self.mount_a.setfattr("top/splitdir/bottom0", "ceph.dir.pin", 0)
        subtrees.append(('/top/splitdir/bottom0', 0))
        self._wait_subtrees(subtrees, status=status, rank=2)
        self.mount_a.create_n_files("top/splitdir/file", split_size-2) # -2 because bottom{0,2} exist
        self.mount_a.setfattr("top/splitdir", "ceph.dir.pin", 1)
        subtrees.append(('/top/splitdir', 1))
        self.mount_a.setfattr("top/splitdir/bottom2", "ceph.dir.pin", 2)
        subtrees.append(('/top/splitdir/bottom2', 2))
        self._wait_subtrees(subtrees, status=status, rank=1)
        self.assertEqual(self.get_splits(rank=1), 0)
        dir_cache = self.fs.read_cache("top/", rank=1)
        log.info(f"splitdir = {dir_cache}")

        # create the final dentry to trigger split
        self.mount_a.run_shell_payload("touch top/splitdir/fileN")

        if killpoint is not None:
            kill_rank = next(filter(lambda k: k[0] == killpoint, DIRFRAG_KILLPOINTS))[1]
            self.fs.wait_for_death(timeout=60, status=status, rank=kill_rank)
            rinfo = self.fs.get_rank(rank=kill_rank, status=status)
            self.delete_mds_coredump(rinfo['name'])

        self.wait_until_true(
            lambda: self.get_splits(rank=1) >= 1,
            timeout=30
        )

        self.wait_until_true(
            lambda: len(self.get_dir_ino("/top/splitdir", rank=1)['dirfrags']) == 2,
            timeout=30
        )

        ino = self.mount_a.path_to_ino("top/splitdir")
        ino = "0x{:x}".format(ino)
        frags = self.get_dir_ino("/top/splitdir", rank=1)['dirfrags']
        self.assertEqual(len(frags), 2)
        self.assertEqual(frags[0]['dirfrag'], ino+".0*")
        self.assertEqual(frags[1]['dirfrag'], ino+".1*")
        self.assertEqual(
            sum([len(f['dentries']) for f in frags]),
            split_size + 1
        )

        self.assertEqual(self.get_merges(rank=1), 0)

        self.mount_a.run_shell_payload("rm -f top/splitdir/file*")

        self.wait_until_true(
            lambda: self.get_merges(rank=1) == 1,
            timeout=30
        )

        self.assertEqual(len(self.get_dir_ino("/top/splitdir", rank=1)["dirfrags"]), 1)

    def test_oversize(self):
        self._test_oversize()

    def test_rapid_creation(self):
        """
        That the fast-splitting limit of 1.5x normal limit is
        applied when creating dentries quickly.
        """

        split_size = 100
        merge_size = 1

        self._configure(
            mds_bal_split_size=split_size,
            mds_bal_merge_size=merge_size,
            mds_bal_split_bits=3,
            mds_bal_fragment_size_max=int(split_size * 1.5 + 2)
        )

        # We test this only at a single split level.  If a client was sending
        # IO so fast that it hit a second split before the first split
        # was complete, it could violate mds_bal_fragment_size_max -- there
        # is a window where the child dirfrags of a split are unfrozen
        # (so they can grow), but still have STATE_FRAGMENTING (so they
        # can't be split).

        # By writing 4x the split size when the split bits are set
        # to 3 (i.e. 4-ways), I am reasonably sure to see precisely
        # one split.  The test is to check whether that split
        # happens soon enough that the client doesn't exceed
        # 2x the split_size (the "immediate" split mode should
        # kick in at 1.5x the split size).

        self.assertEqual(self.get_splits(), 0)
        self.mount_a.create_n_files("splitdir/file", split_size * 4)
        self.wait_until_equal(
            self.get_splits,
            1,
            reject_fn=lambda s: s > 1,
            timeout=30
        )

    def test_deep_split(self):
        """
        That when the directory grows many times larger than split size,
        the fragments get split again.
        """

        split_size = 100
        merge_size = 1  # i.e. don't merge frag unless its empty
        split_bits = 1

        branch_factor = 2**split_bits

        # Arbitrary: how many levels shall we try fragmenting before
        # ending the test?
        max_depth = 5

        self._configure(
            mds_bal_split_size=split_size,
            mds_bal_merge_size=merge_size,
            mds_bal_split_bits=split_bits
        )

        # Each iteration we will create another level of fragments.  The
        # placement of dentries into fragments is by hashes (i.e. pseudo
        # random), so we rely on statistics to get the behaviour that
        # by writing about 1.5x as many dentries as the split_size times
        # the number of frags, we will get them all to exceed their
        # split size and trigger a split.
        depth = 0
        files_written = 0
        splits_expected = 0
        while depth < max_depth:
            log.info("Writing files for depth {0}".format(depth))
            target_files = branch_factor**depth * int(split_size * 1.5)
            create_files = target_files - files_written

            self.run_ceph_cmd("log",
                "{0} Writing {1} files (depth={2})".format(
                    self.__class__.__name__, create_files, depth
                ))
            self.mount_a.create_n_files("splitdir/file_{0}".format(depth),
                                        create_files)
            self.run_ceph_cmd("log","{0} Done".format(self.__class__.__name__))

            files_written += create_files
            log.info("Now have {0} files".format(files_written))

            splits_expected += branch_factor**depth
            log.info("Waiting to see {0} splits".format(splits_expected))
            try:
                self.wait_until_equal(
                    self.get_splits,
                    splits_expected,
                    timeout=30,
                    reject_fn=lambda x: x > splits_expected
                )

                frags = self.get_dir_ino("/splitdir")['dirfrags']
                self.assertEqual(len(frags), branch_factor**(depth+1))
                self.assertEqual(
                    sum([len(f['dentries']) for f in frags]),
                    target_files
                )
            except:
                # On failures, log what fragmentation we actually ended
                # up with.  This block is just for logging, at the end
                # we raise the exception again.
                frags = self.get_dir_ino("/splitdir")['dirfrags']
                log.info("depth={0} splits_expected={1} files_written={2}".format(
                    depth, splits_expected, files_written
                ))
                log.info("Dirfrags:")
                for f in frags:
                    log.info("{0}: {1}".format(
                        f['dirfrag'], len(f['dentries'])
                    ))
                raise

            depth += 1

        # Remember the inode number because we will be checking for
        # objects later.
        dir_inode_no = self.mount_a.path_to_ino("splitdir")

        self.mount_a.run_shell(["rm", "-rf", "splitdir/"])
        self.mount_a.umount_wait()

        self.fs.mds_asok(['flush', 'journal'])

        def _check_pq_finished():
            num_strays = self.fs.mds_asok(['perf', 'dump', 'mds_cache'])['mds_cache']['num_strays']
            pq_ops = self.fs.mds_asok(['perf', 'dump', 'purge_queue'])['purge_queue']['pq_executing']
            return num_strays == 0 and pq_ops == 0

        # Wait for all strays to purge
        self.wait_until_true(
            lambda: _check_pq_finished(),
            timeout=1200
        )
        # Check that the metadata pool objects for all the myriad
        # child fragments are gone
        metadata_objs = self.fs.radosmo(["ls"], stdout=StringIO()).strip()
        frag_objs = []
        for o in metadata_objs.split("\n"):
            if o.startswith("{0:x}.".format(dir_inode_no)):
                frag_objs.append(o)
        self.assertListEqual(frag_objs, [])

    def test_split_straydir(self):
        """
        That stray dir is split when it becomes too large.
        """
        def _count_fragmented():
            mdsdir_cache = self.fs.read_cache("~mdsdir", 1)
            num = 0
            for ino in mdsdir_cache:
                if ino["ino"] == 0x100:
                    continue
                if len(ino["dirfrags"]) > 1:
                    log.info("straydir 0x{:X} is fragmented".format(ino["ino"]))
                    num += 1;
            return num

        split_size = 50
        merge_size = 5
        split_bits = 1

        self._configure(
            mds_bal_split_size=split_size,
            mds_bal_merge_size=merge_size,
            mds_bal_split_bits=split_bits,
            mds_bal_fragment_size_max=(split_size * 100)
        )

        # manually split/merge
        self.assertEqual(_count_fragmented(), 0)
        self.fs.mds_asok(["dirfrag", "split", "~mdsdir/stray8", "0/0", "1"])
        self.fs.mds_asok(["dirfrag", "split", "~mdsdir/stray9", "0/0", "1"])
        self.wait_until_true(
            lambda: _count_fragmented() == 2,
            timeout=30
        )

        time.sleep(30)

        self.fs.mds_asok(["dirfrag", "merge", "~mdsdir/stray8", "0/0"])
        self.wait_until_true(
            lambda: _count_fragmented() == 1,
            timeout=30
        )

        time.sleep(30)

        # auto merge

        # merging stray dirs is driven by MDCache::advance_stray()
        # advance stray dir 10 times
        for _ in range(10):
            self.fs.mds_asok(['flush', 'journal'])

        self.wait_until_true(
            lambda: _count_fragmented() == 0,
            timeout=30
        )

        # auto split

        # there are 10 stray dirs. advance stray dir 20 times
        self.mount_a.create_n_files("testdir1/file", split_size * 20)
        self.mount_a.run_shell(["mkdir", "testdir2"])
        testdir1_path = os.path.join(self.mount_a.mountpoint, "testdir1")
        for i in self.mount_a.ls(testdir1_path):
            self.mount_a.run_shell(["ln", "testdir1/{0}".format(i), "testdir2/"])

        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        self.mount_a.wait_until_mounted()

        # flush journal and restart mds. after restart, testdir2 is not in mds' cache
        self.fs.mds_asok(['flush', 'journal'])
        self.mds_cluster.mds_fail_restart()
        self.fs.wait_for_daemons()
        # splitting stray dirs is driven by MDCache::advance_stray()
        # advance stray dir after unlink 'split_size' files.
        self.fs.mds_asok(['config', 'set', 'mds_log_events_per_segment', str(split_size)])

        self.assertEqual(_count_fragmented(), 0)
        self.mount_a.run_shell(["rm", "-rf", "testdir1"])
        self.wait_until_true(
            lambda: _count_fragmented() > 0,
            timeout=30
        )

    def test_dir_merge_with_snap_items(self):
        """
        That directory remain fragmented when snapshot items are taken into account.
        """
        split_size = 1000
        merge_size = 100
        self._configure(
            mds_bal_split_size=split_size,
            mds_bal_merge_size=merge_size,
            mds_bal_split_bits=1
        )

        # split the dir
        create_files = split_size + 50
        self.mount_a.create_n_files("splitdir/file_", create_files)

        self.wait_until_true(
            lambda: self.get_splits() == 1,
            timeout=30
        )

        frags = self.get_dir_ino("/splitdir")['dirfrags']
        self.assertEqual(len(frags), 2)
        self.assertEqual(frags[0]['dirfrag'], "0x10000000000.0*")
        self.assertEqual(frags[1]['dirfrag'], "0x10000000000.1*")
        self.assertEqual(
            sum([len(f['dentries']) for f in frags]), create_files
        )

        self.assertEqual(self.get_merges(), 0)

        self.mount_a.run_shell(["mkdir", "splitdir/.snap/snap_a"])
        self.mount_a.run_shell(["mkdir", "splitdir/.snap/snap_b"])
        self.mount_a.run_shell(["rm", "-f", run.Raw("splitdir/file*")])

        time.sleep(30)

        self.assertEqual(self.get_merges(), 0)
        self.assertEqual(len(self.get_dir_ino("/splitdir")["dirfrags"]), 2)

    def _run_dir_frag(self, killpoint):
        self._test_oversize(killpoint=killpoint)

def make_test_killpoints(killpoint):
    def test_export_killpoints(self):
        self.init = False
        self._run_dir_frag(killpoint)
        log.info("Test passed for killpoint %d" %killpoint)
    return test_export_killpoints

for (killpoint, rank) in DIRFRAG_KILLPOINTS:
    test_export_killpoints = make_test_killpoints(killpoint)
    setattr(TestFragmentation, "test_dirfrag_killpoints_%d" % (killpoint), test_export_killpoints)
