import time
import logging
import struct
from io import BytesIO, StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class OpenFileTable(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _check_oft_counter(self, name, count):
        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        if perf_dump['oft'][name] == count:
            return True
        return False

    def test_max_items_per_obj(self):
        """
        The maximum number of openfiles omap objects keys are now equal to
        osd_deep_scrub_large_omap_object_key_threshold option.
        """
        self.set_conf("mds", "osd_deep_scrub_large_omap_object_key_threshold", "5")

        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        # Write some bytes to a file
        size_mb = 1

        # Hold the file open
        file_count = 8
        for i in range(0, file_count):
            filename = "open_file{}".format(i)
            self.mount_a.open_background(filename)
            self.mount_a.write_n_mb(filename, size_mb)

        time.sleep(10)

        """
        With osd_deep_scrub_large_omap_object_key_threshold value as 5 and
        opening 8 files we should have a new rados object with name
        mds0_openfiles.1 to hold the extra keys.
        """

        self.fs.radosm(["stat", "mds0_openfiles.1"])

    def test_perf_counters(self):
        """
        Opening a file should increment omap_total_updates by 1.
        """

        self.set_conf("mds", "osd_deep_scrub_large_omap_object_key_threshold", "1")
        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_updates_0 = perf_dump['oft']['omap_total_updates']
        log.info("omap_total_updates_0:{}".format(omap_total_updates_0))
        
        # Open the file
        p = self.mount_a.open_background("omap_counter_test_file")
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_updates', 2), timeout=120)
        
        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_updates_1 = perf_dump['oft']['omap_total_updates']
        log.info("omap_total_updates_1:{}".format(omap_total_updates_1))
        
        self.assertTrue((omap_total_updates_1 - omap_total_updates_0) == 2)
        
        # Now close the file
        self.mount_a.kill_background(p)
        # Ensure that the file does not exist any more
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_removes', 1), timeout=120)
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_kv_pairs', 1), timeout=120)

        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_removes = perf_dump['oft']['omap_total_removes']
        omap_total_kv_pairs = perf_dump['oft']['omap_total_kv_pairs']
        log.info("omap_total_removes:{}".format(omap_total_removes))
        log.info("omap_total_kv_pairs:{}".format(omap_total_kv_pairs))
        self.assertTrue(omap_total_removes == 1)
        self.assertTrue(omap_total_kv_pairs == 1)

    def _list_omap_keys(self, obj):
        out = self.fs.radosmo(["listomapkeys", obj], stdout=StringIO())
        return [k for k in out.strip().split('\n') if k]

    def _has_journal_keys(self, obj):
        return any(k.startswith("_journal.") for k in self._list_omap_keys(obj))

    def _write_metadata_blob(self, args, blob):
        """
        Run a rados command that takes its value from --input-file, staging
        the binary blob in a temp file on the remote.
        """
        remote = self.fs.mon_manager.controller
        path = remote.sh("mktemp").strip()
        try:
            remote.write_file(path, BytesIO(blob))
            self.fs.radosm(args + ["--input-file", path])
        finally:
            remote.run(args=["rm", "-f", path])

    def test_load_interrupted_shrinking_commit(self):
        """
        A commit that reduces the number of openfiles objects journals the
        new (smaller) object count into the headers of the trailing objects
        before the first object's header is rewritten. If the MDS dies
        between the journal and apply phases, a trailing object is left
        with a newer header that claims fewer objects than its own index
        while still holding its old keys. Loading such a table must not
        crash the MDS.
        """

        self.set_conf("mds", "osd_deep_scrub_large_omap_object_key_threshold", "5")
        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        # Spill the table into a second object (mds0_openfiles.1)
        procs = []
        for i in range(8):
            procs.append(self.mount_a.open_background(f"open_file{i}"))

        def obj1_populated():
            self.fs.rank_asok(["flush", "journal"])
            keys = self._list_omap_keys("mds0_openfiles.1")
            return any(not k.startswith("_journal.") for k in keys)
        self.wait_until_true(obj1_populated, timeout=120)

        self.fs.fail()

        # Recreate what an interrupted shrinking commit leaves on disk:
        # mds0_openfiles.0 keeps its old header (num_objs >= 2), while
        # mds0_openfiles.1 has a newer JOURNAL_START header with num_objs=1,
        # a _journal.0 key and its (not yet removed) anchors.
        hdr = self.fs.radosmo(["getomapheader", "mds0_openfiles.0", "-"],
                              stdout=BytesIO())
        (magic_len,) = struct.unpack_from("<I", hdr, 0)
        off = 4 + magic_len
        magic = hdr[4:off]
        struct_v, struct_compat, _ = struct.unpack_from("<BBI", hdr, off)
        version, num_objs, jstate = struct.unpack_from("<QIB", hdr, off + 6)
        log.info(f"mds0_openfiles.0 header: version={version} "
                 f"num_objs={num_objs} jstate={jstate}")
        self.assertGreaterEqual(num_objs, 2)

        JOURNAL_START = 1
        new_version = version + 1
        body = struct.pack("<QIB", new_version, 1, JOURNAL_START)
        new_hdr = struct.pack("<I", magic_len) + magic + \
            struct.pack("<BBI", struct_v, struct_compat, len(body)) + body

        anchors = [k for k in self._list_omap_keys("mds0_openfiles.1")
                   if not k.startswith("_journal.")]
        # journal entry: version, to_update (empty map), to_remove (set)
        journal = struct.pack("<QI", new_version, 0) + \
            struct.pack("<I", len(anchors))
        for k in anchors:
            journal += struct.pack("<I", len(k)) + k.encode()

        self._write_metadata_blob(["setomapheader", "mds0_openfiles.1"], new_hdr)
        self._write_metadata_blob(["setomapval", "mds0_openfiles.1", "_journal.0"],
                                  journal)

        # The table is loaded (asynchronously) during up:replay
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        gid = self.fs.get_rank(rank=0)['gid']

        # The incomplete journal is discarded once the table is loaded
        self.wait_until_true(
            lambda: not self._has_journal_keys("mds0_openfiles.1"), timeout=60)

        # Closing the files removes the anchors loaded from the second
        # object, which must still be accounted for in the object count.
        for p in procs:
            self.mount_a.kill_background(p)

        def obj1_drained():
            self.fs.rank_asok(["flush", "journal"])
            keys = self._list_omap_keys("mds0_openfiles.1")
            return not any(not k.startswith("_journal.") for k in keys)
        self.wait_until_true(obj1_drained, timeout=120)

        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_rank(rank=0)['gid'], gid)
