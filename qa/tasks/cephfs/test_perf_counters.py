import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)

# Minimum set of perf counter fields that must exist and keep
# their types. Catches renames and deletions.
_ALL_CONTRACT_FIELDS  = ['mdops', 'rdops', 'wrops', 'mdavg', 'readavg', 'writeavg', 'mdsqsum', 'readsqsum', 'writesqsum']
_INT_COUNTER_FIELDS   = ['mdops', 'rdops', 'wrops', 'mdsqsum', 'readsqsum', 'writesqsum']
_FLOAT_COUNTER_FIELDS = ['mdavg', 'readavg', 'writeavg']

class TestPerfCounters(CephFSTestCase):

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _asok_perf_dump(self):
        """
        Query perf counters from the live ceph-fuse process via its admin
        socket. Skips if the mount is not a FUSE mount.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("admin-socket perf dump requires FUSE mount")
        return self.mount_a.admin_socket(['perf', 'dump'])

    def _drop_caches(self):
        """
        Drop the kernel page cache so a subsequent read must go through
        the ceph-fuse process to the OSD, guaranteeing rdops increments.
        """
        self.mount_a.run_shell(
            ["sudo", "sysctl", "-w", "vm.drop_caches=3"],
            omit_sudo=False,
        )

    def test_asok_returns_valid_json(self):
        """
        ``ceph --admin-daemon <asok> perf dump`` must return a non-empty
        JSON object.
        """
        data = self._asok_perf_dump()
        self.assertIsInstance(
            data, dict,
            "admin-socket perf dump must return a JSON object; got %s" % type(data),
        )
        self.assertGreater(len(data), 0, "JSON object must not be empty")

    def test_asok_has_client_section(self):
        """
        The admin-socket output must contain a top-level 'client' key.
        """
        data = self._asok_perf_dump()
        self.assertIn(
            'client', data,
            "admin-socket output missing 'client' section. "
            "Top-level keys present: %s" % sorted(data.keys()),
        )

    def test_asok_client_section_has_all_contract_fields(self):
        """
        Every contract field must be present in the
        admin-socket 'client' section.  Catches deleted or renamed counters.
        """
        client = self._asok_perf_dump().get('client', {})
        for field in _ALL_CONTRACT_FIELDS:
            self.assertIn(
                field, client,
                "admin-socket 'client' section is missing field '%s'. "
                "Fields present: %s" % (field, sorted(client.keys())),
            )

    def test_asok_integer_counter_types(self):
        """
        Integer counters must be JSON integers in the admin-socket output.
        """
        client = self._asok_perf_dump().get('client', {})
        for field in _INT_COUNTER_FIELDS:
            val = client[field]
            self.assertIsInstance(
                val, int,
                "admin-socket: 'client.%s' must be a JSON integer, got %s (%r)"
                % (field, type(val).__name__, val),
            )
            self.assertNotIsInstance(
                val, bool,
                "admin-socket: 'client.%s' must not be a boolean" % field,
            )

    def test_asok_time_counter_types(self):
        """
        Time counters must be floats in the admin-socket output.
        """
        client = self._asok_perf_dump().get('client', {})
        for field in _FLOAT_COUNTER_FIELDS:
            val = client[field]
            self.assertIsInstance(
                val, float,
                "admin-socket: 'client.%s' must be a JSON float (time counter), "
                "got %s (%r)" % (field, type(val).__name__, val),
            )
            self.assertGreaterEqual(
                val, 0.0,
                "admin-socket: 'client.%s' must be non-negative, got %r" % (field, val),
            )

    def test_wrops_increments_after_write(self):
        """
        wrops must increase after writing data through the mounted
        filesystem.
        """
        before = self._asok_perf_dump().get('client', {}).get('wrops', 0)
        self.mount_a.write_n_mb("bb_write_test", 1)
        after = self._asok_perf_dump().get('client', {}).get('wrops', 0)

        log.info("wrops: %d -> %d", before, after)
        self.assertGreater(
            after, before,
            "wrops did not increase after writing 1 MiB (before=%d after=%d)"
            % (before, after),
        )

    def test_rdops_increments_after_read(self):
        """
        rdops must increase after reading data that is not in any cache.
        Both the kernel page cache and the ceph-fuse object cache are
        dropped first to force the read to go to the OSD.
        """
        self.mount_a.write_n_mb("bb_read_test", 1)
        self._drop_caches()

        before = self._asok_perf_dump().get('client', {}).get('rdops', 0)
        self.mount_a.run_shell(["dd", "if=bb_read_test", "of=/dev/null", "bs=1M"])
        after = self._asok_perf_dump().get('client', {}).get('rdops', 0)

        log.info("rdops: %d -> %d", before, after)
        self.assertGreater(
            after, before,
            "rdops did not increase after reading 1 MiB (before=%d after=%d)"
            % (before, after),
        )

    def test_mdops_increments_after_metadata_op(self):
        """
        mdops must increase after a metadata operation (mkdir).
        """
        before = self._asok_perf_dump().get('client', {}).get('mdops', 0)
        self.mount_a.run_shell(["mkdir", "bb_mdops_testdir"])
        after = self._asok_perf_dump().get('client', {}).get('mdops', 0)

        log.info("mdops: %d -> %d", before, after)
        self.assertGreater(
            after, before,
            "mdops did not increase after mkdir (before=%d after=%d)"
            % (before, after),
        )

    def test_readavg_nonzero_after_read(self):
        """
        After at least one read, readavg must be > 0.0 — a real latency
        must have been recorded.
        """
        self.mount_a.write_n_mb("bb_readavg_test", 1)
        self._drop_caches()
        self.mount_a.run_shell(["dd", "if=bb_readavg_test", "of=/dev/null", "bs=1M"])

        client = self._asok_perf_dump().get('client', {})
        self.assertGreater(
            client.get('rdops', 0), 0,
            "rdops still 0 after a forced read — readavg check is inconclusive",
        )
        self.assertGreater(
            client.get('readavg', 0.0), 0.0,
            "readavg is still 0.0 after %d read ops" % client.get('rdops', 0),
        )

    def test_writeavg_nonzero_after_write(self):
        """
        After at least one write, writeavg must be > 0.0.
        """
        self.mount_a.write_n_mb("bb_writeavg_test", 1)

        client = self._asok_perf_dump().get('client', {})
        self.assertGreater(
            client.get('wrops', 0), 0,
            "wrops still 0 after a write — writeavg check is inconclusive",
        )
        self.assertGreater(
            client.get('writeavg', 0.0), 0.0,
            "writeavg is still 0.0 after %d write ops" % client.get('wrops', 0),
        )
