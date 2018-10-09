
import json
import logging

from mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


# OSDMap constants for pool.type
TYPE_REPLICATED = 1
TYPE_ERASURE = 3


def nearest_power_of_two(n):
    v = int(n)

    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16

    # High bound power of two
    v += 1

    # Low bound power of tow
    x = v >> 1

    return x if (v - n) > (n - x) else v


class TestPoolsets(MgrTestCase):
    # Some of these tests involve partially filling a pool
    REQUIRE_MEMSTORE = True

    def setUp(self):
        super(MgrTestCase, self).setUp()
        self._load_module("progress")
        self._load_module("poolsets")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            self.mgr_cluster.mon_manager.remove_pool(pool['pool_name'])

    def _poolset_cmd(self, *args):
        """
        A helper for brevity
        """
        return self._mon_cmd('poolset', *args)

    def _mon_cmd(self, *args):
        """
        A helper for brevity
        """
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _get_osd_count(self):
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    # TODO: this and several other methods are common with TestProgress
    def _write_some_data(self, t):
        """
        To adapt to test systems of varying performance, we write
        data for a defined time period, rather than to a defined
        capacity.  This will hopefully result in a similar timescale
        for PG recovery after an OSD failure.
        """

        args = [
            "rados", "-p", self.POOL, "bench", str(t), "write", "-t", "16"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _write_bytes(self, pool, byte_count):
        object_count = byte_count / (4 * 1024 * 1024)
        self.mgr_cluster.admin_remote.run(args=[
            "rados", "-p", pool, "bench", "3600", "write", "-t", "16",
               "--no-cleanup", "--max-objects", str(object_count)
            ], wait=True)

    def test_100_pc(self):
        """
        Simplest case, just create a poolset that uses all
        available capacity
        """

        target_pg = int(self.mgr_cluster.get_config("mon_target_pg_per_osd"))
        self._poolset_cmd("create", "rados", "diskhog", "100%")

        # The pool should have been created with a pg_num of mon_target_pg_per_osd,
        # divided by the replication factor, times the number of OSDs
        pools = self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']
        self.assertEqual(len(pools), 1)

        # Replicate the logic from the module
        expected_pg_num = nearest_power_of_two(
                (target_pg * self._get_osd_count()) / pools[0]['size'])
        self.assertEqual(pools[0]['pg_num_target'], expected_pg_num)

        json_output = self._poolset_cmd("ls")
        log.info("ls: {0}".format(json_output))
        ls_data = json.loads(json_output)
        self.assertEqual(len(ls_data), 1)
        self.assertEqual(ls_data[0]['name'], "diskhog")

        self._poolset_cmd("delete", "diskhog")
        pools = self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']
        self.assertEqual(len(pools), 0)

    def test_over_capacity(self):
        """
        Create pools with a requested capacity that exceeds what's possible,
        such that some shrinking has to happen when we created the last few pools.
        """
        max_pg = int(self.mgr_cluster.get_config("mon_max_pg_per_osd"))
        target_pg = int(self.mgr_cluster.get_config("mon_target_pg_per_osd"))
        osd_count = self._get_osd_count()

        oversize_ratio = float(max_pg) / target_pg

        pool_fraction = 0.2
        pool_pct = str(int(pool_fraction * 100)) + "%"

        # We'll go 20% over the max_pg count
        num_pools = int((oversize_ratio * 1.2) / pool_fraction)

        for i in range(0, num_pools):
            # Creating pools, and implicitly verifying that the creations
            # succeed
            self._poolset_cmd("create", "rados",
                              "test_ps_{0}".format(i), pool_pct)

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        self.assertEqual(len(osd_map['pools']), num_pools)

        # Read back what replication factor we ended up with
        replication_factor = osd_map['pools'][0]['size']

        pg_target_sum = sum([p['pg_num_target'] for p in osd_map['pools']])

        self.assertLess(pg_target_sum * replication_factor, max_pg * osd_count)

        # Fuzzy check that we ended up with a "reasonably high" PG count,
        # greater than half the target (just checking that we didn't create
        # a gratuitously small number)
        self.assertGreater(pg_target_sum * replication_factor, target_pg * osd_count / 2)


    def test_auto_grow(self):
        """
        Create a pool with no initial size estimate, and see its
        pg allocation grow as data is written to it
        """
        self._poolset_cmd("create", "rados", "grower")
        replication_factor = self._get_pool('grower')['size']
        target_pg = int(self.mgr_cluster.get_config("mon_target_pg_per_osd"))

        # Copy of constant from the poolsets module
        min_pg_num = 8

        # Work out total raw capacity of cluster
        df_json = self._mon_cmd("osd", "df", "--format=json-pretty")
        df = json.loads(df_json)
        total_capacity = df['summary']['total_kb'] * 1024

        # Write three times the amount of data associated with 8 PGs, which
        # should reliably trigger a factor of two increase in the pool's pg num
        target_usage_fraction = float((min_pg_num * 3) * replication_factor) \
                / (target_pg * self._get_osd_count())
        write_bytes = int((target_usage_fraction * total_capacity) / replication_factor)

        log.info("Writing {0} bytes".format(write_bytes))
        self._write_bytes("grower", write_bytes)

        # We should see the cluster respond by growing the pg num
        self.wait_until_equal(
                lambda: self._get_pool("grower")['pg_num'],
                min_pg_num * 2,
                timeout = 60)

    def _test_crush_trees(self):
        """
        Given an OSD topology with two CRUSH roots, create
        a poolset that targets both, and check that the allocations
        of PG num match the target pg per osd within each root
        """
        pass

    def _get_pool(self, name):
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return [p for p in osd_map['pools'] if p['pool_name'] == name][0]


    def test_create_options(self):
        """
        Cover some of the non-default ways of creating a poolset,
        erasure coding, custom replica counts, etc.
        """

        # That when selecting EC, a data pool is created with EC and
        # the metadata pool still uses replication
        self._poolset_cmd("create", "cephfs", "myps1", "--erasure-coding")
        self.assertEqual(self._get_pool("myps1.data")['type'], TYPE_ERASURE)
        self.assertEqual(self._get_pool("myps1.meta")['type'],
                         TYPE_REPLICATED)
        self._poolset_cmd("delete", "myps1")

        # That when selecting an EC profile, it is reflected in the
        # resulting pool
        self._mon_cmd("osd", "erasure-code-profile", "set", "myprofile",
            "plugin=jerasure", "technique=reed_sol_van", "k=4", "m=2")
        self._poolset_cmd("create", "cephfs", "myps2",
                          "--erasure-code-profile=myprofile")
        self.assertEqual(self._get_pool("myps2.data")['erasure_code_profile'],
                         "myprofile")
        self._poolset_cmd("delete", "myps2")

        # The when selecting a custom replica count, it takes effect
        self._poolset_cmd("create", "rados", "myps3",
                          "--replicas=1")
        self.assertEqual(self._get_pool("myps3")['size'], 1)
        self._poolset_cmd("delete", "myps3")

