
import json
import logging

from mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)



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
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'poolset', *args
        )

    def _osd_count(self):
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
                (target_pg * self._osd_count()) / pools[0]['size'])
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
        osd_count = self._osd_count()

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


    def _test_auto_grow(self):
        """
        Create a pool with no initial size estimate, and see its
        pg allocation grow as data is written to it
        """
        self._poolset_cmd("create", "rados", "grower")
        replication_factor = osd_map['pools'][0]['size']
        target_pg = int(self.mgr_cluster.get_config("mon_target_pg_per_osd"))

        total_target_pgs = (target_pg * self._osd_count) / replication_factor

        # TODO calculate the sum capacity of all OSDs, and the
        # expected target PG count for the cluster, and 
        #self._write_some_data(

    def _test_crush_trees(self):
        """
        Given an OSD topology with two CRUSH roots, create
        a poolset that targets both, and check that the allocations
        of PG num match the target pg per osd within each root
        """
        pass
