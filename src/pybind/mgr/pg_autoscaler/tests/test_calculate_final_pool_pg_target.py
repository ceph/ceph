# python unit test
from typing import Any, Dict, List, Tuple, Set
import unittest
from tests import mock
import pytest
import json
from collections import defaultdict
from pg_autoscaler import module
from pg_autoscaler.module import GroupKey, PoolGroup

class RootMapItem:

    def __init__(self, pool_count, pg_target, pg_left):
        self.pool_count = pool_count
        self.pg_target = pg_target
        self.pg_left = pg_left
        self.pool_used = 0
        self.pg_total = pg_left


class TestPgAutoscaler(object):

    def setup_method(self):
        # a bunch of attributes for testing.
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)

    def create_group(self,
        pools: Dict[str, Any],
        root_map: Dict[int, RootMapItem],
        bias: int,
    ) -> Dict[int, Dict[GroupKey, PoolGroup]]:
        pool_group = defaultdict(dict)
        for pool_name, p in pools.items():
            root_id = p['root_id']
            bulk = p['bulk']
            autoscale = p['autoscale']
            capacity_ratio = p['capacity_ratio']
            pg_target_managed = int(capacity_ratio * root_map[root_id].pg_left)
            pg_target_unmanaged = p['pg_num_target'] * p['size']
            pg_target = pg_target_managed if autoscale else pg_target_unmanaged
            pool_key = (pg_target, p['size'], bias, bulk, autoscale)
            if pool_key not in pool_group[root_id]:
                pool_group[root_id][pool_key] = PoolGroup(pg_target, p['size'], bias, autoscale)
            pool_group[root_id][pool_key].add(pool_name, p)

        return pool_group

    def helper_test(self,
                    pools: Dict[str, Any],
                    root_map: Dict[int, RootMapItem],
                    bias: int,
                    overlapped_roots: Set,
        ):
        # Here we simulate how _get_pool_pg_target() works.

        pool_group = self.create_group(pools, root_map, bias)

        # first pass
        for root_id in root_map:
            final_ratios, pool_pg_targets, final_pg_targets, out_pools = self.autoscaler._calculate_final_pool_pg_target(
                root_map, root_id, 'first', pool_group[root_id])
            i = 0
            for pool_name, p in pools.items():
                while i < len(final_ratios) and p['pool'] != out_pools[i]['pool']:
                    i+=1
                if i < len(final_ratios):
                    assert p['expected_final_pg_target'] == final_pg_targets[i]
                    assert p['expected_final_ratio'] == final_ratios[i]

        # second pass
        for root_id in root_map:
            final_ratios, pool_pg_targets, final_pg_targets, out_pools = self.autoscaler._calculate_final_pool_pg_target(
                root_map, root_id, 'second', pool_group[root_id])
            i = 0
            for pool_name, p in pools.items():
                while i < len(final_ratios) and p['pool'] != out_pools[i]['pool']:
                    i+=1
                if i < len(final_ratios):
                    assert p['expected_final_pg_target'] == final_pg_targets[i]
                    assert p['expected_final_ratio'] == final_ratios[i]
                    assert not p['expected_bulk_pool']
        # third pass
        for root_id in root_map:
            final_ratios, pool_pg_targets, final_pg_targets, out_pools = self.autoscaler._calculate_final_pool_pg_target(
                root_map, root_id, 'third', pool_group[root_id])
            i = 0
            for pool_name, p in pools.items():
                if root_id in overlapped_roots:
                    # skip pools with overlapping roots
                    assert p['no_scale']
                    continue
                while i < len(final_ratios) and p['pool'] != out_pools[i]['pool']:
                    i+=1
                if i < len(final_ratios):
                    assert p['expected_final_pg_target'] == final_pg_targets[i]
                    assert p['expected_final_ratio'] == final_ratios[i]
                    assert not p['even_pools']
        #fourth pass
        for root_id in root_map:
            print(pool_group[root_id])
            final_ratios, pool_pg_targets, final_pg_targets, out_pools = self.autoscaler._calculate_final_pool_pg_target(
                root_map, root_id, 'fourth', pool_group[root_id])
            i = 0
            for pool_name, p in pools.items():
                while i < len(final_ratios) and p['pool'] != out_pools[i]['pool']:
                    i+=1
                if i < len(final_ratios):
                    assert p['expected_final_pg_target'] == final_pg_targets[i]
                    assert p['expected_final_ratio'] == final_ratios[i]
                    assert p['even_pools']
    def test_even_pools_one_meta_three_bulk(self):
        pools = {

            "meta_0": {

                "pool": 0,
                "pool_name": "meta_0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/448,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk_0": {

                "pool": 1,
                "pool_name": "bulk_0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/448,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk_1": {

                "pool": 2,
                "pool_name": "bulk_1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/448,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk_2": {

                "pool": 3,
                "pool_name": "bulk_2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/448,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 448, 448),
            1: RootMapItem(4, 400, 400),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_even_pools_two_meta_two_bulk(self):
        pools = {

            "meta0": {

                "pool": 0,
                "pool_name": "meta0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/400,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/400,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk0": {

                "pool": 2,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk1": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_one_meta_three_bulk(self):
        pools = {

            "meta0": {

                "pool": 0,
                "pool_name": "meta0",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/400,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk0": {

                "pool": 1,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/400,
                "expected_bulk_pool": True,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk1": {

                "pool": 2,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk2": {

                "pool": 3,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_two_meta_two_bulk(self):
        pools = {

            "meta0": {

                "pool": 0,
                "pool_name": "meta0",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/400,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/400,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk0": {

                "pool": 2,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk1": {

                "pool": 3,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 128/400,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_with_diff_roots(self):
        pools = {

            "meta0": {

                "pool": 0,
                "pool_name": "meta0",
                "pg_num_target": 32,
                "capacity_ratio": 0.3,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk2": {

                "pool": 2,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": True,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk4": {

                "pool": 4,
                "pool_name": "bulk4",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_even_pools_with_diff_roots(self):
        pools = {

            "meta0": {

                "pool": 0,
                "pool_name": "meta0",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk1": {

                "pool": 2,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk2": {

                "pool": 3,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "bulk3": {

                "pool": 4,
                "pool_name": "bulk4",
                "pg_num_target": 32,
                "capacity_ratio": 0.25,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_with_overlapped_roots(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 512/5000,
                "even_pools": True,
                "expected_bulk_pool": False,
                "size": 1,
                "no_scale": True,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test4": {

                "pool": 4,
                "pool_name": "test4",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": True,
                "bulk": False,

"autoscale": True,
                "pg_autoscale_mode": False,            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        bias = 1
        overlapped_roots = {0, 1}
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_even_bulk_pools_with_same_root_id_and_capacity(self):
        pools = {

            "bulk0": {

                "pool": 0,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 3,
                "no_scale": False,
                "bulk": True,

"autoscale": True,
                "pg_autoscale_mode": False,            },

            "bulk1": {

                "pool": 1,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 3,
                "no_scale": False,
                "bulk": True,

"autoscale": True,
                "pg_autoscale_mode": False,            },

            "bulk2": {

                "pool": 2,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/5000,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 3,
                "no_scale": False,
                "bulk": True,

"autoscale": True,
                "pg_autoscale_mode": False,            },

        }
        root_map = {

            0: RootMapItem(3, 5000, 5000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_quantize_overestimate(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.62,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 5000, 5000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_quantize_underestimate(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.35,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/5000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


        }
        root_map = {

            0: RootMapItem(3, 5000, 5000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_same_cost(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.36,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/98,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.61,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/98,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


        }
        root_map = {

            0: RootMapItem(2, 98, 98),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_pools_same_weight_and_cost(self):
        pools = {


            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.35,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.32,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.35,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.32,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


        }
        root_map = {

            0: RootMapItem(2, 100, 100),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_base_overcount(self):
        pools = {


            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.48,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


        }
        root_map = {

            0: RootMapItem(2, 150, 150),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_uneven_distribution(self):
        pools = {


            "data1": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.33,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 3,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


            "data2": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.33,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 3,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

            "data3": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.33,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1536/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 3,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 6000, 6000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_bias_greater_than_one(self):
        pools = {


            "data1": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },


            "data2": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 2048/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 6000, 6000),

        }

        bias = 4
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_pg_autoscale_off(self):
        pools = {


            "data1": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.75,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": False,
                "pg_autoscale_mode": False,
            },


            "data2": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1024/6000,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 6000, 6000),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_autoscale_mixed(self):
        pools = {


            "data1": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 64,
                "capacity_ratio": 0.75,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": False,
                "pg_autoscale_mode": True,
            },


            "data2": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": False,
                "pg_autoscale_mode": True,
            },

            "data3": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.43,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 150, 150),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)

    def test_autoscale_mixed_bulk(self):
        pools = {


            "data1": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 64,
                "capacity_ratio": 0.75,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 64/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": False,
                "pg_autoscale_mode": True,
            },


            "data2": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": False,
                "pg_autoscale_mode": True,
            },

            "data3": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.43,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 32/150,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
                "autoscale": True,
                "pg_autoscale_mode": False,
            },

        }
        root_map = {

            0: RootMapItem(3, 150, 150),

        }

        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, overlapped_roots)