# python unit test
import unittest
from tests import mock
import pytest
import json
from pg_autoscaler import module


class RootMapItem:

    def __init__(self, pool_count, pg_target, pg_left):

        self.pool_count = pool_count
        self.pg_target = pg_target
        self.pg_left = pg_left
        self.pool_used = 0


class TestPgAutoscaler(object):

    def setup(self):
        # a bunch of attributes for testing.
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)

    def helper_test(self, pools, root_map, bias, overlapped_roots):
        # Here we simulate how _get_pool_pg_target() works.

        bulk_pools = {}
        even_pools = {}

        # first pass
        for pool_name, p in pools.items():
            root_id = p['root_id']
            if root_id in overlapped_roots:
                # skip pools with overlapping roots
                assert p['no_scale']
                continue

            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(
                p, pool_name, root_map,
                p['root_id'], p['capacity_ratio'],
                bias, even_pools, bulk_pools, 'first', p['bulk'])

            if final_ratio == None:
                # no final_ratio means current pool is an even pool
                # and we do not have to do any assertion on it.
                continue

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio
            assert not p['expected_bulk_pool'] and pool_name not in bulk_pools

        # second pass
        for pool_name, p in bulk_pools.items():
            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(
                p, pool_name, root_map,
                p['root_id'], p['capacity_ratio'],
                bias, even_pools, bulk_pools, 'second', p['bulk'])

            if final_ratio == None:
                # no final_ratio means current pool is an even pool
                # and we do not have to do any assertion on it.
                continue

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio
            assert not p['even_pools'] and pool_name not in even_pools

        #third pass
        for pool_name, p in even_pools.items():
            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(
                p, pool_name, root_map,
                p['root_id'], p['capacity_ratio'],
                bias, even_pools, bulk_pools, 'third',  p['bulk'])

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio
            assert p['even_pools'] and pool_name in even_pools

    def test_even_pools_one_meta_three_bulk(self):
        pools = {

            "meta_0": {

                "pool": 0,
                "pool_name": "meta_0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.2,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk_0": {

                "pool": 1,
                "pool_name": "bulk_0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 1/3,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk_1": {

                "pool": 2,
                "pool_name": "bulk_1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 1/3,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk_2": {

                "pool": 3,
                "pool_name": "bulk_2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 1/3,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
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
                "expected_final_ratio": 0.2,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.2,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk0": {

                "pool": 2,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk1": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
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
                "expected_final_ratio": 0.1,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk0": {

                "pool": 1,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk1": {

                "pool": 2,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk2": {

                "pool": 3,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
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
                "expected_final_ratio": 0.1,
                "expected_bulk_pool": False,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": False, 
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.1,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk0": {

                "pool": 2,
                "pool_name": "bulk0",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk1": {

                "pool": 3,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
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
                "expected_final_ratio": 0.3,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.6,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk2": {

                "pool": 2,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.6,
                "expected_bulk_pool": True,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 1,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk4": {

                "pool": 4,
                "pool_name": "bulk4",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 1,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
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
                "expected_final_ratio": 0.4,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "meta1": {

                "pool": 1,
                "pool_name": "meta1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.6,
                "expected_bulk_pool": False,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
                "bulk": False,
            },

            "bulk1": {

                "pool": 2,
                "pool_name": "bulk1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk2": {

                "pool": 3,
                "pool_name": "bulk2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 1024,
                "expected_final_ratio": 0.5,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
            },

            "bulk3": {

                "pool": 4,
                "pool_name": "bulk4",
                "pg_num_target": 32,
                "capacity_ratio": 0.25,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 1,
                "expected_bulk_pool": True,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
                "bulk": True,
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
                "expected_final_ratio": 0.4,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.6,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.6,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.5,
                "even_pools": False,
                "size": 1,
                "no_scale": True,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 1,
                "even_pools": True,
                "size": 1,
                "no_scale": True,
            },

            "test4": {

                "pool": 4,
                "pool_name": "test4",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 1,
                "even_pools": True,
                "size": 1,
                "no_scale": True,
            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        bias = 1
        overlapped_roots = {0, 1}
        self.helper_test(pools, root_map, bias, overlapped_roots)
