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

    def helper_test(self, pools, root_map, bias, profile, overlapped_roots):
        # Here we simulate how _calc_pool_target() works.
        even_pools = {}
        for pool_name, p in pools.items():
            root_id = p['root_id']
            if root_id in overlapped_roots and profile == "scale-down":
                # for scale-down profile skip pools
                # with overlapping roots
                assert p['no_scale']
                continue

            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(p, pool_name, root_map,
                                                                                                 p['root_id'], p['capacity_ratio'], even_pools, bias, True, profile)

            if final_ratio == None:
                # no final_ratio means current pool is an even pool
                # and we do not have to do any assertion on it.
                # You will never hit this case with a scale up profile.
                continue

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio

            if profile == "scale-down":
                # We only care about even_pools when profile is a scale-down
                assert not p['even_pools'] and pool_name not in even_pools

        if profile == "scale-down":
            for pool_name, p in even_pools.items():
                final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(p, pool_name, root_map,
                                                                                                     p['root_id'], p['capacity_ratio'], even_pools, bias, False, profile)

                assert p['expected_final_pg_target'] == final_pg_target
                assert p['expected_final_ratio'] == final_ratio
                assert p['even_pools'] and pool_name in even_pools

    def test_all_even_pools_scale_up(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.2,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.2,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 0.2,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.1,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        profile = "scale-up"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_all_even_pools_scale_down(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.25,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.25,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.2,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.25,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 128,
                "expected_final_ratio": 0.25,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        profile = "scale-down"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_uneven_pools_scale_up(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.1,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 256,
                "expected_final_ratio": 0.5,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.1,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 32,
                "expected_final_ratio": 0.1,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        profile = "scale-up"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_uneven_pools_scale_down(self):
        pools = {

            "test0": {

                "pool": 0,
                "pool_name": "test0",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 1/3,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test1": {

                "pool": 1,
                "pool_name": "test1",
                "pg_num_target": 32,
                "capacity_ratio": 0.5,
                "root_id": 0,
                "expected_final_pg_target": 256,
                "expected_final_ratio": 0.5,
                "even_pools": False,
                "size": 1,
                "no_scale": False,
            },

            "test2": {

                "pool": 2,
                "pool_name": "test2",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 1/3,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 64,
                "expected_final_ratio": 1/3,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(4, 400, 400),
            1: RootMapItem(4, 400, 400),

        }

        profile = "scale-down"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_uneven_pools_with_diff_roots_scale_up(self):
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
                "no_scale": False,
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
                "no_scale": False,
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
                "no_scale": False,
            },

            "test3": {

                "pool": 3,
                "pool_name": "test3",
                "pg_num_target": 32,
                "capacity_ratio": 0.1,
                "root_id": 0,
                "expected_final_pg_target": 512,
                "expected_final_ratio": 0.1,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

            "test4": {

                "pool": 4,
                "pool_name": "test4",
                "pg_num_target": 32,
                "capacity_ratio": 0.4,
                "root_id": 1,
                "expected_final_pg_target": 2048,
                "expected_final_ratio": 0.4,
                "even_pools": True,
                "size": 1,
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        profile = "scale-up"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_uneven_pools_with_diff_roots_scale_down(self):
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
                "no_scale": False,
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
                "no_scale": False,
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
                "no_scale": False,
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
                "no_scale": False,
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
                "no_scale": False,
            },

        }

        root_map = {

            0: RootMapItem(3, 5000, 5000),
            1: RootMapItem(2, 5000, 5000),

        }

        profile = "scale-down"
        bias = 1
        overlapped_roots = set()
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)

    def test_uneven_pools_with_overllaped_roots_scale_down(self):
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

        profile = "scale-down"
        bias = 1
        overlapped_roots = {0, 1}
        self.helper_test(pools, root_map, bias, profile, overlapped_roots)
