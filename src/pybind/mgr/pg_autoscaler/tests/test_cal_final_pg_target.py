#python unit test
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
        # a bunch of attributes for testing
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)

    def helper_test(self, pools, root_map, bias):

        even_pools = {}
        for pool_name, p in pools.items():
            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(p, pool_name, root_map, p['root_id'], p['capacity_ratio'], even_pools, bias, True)
            
            if final_ratio == None:
                continue

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio
            assert not p['even_pools'] and pool_name not in even_pools  

        for pool_name, p in even_pools.items():
            final_ratio, pool_pg_target, final_pg_target = self.autoscaler._calc_final_pg_target(p, pool_name, root_map, p['root_id'], p['capacity_ratio'], even_pools, bias, False) 

            assert p['expected_final_pg_target'] == final_pg_target
            assert p['expected_final_ratio'] == final_ratio
            assert p['even_pools'] and pool_name in even_pools

    def test_all_even_pools(self):
        pools = {

                "test0":{
                    
                    "pool": 0,
                    "pool_name": "test0",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.2,
                    "root_id":"0",
                    "expected_final_pg_target": 128,
                    "expected_final_ratio": 0.25,
                    "even_pools": True,
                    "size": 1,
                    },

                "test1":{

                    "pool": 1,
                    "pool_name": "test1",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.2,
                    "root_id":"0",
                    "expected_final_pg_target": 128,
                    "expected_final_ratio": 0.25,
                    "even_pools": True,
                    "size": 1,
                    },

                "test2":{

                    "pool": 2,
                    "pool_name": "test2",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.2,
                    "root_id":"0",
                    "expected_final_pg_target": 128,
                    "expected_final_ratio": 0.25,
                    "even_pools": True,
                    "size": 1,
                    },

                "test3":{

                    "pool": 3,
                    "pool_name": "test3",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.1,
                    "root_id": "0",
                    "expected_final_pg_target": 128,
                    "expected_final_ratio": 0.25,
                    "even_pools": True,
                    "size": 1,
                    },

                }

        root_map = {

                "0": RootMapItem(4, 400, 400),
                "1": RootMapItem(4, 400, 400),

                }

        bias = 1
        self.helper_test(pools, root_map, bias)

    def test_uneven_pools(self):
        pools = {

                "test0":{
                    
                    "pool": 0,
                    "pool_name": "test0",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.1,
                    "root_id":"0",
                    "expected_final_pg_target": 64,
                    "expected_final_ratio": 1/3,
                    "even_pools": True,
                    "size": 1,
                    },

                "test1":{

                    "pool": 1,
                    "pool_name": "test1",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.5,
                    "root_id":"0",
                    "expected_final_pg_target": 256,
                    "expected_final_ratio": 0.5,
                    "even_pools": False,
                    "size": 1,
                    },

                "test2":{

                    "pool": 2,
                    "pool_name": "test2",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.1,
                    "root_id":"0",
                    "expected_final_pg_target": 64,
                    "expected_final_ratio": 1/3,
                    "even_pools": True,
                    "size": 1,
                    },

                "test3":{

                    "pool": 3,
                    "pool_name": "test3",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.1,
                    "root_id": "0",
                    "expected_final_pg_target": 64,
                    "expected_final_ratio": 1/3,
                    "even_pools": True,
                    "size": 1,
                    },

                }

        root_map = {

                "0": RootMapItem(4, 400, 400),
                "1": RootMapItem(4, 400, 400),

                }

        bias = 1
        self.helper_test(pools, root_map, bias)

    def test_uneven_pools_with_diff_roots(self):
        pools = {

                "test0":{
                    
                    "pool": 0,
                    "pool_name": "test0",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.4,
                    "root_id":"0",
                    "expected_final_pg_target": 2048,
                    "expected_final_ratio": 0.4,
                    "even_pools": False,
                    "size": 1,
                    },

                "test1":{

                    "pool": 1,
                    "pool_name": "test1",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.6,
                    "root_id":"1",
                    "expected_final_pg_target": 2048,
                    "expected_final_ratio": 0.6,
                    "even_pools": False,
                    "size": 1,
                    },

                "test2":{

                    "pool": 2,
                    "pool_name": "test2",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.5,
                    "root_id":"0",
                    "expected_final_pg_target": 2048,
                    "expected_final_ratio": 0.5,
                    "even_pools": False,
                    "size": 1,
                    },

                "test3":{

                    "pool": 3,
                    "pool_name": "test3",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.1,
                    "root_id": "0",
                    "expected_final_pg_target": 512,
                    "expected_final_ratio": 1,
                    "even_pools": True,
                    "size": 1,
                    },

                "test4":{

                    "pool": 4,
                    "pool_name": "test4",
                    "pg_num_target": 32,
                    "capacity_ratio": 0.4,
                    "root_id": "1",
                    "expected_final_pg_target": 2048,
                    "expected_final_ratio": 1,
                    "even_pools": True,
                    "size": 1,
                    },

                }

        root_map = {

                "0": RootMapItem(3, 5000, 5000),
                "1": RootMapItem(2, 5000, 5000),

                }

        bias = 1
        self.helper_test(pools, root_map, bias)
