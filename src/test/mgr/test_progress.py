#python unit test
import unittest
import os
import sys
from mock import Mock
import pytest
import json
os.environ['UNITTEST'] = "1"
sys.path.insert(0, "../../pybind/mgr")
from progress import module

class TestPgRecoveryEvent(object):
    # Testing PgRecoveryEvent class

    def setup(self):
        # Creating the class and Mocking 
        # a bunch of attributes for testing
        self.test_class = module.PgRecoveryEvent
        self._pgs = [module.PgId(1,i) for i in range(3)]
        self._evacuate_osds = [0]
        self._original_pg_count = len(self._pgs)
        self._original_bytes_recovered = None
        self.module_class = Mock()
        self.module_class.info = Mock()
        self._refresh = Mock()
        self._progress = 0.0
        self._message = None
        self._start_epoch = 30

    def test_pg_update(self):
        # Test for a completed event when the pg states show active+clear
        pg_dump = {
                "pg_stats":[
        {
          "state": "active+clean",
          "stat_sum": {
            "num_bytes": 10,
            "num_bytes_recovered": 10
          },
          "up": [
            3,
            1
          ],
          "acting": [
            3,
            1
          ],
          "pgid": "1.0",
          "reported_epoch": "30"
        },
       {
          "state": "active+clean",
          "stat_sum": {
            "num_bytes": 10,
            "num_bytes_recovered": 10
          },
          "up": [
            3,
            1
          ],
          "acting": [
            3,
            1
          ],
          "pgid": "1.1",
          "reported_epoch": "30"
        },
       {
          "state": "active+clean",
          "stat_sum": {
            "num_bytes": 10,
            "num_bytes_recovered": 10
          },
          "up": [
            3,
            1
          ],
          "acting": [
            3,
            1
          ],
          "pgid": "1.2",
          "reported_epoch": "30"
        }
        ]
        }

        self.test_class.pg_update(self,pg_dump,self.module_class)
        assert self._progress == 1.0

class OSDMap: 
    
    # This is an artificial class to help
    # _osd_in_out function have all the 
    # necessary characteristics, some
    # of the funcitons are copied from
    # mgr_module

    def __init__(self, dump, pg_stats):
        self._dump = dump
        self._pg_stats = pg_stats
        
    def _pg_to_up_acting_osds(self, pool_id, ps):
        pg_id = str(pool_id) + "." + str(ps)
        for pg in self._pg_stats["pg_stats"]:
            if pg["pg_id"] == pg_id:
                ret = {
                        "up_primary": pg["up_primary"],
                        "acting_primary": pg["acting_primary"],
                        "up": pg["up"],
                        "acting": pg["acting"]
                        }
                return ret

    def dump(self):
        return self._dump

    def get_pools(self):
        d = self._dump()
        return dict([(p['pool'], p) for p in d['pools']])

    def get_pools_by_name(self):
        d = self._dump()
        return dict([(p['pool_name'], p) for p in d['pools']])

    def pg_to_up_acting_osds(self, pool_id, ps):
        return self._pg_to_up_acting_osds(pool_id, ps)

class TestModule(object):
    # Testing Module Class
    
    def setup(self):
        # Creating the class and Mocking
        # attributes for testing
        self.test_class = module
        self.test_class._module = Mock()
        self.test_class.PgRecoveryEvent.pg_update = Mock()
        self.test_module = module.Module
        self.log = Mock()
        self.get = Mock()
        self._complete = Mock()
        self._events = {}

    def test_osd_in_out(self):
        # test for the correct event being
        # triggered and completed.
        old_pg_stats = {
            "pg_stats":[
                {
                "pg_id": "1.0",
                "up_primary": 3,
                "acting_primary": 3,
                "up": [
                    3,
                    0
                    ],
                "acting": [
                    3,
                    0
                    ]
        
                },

                ]
            }
        new_pg_stats = {
            "pg_stats":[
                {
              "pg_id": "1.0",
              "up_primary": 0,
              "acting_primary": 0,
              "up": [
                0,
                2
              ],
              "acting": [
                0,
                2
              ]
            },
                ]
            }

        old_dump ={ 
            "pools": [
                {
                    "pool": 1,
                    "pg_num": 1
                    }
                ]
            }

        new_dump = {
                "pools": [
                    {
                        "pool": 1,
                        "pg_num": 1
                        }
                    ]
                }

        new_map = OSDMap(new_dump, new_pg_stats)
        old_map = OSDMap(old_dump, old_pg_stats)
        marked_in = "in"
        marked_out = "out"
        osd_id = 3
        self.test_module._osd_in_out(self, old_map, old_dump, new_map, osd_id, marked_out)
        self.test_module._osd_in_out(self, old_map, old_dump, new_map, osd_id, marked_in)
        # check if complete function is called
        self._complete.assert_called_once() 
        # check if a PgRecovery Event was created and pg_update gets triggered
        self.test_class.PgRecoveryEvent.pg_update.asset_called_once()
