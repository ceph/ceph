#python unit test
import unittest
import os
import sys
from tests import mock

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
        module._module = mock.Mock() # just so Event._refresh() works
        self.test_event = module.PgRecoveryEvent(None, None, [module.PgId(1,i) for i in range(3)], [0], 30, False)

    def test_pg_update(self):
        # Test for a completed event when the pg states show active+clean
        pg_progress = {
            "pgs": {
                "1.0": {
                    "state": "active+clean",
                    "num_bytes": 10,
                    "num_bytes_recovered": 10,
                    "reported_epoch": 30,
                },
                "1.1": {
                    "state": "active+clean",
                    "num_bytes": 10,
                    "num_bytes_recovered": 10,
                    "reported_epoch": 30,
                },
                "1.2": {
                    "state": "active+clean",
                    "num_bytes": 10,
                    "num_bytes_recovered": 10,
                    "reported_epoch": 30,
                },
            },
            "pg_ready": True,
        }
        self.test_event.pg_update(pg_progress, mock.Mock())
        assert self.test_event._progress == 1.0


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
        # Creating the class and Mocking a
        # bunch of attributes for testing

        module.PgRecoveryEvent.pg_update = mock.Mock()
        module.Module._ceph_get_option = mock.Mock()  # .__init__
        module.Module._configure_logging = lambda *args: ...  # .__init__
        self.test_module = module.Module('module_name', 0, 0)  # so we can see if an event gets created
        self.test_module.get = mock.Mock() # so we can call pg_update
        self.test_module._complete = mock.Mock() # we want just to see if this event gets called
        self.test_module.get_osdmap = mock.Mock() # so that self.get_osdmap().get_epoch() works
        module._module = mock.Mock() # so that Event.refresh() works

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
        self.test_module._osd_in_out(old_map, old_dump, new_map, 3, "out")
        # check if only one event is created
        assert len(self.test_module._events) == 1
        self.test_module._osd_in_out(old_map, old_dump, new_map, 3, "in")
        # check if complete function is called
        assert self.test_module._complete.call_count == 1
        # check if a PgRecovery Event was created and pg_update gets triggered
        assert module.PgRecoveryEvent.pg_update.call_count == 2
