import json
import pickle
import pytest
import unittest
from collections import defaultdict
from unittest import mock

import telemetry
from typing import cast, Any, DefaultDict, Dict, List, Optional, Tuple, TypeVar, TYPE_CHECKING, Union

OptionValue = Optional[Union[bool, int, float, str]]

Collection = telemetry.module.Collection
ALL_CHANNELS = telemetry.module.ALL_CHANNELS
MODULE_COLLECTION = telemetry.module.MODULE_COLLECTION

COLLECTION_BASE = ["basic_base", "device_base", "crash_base", "ident_base"]

class TestTelemetry:
    @pytest.mark.parametrize("preconfig,postconfig,prestore,poststore,expected",
            [
                (
                    # user is not opted-in
                    {
                        'last_opt_revision': 1,
                        'enabled': False,
                    },
                    {
                        'last_opt_revision': 1,
                        'enabled': False,
                    },
                    {
                        # None
                    },
                    {
                        'collection': []
                    },
                    {
                        'is_opted_in': False,
                        'is_enabled_collection':
                        {
                            'basic_base': False,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
                (
                    # user is opted-in to an old revision
                    {
                        'last_opt_revision': 2,
                        'enabled': True,
                    },
                    {
                        'last_opt_revision': 2,
                        'enabled': True,
                    },
                    {
                        # None
                    },
                    {
                        'collection': []
                    },
                    {
                        'is_opted_in': False,
                        'is_enabled_collection':
                        {
                            'basic_base': False,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
                (
                    # user is opted-in to the latest revision
                    {
                        'last_opt_revision': 3,
                        'enabled': True,
                    },
                    {
                        'last_opt_revision': 3,
                        'enabled': True,
                    },
                    {
                        # None
                    },
                    {
                        'collection': COLLECTION_BASE
                    },
                    {
                        'is_opted_in': True,
                        'is_enabled_collection':
                        {
                            'basic_base': True,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
            ])
    def test_upgrade(self,
                preconfig: Dict[str, Any], \
                postconfig: Dict[str, Any], \
                prestore: Dict[str, Any], \
                poststore: Dict[str, Any], \
                expected: Dict[str, Any]) -> None:

        m = telemetry.Module('telemetry', '', '')

        if preconfig is not None:
            for k, v in preconfig.items():
                # no need to mock.patch since _ceph_set_module_option() which
                # is called from set_module_option() is already mocked for
                # tests, and provides setting default values for all module
                # options
                m.set_module_option(k, v)

        m.config_update_module_option()
        m.load()

        collection = json.loads(m.get_store('collection'))

        assert collection == poststore['collection']
        assert m.is_opted_in() == expected['is_opted_in']
        assert m.is_enabled_collection(Collection.basic_base) == expected['is_enabled_collection']['basic_base']
        assert m.is_enabled_collection(Collection.basic_mds_metadata) == expected['is_enabled_collection']['basic_mds_metadata']

    def test_gather_crashinfo_reads_shared_crash_store(self) -> None:
        """gather_crashinfo() should read crash records via get_store_ex(),
        not one remote('crash', 'do_info', ...) call per crash.
        """
        m = telemetry.Module('telemetry', '', '')
        m.load()

        crash = {
            'crash_id': '2026-01-01_00:00:00.000000Z_deadbeef',
            'entity_name': 'osd.1',
            'utsname_hostname': 'host1',
            'mgr_module': 'devicehealth',
            'backtrace': ['frame1', 'frame2'],
        }

        def fake_get_store_ex(module: str, key: str) -> Optional[str]:
            assert module == 'crash'
            if key == f'crash/{crash["crash_id"]}':
                return json.dumps(crash)
            return None

        with mock.patch.object(m, 'remote', return_value=(0, crash['crash_id'], '')), \
             mock.patch.object(m, 'get_store_ex', side_effect=fake_get_store_ex) as mocked:
            crashlist = m.gather_crashinfo()

        mocked.assert_called_once_with('crash', f'crash/{crash["crash_id"]}')
        assert len(crashlist) == 1
        assert crashlist[0]['entity_name'].startswith('osd.')
        assert 'utsname_hostname' not in crashlist[0]
        assert crashlist[0]['backtrace'][-1] == '<redacted>'

    def test_gather_crashinfo_skips_unreadable_crash(self) -> None:
        m = telemetry.Module('telemetry', '', '')
        m.load()

        with mock.patch.object(m, 'remote', return_value=(0, 'some-id', '')), \
             mock.patch.object(m, 'get_store_ex', return_value=None):
            crashlist = m.gather_crashinfo()

        assert crashlist == []

    def test_gather_crashinfo_falls_back_to_remote_when_not_shared(self) -> None:
        """An un-upgraded crash module (no SHARED_STORE) shouldn't break
        the whole crash report; should fall back to remote('crash', 'do_info').
        """
        m = telemetry.Module('telemetry', '', '')
        m.load()

        crash = {
            'crash_id': 'some-id',
            'entity_name': 'osd.1',
            'utsname_hostname': 'host1',
            'backtrace': ['frame1'],
        }

        def fake_remote(module: str, method: str, *args: Any) -> Any:
            if method == 'ls':
                return (0, crash['crash_id'], '')
            assert method == 'do_info'
            assert args == (crash['crash_id'],)
            return (0, json.dumps(crash), '')

        with mock.patch.object(m, 'remote', side_effect=fake_remote), \
             mock.patch.object(m, 'get_store_ex',
                               side_effect=PermissionError("not shared")):
            crashlist = m.gather_crashinfo()

        assert len(crashlist) == 1
        assert crashlist[0]['crash_id'] == crash['crash_id']

    def test_defaultdict_helpers_are_picklable(self) -> None:
        """Regression test for pickle serialization of defaultdict factories.

        The telemetry report returned through mgr.remote() must be pickle-serializable.
        Verify that the module-level defaultdict factories can be pickled.
        """
        factories = [
            telemetry.module._defaultdict_list,
            telemetry.module._defaultdict_dict,
            telemetry.module._defaultdict_defaultdict_int,
            telemetry.module._defaultdict_histogram,
        ]

        for factory in factories:
            d = defaultdict(factory)
            restored = pickle.loads(pickle.dumps(d))
            assert restored is not None, \
                f"defaultdict({factory.__name__}) failed pickle round-trip"
