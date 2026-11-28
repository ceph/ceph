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

    def test_report_of_unsaved_channels(self) -> None:
        # The Dashboard previews the channels selected in its form, which are
        # not saved yet, and the report must match `ceph telemetry show`.
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', True)
        for channel in ALL_CHANNELS:
            m.set_module_option(f'channel_{channel}', channel == 'device')
        m.config_update_module_option()
        m.load()
        m.db_collection = [c['name'].name for c in MODULE_COLLECTION]
        m.gather_crashinfo = mock.Mock(return_value=[{'crash_id': 'id'}])  # type: ignore
        m.gather_perf_counters = mock.Mock(return_value={'osd.1': {}})  # type: ignore
        m.gather_device_report = mock.Mock(return_value={})  # type: ignore
        for getter in ['get_stats_per_pool', 'get_stats_per_pg', 'get_io_rate',
                       'get_osd_histograms', 'get_mempool', 'get_heap_stats',
                       'get_rocksdb_stats']:
            setattr(m, getter, mock.Mock(return_value={}))
        collection = list(m.db_collection)

        saved = m.get_report_locked(channels=None)
        assert saved['channels'] == ['device']
        assert 'crashes' not in saved
        assert 'perf_counters' not in saved

        channels = ['crash', 'perf']
        report = m.get_report_locked('all', channels)['report']
        assert report['channels'] == channels
        assert report['crashes'] == [{'crash_id': 'id'}]
        for section in ['perf_counters', 'stats_per_pool', 'stats_per_pg', 'io_rate',
                        'osd_perf_histograms', 'mempool', 'heap_stats', 'rocksdb_stats']:
            assert section in report

        _, out, _ = m.show(channels)
        shown, _ = json.JSONDecoder().raw_decode(out)  # ignores the trailing device hint
        for r in (report, shown):
            r.pop('report_timestamp')
        assert report == shown

        # no channel selected must not fall back to the saved channels
        empty = m.get_report_locked('all', [])['report']
        assert empty['channels'] == []
        assert 'crashes' not in empty
        assert 'perf_counters' not in empty

        # nothing was changed
        assert m.db_collection == collection
        assert m.get_active_channels() == ['device']
