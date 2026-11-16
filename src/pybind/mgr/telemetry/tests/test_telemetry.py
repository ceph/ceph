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


# Opaque on purpose: the consumer passes the provider's payload through unchanged.
DASHBOARD_PAYLOAD = {
    'opaque_section': {'nested': [1, 2, 3], 'flag': True},
    'another_section': 'value',
}


class TestDashboardTelemetry:
    @staticmethod
    def _module(collections: List[str]) -> Any:
        m = telemetry.Module('telemetry', '', '')
        m.config_update_module_option()
        m.load()
        m.db_collection = list(collections)
        return m

    @staticmethod
    def _get(data_name: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            'mon_map': {'created': '', 'mons': [], 'features': {},
                        'min_mon_release': 0},
            'osd_map': {'pools': [], 'osds': [], 'require_osd_release': '',
                        'require_min_compat_client': ''},
            'service_map': {'services': {}},
            'fs_map': {'filesystems': [], 'feature_flags': {}, 'standbys': []},
            'df': {'pools': [], 'stats': {'total_used_bytes': 0,
                                          'total_bytes': 0,
                                          'total_avail_bytes': 0},
                   'stats_by_class': {}},
        }
        return data[data_name]

    def _compile(self, m: Any, remote: Any) -> Tuple[Dict[str, Any], Any]:
        with mock.patch.object(m, 'get', side_effect=self._get), \
                mock.patch.object(m, 'gather_configs', return_value={}), \
                mock.patch.object(m, 'gather_crush_info', return_value={}), \
                mock.patch.object(m, 'gather_osd_metadata', return_value={}), \
                mock.patch.object(m, 'gather_mon_metadata', return_value={}), \
                mock.patch.object(m, 'list_servers', return_value=[]), \
                mock.patch.object(m, 'get_rook_data'), \
                mock.patch.object(m, 'remote', side_effect=remote) as r:
            return m.compile_report(channels=['basic']), r

    @staticmethod
    def _remote(dashboard: Any) -> Any:
        def remote(module: str, method: str, *args: Any) -> Any:
            assert method == 'gather_telemetry'
            if module == 'balancer':
                return {'active': True, 'mode': 'upmap'}
            if isinstance(dashboard, Exception):
                raise dashboard
            return dashboard
        return remote

    def test_collection_registered(self) -> None:
        entries = [c for c in MODULE_COLLECTION
                   if c['name'] == Collection.basic_dashboard_usage]
        assert len(entries) == 1
        assert entries[0]['channel'] == 'basic'
        assert entries[0]['nag'] is True

    def test_dashboard_section_comes_from_provider(self) -> None:
        m = self._module(COLLECTION_BASE + ['basic_dashboard_usage'])
        report, remote = self._compile(m, self._remote(DASHBOARD_PAYLOAD))

        assert report['dashboard'] == DASHBOARD_PAYLOAD
        remote.assert_any_call('dashboard', 'gather_telemetry')
        assert 'basic_dashboard_usage' in report['collections_opted_in']
        assert report['balancer'] == {'active': True, 'mode': 'upmap'}

    def test_dashboard_not_reported_without_opt_in(self) -> None:
        m = self._module(COLLECTION_BASE)
        report, remote = self._compile(m, self._remote(DASHBOARD_PAYLOAD))

        assert 'dashboard' not in report
        assert 'basic_dashboard_usage' not in report['collections_opted_in']
        assert 'basic_dashboard_usage' in report['collections_available']
        for call in remote.call_args_list:
            assert call.args[0] != 'dashboard'

    def test_other_collections_do_not_enable_dashboard(self) -> None:
        other = [c['name'].name for c in MODULE_COLLECTION
                 if c['name'] != Collection.basic_dashboard_usage
                 and c['channel'] == 'basic']
        m = self._module(other)
        assert not m.is_enabled_collection(Collection.basic_dashboard_usage)

    def test_dashboard_module_unavailable(self) -> None:
        m = self._module(COLLECTION_BASE + ['basic_dashboard_usage'])
        report, _ = self._compile(m, self._remote(ImportError('Module not found')))

        assert report['dashboard'] == {}
        assert report['balancer'] == {'active': True, 'mode': 'upmap'}
        assert 'mon' in report

    @pytest.mark.parametrize('error', [
        RuntimeError('Remote method threw exception: boom'),
        NameError('Method not found'),
    ])
    def test_dashboard_unexpected_error_is_not_swallowed(self, error: Exception) -> None:
        # Only a missing module is expected; a provider that raises or does
        # not expose gather_telemetry() is an integration error.
        m = self._module(COLLECTION_BASE + ['basic_dashboard_usage'])
        with pytest.raises(type(error)):
            self._compile(m, self._remote(error))

    def test_opted_in_users_are_nagged_about_dashboard_collection(self) -> None:
        all_names = [c['name'].name for c in MODULE_COLLECTION]
        without_dashboard = [n for n in all_names if n != 'basic_dashboard_usage']

        m = self._module(without_dashboard)
        m.enabled = True
        assert m.should_nag()

        m = self._module(all_names)
        m.enabled = True
        assert not m.should_nag()

    def test_dashboard_never_read_from_kv(self) -> None:
        m = self._module(COLLECTION_BASE + ['basic_dashboard_usage'])
        with mock.patch.object(m, 'mon_command') as mc:
            self._compile(m, self._remote(DASHBOARD_PAYLOAD))
        for call in mc.call_args_list:
            assert 'dashboard' not in str(call)
