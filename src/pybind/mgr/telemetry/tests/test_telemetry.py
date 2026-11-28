from contextlib import contextmanager
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

    @contextmanager
    def _mock_cluster(self, m: Any):
        def mock_get(key: str) -> Any:
            if key == 'mon_map':
                return {'created': '2026-01-01', 'mons': [], 'features': {}, 'min_mon_release': 19}
            if key == 'osd_map':
                return {
                    'pools': [{
                        'pool': 1,
                        'pool_name': 'test_pool',
                        'pg_num': 8,
                        'pg_placement_num': 8,
                        'size': 3,
                        'min_size': 2,
                        'pg_autoscale_mode': 'on',
                        'target_max_bytes': 0,
                        'target_max_objects': 0,
                        'type': 1,
                        'erasure_code_profile': '',
                        'cache_mode': 'none',
                        'application_metadata': {'mgr': {}},
                        'options': {'compression_mode': 'none'},
                        'flags_names': 'hashpspool',
                    }],
                    'osds': [],
                    'require_osd_release': 19,
                    'require_min_compat_client': 'reef',
                    'stretch_mode': {
                        'stretch_mode_enabled': True,
                        'stretch_bucket_count': 2,
                        'degraded_stretch_mode': False,
                        'recovering_stretch_mode': False,
                        'stretch_mode_bucket': 1,
                    },
                    'flags': 0,
                    'pg_temp': {},
                    'nearfull_ratio': 0.85,
                    'full_ratio': 0.90,
                    'backfillfull_ratio': 0.90,
                }
            if key == 'service_map':
                return {'services': {}}
            if key == 'fs_map':
                return {'filesystems': [], 'feature_flags': {}, 'standbys': []}
            if key == 'df':
                return {
                    'pools': [{
                        'id': 1,
                        'name': 'test_pool',
                        'stats': {
                            'avail_raw': 1000,
                            'bytes_used': 500,
                            'compress_bytes_used': 0,
                            'compress_under_bytes': 0,
                            'data_bytes_used': 500,
                            'dirty': 0,
                            'max_avail': 1000,
                            'objects': 10,
                            'omap_bytes_used': 0,
                            'percent_used': 0.5,
                            'quota_bytes': 0,
                            'quota_objects': 0,
                            'rd': 0,
                            'rd_bytes': 0,
                            'stored': 500,
                            'stored_data': 500,
                            'stored_omap': 0,
                            'stored_raw': 500,
                            'wr': 0,
                            'wr_bytes': 0,
                        }
                    }],
                    'stats': {'total_used_bytes': 500, 'total_bytes': 1000, 'total_avail_bytes': 500},
                    'stats_by_class': {'hdd': {'total_used_bytes': 500}}
                }
            if key == 'health':
                return {'status': 'HEALTH_OK', 'checks': {}}
            if key == 'mgr_map':
                return {'modules': []}
            if key == 'config_options':
                return {'options': []}
            return None

        with mock.patch.object(m, 'get', side_effect=mock_get), \
             mock.patch.object(m, 'gather_configs', return_value={}), \
             mock.patch.object(m, 'gather_crashinfo', return_value=[]), \
             mock.patch.object(m, 'gather_crush_info', return_value={}), \
             mock.patch.object(m, 'gather_device_report', return_value={'devices': {}}), \
             mock.patch.object(m, 'gather_mds_metadata', return_value={'arch': {'x86_64': 1}}), \
             mock.patch.object(m, 'remote', return_value={'active': True, 'mode': 'upmap'}), \
             mock.patch.object(m, 'list_servers', return_value=[]):
            yield

    def test_get_preview_report_locked_when_telemetry_disabled(self) -> None:
        """Regression test for telemetry-disabled preview report.

        When telemetry is disabled, preview report must contain all available collections
        in collections_opted_in and all representative collection-dependent sections
        (e.g., balancer, mds metadata, usage by class, stretch cluster, pool stats/options/flags),
        while restoring original db_collection to empty list.
        """
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', False)
        m.config_update_module_option()
        m.load()

        assert m.enabled is False
        assert m.db_collection == []

        with self._mock_cluster(m):
            preview_res = m.get_preview_report_locked('all')
            assert 'report' in preview_res and 'device_report' in preview_res
            all_collections = [c['name'].name for c in MODULE_COLLECTION]
            assert preview_res['report']['collections_opted_in'] == all_collections

            # Verify representative collection-dependent sections and fields are present
            assert preview_res['report']['balancer'] == {'active': True, 'mode': 'upmap'}
            assert 'mds' in preview_res['report']['metadata']
            assert 'stats_by_class' in preview_res['report']['usage']
            assert 'stretch_cluster' in preview_res['report']
            assert 'stats' in preview_res['report']['pools'][0]
            assert 'options' in preview_res['report']['pools'][0]
            assert 'flags_names' in preview_res['report']['pools'][0]

            # Original state must be preserved
            assert m.db_collection == []

            # In contrast, get_report_locked reports only opted-in collections (empty)
            opted_in_res = m.get_report_locked('all')
            assert opted_in_res['report']['collections_opted_in'] == []
            assert 'mds' not in opted_in_res['report']['metadata']
            assert 'stats_by_class' not in opted_in_res['report']['usage']
            assert 'stretch_cluster' not in opted_in_res['report']
            assert 'stats' not in opted_in_res['report']['pools'][0]
            assert 'options' not in opted_in_res['report']['pools'][0]
            assert 'flags_names' not in opted_in_res['report']['pools'][0]
            assert m.db_collection == []

    def test_get_preview_report_locked_with_partially_opted_in_collections(self) -> None:
        """Verify that preview starting with partially populated db_collection restores exact state."""
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', False)
        m.config_update_module_option()
        m.load()

        partial_collection = ['basic_base', 'device_base', 'crash_base']
        m.db_collection = list(partial_collection)

        with self._mock_cluster(m):
            preview_res = m.get_preview_report_locked('all')
            all_collections = [c['name'].name for c in MODULE_COLLECTION]
            assert preview_res['report']['collections_opted_in'] == all_collections
            # Exact original partial collection state must be restored
            assert m.db_collection == partial_collection

    def test_get_preview_report_locked_restores_state_on_exception(self) -> None:
        """Verify that get_preview_report_locked restores original db_collection on error."""
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', False)
        m.config_update_module_option()
        m.load()

        original_collection = ['basic_base', 'device_base']
        m.db_collection = list(original_collection)

        with mock.patch.object(m, 'get_report', side_effect=RuntimeError('simulated preview failure')):
            with pytest.raises(RuntimeError, match='simulated preview failure'):
                m.get_preview_report_locked('all')

        assert m.db_collection == original_collection

    def test_get_preview_report_locked_when_telemetry_enabled(self) -> None:
        """Verify report preview behavior when telemetry is already enabled."""
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', True)
        m.config_update_module_option()
        m.load()

        all_collections = [c['name'].name for c in MODULE_COLLECTION]
        m.db_collection = list(all_collections)

        with self._mock_cluster(m):
            preview_res = m.get_preview_report_locked('all')
            assert preview_res['report']['collections_opted_in'] == all_collections
            assert m.db_collection == all_collections

            preview_default = m.get_preview_report_locked('default')
            assert preview_default['collections_opted_in'] == all_collections

            preview_device = m.get_preview_report_locked('device')
            assert isinstance(preview_device, dict)

    def test_cli_preview_matches_dashboard_preview(self) -> None:
        """Verify Dashboard preview report matches canonical CLI preview report."""
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', False)
        m.config_update_module_option()
        m.load()

        with self._mock_cluster(m):
            # Dashboard preview call
            dash_preview = m.get_preview_report_locked('all')

            # CLI preview_all call
            code, out, _ = m.preview_all()
            assert code == 0
            cli_preview = json.loads(out)

            # Compare reports excluding dynamic timestamp
            dash_report = dict(dash_preview['report'])
            cli_report = dict(cli_preview['report'])
            dash_report.pop('report_timestamp', None)
            cli_report.pop('report_timestamp', None)

            assert dash_report == cli_report
            assert dash_preview['device_report'] == cli_preview['device_report']

    def test_cli_preview_and_show_commands(self) -> None:
        """Verify CLI preview and show commands behavior when disabled and up to date."""
        m = telemetry.Module('telemetry', '', '')
        m.set_module_option('enabled', False)
        m.config_update_module_option()
        m.load()

        with self._mock_cluster(m):
            # 1. When telemetry is disabled:
            # CLI preview commands return preview report with all collections
            code, out, _ = m.preview()
            assert code == 0
            report = json.loads(out.split('\nDevice report')[0])
            all_collections = [c['name'].name for c in MODULE_COLLECTION]
            assert report['collections_opted_in'] == all_collections

            code, out, _ = m.preview_all()
            assert code == 0
            report_all = json.loads(out)
            assert 'report' in report_all and 'device_report' in report_all
            assert report_all['report']['collections_opted_in'] == all_collections

            code, out, _ = m.preview_device()
            assert code == 0
            report_dev = json.loads(out)
            assert isinstance(report_dev, dict)

            # CLI show commands indicate telemetry is off
            code, out, _ = m.show()
            assert code == 0
            assert 'Telemetry is off' in out

            code, out, _ = m.show_all()
            assert code == 0
            assert 'Telemetry is off' in out

            code, out, _ = m.show_device()
            assert code == 0
            assert 'Telemetry is off' in out

            # 2. When telemetry is enabled and fully opted-in:
            m.enabled = True
            m.db_collection = list(all_collections)

            code, out, _ = m.preview()
            assert code == 0
            assert 'Telemetry is up to date' in out

            code, out, _ = m.preview_all()
            assert code == 0
            assert 'Telemetry is up to date' in out

            code, out, _ = m.preview_device()
            assert code == 0
            assert 'device channel is on and up to date' in out
