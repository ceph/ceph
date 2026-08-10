# -*- coding: utf-8 -*-
import json
import unittest
from unittest.mock import MagicMock

from .. import mgr
from ..services.telemetry import DashboardTelemetryService

_ALL_PERSONA_KEYS = (
    'admin', 'read_only', 'block_storage_operator',
    'file_system_operator', 'object_storage_operator', 'monitoring',
)


def _make_user(role_names):
    """Return a mock User whose .roles yields objects with .name attributes."""
    user = MagicMock()
    user.roles = [MagicMock(name=r) for r in role_names]
    for role_obj, role_name in zip(user.roles, role_names):
        role_obj.name = role_name
    return user


class TestDashboardTelemetryServicePersonaClassification(unittest.TestCase):
    """
    Tests for _detect_and_cache_user_personas.
    Covers every system role, priority ordering, diversity count, and
    primary persona derivation.
    """

    def _run(self, users_dict):
        """Set up mocks, run _detect_and_cache_user_personas, return result."""
        mock_db = MagicMock()
        mock_db.users = users_dict
        mgr.ACCESS_CTRL_DB = mock_db
        mgr.set_store = MagicMock()
        mgr.get_store = MagicMock(return_value=None)
        return DashboardTelemetryService._detect_and_cache_user_personas()

    # ------------------------------------------------------------------
    # Per-role classification (all 9 system roles)
    # ------------------------------------------------------------------

    def test_administrator_maps_to_admin(self):
        result = self._run({'u1': _make_user(['administrator'])})
        self.assertEqual(result['admin'], 1)

    def test_read_only_maps_to_read_only(self):
        result = self._run({'u1': _make_user(['read-only'])})
        self.assertEqual(result['read_only'], 1)

    def test_block_manager_maps_to_block_storage_operator(self):
        result = self._run({'u1': _make_user(['block-manager'])})
        self.assertEqual(result['block_storage_operator'], 1)

    def test_pool_manager_maps_to_block_storage_operator(self):
        result = self._run({'u1': _make_user(['pool-manager'])})
        self.assertEqual(result['block_storage_operator'], 1)

    def test_cephfs_manager_maps_to_file_system_operator(self):
        result = self._run({'u1': _make_user(['cephfs-manager'])})
        self.assertEqual(result['file_system_operator'], 1)

    def test_ganesha_manager_maps_to_file_system_operator(self):
        result = self._run({'u1': _make_user(['ganesha-manager'])})
        self.assertEqual(result['file_system_operator'], 1)

    def test_smb_manager_maps_to_file_system_operator(self):
        result = self._run({'u1': _make_user(['smb-manager'])})
        self.assertEqual(result['file_system_operator'], 1)

    def test_rgw_manager_maps_to_object_storage_operator(self):
        result = self._run({'u1': _make_user(['rgw-manager'])})
        self.assertEqual(result['object_storage_operator'], 1)

    def test_cluster_manager_maps_to_admin(self):
        result = self._run({'u1': _make_user(['cluster-manager'])})
        self.assertEqual(result['admin'], 1)

    # ------------------------------------------------------------------
    # Priority ordering: highest-priority role wins per user
    # ------------------------------------------------------------------

    def test_administrator_wins_over_block_manager(self):
        result = self._run({'u1': _make_user(['block-manager', 'administrator'])})
        self.assertEqual(result['admin'], 1)
        self.assertEqual(result['block_storage_operator'], 0)

    def test_cluster_manager_wins_over_read_only(self):
        result = self._run({'u1': _make_user(['read-only', 'cluster-manager'])})
        self.assertEqual(result['admin'], 1)
        self.assertEqual(result['read_only'], 0)

    def test_block_manager_wins_over_pool_manager(self):
        result = self._run({'u1': _make_user(['pool-manager', 'block-manager'])})
        self.assertEqual(result['block_storage_operator'], 1)
        # exactly one persona counted per user, never double-counted
        self.assertEqual(sum(result[k] for k in _ALL_PERSONA_KEYS), 1)

    # ------------------------------------------------------------------
    # Custom / unknown roles are silently skipped
    # ------------------------------------------------------------------

    def test_custom_role_not_counted(self):
        result = self._run({'u1': _make_user(['my-custom-role'])})
        self.assertEqual(sum(result[k] for k in _ALL_PERSONA_KEYS), 0)

    def test_user_with_no_roles_not_counted(self):
        result = self._run({'u1': _make_user([])})
        self.assertEqual(sum(result[k] for k in _ALL_PERSONA_KEYS), 0)

    # ------------------------------------------------------------------
    # primary_usage_persona
    # ------------------------------------------------------------------

    def test_primary_persona_is_most_frequent(self):
        users = {
            'u1': _make_user(['administrator']),
            'u2': _make_user(['administrator']),
            'u3': _make_user(['read-only']),
        }
        result = self._run(users)
        self.assertEqual(result['primary_usage_persona'], 'admin')

    def test_primary_persona_is_none_when_no_users(self):
        result = self._run({})
        self.assertEqual(result['primary_usage_persona'], 'none')

    def test_primary_persona_tie_returns_first_in_insertion_order(self):
        # persona_counts is built in fixed order: admin first.
        # Python's max() returns the first maximum in a tie, so 'admin' wins.
        users = {
            'u1': _make_user(['administrator']),
            'u2': _make_user(['read-only']),
        }
        result = self._run(users)
        self.assertEqual(result['primary_usage_persona'], 'admin')

    # ------------------------------------------------------------------
    # persona_diversity
    # ------------------------------------------------------------------

    def test_persona_diversity_zero_for_empty_cluster(self):
        result = self._run({})
        self.assertEqual(result['persona_diversity'], 0)

    def test_persona_diversity_one_when_single_persona_active(self):
        users = {
            'u1': _make_user(['administrator']),
            'u2': _make_user(['administrator']),
        }
        result = self._run(users)
        self.assertEqual(result['persona_diversity'], 1)

    def test_persona_diversity_counts_distinct_active_personas(self):
        users = {
            'u1': _make_user(['administrator']),
            'u2': _make_user(['read-only']),
            'u3': _make_user(['block-manager']),
            'u4': _make_user(['rgw-manager']),
        }
        result = self._run(users)
        self.assertEqual(result['persona_diversity'], 4)

    # ------------------------------------------------------------------
    # Error handling: ACCESS_CTRL_DB unavailable
    # ------------------------------------------------------------------

    def test_access_ctrl_db_error_returns_zeroed_result(self):
        mgr.ACCESS_CTRL_DB = MagicMock()
        mgr.ACCESS_CTRL_DB.users = MagicMock(
            side_effect=Exception('DB unavailable'))
        result = DashboardTelemetryService._detect_and_cache_user_personas()
        self.assertEqual(sum(result[k] for k in _ALL_PERSONA_KEYS), 0)
        self.assertEqual(result['primary_usage_persona'], 'none')
        self.assertEqual(result['persona_diversity'], 0)

    # ------------------------------------------------------------------
    # KV store: result is persisted after detection
    # ------------------------------------------------------------------

    def test_result_is_written_to_kv_store(self):
        users = {
            'u1': _make_user(['administrator']),
            'u2': _make_user(['read-only']),
        }
        result = self._run(users)
        mgr.set_store.assert_called_once()
        key, value = mgr.set_store.call_args[0]
        self.assertEqual(key, DashboardTelemetryService.KV_USER_PERSONA)
        stored = json.loads(value)
        self.assertEqual(stored['admin'], result['admin'])
        self.assertEqual(stored['read_only'], result['read_only'])


if __name__ == '__main__':
    unittest.main()
