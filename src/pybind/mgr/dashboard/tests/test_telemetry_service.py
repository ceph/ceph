# -*- coding: utf-8 -*-
import json
import unittest
from unittest.mock import MagicMock

from .. import mgr
from ..services.telemetry import DashboardTelemetryService


class TestDashboardTelemetryServiceAdoption(unittest.TestCase):
    """
    Tests for _detect_and_cache_adoption.
    Covers all four adoption flags, error fallback, and KV store persistence.
    """

    def _run(self, grafana_url='', alertmanager_url='', prometheus_addr=None):
        """Set up mocks and run _detect_and_cache_adoption, return result."""
        mgr.set_store = MagicMock()
        mgr.get_store = MagicMock(return_value=None)

        def _get_module_option_ex(module, key):
            if module == 'dashboard' and key == 'GRAFANA_API_URL':
                return grafana_url
            if module == 'dashboard' and key == 'ALERTMANAGER_API_HOST':
                return alertmanager_url
            if module == 'prometheus' and key == 'server_addr':
                return prometheus_addr
            return None

        mgr.get_module_option_ex = MagicMock(side_effect=_get_module_option_ex)
        # pylint: disable=protected-access
        return DashboardTelemetryService._detect_and_cache_adoption()

    # ------------------------------------------------------------------
    # dashboard_enabled is always True
    # ------------------------------------------------------------------

    def test_dashboard_always_enabled(self):
        result = self._run()
        self.assertTrue(result['dashboard_enabled'])

    # ------------------------------------------------------------------
    # prometheus_enabled
    # ------------------------------------------------------------------

    def test_prometheus_enabled_when_addr_set(self):
        result = self._run(prometheus_addr='0.0.0.0')
        self.assertTrue(result['prometheus_enabled'])

    def test_prometheus_disabled_when_addr_not_set(self):
        result = self._run(prometheus_addr=None)
        self.assertFalse(result['prometheus_enabled'])

    # ------------------------------------------------------------------
    # grafana_enabled
    # ------------------------------------------------------------------

    def test_grafana_enabled_when_url_set(self):
        result = self._run(grafana_url='http://grafana:3000')
        self.assertTrue(result['grafana_enabled'])

    def test_grafana_disabled_when_url_empty(self):
        result = self._run(grafana_url='')
        self.assertFalse(result['grafana_enabled'])

    # ------------------------------------------------------------------
    # alertmanager_enabled
    # ------------------------------------------------------------------

    def test_alertmanager_enabled_when_url_set(self):
        result = self._run(alertmanager_url='http://alertmanager:9093')
        self.assertTrue(result['alertmanager_enabled'])

    def test_alertmanager_disabled_when_url_empty(self):
        result = self._run(alertmanager_url='')
        self.assertFalse(result['alertmanager_enabled'])

    # ------------------------------------------------------------------
    # Full stack enabled
    # ------------------------------------------------------------------

    def test_all_enabled(self):
        result = self._run(
            grafana_url='http://grafana:3000',
            alertmanager_url='http://alertmanager:9093',
            prometheus_addr='0.0.0.0',
        )
        self.assertTrue(result['dashboard_enabled'])
        self.assertTrue(result['prometheus_enabled'])
        self.assertTrue(result['grafana_enabled'])
        self.assertTrue(result['alertmanager_enabled'])

    def test_all_disabled(self):
        result = self._run()
        self.assertTrue(result['dashboard_enabled'])
        self.assertFalse(result['prometheus_enabled'])
        self.assertFalse(result['grafana_enabled'])
        self.assertFalse(result['alertmanager_enabled'])

    # ------------------------------------------------------------------
    # Error handling: get_module_option_ex raises
    # ------------------------------------------------------------------

    def test_error_returns_safe_defaults(self):
        mgr.set_store = MagicMock()
        mgr.get_module_option_ex = MagicMock(
            side_effect=Exception('module unavailable'))
        # pylint: disable=protected-access
        result = DashboardTelemetryService._detect_and_cache_adoption()
        self.assertTrue(result['dashboard_enabled'])
        self.assertFalse(result['prometheus_enabled'])
        self.assertFalse(result['grafana_enabled'])
        self.assertFalse(result['alertmanager_enabled'])

    # ------------------------------------------------------------------
    # KV store: result is persisted after detection
    # ------------------------------------------------------------------

    def test_result_is_written_to_kv_store(self):
        result = self._run(grafana_url='http://grafana:3000')
        mgr.set_store.assert_called_once()
        key, value = mgr.set_store.call_args[0]
        self.assertEqual(key, DashboardTelemetryService.KV_ADOPTION)
        stored = json.loads(value)
        self.assertEqual(stored['grafana_enabled'], result['grafana_enabled'])
        self.assertEqual(stored['dashboard_enabled'], result['dashboard_enabled'])


if __name__ == '__main__':
    unittest.main()
