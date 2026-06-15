# -*- coding: utf-8 -*-
"""
Dashboard Telemetry Service.

Handles detection and caching of dashboard telemetry metrics:
  - Monitoring stack adoption (dashboard, prometheus, grafana, alertmanager)

Business logic is centralized here; controllers delegate to this service.
"""

import json
import logging
from typing import TypedDict

from .. import mgr
from ..plugins.ttl_cache import ttl_cache

logger = logging.getLogger('services.telemetry')

# KV store keys
KV_ADOPTION = 'telemetry/metrics/adoption'


# Type definitions for telemetry metrics
class AdoptionMetrics(TypedDict):
    """Monitoring stack adoption metrics."""
    dashboard_enabled: bool
    prometheus_enabled: bool
    grafana_enabled: bool
    alertmanager_enabled: bool


class TelemetryMetrics(TypedDict):
    """All dashboard telemetry metrics."""
    adoption: AdoptionMetrics


class DashboardTelemetryService:
    """
    Service for dashboard telemetry metrics.
    
    Provides cached detection of monitoring stack adoption.
    Results are stored in KV store for cross-module access (telemetry module).
    """

    @classmethod
    @ttl_cache(300, label='adoption_metrics')
    def get_adoption_metrics(cls) -> AdoptionMetrics:
        """
        Get cached adoption metrics.
        
        Returns cached value if available, otherwise detects and caches.
        Cache TTL: 5 minutes.
        
        Returns:
            AdoptionMetrics with monitoring stack status
        """
        adoption_raw = mgr.get_store(KV_ADOPTION)

        if not adoption_raw:
            return cls._detect_and_cache_adoption()

        try:
            return json.loads(adoption_raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                'telemetry: invalid cached adoption metrics; recomputing'
            )
            return cls._detect_and_cache_adoption()

    @classmethod
    def _detect_and_cache_adoption(cls) -> AdoptionMetrics:
        """
        Detect monitoring stack adoption and cache result.
        
        Checks dashboard/prometheus module configuration to determine
        which monitoring components are enabled.
        
        Returns:
            AdoptionMetrics with current adoption status
        """
        try:
            grafana_url = mgr.get_module_option_ex(
                'dashboard',
                'GRAFANA_API_URL'
            ) or ''

            alertmanager_url = mgr.get_module_option_ex(
                'dashboard',
                'ALERTMANAGER_API_HOST'
            ) or ''

            result: AdoptionMetrics = {
                'dashboard_enabled': True,
                'prometheus_enabled': bool(
                    mgr.get_module_option_ex('prometheus', 'server_addr')
                ),
                'grafana_enabled': bool(grafana_url),
                'alertmanager_enabled': bool(alertmanager_url),
            }

        except Exception as e:  # pylint: disable=broad-except
            logger.error('Failed to detect adoption metrics: %s', e)
            result = {
                'dashboard_enabled': True,
                'prometheus_enabled': False,
                'grafana_enabled': False,
                'alertmanager_enabled': False,
            }

        # Store in KV for telemetry module to read
        mgr.set_store(KV_ADOPTION, json.dumps(result))
        return result

    @classmethod
    def refresh_adoption(cls) -> AdoptionMetrics:
        """
        Force refresh of adoption metrics.
        
        Bypasses cache and KV store, performs fresh detection.
        Useful for manual refresh via API endpoint.
        
        Returns:
            Freshly detected AdoptionMetrics
        """
        return cls._detect_and_cache_adoption()

    @classmethod
    def get_metrics(cls) -> TelemetryMetrics:
        """
        Get all telemetry metrics.
        
        Returns:
            TelemetryMetrics containing all metric categories
        """
        return {
            'adoption': cls.get_adoption_metrics(),
        }


