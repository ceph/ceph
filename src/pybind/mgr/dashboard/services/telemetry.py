# -*- coding: utf-8 -*-
import json
import logging
from typing import TypedDict

from .. import mgr
from ..plugins.ttl_cache import ttl_cache

logger = logging.getLogger('services.telemetry')


class AdoptionMetrics(TypedDict):  # pylint: disable=inherit-non-class
    dashboard_enabled: bool
    prometheus_enabled: bool
    grafana_enabled: bool
    alertmanager_enabled: bool


class TelemetryMetrics(TypedDict):  # pylint: disable=inherit-non-class
    adoption: AdoptionMetrics


class DashboardTelemetryService:
    KV_ADOPTION = 'telemetry/metrics/adoption'

    @classmethod
    @ttl_cache(300, label='adoption_metrics')
    def get_adoption_metrics(cls) -> AdoptionMetrics:
        adoption_raw = mgr.get_store(cls.KV_ADOPTION)
        if not adoption_raw:
            return cls._detect_and_cache_adoption()
        try:
            return json.loads(adoption_raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                'telemetry: invalid cached adoption metrics; recomputing')
            return cls._detect_and_cache_adoption()

    @classmethod
    def _detect_and_cache_adoption(cls) -> AdoptionMetrics:
        try:
            grafana_url = mgr.get_module_option_ex(
                'dashboard', 'GRAFANA_API_URL') or ''
            alertmanager_url = mgr.get_module_option_ex(
                'dashboard', 'ALERTMANAGER_API_HOST') or ''
            result: AdoptionMetrics = {
                'dashboard_enabled': True,
                'prometheus_enabled': bool(
                    mgr.get_module_option_ex('prometheus', 'server_addr')
                ),
                'grafana_enabled': bool(grafana_url),
                'alertmanager_enabled': bool(alertmanager_url),
            }
        except Exception as e:  # pylint: disable=broad-except
            logger.error('telemetry: failed to detect adoption metrics: %s', e)
            result = {
                'dashboard_enabled': True,
                'prometheus_enabled': False,
                'grafana_enabled': False,
                'alertmanager_enabled': False,
            }
        mgr.set_store(cls.KV_ADOPTION, json.dumps(result))
        return result

    @classmethod
    def refresh_adoption(cls) -> AdoptionMetrics:
        return cls._detect_and_cache_adoption()

    @classmethod
    def get_metrics(cls) -> TelemetryMetrics:
        return {
            'adoption': cls.get_adoption_metrics(),
        }
