# -*- coding: utf-8 -*-
"""
Dashboard Metrics controller.

Tracks anonymous UI usage:
  - Monitoring stack adoption detection
    (dashboard, prometheus, grafana, alertmanager)

KV keys (all under shared mgr store, readable by telemetry module):
  ui_metrics/adoption    json str
"""

import json
import logging

from typing import Any, Dict

from .. import mgr
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController

logger = logging.getLogger('controllers.ui_metrics')

KV_ADOPTION = 'ui_metrics/adoption'


def collect_metrics() -> Dict[str, Any]:
    """Returns adoption telemetry metrics."""
    adoption_raw = mgr.get_store(KV_ADOPTION)

    if adoption_raw:
        adoption = json.loads(adoption_raw)
    else:
        adoption = _detect_and_cache_adoption()

    return {
        'adoption': adoption,
    }


def _detect_and_cache_adoption() -> Dict[str, Any]:
    """
    Detect enabled monitoring stack integrations.

    Uses dashboard/prometheus module configuration only.
    Result is cached in KV store so telemetry can read it
    without recomputing every scrape.
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

        result: Dict[str, Any] = {
            'dashboard_enabled': True,
            'prometheus_enabled': bool(
                mgr.get_module_option_ex(
                    'prometheus',
                    'server_addr'
                )
            ),
            'grafana_enabled': bool(grafana_url),
            'alertmanager_enabled': bool(alertmanager_url),
        }

    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            'ui_metrics: failed to collect adoption metrics: %s',
            e
        )
        return {
            'dashboard_enabled': True,
            'prometheus_enabled': False,
            'grafana_enabled': False,
            'alertmanager_enabled': False,
        }

    mgr.set_store(KV_ADOPTION, json.dumps(result))
    return result


# ---------------------------------------------------------------------------
# REST Controller
# ---------------------------------------------------------------------------

@APIRouter('/ui_metrics')
@APIDoc('Dashboard UI usage metrics (anonymous, aggregated)', 'UIMetrics')
class UIMetrics(RESTController):

    def list(self) -> Dict[str, Any]:
        return collect_metrics()

    @Endpoint()
    @EndpointDoc(
        'Detect enabled monitoring stack integrations'
    )
    def adoption(self) -> Dict[str, Any]:
        return _detect_and_cache_adoption()