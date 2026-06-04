# -*- coding: utf-8 -*-
"""
Dashboard UI Metrics controller.

Tracks anonymous UI usage:
  - Page visit counts per page (incremented by server-side API hooks)
  - Session login count and last-login timestamp (posted by Angular on login)
  - Storage protocol detection via pool application_metadata

KV keys (all under shared mgr store, readable by telemetry + prometheus modules):
  ui_metrics/page_visits/<page>           int
  ui_metrics/sessions/login_count         int
  ui_metrics/sessions/last_login_epoch    int (unix epoch)
  ui_metrics/protocols_enabled            json str
"""

import json
import logging

from typing import Any, Dict

from .. import mgr
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController

logger = logging.getLogger('controllers.ui_metrics')


KV_PROTOCOLS = 'ui_metrics/protocols_enabled'


def collect_metrics() -> Dict[str, Any]:
    """Returns protocol telemetry metrics."""
    prot_raw = mgr.get_store(KV_PROTOCOLS)

    if prot_raw:
        protocols = json.loads(prot_raw)
    else:
        protocols = _detect_and_cache_protocols()

    return {
        'protocols_enabled': protocols,
    }


def _detect_and_cache_protocols() -> Dict[str, Any]:
    """
    Inspect osd_map pool application_metadata to determine which protocols
    are in use:
        'rbd'    → Block  (RBD)
        'cephfs' → File   (CephFS)
        'rgw'    → Object (RGW / S3)
    Result is cached in KV store so telemetry and prometheus can read it
    without re-inspecting osd_map every scrape.
    """
    try:
        osd_map = mgr.get('osd_map') or {}
        pools = osd_map.get('pools', [])
    except Exception as e:  # pylint: disable=broad-except
        logger.error('ui_metrics: failed to read osd_map: %s', e)
        return {'block': False, 'file': False, 'object': False}

    result: Dict[str, Any] = {
        'block':  any('rbd'    in (p.get('application_metadata') or {}) for p in pools),
        'file':   any('cephfs' in (p.get('application_metadata') or {}) for p in pools),
        'object': any('rgw'    in (p.get('application_metadata') or {}) for p in pools),
        'pools_checked': len(pools),
    }
    mgr.set_store(KV_PROTOCOLS, json.dumps(result))
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
        'Detect enabled storage protocols from pool application tags'
    )
    def protocols(self) -> Dict[str, Any]:
        return _detect_and_cache_protocols()