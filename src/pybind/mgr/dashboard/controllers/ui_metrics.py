# -*- coding: utf-8 -*-
"""
Dashboard Metrics controller.

Provides REST API endpoints for dashboard telemetry metrics.
Business logic is in services/telemetry.py
"""

from typing import Any, Dict

from . import APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController
from ..services.telemetry import DashboardTelemetryService, ProtocolsEnabled

# ---------------------------------------------------------------------------
# REST Controller
# ---------------------------------------------------------------------------

@APIRouter('/telemetry/metrics')
@APIDoc('Dashboard usage metrics (anonymous, aggregated)', 'TelemetryMetrics')
class TelemetryMetrics(RESTController):
    """Thin controller - delegates to DashboardTelemetryService"""

    def list(self):
        """Get all telemetry metrics"""
        return DashboardTelemetryService.get_metrics()

    @Endpoint()
    @EndpointDoc('Detect enabled storage protocols from pool application tags')
    def protocols(self) -> ProtocolsEnabled:
        """Force re-detection of protocols (bypasses cache)"""
        return DashboardTelemetryService.refresh_protocols()