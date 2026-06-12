# -*- coding: utf-8 -*-
"""Dashboard Telemetry Metrics Controller."""

from ..services.telemetry import (
    DashboardTelemetryService,
    AdoptionMetrics
)
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController


@APIRouter('/telemetry/metrics')
@APIDoc('Dashboard telemetry metrics', 'TelemetryMetrics')
class TelemetryMetrics(RESTController):
    """Thin controller for telemetry metrics - delegates to service layer."""

    def list(self):
        """Get all telemetry metrics."""
        return DashboardTelemetryService.get_metrics()

    @Endpoint()
    @EndpointDoc('Force refresh adoption metrics')
    def adoption(self) -> AdoptionMetrics:
        """Force detection and refresh of adoption metrics."""
        return DashboardTelemetryService.refresh_adoption()