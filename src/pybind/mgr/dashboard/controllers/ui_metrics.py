# -*- coding: utf-8 -*-

from . import APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController
from ..services.telemetry import DashboardTelemetryService, UserPersonaDistribution

@APIRouter('/telemetry/metrics')
@APIDoc('Dashboard usage metrics (anonymous, aggregated)', 'TelemetryMetrics')
class TelemetryMetrics(RESTController):

    def list(self):
        return DashboardTelemetryService.get_metrics()

    @Endpoint(path='/user_personas')
    @EndpointDoc('Get user persona distribution from RBAC roles')
    def user_personas(self) -> UserPersonaDistribution:
        return DashboardTelemetryService.get_user_persona_distribution()
