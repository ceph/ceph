# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..exceptions import DashboardException
from ..grafana import GrafanaRestClient, push_local_dashboards
from ..security import Scope
from ..settings import Settings
from . import ApiController, BaseController, ControllerDoc, Endpoint, \
    EndpointDoc, ReadPermission, UpdatePermission

URL_SCHEMA = {
    "instance": (str, "grafana instance")
}


@ApiController('/grafana', Scope.GRAFANA)
@ControllerDoc("Grafana Management API", "Grafana")
class Grafana(BaseController):

    @Endpoint()
    @ReadPermission
    @EndpointDoc("List Grafana URL Instance",
                 responses={200: URL_SCHEMA})
    def url(self):
        response = {'instance': Settings.GRAFANA_API_URL}
        return response

    @Endpoint()
    @ReadPermission
    def validation(self, params):
        grafana = GrafanaRestClient()
        method = 'GET'
        url = Settings.GRAFANA_API_URL.rstrip('/') + \
            '/api/dashboards/uid/' + params
        response = grafana.url_validation(method, url)
        return response

    @Endpoint(method='POST')
    @UpdatePermission
    def dashboards(self):
        response = dict()
        try:
            response['success'] = push_local_dashboards()
        except Exception as e:  # pylint: disable=broad-except
            raise DashboardException(
                msg=str(e),
                component='grafana',
                http_status_code=500,
            )
        return response
