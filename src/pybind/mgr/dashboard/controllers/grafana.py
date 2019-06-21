# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import (ApiController, BaseController, Endpoint, ReadPermission,
               UpdatePermission)
from ..exceptions import DashboardException
from ..grafana import GrafanaRestClient, push_local_dashboards
from ..security import Scope
from ..settings import Settings


@ApiController('/grafana', Scope.GRAFANA)
class Grafana(BaseController):

    @Endpoint()
    @ReadPermission
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
                msg=e.message,
                component='grafana',
                http_status_code=500,
            )
        return response
