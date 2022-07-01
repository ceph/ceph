# -*- coding: utf-8 -*-
from .. import mgr
from ..grafana import GrafanaRestClient, push_local_dashboards
from ..security import Scope
from ..services.exception import handle_error
from ..settings import Settings
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, \
    ReadPermission, UpdatePermission

URL_SCHEMA = {
    "instance": (str, "grafana instance")
}


@APIRouter('/grafana', Scope.GRAFANA)
@APIDoc("Grafana Management API", "Grafana")
class Grafana(BaseController):
    @Endpoint()
    @ReadPermission
    @EndpointDoc("List Grafana URL Instance", responses={200: URL_SCHEMA})
    def url(self):
        grafana_url = mgr.get_module_option('GRAFANA_API_URL')
        grafana_frontend_url = mgr.get_module_option('GRAFANA_FRONTEND_API_URL')
        if grafana_frontend_url != '' and grafana_url == '':
            url = ''
        else:
            url = (mgr.get_module_option('GRAFANA_FRONTEND_API_URL')
                   or mgr.get_module_option('GRAFANA_API_URL')).rstrip('/')
        response = {'instance': url}
        return response

    @Endpoint()
    @ReadPermission
    @handle_error('grafana')
    def validation(self, params):
        grafana = GrafanaRestClient()
        method = 'GET'
        url = str(Settings.GRAFANA_API_URL).rstrip('/') + \
            '/api/dashboards/uid/' + params
        response = grafana.url_validation(method, url)
        return response

    @Endpoint(method='POST')
    @UpdatePermission
    @handle_error('grafana', 500)
    def dashboards(self):
        response = dict()
        response['success'] = push_local_dashboards()
        return response
