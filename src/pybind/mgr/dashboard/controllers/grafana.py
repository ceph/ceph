# -*- coding: utf-8 -*-
from __future__ import absolute_import

import requests

from . import ApiController, BaseController, Endpoint, ReadPermission
from ..security import Scope
from ..settings import Settings


class GrafanaRestClient(object):

    def url_validation(self, method, path):
        response = requests.request(
            method,
            path)

        return response.status_code


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
