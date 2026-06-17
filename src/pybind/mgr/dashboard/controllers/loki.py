# -*- coding: utf-8 -*-
import json
from typing import Optional

import requests

from ..exceptions import DashboardException
from ..security import Scope
from ..settings import Settings
from . import APIDoc, APIRouter, RESTController


class LokiRESTController(RESTController):

    def _get_api_url(self, host: str) -> str:
        return f'{host.rstrip("/")}/loki/api/v1'

    def loki_proxy(self, method: str, path: str, params: Optional[dict] = None,
                   payload: Optional[dict] = None):
        response = self._proxy(self._get_api_url(Settings.LOKI_API_HOST),
                               method, path, 'Loki', params, payload,
                               verify=Settings.LOKI_API_SSL_VERIFY)
        return response

    def _proxy(self, base_url: str, method: str, path: str, api_name: str,
               params: Optional[dict] = None, payload: Optional[dict] = None,
               verify: bool = True):
        content = None
        try:
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify)
        except Exception as e:
            raise DashboardException(
                "Could not reach {}'s API on {} error {}".format(api_name, base_url, e),
                http_status_code=404,
                component='loki')
        try:
            if response.content:
                content = json.loads(response.content, strict=False)
        except json.JSONDecodeError as e:
            raise DashboardException(
                "Error parsing Loki response: {}".format(e.msg),
                component='loki')
        if content['status'] == 'success':
            if 'data' in content:
                return content['data']
            return content
        raise DashboardException(content, http_status_code=400, component='loki')

    def get_client_version():
        try:
            client_version = APIVersion.from_mime_type(
                cherrypy.request.headers['Accept'])
        except Exception:
            raise cherrypy.HTTPError(
                415, "Unable to find version in request header")

@APIRouter('/loki', Scope.LOG)
@APIDoc("Loki Management API", "Loki")
class Loki(LokiRESTController):
    """
    Proxy controller for the Loki HTTP query API.

    Supports instant metric queries (GET /loki/api/v1/query) and range queries
    for logs and metrics (GET /loki/api/v1/query_range).
    """

    @RESTController.Collection(method='GET', path='/query')
    def query(self, **params):
        """
        Instant query against Loki.

        Evaluates a LogQL query at a single point in time. Intended for metric
        queries such as rate() or count_over_time(). Accepts Loki query params:
        query, limit, time, direction.
        """
        params['query'] = params.pop('params')
        return self.loki_proxy('GET', '/query', params)

    @RESTController.Collection(method='GET', path='/query_range')
    def query_range(self, **params):
        """
        Range query against Loki.

        Queries logs or metrics over a time range. Accepts Loki query params:
        query, limit, start, end, since, step, interval, direction.
        """
        params['query'] = params.pop('params')
        return self.loki_proxy('GET', '/query_range', params)
