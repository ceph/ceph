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

    def _resolve_loki_host(self) -> str:
        host = (Settings.LOKI_API_HOST or '').strip()
        if host and '://' in host:
            return host.rstrip('/')

        raise DashboardException(
            "Loki API host is not configured. Deploy the loki service or set "
            "'ceph dashboard set-loki-api-host <scheme>://<host>:3100'.",
            http_status_code=503,
            component='loki')

    @staticmethod
    def _prepare_loki_params(params: Optional[dict]) -> Optional[dict]:
        if params and 'params' in params:
            params = dict(params)
            params['query'] = params.pop('params')
        return params

    def loki_proxy(self, method: str, path: str, params: Optional[dict] = None,
                   payload: Optional[dict] = None):
        return self._proxy(self._get_api_url(self._resolve_loki_host()),
                           method, path, 'Loki', params, payload,
                           verify=Settings.LOKI_API_SSL_VERIFY)

    def _proxy(self, base_url: str, method: str, path: str, api_name: str,
               params: Optional[dict] = None, payload: Optional[dict] = None,
               verify: bool = True):
        content = None
        try:
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify)
        except requests.RequestException as e:
            raise DashboardException(
                "Could not reach {}'s API on {} error {}".format(api_name, base_url, e),
                http_status_code=502,
                component='loki')
        try:
            if response.content:
                content = json.loads(response.content, strict=False)
        except json.JSONDecodeError as e:
            raise DashboardException(
                "Error parsing Loki response: {}".format(e.msg),
                component='loki')
        if not isinstance(content, dict) or 'status' not in content:
            raise DashboardException(
                'Invalid Loki response',
                http_status_code=502,
                component='loki')
        if content.get('status') == 'success':
            return content.get('data', content)
        raise DashboardException(content, http_status_code=400, component='loki')


@APIRouter('/loki', Scope.LOG)
@APIDoc("Loki Management API", "Loki")
class Loki(LokiRESTController):
    """
    Proxy controller for the Loki HTTP query API.

    Supports instant and range LogQL queries, plus label discovery endpoints.
    """

    @RESTController.Collection(method='GET', path='/query')
    def query(self, **params):
        """
        Instant query against Loki.

        Evaluates a LogQL query at a single point in time. Intended for metric
        queries such as rate() or count_over_time(). Accepts Loki query params:
        query, limit, time, direction.
        """
        return self.loki_proxy('GET', '/query', self._prepare_loki_params(params))

    @RESTController.Collection(method='GET', path='/query_range')
    def query_range(self, **params):
        """
        Range query against Loki.

        Queries logs or metrics over a time range. Accepts Loki query params:
        query, limit, start, end, since, step, interval, direction.
        """
        return self.loki_proxy('GET', '/query_range', self._prepare_loki_params(params))

    @RESTController.Collection(method='GET', path='/labels')
    def labels(self, **params):
        """
        List known label names within a time span.

        Accepts Loki query params: start, end, since, query.
        """
        return self.loki_proxy('GET', '/labels', self._prepare_loki_params(params))

    @RESTController.Collection(method='GET', path='/label/{name}/values')
    def label_values(self, name, **params):
        """
        List known values for a label within a time span.

        Accepts Loki query params: start, end, since, query.
        """
        return self.loki_proxy('GET', f'/label/{name}/values',
                               self._prepare_loki_params(params))
