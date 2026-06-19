# -*- coding: utf-8 -*-
import json
import os
import tempfile
import time
from typing import List, NamedTuple, Optional

import requests
from mgr_util import build_url

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..services.settings import SettingsService
from ..settings import Options, Settings
from . import APIDoc, APIRouter, RESTController, UIRouter


class Credentials(NamedTuple):
    user: str
    password: str
    ca_cert_file: Optional[str]
    cert_file: Optional[str]
    pkey_file: Optional[str]


class LokiRESTController(RESTController):

    LOKI_DEFAULT_PORT = 3100

    _credentials_cache = {}
    _cache_timestamp = {}

    def close_unlink_files(self, files):
        # type (List[str])
        valid_entries = [f for f in files if f is not None]
        for f in valid_entries:
            f.close()
            os.unlink(f.name)

    def _is_cache_valid(self, module_name):
        if module_name not in self._cache_timestamp:
            return False
        current_time = time.time()
        return ((current_time - self._cache_timestamp[module_name])
                < Settings.PROM_ALERT_CREDENTIAL_CACHE_TTL)

    def _get_cached_credentials(self, module_name):
        if self._is_cache_valid(module_name):
            return self._credentials_cache.get(module_name)
        old_creds = self._credentials_cache.get(module_name)
        if old_creds:
            self.close_unlink_files([
                old_creds.ca_cert_file,
                old_creds.cert_file,
                old_creds.pkey_file
            ])
            self._credentials_cache.pop(module_name, None)
            self._cache_timestamp.pop(module_name, None)
        return None

    def _cache_credentials(self, module_name, credentials):
        self._credentials_cache[module_name] = credentials
        self._cache_timestamp[module_name] = time.time()

    def get_access_info(self, module_name):
        """
        Fetches credentials and certificate files for Loki API access.

        When the secure monitoring stack is enabled, Loki uses the same
        credentials and client certificates as Prometheus.
        """

        def write_to_tmp_file(content):
            if content is None:
                return None
            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_file.write(content.encode('utf-8'))
            tmp_file.flush()
            tmp_file.close()
            return tmp_file

        orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
        is_cephadm = orch_backend == 'cephadm'

        if module_name not in ['loki', 'grafana', 'dashboard']:
            raise DashboardException(f'Invalid module name {module_name}',
                                     component='loki')

        user = None
        password = None
        cert_file = None
        pkey_file = None
        ca_cert_file = None

        if module_name == 'grafana':
            return Credentials(Settings.GRAFANA_API_USERNAME,
                               Settings.GRAFANA_API_PASSWORD,
                               ca_cert_file, cert_file, pkey_file)

        if module_name == 'dashboard':
            return Credentials(user, password, ca_cert_file, cert_file, pkey_file)

        if not is_cephadm:
            return Credentials(user, password, ca_cert_file, cert_file, pkey_file)

        cached_creds = self._get_cached_credentials(module_name)
        if cached_creds:
            return cached_creds

        orch_client = OrchClient.instance()
        security_config = orch_client.monitoring.get_security_config()
        if not security_config.get('security_enabled', False):
            return Credentials(user, password, ca_cert_file, cert_file, pkey_file)

        if orch_client.available():
            access_info = orch_client.monitoring.get_prometheus_access_info()
            if access_info:
                user = access_info.get('user')
                password = access_info.get('password')
                ca_cert_file = write_to_tmp_file(access_info.get('certificate'))
                cert_file = write_to_tmp_file(mgr.get_localized_store("crt"))
                pkey_file = write_to_tmp_file(mgr.get_localized_store("key"))
                self._cache_credentials(
                    module_name,
                    Credentials(user, password, ca_cert_file, cert_file, pkey_file)
                )

        return Credentials(user, password, ca_cert_file, cert_file, pkey_file)

    def _get_api_url(self, host: str) -> str:
        return f'{host.rstrip("/")}/loki/api/v1'

    def _resolve_loki_host(self) -> str:
        host = (Settings.LOKI_API_HOST or '').strip()
        if host and '://' in host:
            return host.rstrip('/')

        discovered = self._discover_loki_host()
        if discovered:
            return discovered

        raise DashboardException(
            "Loki API host is not configured. Deploy the loki service or set "
            "'ceph dashboard set-loki-api-host <scheme>://<host>:3100'.",
            http_status_code=503,
            component='loki')

    def _discover_loki_host(self) -> Optional[str]:
        try:
            orch = OrchClient.instance()
            if not orch.available():
                return None
            daemons = orch.services.list_daemons(daemon_type='loki')
            for daemon in daemons:
                if daemon.status != 1 or not daemon.hostname:
                    continue
                addr = daemon.ip if daemon.ip else daemon.hostname
                port = daemon.ports[0] if daemon.ports else self.LOKI_DEFAULT_PORT
                return build_url(scheme='http', host=addr, port=port)
        except Exception:
            return None
        return None

    @staticmethod
    def _prepare_loki_params(params: Optional[dict]) -> Optional[dict]:
        if params and 'params' in params:
            params = dict(params)
            params['query'] = params.pop('params')
        return params

    def loki_proxy(self, method: str, path: str, params: Optional[dict] = None,
                   payload: Optional[dict] = None):
        host = self._resolve_loki_host()
        if self._uses_grafana_proxy(host):
            user, password, ca_cert_file, cert_file, key_file = self.get_access_info('grafana')
            verify = Settings.GRAFANA_API_SSL_VERIFY
            cert = None
            proxy_mode = 'grafana'
            base_url = host
        else:
            user, password, ca_cert_file, cert_file, key_file = self.get_access_info('loki')
            verify = ca_cert_file.name if ca_cert_file else Settings.LOKI_API_SSL_VERIFY
            cert = (cert_file.name, key_file.name) if cert_file and key_file else None
            proxy_mode = 'loki'
            base_url = self._get_api_url(host)
        return self._proxy(base_url,
                           method, path, 'Loki', params, payload,
                           user=user, password=password, verify=verify,
                           cert=cert, proxy_mode=proxy_mode)

    @staticmethod
    def _uses_grafana_proxy(host: str) -> bool:
        grafana_url = (Settings.GRAFANA_API_URL or '').rstrip('/')
        return bool(grafana_url and host.startswith(grafana_url))

    def grafana_proxy(self, method: str, path: str, params: Optional[dict] = None,
                      payload: Optional[dict] = None):
        user, password, _, _, _ = self.get_access_info('grafana')
        return self._proxy((Settings.GRAFANA_API_URL or '').rstrip('/'),
                           method, path, 'Grafana', params, payload,
                           user=user, password=password,
                           verify=Settings.GRAFANA_API_SSL_VERIFY,
                           proxy_mode='grafana')

    def dashboard_proxy(self, method: str, base_url: str, path: str,
                        params: Optional[dict] = None, payload: Optional[dict] = None,
                        token: Optional[str] = None, verify=False):
        headers = {
            'Accept': 'application/vnd.ceph.api.v1.0+json',
        }
        if token:
            headers['Authorization'] = 'Bearer ' + token
        else:
            headers['Content-Type'] = 'application/json'
        return self._proxy(base_url.rstrip('/'), method, path, 'Dashboard', params, payload,
                           verify=verify, proxy_mode='dashboard', headers=headers)

    def _proxy(self, base_url, method, path, api_name, params=None, payload=None, verify=True,
               user=None, password=None, cert=None, proxy_mode='loki', headers=None):
        content = None
        try:
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(user, password) if user and password else None
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify,
                                        cert=cert,
                                        auth=auth,
                                        headers=headers)
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
                "Error parsing {} response: {}".format(api_name, e.msg),
                component='loki')
        if proxy_mode in ('grafana', 'dashboard'):
            return content
        if content['status'] == 'success':
            if 'data' in content:
                return content['data']
            return content
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


@UIRouter('/loki', Scope.LOG)
class LokiSettings(RESTController):
    def get(self, name):
        with SettingsService.attribute_handler(name) as settings_name:
            setting = getattr(Options, settings_name)
        return {
            'name': settings_name,
            'default': setting.default_value,
            'type': setting.types_as_str(),
            'value': getattr(Settings, settings_name)
        }
