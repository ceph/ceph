# -*- coding: utf-8 -*-
import json
import os
import tempfile
import time
from datetime import datetime
from typing import NamedTuple, Optional

import requests

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services import ceph_service
from ..services.orchestrator import OrchClient
from ..services.settings import SettingsService
from ..settings import Options, Settings
from ..tools import str_to_bool
from . import APIDoc, APIRouter, BaseController, Endpoint, RESTController, Router, UIRouter


class Credentials(NamedTuple):
    user: str
    password: str
    ca_cert_file: Optional[str]
    cert_file: Optional[str]
    pkey_file: Optional[str]


@Router('/api/prometheus_receiver', secure=False)
class PrometheusReceiver(BaseController):
    """
    The receiver is needed in order to receive alert notifications (reports)
    """
    notifications = []

    @Endpoint('POST', path='/', version=None)
    def fetch_alert(self, **notification):
        notification['notified'] = datetime.now().isoformat()
        notification['id'] = str(len(self.notifications))
        self.notifications.append(notification)


class PrometheusRESTController(RESTController):

    # Cache for credentials for 1-minute
    _credentials_cache = {}
    _cache_timestamp = {}

    def close_unlink_files(self, files):
        # type (List[str])
        valid_entries = [f for f in files if f is not None]
        for f in valid_entries:
            f.close()
            os.unlink(f.name)

    def _is_cache_valid(self, module_name):
        """Check if cached credentials are still valid (1 minute)"""
        if module_name not in self._cache_timestamp:
            return False
        current_time = time.time()
        return ((current_time - self._cache_timestamp[module_name])
                < Settings.PROM_ALERT_CREDENTIAL_CACHE_TTL)

    def _get_cached_credentials(self, module_name):
        """
        Get cached credentials if they exist and are valid
        Clears the cached credentials if invalid
        """
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
        """Cache credentials with current timestamp"""
        self._credentials_cache[module_name] = credentials
        self._cache_timestamp[module_name] = time.time()

    def prometheus_proxy(self, method, path, params=None, payload=None):
        # type (str, str, dict, dict)
        user, password, ca_cert_file, cert_file, key_file = self.get_access_info('prometheus')
        verify = ca_cert_file.name if ca_cert_file else Settings.PROMETHEUS_API_SSL_VERIFY
        cert = (cert_file.name, key_file.name) if cert_file and key_file else None
        response = self._proxy(self._get_api_url(Settings.PROMETHEUS_API_HOST),
                               method, path, 'Prometheus', params, payload,
                               user=user, password=password, verify=verify,
                               cert=cert)
        return response

    def alert_proxy(self, method, path, params=None, payload=None):
        # type (str, str, dict, dict)
        user, password, ca_cert_file, cert_file, key_file = self.get_access_info('alertmanager')
        verify = ca_cert_file.name if ca_cert_file else Settings.ALERTMANAGER_API_SSL_VERIFY
        cert = (cert_file.name, key_file.name) if cert_file and key_file else None
        response = self._proxy(self._get_api_url(Settings.ALERTMANAGER_API_HOST, version='v2'),
                               method, path, 'Alertmanager', params, payload,
                               user=user, password=password, verify=verify,
                               cert=cert, is_alertmanager=True)
        return response

    def get_access_info(self, module_name):
        """
        Fetches credentials and certificate files for Prometheus/Alertmanager API access.
        Cases handled:
        - If secure_monitoring_stack and/or mgmt_gateway enabled:
                fetch credentials (user, password, certs).
        - If oauth2-proxy enabled: fetch credentials,
                but only certs are used (user/password ignored).
        - If not cephadm backend: returns credentials with all fields as None.
        Returns:
            Credentials namedtuple with user, password, ca_cert_file, cert_file, pkey_file.
        """

        def write_to_tmp_file(content):
            # type (str)
            if content is None:
                return None
            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            tmp_file.write(content.encode('utf-8'))
            tmp_file.flush()  # tmp_file must not be gc'ed
            tmp_file.close()
            return tmp_file

        orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
        is_cephadm = orch_backend == 'cephadm'

        if module_name not in ['prometheus', 'alertmanager']:
            raise DashboardException(f'Invalid module name {module_name}',
                                     coFalsemponent='prometheus')

        user = None
        password = None
        cert_file = None
        pkey_file = None
        ca_cert_file = None

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
            if module_name == 'prometheus':
                access_info = orch_client.monitoring.get_prometheus_access_info()
            elif module_name == 'alertmanager':
                access_info = orch_client.monitoring.get_alertmanager_access_info()
            else:
                access_info = None
            if access_info:
                user = access_info.get('user')
                password = access_info.get('password')
                ca_cert_file = write_to_tmp_file(access_info.get('certificate'))
                cert_file = write_to_tmp_file(mgr.get_localized_store("crt"))
                pkey_file = write_to_tmp_file(mgr.get_localized_store("key"))
                # Cache the credentials
                self._cache_credentials(
                    module_name,
                    Credentials(user, password, ca_cert_file, cert_file, pkey_file)
                )

        return Credentials(user, password, ca_cert_file, cert_file, pkey_file)

    def _get_api_url(self, host, version='v1'):
        return f'{host.rstrip("/")}/api/{version}'

    def balancer_status(self):
        return ceph_service.CephService.send_command('mon', 'balancer status')

    def _proxy(self, base_url, method, path, api_name, params=None, payload=None, verify=True,
               user=None, password=None, is_alertmanager=False, cert=None):
        # type (str, str, str, str, dict, dict, bool)
        content = None
        try:
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(user, password) if user and password else None
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify,
                                        cert=cert,
                                        auth=auth)
        except Exception as e:
            raise DashboardException(
                "Could not reach {}'s API on {} error {}".format(api_name, base_url, e),
                http_status_code=404,
                component='prometheus')
        try:
            if response.content:
                content = json.loads(response.content, strict=False)
        except json.JSONDecodeError as e:
            raise DashboardException(
                "Error parsing Prometheus Alertmanager response: {}".format(e.msg),
                component='prometheus')
        if is_alertmanager:
            return content
        balancer_status = self.balancer_status()
        if content['status'] == 'success':  # pylint: disable=R1702
            alerts_info = []
            if 'data' in content:
                if balancer_status['active'] and balancer_status['no_optimization_needed'] and path == '/alerts':  # noqa E501  #pylint: disable=line-too-long
                    alerts_info = [alert for alert in content['data'] if alert['labels']['alertname'] != 'CephPGImbalance']  # noqa E501  #pylint: disable=line-too-long
                    return alerts_info
                return content['data']
            return content
        raise DashboardException(content, http_status_code=400, component='prometheus')


@APIRouter('/prometheus', Scope.PROMETHEUS)
@APIDoc("Prometheus Management API", "Prometheus")
class Prometheus(PrometheusRESTController):
    AVG_CONSUMPTION_QUERY = 'sum(rate(ceph_osd_stat_bytes_used[7d])) * 86400'
    TIME_UNTIL_FULL_QUERY = \
        '(sum(ceph_osd_stat_bytes)) / (sum(rate(ceph_osd_stat_bytes_used[7d])) * 86400)'
    TOTAL_RAW_USED_QUERY = 'sum(ceph_osd_stat_bytes_used)'
    RAW_USED_BY_STORAGE_TYPE_QUERY = \
        'sum by (application) (ceph_pool_bytes_used * on(pool_id) ' \
        'group_left(instance, name, application) ' \
        'ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})'
    FULL_NEARFULL_QUERY = '{__name__=~"ceph_osd_full_ratio|ceph_osd_nearfull_ratio"}'

    def _run_prometheus_query(self, query, query_path='/query', **params):
        params['query'] = query
        return self.prometheus_proxy('GET', query_path, params)

    @staticmethod
    def _first_prometheus_value(result):
        if result and 'value' in result[0] and len(result[0]['value']) > 1:
            return result[0]['value'][1]
        return None

    def list(self, cluster_filter=False, **params):
        if cluster_filter:
            try:
                fsid = mgr.get('config')['fsid']
            except KeyError:
                raise DashboardException("Cluster fsid not found", component='prometheus')
            return self.alert_proxy('GET', f'/alerts?filter=cluster={fsid}', params)
        return self.alert_proxy('GET', '/alerts', params)

    @RESTController.Collection(method='GET')
    def rules(self, **params):
        return self.prometheus_proxy('GET', '/rules', params)

    @RESTController.Collection(method='GET', path='/data')
    def get_prometeus_data(self, **params):
        params['query'] = params.pop('params')
        return self.prometheus_proxy('GET', '/query_range', params)

    @RESTController.Collection(method='GET', path='/silences')
    def get_silences(self, **params):
        return self.alert_proxy('GET', '/silences', params)

    @RESTController.Collection(method='POST', path='/silence', status=201)
    def create_silence(self, **params):
        return self.alert_proxy('POST', '/silences', payload=params)

    @RESTController.Collection(method='DELETE', path='/silence/{s_id}', status=204)
    def delete_silence(self, s_id):
        return self.alert_proxy('DELETE', '/silence/' + s_id) if s_id else None

    @RESTController.Collection(method='GET', path='/alertgroup')
    def get_alertgroup(self, cluster_filter=False, **params):
        if str_to_bool(cluster_filter):
            try:
                fsid = mgr.get('config')['fsid']
            except KeyError:
                raise DashboardException("Cluster fsid not found", component='prometheus')
            return self.alert_proxy('GET', f'/alerts/groups?filter=cluster={fsid}', params)
        return self.alert_proxy('GET', '/alerts/groups', params)

    @RESTController.Collection(method='GET', path='/prometheus_query_data')
    def get_prometeus_query_data(self, **params):
        params['query'] = params.pop('params')
        return self.prometheus_proxy('GET', '/query', params)

    @RESTController.Collection(method='GET', path='/overview/storage')
    def get_overview_storage(self):
        average_consumption = self._run_prometheus_query(self.AVG_CONSUMPTION_QUERY)
        time_until_full = self._run_prometheus_query(self.TIME_UNTIL_FULL_QUERY)
        breakdown = self._run_prometheus_query(self.RAW_USED_BY_STORAGE_TYPE_QUERY)
        thresholds = self._run_prometheus_query(self.FULL_NEARFULL_QUERY)

        return {
            'average_consumption_per_day': self._first_prometheus_value(average_consumption),
            'time_until_full_days': self._first_prometheus_value(time_until_full),
            'breakdown': [
                {
                    'application': item.get('metric', {}).get('application'),
                    'value': item.get('value', [None, None])[1]
                } for item in breakdown
            ],
            'osd_full_ratio': next(
                (
                    item.get('value', [None, None])[1] for item in thresholds
                    if item.get('metric', {}).get('__name__') == 'ceph_osd_full_ratio'
                ),
                None
            ),
            'osd_nearfull_ratio': next(
                (
                    item.get('value', [None, None])[1] for item in thresholds
                    if item.get('metric', {}).get('__name__') == 'ceph_osd_nearfull_ratio'
                ),
                None
            )
        }

    @RESTController.Collection(method='GET', path='/overview/storage/trend')
    def get_overview_storage_trend(self, start, end, step):
        result = self._run_prometheus_query(self.TOTAL_RAW_USED_QUERY,
                                            query_path='/query_range',
                                            start=start, end=end, step=step)
        return {
            'total_raw_used': result[0].get('values', []) if result else []
        }


@APIRouter('/prometheus/notifications', Scope.PROMETHEUS)
@APIDoc("Prometheus Notifications Management API", "PrometheusNotifications")
class PrometheusNotifications(RESTController):

    def list(self, **params):
        if 'from' in params:
            f = params['from']
            if f == 'last':
                return PrometheusReceiver.notifications[-1:]
            return PrometheusReceiver.notifications[int(f) + 1:]
        return PrometheusReceiver.notifications


@UIRouter('/prometheus', Scope.PROMETHEUS)
class PrometheusSettings(RESTController):
    def get(self, name):
        with SettingsService.attribute_handler(name) as settings_name:
            setting = getattr(Options, settings_name)
        return {
            'name': settings_name,
            'default': setting.default_value,
            'type': setting.types_as_str(),
            'value': getattr(Settings, settings_name)
        }
