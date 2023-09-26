# -*- coding: utf-8 -*-
import json
import os
import tempfile
from datetime import datetime

import requests

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services import ceph_service
from ..services.settings import SettingsService
from ..settings import Options, Settings
from . import APIDoc, APIRouter, BaseController, Endpoint, RESTController, Router, UIRouter


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
    def prometheus_proxy(self, method, path, params=None, payload=None):
        # type (str, str, dict, dict)
        user, password, cert_file = self.get_access_info('prometheus')
        verify = cert_file.name if cert_file else Settings.PROMETHEUS_API_SSL_VERIFY
        response = self._proxy(self._get_api_url(Settings.PROMETHEUS_API_HOST),
                               method, path, 'Prometheus', params, payload,
                               user=user, password=password, verify=verify)
        if cert_file:
            cert_file.close()
            os.unlink(cert_file.name)
        return response

    def alert_proxy(self, method, path, params=None, payload=None):
        # type (str, str, dict, dict)
        user, password, cert_file = self.get_access_info('alertmanager')
        verify = cert_file.name if cert_file else Settings.ALERTMANAGER_API_SSL_VERIFY
        response = self._proxy(self._get_api_url(Settings.ALERTMANAGER_API_HOST, version='v2'),
                               method, path, 'Alertmanager', params, payload,
                               user=user, password=password, verify=verify, is_alertmanager=True)
        if cert_file:
            cert_file.close()
            os.unlink(cert_file.name)
        return response

    def get_access_info(self, module_name):
        # type (str, str, str)
        if module_name not in ['prometheus', 'alertmanager']:
            raise DashboardException(f'Invalid module name {module_name}', component='prometheus')
        user = None
        password = None
        cert_file = None

        orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
        if orch_backend == 'cephadm':
            secure_monitoring_stack = mgr.get_module_option_ex('cephadm',
                                                               'secure_monitoring_stack',
                                                               False)
            if secure_monitoring_stack:
                cmd = {'prefix': f'orch {module_name} get-credentials'}
                ret, out, _ = mgr.mon_command(cmd)
                if ret == 0 and out is not None:
                    access_info = json.loads(out)
                    user = access_info['user']
                    password = access_info['password']
                    certificate = access_info['certificate']
                    cert_file = tempfile.NamedTemporaryFile(delete=False)
                    cert_file.write(certificate.encode('utf-8'))
                    cert_file.flush()

        return user, password, cert_file

    def _get_api_url(self, host, version='v1'):
        return f'{host.rstrip("/")}/api/{version}'

    def balancer_status(self):
        return ceph_service.CephService.send_command('mon', 'balancer status')

    def _proxy(self, base_url, method, path, api_name, params=None, payload=None, verify=True,
               user=None, password=None, is_alertmanager=False):
        # type (str, str, str, str, dict, dict, bool)
        content = None
        try:
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(user, password) if user and password else None
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify,
                                        auth=auth)
        except Exception:
            raise DashboardException(
                "Could not reach {}'s API on {}".format(api_name, base_url),
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
    def list(self, **params):
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
    def get_alertgroup(self, **params):
        return self.alert_proxy('GET', '/alerts/groups', params)

    @RESTController.Collection(method='GET', path='/prometheus_query_data')
    def get_prometeus_query_data(self, **params):
        params['query'] = params.pop('params')
        return self.prometheus_proxy('GET', '/query', params)


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
