# -*- coding: utf-8 -*-

import json
from datetime import datetime

import requests

from ..exceptions import DashboardException
from ..security import Scope
from ..settings import Settings
from . import ApiController, BaseController, Controller, ControllerDoc, Endpoint, RESTController


@Controller('/api/prometheus_receiver', secure=False)
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
        return self._proxy(self._get_api_url(Settings.PROMETHEUS_API_HOST),
                           method, path, 'Prometheus', params, payload,
                           verify=Settings.PROMETHEUS_API_SSL_VERIFY)

    def alert_proxy(self, method, path, params=None, payload=None):
        # type (str, str, dict, dict)
        return self._proxy(self._get_api_url(Settings.ALERTMANAGER_API_HOST),
                           method, path, 'Alertmanager', params, payload,
                           verify=Settings.ALERTMANAGER_API_SSL_VERIFY)

    def _get_api_url(self, host):
        return host.rstrip('/') + '/api/v1'

    def _proxy(self, base_url, method, path, api_name, params=None, payload=None, verify=True):
        # type (str, str, str, str, dict, dict, bool)
        try:
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify)
        except Exception:
            raise DashboardException(
                "Could not reach {}'s API on {}".format(api_name, base_url),
                http_status_code=404,
                component='prometheus')
        content = json.loads(response.content)
        if content['status'] == 'success':
            if 'data' in content:
                return content['data']
            return content
        raise DashboardException(content, http_status_code=400, component='prometheus')


@ApiController('/prometheus', Scope.PROMETHEUS)
@ControllerDoc("Prometheus Management API", "Prometheus")
class Prometheus(PrometheusRESTController):
    def list(self, **params):
        return self.alert_proxy('GET', '/alerts', params)

    @RESTController.Collection(method='GET')
    def rules(self, **params):
        return self.prometheus_proxy('GET', '/rules', params)

    @RESTController.Collection(method='GET', path='/silences')
    def get_silences(self, **params):
        return self.alert_proxy('GET', '/silences', params)

    @RESTController.Collection(method='POST', path='/silence', status=201)
    def create_silence(self, **params):
        return self.alert_proxy('POST', '/silences', payload=params)

    @RESTController.Collection(method='DELETE', path='/silence/{s_id}', status=204)
    def delete_silence(self, s_id):
        return self.alert_proxy('DELETE', '/silence/' + s_id) if s_id else None


@ApiController('/prometheus/notifications', Scope.PROMETHEUS)
@ControllerDoc("Prometheus Notifications Management API", "PrometheusNotifications")
class PrometheusNotifications(RESTController):

    def list(self, **params):
        if 'from' in params:
            f = params['from']
            if f == 'last':
                return PrometheusReceiver.notifications[-1:]
            return PrometheusReceiver.notifications[int(f) + 1:]
        return PrometheusReceiver.notifications
