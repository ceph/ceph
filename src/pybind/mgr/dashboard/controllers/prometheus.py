# -*- coding: utf-8 -*-
from __future__ import absolute_import

from datetime import datetime
import json
import requests

from . import Controller, ApiController, BaseController, RESTController, Endpoint
from ..security import Scope
from ..settings import Settings
from ..exceptions import DashboardException


@Controller('/api/prometheus_receiver', secure=False)
class PrometheusReceiver(BaseController):
    '''The receiver is needed in order to receive alert notifications (reports)'''
    notifications = []

    @Endpoint('POST', path='/')
    def fetch_alert(self, **notification):
        notification['notified'] = datetime.now().isoformat()
        self.notifications.append(notification)


class PrometheusRESTController(RESTController):
    def prometheus_proxy(self, method, path, params=None, payload=None):
        return self._proxy(self._get_api_url(Settings.PROMETHEUS_API_HOST),
                           method, path, params, payload)

    def alert_proxy(self, method, path, params=None, payload=None):
        return self._proxy(self._get_api_url(Settings.ALERTMANAGER_API_HOST),
                           method, path, params, payload)

    def _get_api_url(self, host):
        return host.rstrip('/') + '/api/v1'

    def _proxy(self, baseUrl, method, path, params=None, payload=None):
        try:
            response = requests.request(method, baseUrl + path, params=params, json=payload)
        except Exception as e:
            raise DashboardException('Could not reach external API', http_status_code=404,
                                     component='prometheus')
        content = json.loads(response.content)
        if content['status'] == 'success':
            return content['data']
        raise DashboardException(content, http_status_code=400, component='prometheus')


@ApiController('/prometheus', Scope.PROMETHEUS)
class Prometheus(PrometheusRESTController):
    def list(self, **params):
        return self.alert_proxy('GET', '/alerts', params)

    @RESTController.Collection(method='GET')
    def query(self, **params):
        return self.prometheus_proxy('GET', '/query', params)

    @RESTController.Collection(method='GET', path='/silences')
    def get_silences(self, **params):
        return self.alert_proxy('GET', '/silences', params)

    @RESTController.Collection(method='POST', path='/silence', status=201)
    def create_silence(self, **params):
        return self.alert_proxy('POST', '/silences', payload=params)

    @RESTController.Collection(method='DELETE', path='/silence/{id}', status=204)
    def delete_silence(self, id):
        return self.alert_proxy('DELETE', '/silence/' + id) if id else None

    @RESTController.Collection('POST')
    def get_notifications_since(self, **last_notification):
        notifications = PrometheusReceiver.notifications
        if last_notification not in notifications:
            return notifications[-1:]
        index = notifications.index(last_notification)
        return notifications[index + 1:]
