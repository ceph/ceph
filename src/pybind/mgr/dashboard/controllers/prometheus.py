# -*- coding: utf-8 -*-
from __future__ import absolute_import

from datetime import datetime
import json
import requests

from . import Controller, ApiController, BaseController, RESTController, Endpoint
from ..security import Scope
from ..settings import Settings


@Controller('/api/prometheus_receiver', secure=False)
class PrometheusReceiver(BaseController):
    ''' The receiver is needed in order to receive alert notifications (reports) '''
    notifications = []

    @Endpoint('POST', path='/')
    def fetch_alert(self, **notification):
        notification['notified'] = datetime.now().isoformat()
        notification['id'] = str(len(self.notifications))
        self.notifications.append(notification)


@ApiController('/prometheus', Scope.PROMETHEUS)
class Prometheus(RESTController):

    def _get_api_url(self):
        return Settings.ALERTMANAGER_API_HOST.rstrip('/') + '/api/v1'

    def _api_request(self, url_suffix, params=None):
        url = self._get_api_url() + url_suffix
        response = requests.request('GET', url, params=params)
        payload = json.loads(response.content)
        return payload['data'] if 'data' in payload else []

    def list(self, **params):
        return self._api_request('/alerts', params)


@ApiController('/prometheus/notifications', Scope.PROMETHEUS)
class PrometheusNotifications(RESTController):

    def list(self, **params):
        if 'from' in params:
            f = params['from']
            if f == 'last':
                return PrometheusReceiver.notifications[-1:]
            return PrometheusReceiver.notifications[int(f) + 1:]
        return PrometheusReceiver.notifications
