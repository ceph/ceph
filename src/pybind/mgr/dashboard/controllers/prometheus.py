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
    # The receiver is needed in order to receive alert notifications (reports)
    notifications = []

    @Endpoint('POST', path='/')
    def fetch_alert(self, **notification):
        notification['notified'] = datetime.now().isoformat()
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

    @RESTController.Collection('POST')
    def get_notifications_since(self, **last_notification):
        notifications = PrometheusReceiver.notifications
        if last_notification not in notifications:
            return notifications[-1:]
        index = notifications.index(last_notification)
        return notifications[index + 1:]
