# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import logging
import os
import time
import requests

from .exceptions import GrafanaError
from .settings import Settings


logger = logging.getLogger('grafana')


class GrafanaRestClient(object):

    @staticmethod
    def url_validation(method, path):
        response = requests.request(
            method,
            path)

        return response.status_code

    @staticmethod
    def push_dashboard(dashboard_obj):
        if not Settings.GRAFANA_API_URL:
            raise GrafanaError("The Grafana API URL is not set")
        if not Settings.GRAFANA_API_URL.startswith('http'):
            raise GrafanaError("The Grafana API URL is invalid")
        if not Settings.GRAFANA_API_USERNAME:
            raise GrafanaError("The Grafana API username is not set")
        if not Settings.GRAFANA_API_PASSWORD:
            raise GrafanaError("The Grafana API password is not set")
        url = Settings.GRAFANA_API_URL.rstrip('/') + \
            '/api/dashboards/db'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        payload = {
            'dashboard': dashboard_obj,
            'overwrite': True,
        }
        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                auth=(Settings.GRAFANA_API_USERNAME,
                      Settings.GRAFANA_API_PASSWORD),
            )
        except requests.ConnectionError:
            raise GrafanaError("Could not connect to Grafana server")
        response.raise_for_status()
        return response.status_code, response.json()


class Retrier(object):
    def __init__(self, tries, sleep, func, *args, **kwargs):
        """
        Wraps a function. An instance of this class may be called to call that
        function, retrying if it raises an exception. Sleeps between retries,
        eventually reraising the original exception when retries are exhausted.
        Once the function returns a value, that value is returned.

        :param tries: How many times to try, before reraising the exception
        :type tries: int
        :param sleep: How many seconds to wait between tries
        :type sleep: int|float
        :param func: The function to execute
        :type func: function
        :param args: Any arguments to pass to the function
        :type args: list
        :param kwargs: Any keyword arguments to pass to the function
        :type kwargs: dict
        """
        assert tries >= 1
        self.tries = int(tries)
        self.tried = 0
        self.sleep = sleep
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        result = None
        while self.tried < self.tries:
            try:
                result = self.func(*self.args, **self.kwargs)
            except Exception:  # pylint: disable=broad-except
                if self.tried == self.tries - 1:
                    raise
                else:
                    self.tried += 1
                    time.sleep(self.sleep)
            else:
                return result


def load_local_dashboards():
    if os.environ.get('CEPH_DEV') == '1' or 'UNITTEST' in os.environ:
        path = os.path.abspath(os.path.join(
            os.path.dirname(__file__),
            '../../../../monitoring/grafana/dashboards/'
        ))
    else:
        path = '/etc/grafana/dashboards/ceph-dashboard'
    dashboards = dict()
    for item in [p for p in os.listdir(path) if p.endswith('.json')]:
        db_path = os.path.join(path, item)
        with open(db_path) as f:
            dashboards[item] = json.loads(f.read())
    return dashboards


def push_local_dashboards(tries=1, sleep=0):
    try:
        dashboards = load_local_dashboards()
    except (EnvironmentError, ValueError):
        logger.exception("Failed to load local dashboard files")
        raise

    def push():
        try:
            grafana = GrafanaRestClient()
            for body in dashboards.values():
                grafana.push_dashboard(body)
        except Exception:
            logger.exception("Failed to push dashboards to Grafana")
            raise
    retry = Retrier(tries, sleep, push)
    retry()
    return True
