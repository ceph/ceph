# -*- coding: utf-8 -*-

import json
import requests

from . import APIDoc, APIRouter, CreatePermission, UpdatePermission, ReadPermission, Endpoint, EndpointDoc, RESTController
from ..exceptions import DashboardException
from ..settings import Settings
from ..security import Scope


@APIRouter('/multi-cluster', Scope.CONFIG_OPT)
@APIDoc('Multi-cluster Management API', 'Multi-cluster')
class MultiCluster(RESTController):
    def _proxy(self, method, base_url, path, params=None, payload=None, verify=False):
        try:
            headers = {
                'Accept': 'application/vnd.ceph.api.v1.0+json',
                'Content-Type': 'application/json',
            }
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify, headers=headers)
        except Exception as e:
            raise DashboardException(
                "Could not reach {}".format(base_url+path),
                http_status_code=404,
                component='dashboard')

        try:
            content = json.loads(response.content, strict=False)
        except json.JSONDecodeError as e:
            raise DashboardException(
                "Error parsing Dashboard API response: {}".format(e.msg),
                component='dashboard')
        return content


    @Endpoint('POST')
    @CreatePermission
    @EndpointDoc("Authenticate to a remote cluster")
    def auth(self, url: str, name: str, username=None, password=None, token=None):
        multicluster_config = {}

        if isinstance(Settings.MULTICLUSTER_CONFIG, str):
            try:
                item_to_dict = json.loads(Settings.MULTICLUSTER_CONFIG)
            except json.JSONDecodeError:
                item_to_dict = {}
            multicluster_config = item_to_dict.copy()
        else:
            multicluster_config = Settings.MULTICLUSTER_CONFIG.copy()

        if 'config' not in multicluster_config:
            multicluster_config['config'] = []

        if token:
            multicluster_config['config'].append({
                'name': name,
                'url': url,
                'token': token
            })

            Settings.MULTICLUSTER_CONFIG = multicluster_config
            return

        if username and password:
            payload = {
                'username': username,
                'password': password
            }
            content = self._proxy('POST', url, 'api/auth', payload=payload)
            if 'token' not in content:
                raise DashboardException(
                    "Could not authenticate to remote cluster",
                    http_status_code=400,
                    component='dashboard')

            else:
                token = content['token']

                multicluster_config['config'].append({
                    'name': name,
                    'url': url,
                    'token': token
                })

                Settings.MULTICLUSTER_CONFIG = multicluster_config
