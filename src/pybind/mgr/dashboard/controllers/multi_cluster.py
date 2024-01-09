# -*- coding: utf-8 -*-

import json

import requests

from ..exceptions import DashboardException
from ..security import Scope
from ..settings import Settings
from ..tools import configure_cors
from . import APIDoc, APIRouter, CreatePermission, Endpoint, EndpointDoc, \
    RESTController, UIRouter, UpdatePermission


@APIRouter('/multi-cluster', Scope.CONFIG_OPT)
@APIDoc('Multi-cluster Management API', 'Multi-cluster')
class MultiCluster(RESTController):
    def _proxy(self, method, base_url, path, params=None, payload=None, verify=False, token=None):
        try:
            if token:
                headers = {
                    'Accept': 'application/vnd.ceph.api.v1.0+json',
                    'Authorization': 'Bearer ' + token,
                }
            else:
                headers = {
                    'Accept': 'application/vnd.ceph.api.v1.0+json',
                    'Content-Type': 'application/json',
                }
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify, headers=headers)
        except Exception as e:
            raise DashboardException(
                "Could not reach {}, {}".format(base_url+path, e),
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
    def auth(self, url: str, name: str, username=None, password=None, token=None, hub_url=None):
        multicluster_config = {}

        if isinstance(Settings.MULTICLUSTER_CONFIG, str):
            try:
                item_to_dict = json.loads(Settings.MULTICLUSTER_CONFIG)
            except json.JSONDecodeError:
                item_to_dict = {}
            multicluster_config = item_to_dict.copy()
        else:
            multicluster_config = Settings.MULTICLUSTER_CONFIG.copy()

        if 'hub_url' not in multicluster_config:
            multicluster_config['hub_url'] = hub_url

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

            token = content['token']
            # Set CORS endpoint on remote cluster
            self._proxy('PUT', url, 'ui-api/multi-cluster/set_cors_endpoint',
                        payload={'url': multicluster_config['hub_url']}, token=token)

            multicluster_config['config'].append({
                'name': name,
                'url': url,
                'token': token
            })

            Settings.MULTICLUSTER_CONFIG = multicluster_config


@UIRouter('/multi-cluster', Scope.CONFIG_OPT)
class MultiClusterUi(RESTController):
    @Endpoint('PUT')
    @UpdatePermission
    def set_cors_endpoint(self, url: str):
        configure_cors(url)
