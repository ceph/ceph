# -*- coding: utf-8 -*-

import json

import requests

from ..exceptions import DashboardException
from ..security import Scope
from ..settings import Settings
from ..tools import configure_cors
from . import APIDoc, APIRouter, CreatePermission, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, UIRouter, UpdatePermission


@APIRouter('/multi-cluster', Scope.CONFIG_OPT)
@APIDoc('Multi-cluster Management API', 'Multi-cluster')
class MultiCluster(RESTController):
    def _proxy(self, method, base_url, path, params=None, payload=None, verify=False,
               token=None):
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
    def auth(self, url: str, cluster_alias: str, username=None,
             password=None, token=None, hub_url=None):

        multi_cluster_config = self.load_multi_cluster_config()

        if not url.endswith('/'):
            url = url + '/'

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

        if token:
            self._proxy('PUT', url, 'ui-api/multi-cluster/set_cors_endpoint',
                        payload={'url': hub_url}, token=token)
            fsid = self._proxy('GET', url, 'api/health/get_cluster_fsid', token=token)
            content = self._proxy('POST', url, 'api/auth/check', payload={'token': token},
                                  token=token)
            if 'username' in content:
                username = content['username']

            if 'config' not in multi_cluster_config:
                multi_cluster_config['config'] = {}

            if fsid in multi_cluster_config['config']:
                existing_entries = multi_cluster_config['config'][fsid]
                if not any(entry['user'] == username for entry in existing_entries):
                    existing_entries.append({
                        "name": fsid,
                        "url": url,
                        "cluster_alias": cluster_alias,
                        "user": username,
                        "token": token,
                    })
            else:
                multi_cluster_config['current_user'] = username
                multi_cluster_config['config'][fsid] = [{
                    "name": fsid,
                    "url": url,
                    "cluster_alias": cluster_alias,
                    "user": username,
                    "token": token,
                }]

            Settings.MULTICLUSTER_CONFIG = multi_cluster_config

    def load_multi_cluster_config(self):
        if isinstance(Settings.MULTICLUSTER_CONFIG, str):
            try:
                itemw_to_dict = json.loads(Settings.MULTICLUSTER_CONFIG)
            except json.JSONDecodeError:
                itemw_to_dict = {}
            multi_cluster_config = itemw_to_dict.copy()
        else:
            multi_cluster_config = Settings.MULTICLUSTER_CONFIG.copy()

        return multi_cluster_config

    @Endpoint('PUT')
    @UpdatePermission
    def set_config(self, config: object):
        multicluster_config = self.load_multi_cluster_config()
        multicluster_config.update({'current_url': config['url']})
        multicluster_config.update({'current_user': config['user']})
        Settings.MULTICLUSTER_CONFIG = multicluster_config
        return Settings.MULTICLUSTER_CONFIG

    @Endpoint('POST')
    @CreatePermission
    # pylint: disable=R0911
    def verify_connection(self, url: str, username=None, password=None, token=None):
        if not url.endswith('/'):
            url = url + '/'

        if token:
            try:
                payload = {
                    'token': token
                }
                content = self._proxy('POST', url, 'api/auth/check', payload=payload)
                if 'permissions' not in content:
                    return content['detail']
                user_content = self._proxy('GET', url, f'api/user/{username}',
                                           token=content['token'])
                if 'status' in user_content and user_content['status'] == '403 Forbidden':
                    return 'User is not an administrator'
            except Exception as e:  # pylint: disable=broad-except
                if '[Errno 111] Connection refused' in str(e):
                    return 'Connection refused'
                return 'Connection failed'

        if username and password:
            try:
                payload = {
                    'username': username,
                    'password': password
                }
                content = self._proxy('POST', url, 'api/auth', payload=payload)
                if 'token' not in content:
                    return content['detail']
                user_content = self._proxy('GET', url, f'api/user/{username}',
                                           token=content['token'])
                if 'status' in user_content and user_content['status'] == '403 Forbidden':
                    return 'User is not an administrator'
            except Exception as e:  # pylint: disable=broad-except
                if '[Errno 111] Connection refused' in str(e):
                    return 'Connection refused'
                return 'Connection failed'
        return 'Connection successful'

    @Endpoint()
    @ReadPermission
    def get_config(self):
        return Settings.MULTICLUSTER_CONFIG


@UIRouter('/multi-cluster', Scope.CONFIG_OPT)
class MultiClusterUi(RESTController):
    @Endpoint('PUT')
    @UpdatePermission
    def set_cors_endpoint(self, url: str):
        configure_cors(url)
