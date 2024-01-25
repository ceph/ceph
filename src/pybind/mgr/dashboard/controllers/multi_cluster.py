# -*- coding: utf-8 -*-

import base64
import json
import time

import requests

from ..exceptions import DashboardException
from ..security import Scope
from ..settings import Settings
from ..tools import configure_cors
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, UIRouter, UpdatePermission


@APIRouter('/multi-cluster', Scope.CONFIG_OPT)
@APIDoc('Multi-cluster Management API', 'Multi-cluster')
class MultiCluster(RESTController):
    def _proxy(self, method, base_url, path, params=None, payload=None, verify=False,
               token=None):
        if not base_url.endswith('/'):
            base_url = base_url + '/'
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
             password=None, token=None, hub_url=None, cluster_fsid=None):

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

            cluster_token = content['token']

            self._proxy('PUT', url, 'ui-api/multi-cluster/set_cors_endpoint',
                        payload={'url': hub_url}, token=cluster_token)

            fsid = self._proxy('GET', url, 'api/health/get_cluster_fsid', token=cluster_token)

            self.set_multi_cluster_config(fsid, username, url, cluster_alias, cluster_token)

        if token and cluster_fsid and username:
            self.set_multi_cluster_config(cluster_fsid, username, url, cluster_alias, token)

    def set_multi_cluster_config(self, fsid, username, url, cluster_alias, token):
        multi_cluster_config = self.load_multi_cluster_config()
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

    @Endpoint('PUT')
    @CreatePermission
    # pylint: disable=unused-variable
    def reconnect_cluster(self, url: str, username=None, password=None, token=None):
        multicluster_config = self.load_multi_cluster_config()
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

        if username and token:
            if "config" in multicluster_config:
                for key, cluster_details in multicluster_config["config"].items():
                    for cluster in cluster_details:
                        if cluster["url"] == url and cluster["user"] == username:
                            cluster['token'] = token
            Settings.MULTICLUSTER_CONFIG = multicluster_config
        return Settings.MULTICLUSTER_CONFIG

    @Endpoint('PUT')
    @UpdatePermission
    # pylint: disable=unused-variable
    def edit_cluster(self, url, cluster_alias, username):
        multicluster_config = self.load_multi_cluster_config()
        if "config" in multicluster_config:
            for key, cluster_details in multicluster_config["config"].items():
                for cluster in cluster_details:
                    if cluster["url"] == url and cluster["user"] == username:
                        cluster['cluster_alias'] = cluster_alias
        Settings.MULTICLUSTER_CONFIG = multicluster_config
        return Settings.MULTICLUSTER_CONFIG

    @Endpoint(method='DELETE')
    @DeletePermission
    def delete_cluster(self, cluster_name, cluster_user):
        multicluster_config = self.load_multi_cluster_config()
        if "config" in multicluster_config:
            keys_to_remove = []
            for key, cluster_details in multicluster_config["config"].items():
                cluster_details_copy = list(cluster_details)
                for cluster in cluster_details_copy:
                    if cluster["name"] == cluster_name and cluster["user"] == cluster_user:
                        cluster_details.remove(cluster)
                        if not cluster_details:
                            keys_to_remove.append(key)

            for key in keys_to_remove:
                del multicluster_config["config"][key]

        Settings.MULTICLUSTER_CONFIG = multicluster_config
        return Settings.MULTICLUSTER_CONFIG

    @Endpoint()
    @ReadPermission
    # pylint: disable=R0911
    def verify_connection(self, url=None, username=None, password=None, token=None):
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

    def is_token_expired(self, jwt_token):
        split_message = jwt_token.split(".")
        base64_message = split_message[1]
        decoded_token = json.loads(base64.urlsafe_b64decode(base64_message + "===="))
        expiration_time = decoded_token['exp']
        current_time = time.time()
        return expiration_time < current_time

    def check_token_status_expiration(self, token):
        if self.is_token_expired(token):
            return 1
        return 0

    def check_token_status_array(self, clusters_token_array):
        token_status_map = {}

        for item in clusters_token_array:
            cluster_name = item['name']
            token = item['token']
            user = item['user']
            status = self.check_token_status_expiration(token)
            token_status_map[cluster_name] = {'status': status, 'user': user}

        return token_status_map

    @Endpoint()
    @ReadPermission
    def check_token_status(self, clustersTokenMap=None):
        clusters_token_map = json.loads(clustersTokenMap)
        return self.check_token_status_array(clusters_token_map)


@UIRouter('/multi-cluster', Scope.CONFIG_OPT)
class MultiClusterUi(RESTController):
    @Endpoint('PUT')
    @UpdatePermission
    def set_cors_endpoint(self, url: str):
        configure_cors(url)
