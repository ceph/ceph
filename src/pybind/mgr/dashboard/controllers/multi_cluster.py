# -*- coding: utf-8 -*-

import base64
import json
import time

import requests

from .. import mgr
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
               token=None, cert=None):
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
                                        json=payload, verify=verify, cert=cert, headers=headers)
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
    def auth(self, url: str, cluster_alias: str, username: str,
             password=None, token=None, hub_url=None, cluster_fsid=None,
             prometheus_api_url=None, ssl_verify=False, ssl_certificate=None, ttl =None):
        if password:
            payload = {
                'username': username,
                'password': password,
                'ttl': ttl
            }
            content = self._proxy('POST', url, 'api/auth', payload=payload)
            if 'token' not in content:
                raise DashboardException(
                    "Could not authenticate to remote cluster",
                    http_status_code=400,
                    component='dashboard')

            cluster_token = content['token']

            self._proxy('PUT', url, 'ui-api/multi-cluster/set_cors_endpoint',
                        payload={'url': hub_url}, token=cluster_token, verify=ssl_verify,
                        cert=ssl_certificate)
            fsid = self._proxy('GET', url, 'api/health/get_cluster_fsid', token=cluster_token)

            # add prometheus targets
            prometheus_url = self._proxy('GET', url, 'api/settings/PROMETHEUS_API_HOST',
                                         token=cluster_token)
            _set_prometheus_targets(prometheus_url['value'])

            self.set_multi_cluster_config(fsid, username, url, cluster_alias,
                                          cluster_token, prometheus_url['value'],
                                          ssl_verify, ssl_certificate)
            return

        if token and cluster_fsid and prometheus_api_url:
            _set_prometheus_targets(prometheus_api_url)
            self.set_multi_cluster_config(cluster_fsid, username, url,
                                          cluster_alias, token, prometheus_api_url,
                                          ssl_verify, ssl_certificate)

    def set_multi_cluster_config(self, fsid, username, url, cluster_alias, token,
                                 prometheus_url=None, ssl_verify=False, ssl_certificate=None):
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
                    "prometheus_url": prometheus_url if prometheus_url else '',
                    "ssl_verify": ssl_verify,
                    "ssl_certificate": ssl_certificate if ssl_certificate else ''
                })
        else:
            multi_cluster_config['current_user'] = username
            multi_cluster_config['config'][fsid] = [{
                "name": fsid,
                "url": url,
                "cluster_alias": cluster_alias,
                "user": username,
                "token": token,
                "prometheus_url": prometheus_url if prometheus_url else '',
                "ssl_verify": ssl_verify,
                "ssl_certificate": ssl_certificate if ssl_certificate else ''
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
    @UpdatePermission
    # pylint: disable=unused-variable
    def reconnect_cluster(self, url: str, username=None, password=None, token=None,
                          ssl_verify=False, ssl_certificate=None):
        multicluster_config = self.load_multi_cluster_config()
        if username and password:
            payload = {
                'username': username,
                'password': password
            }
            content = self._proxy('POST', url, 'api/auth', payload=payload,
                                  verify=ssl_verify, cert=ssl_certificate)
            if 'token' not in content:
                raise DashboardException(
                    "Could not authenticate to remote cluster",
                    http_status_code=400,
                    component='dashboard')

            token = content['token']

        if username and token:
            if "config" in multicluster_config:
                for _, cluster_details in multicluster_config["config"].items():
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
            for key, value in list(multicluster_config['config'].items()):
                if value[0]['name'] == cluster_name and value[0]['user'] == cluster_user:

                    orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
                    try:
                        if orch_backend == 'cephadm':
                            cmd = {
                                'prefix': 'orch prometheus remove-target',
                                'url': value[0]['prometheus_url'].replace('http://', '').replace('https://', '')  # noqa E501 #pylint: disable=line-too-long
                            }
                            mgr.mon_command(cmd)
                    except KeyError:
                        pass

                    del multicluster_config['config'][key]
                    break

        Settings.MULTICLUSTER_CONFIG = multicluster_config
        return Settings.MULTICLUSTER_CONFIG

    @Endpoint('POST')
    @CreatePermission
    # pylint: disable=R0911
    def verify_connection(self, url=None, username=None, password=None, token=None,
                          ssl_verify=False, ssl_certificate=None):
        if token:
            try:
                payload = {
                    'token': token
                }
                content = self._proxy('POST', url, 'api/auth/check', payload=payload,
                                      verify=ssl_verify, cert=ssl_certificate)
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
                content = self._proxy('POST', url, 'api/auth', payload=payload,
                                      verify=ssl_verify, cert=ssl_certificate)
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


def _set_prometheus_targets(prometheus_url: str):
    orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
    if orch_backend == 'cephadm':
        cmd = {
            'prefix': 'orch prometheus set-target',
            'url': prometheus_url.replace('http://', '').replace('https://', '')
        }
        mgr.mon_command(cmd)
