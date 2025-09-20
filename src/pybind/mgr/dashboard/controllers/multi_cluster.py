# -*- coding: utf-8 -*-

import base64
import ipaddress
import json
import logging
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..settings import Settings
from ..tools import configure_cors
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, UIRouter, UpdatePermission

logger = logging.getLogger('controllers.multi_cluster')


@APIRouter('/multi-cluster', Scope.CONFIG_OPT)
@APIDoc('Multi-cluster Management API', 'Multi-cluster')
# pylint: disable=R0904
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
            cert_file_path = verify
            if verify:
                with tempfile.NamedTemporaryFile(delete=False) as cert_file:
                    cert_file.write(cert.encode('utf-8'))
                    cert_file_path = cert_file.name
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=cert_file_path,
                                        headers=headers)
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
             password=None, hub_url=None, ssl_verify=False, ssl_certificate=None, ttl=None):
        try:
            hub_fsid = mgr.get('config')['fsid']
        except KeyError:
            hub_fsid = ''

        if password:
            payload = {
                'username': username,
                'password': password,
                'ttl': ttl
            }
            cluster_token = self.check_cluster_connection(url, payload, username,
                                                          ssl_verify, ssl_certificate,
                                                          'connect')

            cors_endpoints_string = self.get_cors_endpoints_string(hub_url)

            self._proxy('PUT', url, 'ui-api/multi-cluster/set_cors_endpoint',
                        payload={'url': cors_endpoints_string}, token=cluster_token,
                        verify=ssl_verify, cert=ssl_certificate)

            fsid = self._proxy('GET', url, 'api/health/get_cluster_fsid', token=cluster_token,
                               verify=ssl_verify, cert=ssl_certificate)

            managed_by_clusters_content = self._proxy('GET', url,
                                                      'api/settings/MANAGED_BY_CLUSTERS',
                                                      token=cluster_token,
                                                      verify=ssl_verify, cert=ssl_certificate)

            managed_by_clusters_config = managed_by_clusters_content['value']

            if managed_by_clusters_config is not None:
                managed_by_clusters_config.append({'url': hub_url, 'fsid': hub_fsid})

            self._proxy('PUT', url, 'api/settings/MANAGED_BY_CLUSTERS',
                        payload={'value': managed_by_clusters_config}, token=cluster_token,
                        verify=ssl_verify, cert=ssl_certificate)

            # add prometheus targets
            prometheus_url = self._proxy('GET', url, 'api/multi-cluster/get_prometheus_api_url',
                                         token=cluster_token, verify=ssl_verify,
                                         cert=ssl_certificate)
            logger.info('prometheus_url: %s', prometheus_url)
            prometheus_access_info = self._proxy('GET', url,
                                                 'ui-api/multi-cluster/get_prometheus_access_info',  # noqa E501 #pylint: disable=line-too-long
                                                 token=cluster_token, verify=ssl_verify,
                                                 cert=ssl_certificate)

            _set_prometheus_targets(prometheus_url)

            self.set_multi_cluster_config(fsid, username, url, cluster_alias,
                                          cluster_token, prometheus_url, ssl_verify,
                                          ssl_certificate, prometheus_access_info)
            return True
        return False

    def get_cors_endpoints_string(self, hub_url):
        parsed_url = urlparse(hub_url)
        hostname = parsed_url.hostname
        cors_endpoints_set = set()
        cors_endpoints_set.add(hub_url)

        orch = OrchClient.instance()
        inventory_hosts = [host.to_json() for host in orch.hosts.list()]

        for host in inventory_hosts:
            host_addr = host['addr']
            host_ip_url = hub_url.replace(hostname, host_addr)
            host_hostname_url = hub_url.replace(hostname, host['hostname'])

            cors_endpoints_set.add(host_ip_url)
            cors_endpoints_set.add(host_hostname_url)

        cors_endpoints_string = ", ".join(cors_endpoints_set)
        return cors_endpoints_string

    def check_cluster_connection(self, url, payload, username, ssl_verify, ssl_certificate,
                                 action, cluster_token=None):
        try:
            hub_cluster_version = mgr.version.split('ceph version ')[1]
            multi_cluster_content = self._proxy('GET', url, 'api/multi-cluster/get_config',
                                                verify=ssl_verify, cert=ssl_certificate)
            if 'status' in multi_cluster_content and multi_cluster_content['status'] == '404 Not Found':   # noqa E501 #pylint: disable=line-too-long
                raise DashboardException(msg=f'The ceph cluster you are attempting to connect \
                                         to does not support the multi-cluster feature. \
                                         Please ensure that the cluster you are connecting \
                                         to is upgraded to { hub_cluster_version } to enable the \
                                         multi-cluster functionality.',
                                         code='invalid_version', component='multi-cluster')
            content = self._proxy('POST', url, 'api/auth', payload=payload,
                                  verify=ssl_verify, cert=ssl_certificate)
            if 'token' not in content:
                raise DashboardException(msg=content['detail'], code='invalid_credentials',
                                         component='multi-cluster')

            user_content = self._proxy('GET', url, f'api/user/{username}',
                                       token=content['token'], verify=ssl_verify,
                                       cert=ssl_certificate)

            if 'status' in user_content and user_content['status'] == '403 Forbidden':
                raise DashboardException(msg='User is not an administrator',
                                         code='invalid_permission', component='multi-cluster')
            if 'roles' in user_content and 'administrator' not in user_content['roles']:
                raise DashboardException(msg='User is not an administrator',
                                         code='invalid_permission', component='multi-cluster')

        except Exception as e:
            if '[Errno 111] Connection refused' in str(e):
                raise DashboardException(msg='Connection refused',
                                         code='connection_refused', component='multi-cluster')
            raise DashboardException(msg=str(e), code='connection_failed',
                                     component='multi-cluster')

        cluster_token = content['token']

        if cluster_token:
            self.check_connection_errors(url, cluster_token, ssl_verify, ssl_certificate, action)
        return cluster_token

    def check_connection_errors(self, url, cluster_token, ssl_verify, ssl_certificate, action):
        managed_by_clusters_content = self._proxy('GET', url, 'api/settings/MANAGED_BY_CLUSTERS',
                                                  token=cluster_token, verify=ssl_verify,
                                                  cert=ssl_certificate)

        managed_by_clusters_config = managed_by_clusters_content['value']

        if len(managed_by_clusters_config) > 1 and action == 'connect':
            raise DashboardException(msg='Cluster is already managed by another cluster',
                                     code='cluster_managed_by_another_cluster',
                                     component='multi-cluster')

        self.check_security_config(url, cluster_token, ssl_verify, ssl_certificate)

    def check_security_config(self, url, cluster_token, ssl_verify, ssl_certificate):
        remote_security_cfg = self._proxy('GET', url,
                                          'api/multi-cluster/security_config',
                                          token=cluster_token, verify=ssl_verify,
                                          cert=ssl_certificate)
        local_security_cfg = self._get_security_config()

        if remote_security_cfg and local_security_cfg:
            remote_security_enabled = remote_security_cfg['security_enabled']
            local_security_enabled = local_security_cfg['security_enabled']

            def raise_mismatch_exception(config_name, local_enabled):
                enabled_on = "local" if local_enabled else "remote"
                disabled_on = "remote" if local_enabled else "local"
                raise DashboardException(
                    msg=f'{config_name} is enabled on the {enabled_on} cluster, but not on the {disabled_on} cluster. '  # noqa E501 #pylint: disable=line-too-long
                        f'Both clusters should either have {config_name} enabled or disabled.',
                    code=f'{config_name.lower()}_mismatch', component='multi-cluster'
                )

            if remote_security_enabled != local_security_enabled:
                raise_mismatch_exception('Security', local_security_enabled)

    def set_multi_cluster_config(self, fsid, username, url, cluster_alias, token,
                                 prometheus_url=None, ssl_verify=False, ssl_certificate=None,
                                 prometheus_access_info=None):
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
                    "ssl_certificate": ssl_certificate if ssl_certificate else '',
                    "prometheus_access_info": prometheus_access_info
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
                "ssl_certificate": ssl_certificate if ssl_certificate else '',
                "prometheus_access_info": prometheus_access_info
            }]
        Settings.MULTICLUSTER_CONFIG = json.dumps(multi_cluster_config)

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
    def set_config(self, config: Dict[str, Any]):
        multicluster_config = self.load_multi_cluster_config()
        multicluster_config.update({'current_url': config['url']})
        multicluster_config.update({'current_user': config['user']})
        Settings.MULTICLUSTER_CONFIG = json.dumps(multicluster_config)
        return multicluster_config

    @Endpoint('PUT')
    @UpdatePermission
    # pylint: disable=W0613
    def reconnect_cluster(self, url: str, username=None, password=None,
                          ssl_verify=False, ssl_certificate=None, ttl=None,
                          cluster_token=None):
        multicluster_config = self.load_multi_cluster_config()
        if username and password and cluster_token is None:
            payload = {
                'username': username,
                'password': password,
                'ttl': ttl
            }

            cluster_token = self.check_cluster_connection(url, payload, username,
                                                          ssl_verify, ssl_certificate,
                                                          'reconnect')
        else:
            self.check_connection_errors(url, cluster_token, ssl_verify, ssl_certificate,
                                         'reconnect')

        if cluster_token:
            prometheus_url = self._proxy('GET', url, 'api/multi-cluster/get_prometheus_api_url',
                                         token=cluster_token, verify=ssl_verify,
                                         cert=ssl_certificate)

            prometheus_access_info = self._proxy('GET', url,
                                                 'ui-api/multi-cluster/get_prometheus_access_info',  # noqa E501 #pylint: disable=line-too-long
                                                 token=cluster_token, verify=ssl_verify,
                                                 cert=ssl_certificate)

        if username and cluster_token and prometheus_url and prometheus_access_info:
            if "config" in multicluster_config:
                for _, cluster_details in multicluster_config["config"].items():
                    for cluster in cluster_details:
                        if cluster["url"] == url and cluster["user"] == username:
                            cluster['token'] = cluster_token
                            cluster['ssl_verify'] = ssl_verify
                            cluster['ssl_certificate'] = ssl_certificate
                            cluster['prometheus_access_info'] = prometheus_access_info
                            _remove_prometheus_targets(cluster['prometheus_url'])
                            time.sleep(5)
                            cluster['prometheus_url'] = prometheus_url
                            _set_prometheus_targets(prometheus_url)
            Settings.MULTICLUSTER_CONFIG = json.dumps(multicluster_config)
        return True

    @Endpoint('PUT')
    @UpdatePermission
    # pylint: disable=unused-variable
    def edit_cluster(self, name, url, cluster_alias, username, verify=False, ssl_certificate=None):
        multicluster_config = self.load_multi_cluster_config()
        if "config" in multicluster_config:
            for key, cluster_details in multicluster_config["config"].items():
                for cluster in cluster_details:
                    if cluster["name"] == name and cluster["user"] == username:
                        cluster['url'] = url
                        cluster['cluster_alias'] = cluster_alias
                        cluster['ssl_verify'] = verify
                        cluster['ssl_certificate'] = ssl_certificate if verify else ''
        Settings.MULTICLUSTER_CONFIG = json.dumps(multicluster_config)
        return multicluster_config

    @Endpoint(method='DELETE')
    @DeletePermission
    def delete_cluster(self, cluster_name, cluster_user):
        multicluster_config = self.load_multi_cluster_config()
        try:
            hub_fsid = mgr.get('config')['fsid']
        except KeyError:
            hub_fsid = ''
        if "config" in multicluster_config:
            for key, value in list(multicluster_config['config'].items()):
                if value[0]['name'] == cluster_name and value[0]['user'] == cluster_user:
                    cluster_url = value[0]['url']
                    cluster_token = value[0]['token']
                    cluster_ssl_certificate = value[0]['ssl_certificate']
                    cluster_ssl_verify = value[0]['ssl_verify']
                    cluster_prometheus_url = value[0]['prometheus_url']

                    _remove_prometheus_targets(cluster_prometheus_url)

                    managed_by_clusters_content = self._proxy('GET', cluster_url,
                                                              'api/settings/MANAGED_BY_CLUSTERS',
                                                              token=cluster_token,
                                                              verify=cluster_ssl_verify,
                                                              cert=cluster_ssl_certificate)

                    managed_by_clusters_config = managed_by_clusters_content['value']
                    for cluster in managed_by_clusters_config:
                        if cluster['fsid'] == hub_fsid:
                            managed_by_clusters_config.remove(cluster)

                    self._proxy('PUT', cluster_url, 'api/settings/MANAGED_BY_CLUSTERS',
                                payload={'value': managed_by_clusters_config}, token=cluster_token,
                                verify=cluster_ssl_verify, cert=cluster_ssl_certificate)

                    del multicluster_config['config'][key]
                    break

        Settings.MULTICLUSTER_CONFIG = json.dumps(multicluster_config)
        return multicluster_config

    @Endpoint()
    @ReadPermission
    def get_config(self):
        multi_cluster_config = self.load_multi_cluster_config()
        return multi_cluster_config

    def is_token_expired(self, jwt_token):
        split_message = jwt_token.split(".")
        base64_message = split_message[1]
        decoded_token = json.loads(base64.urlsafe_b64decode(base64_message + "===="))
        expiration_time = decoded_token['exp']
        current_time = time.time()
        return expiration_time < current_time

    def get_time_left(self, jwt_token):
        split_message = jwt_token.split(".")
        base64_message = split_message[1]
        decoded_token = json.loads(base64.urlsafe_b64decode(base64_message + "===="))
        expiration_time = decoded_token['exp']
        current_time = time.time()
        time_left = expiration_time - current_time
        return max(0, time_left)

    def check_token_status_expiration(self, token):
        if self.is_token_expired(token):
            return 1
        return 0

    def check_token_status_array(self):
        token_status_map = {}
        multi_cluster_config = self.load_multi_cluster_config()

        if 'config' in multi_cluster_config:
            for _, config in multi_cluster_config['config'].items():
                cluster_name = config[0]['name']
                token = config[0]['token']
                user = config[0]['user']
                status = self.check_token_status_expiration(token)
                time_left = self.get_time_left(token)
                token_status_map[cluster_name] = {
                    'status': status,
                    'user': user,
                    'time_left': time_left
                }

        return token_status_map

    @Endpoint()
    @ReadPermission
    def check_token_status(self):
        return self.check_token_status_array()

    @Endpoint()
    @ReadPermission
    def security_config(self):
        return self._get_security_config()

    def _get_security_config(self):
        orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
        if orch_backend == 'cephadm':
            cmd = {
                'prefix': 'orch get-security-config',
            }
            ret_status, out, _ = mgr.mon_command(cmd)
            if ret_status == 0 and out is not None:
                security_info = json.loads(out)
                security_enabled = security_info['security_enabled']
                mgmt_gw_enabled = security_info['mgmt_gw_enabled']
                return {
                    'security_enabled': bool(security_enabled),
                    'mgmt_gw_enabled': bool(mgmt_gw_enabled)
                }
        return None

    @Endpoint()
    @ReadPermission
    def get_prometheus_api_url(self):
        security_content = self._get_security_config()
        mgmt_gw_enabled = security_content['mgmt_gw_enabled']
        prometheus_url = Settings.PROMETHEUS_API_HOST

        if prometheus_url is not None:
            if '.ceph-dashboard' in prometheus_url:
                prometheus_url = prometheus_url.replace('.ceph-dashboard', '')
            parsed_url = urlparse(prometheus_url)
            scheme = parsed_url.scheme
            hostname = parsed_url.hostname
            try:
                # Check if the hostname is already an IP address
                ipaddress.ip_address(hostname)
                valid_ip_url = True
            except ValueError:
                valid_ip_url = False

            orch = OrchClient.instance()
            inventory_hosts = (
                [host.to_json() for host in orch.hosts.list()]
                if not valid_ip_url
                else []
            )

            def find_node_ip():
                for host in inventory_hosts:
                    if host['hostname'] == hostname or hostname in host['hostname']:
                        return host['addr']
                return None

            node_ip = find_node_ip() if not valid_ip_url else None
            prometheus_url = prometheus_url.replace(hostname, node_ip) if node_ip else prometheus_url  # noqa E501 #pylint: disable=line-too-long
            if mgmt_gw_enabled:
                prometheus_url = f"{scheme}://{node_ip if node_ip else hostname}"
        return prometheus_url

    def find_prometheus_credentials(self, multicluster_config: Dict[str, Any],
                                    target: str) -> Optional[Dict[str, Any]]:
        for _, clusters in multicluster_config['config'].items():
            for cluster in clusters:
                prometheus_url = cluster.get('prometheus_url')
                if prometheus_url:
                    endpoint = (
                        prometheus_url.replace("https://", "").replace("http://", "")
                    )  # since target URLs are without scheme

                    if endpoint == target:
                        return cluster.get('prometheus_access_info')
        return None

    def get_cluster_credentials(self, targets: List[str]) -> Dict[str, Any]:
        clusters_credentials: Dict[str, Dict[str, Any]] = {}
        multi_cluster_config = self.load_multi_cluster_config()

        # Return early if no multi_cluster_config is loaded
        if not multi_cluster_config:
            return clusters_credentials

        try:
            for target in targets:
                credentials = self.find_prometheus_credentials(multi_cluster_config, target)
                if credentials:
                    clusters_credentials[target] = credentials
                    clusters_credentials[target]['cert_file_name'] = ''
                else:
                    logger.error('Credentials not found for target: %s', target)
        except json.JSONDecodeError as e:
            logger.error('Invalid JSON format for multi-cluster config: %s', e)

        return clusters_credentials

    def get_cluster_credentials_files(self, targets: List[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:  # noqa E501 #pylint: disable=line-too-long
        cluster_credentials_files: Dict[str, Any] = {'files': {}}
        clusters_credentials = self.get_cluster_credentials(targets=targets)
        for i, (_, credentials) in enumerate(clusters_credentials.items()):
            cluster_credentials_files['files'][f'prometheus_{i+1}_cert.crt'] = credentials['certificate']  # noqa E501 #pylint: disable=line-too-long
            credentials['cert_file_name'] = f'prometheus_{i+1}_cert.crt'
        return cluster_credentials_files, clusters_credentials


@UIRouter('/multi-cluster', Scope.CONFIG_OPT)
class MultiClusterUi(RESTController):
    @Endpoint('PUT')
    @UpdatePermission
    def set_cors_endpoint(self, url: str):
        configure_cors(url)

    @Endpoint('GET')
    @ReadPermission
    def get_prometheus_access_info(self):
        orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
        if orch_backend == 'cephadm':
            cmd = {
                'prefix': 'orch prometheus get-credentials',
            }
            ret_status, out, _ = mgr.mon_command(cmd)
            if ret_status == 0 and out is not None:
                prom_access_info = json.loads(out)
                user = prom_access_info.get('user', '')
                password = prom_access_info.get('password', '')
                certificate = prom_access_info.get('certificate', '')
            return {
                'user': user,
                'password': password,
                'certificate': certificate
            }
        return None


def _set_prometheus_targets(prometheus_url: str):
    orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
    try:
        if orch_backend == 'cephadm':
            cmd = {
                'prefix': 'orch prometheus set-target',
                'url': prometheus_url
            }
            mgr.mon_command(cmd)
    except KeyError:
        logger.exception('Failed to set prometheus targets')


def _remove_prometheus_targets(prometheus_url: str):
    orch_backend = mgr.get_module_option_ex('orchestrator', 'orchestrator')
    try:
        if orch_backend == 'cephadm':
            cmd = {
                'prefix': 'orch prometheus remove-target',
                'url': prometheus_url.replace('http://', '').replace('https://', '')
            }
            mgr.mon_command(cmd)
    except KeyError:
        logger.exception('Failed to remove prometheus targets')
