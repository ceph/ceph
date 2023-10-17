from . import APIDoc, APIRouter, CreatePermission, UpdatePermission, ReadPermission, Endpoint, EndpointDoc, RESTController
import requests
from ..exceptions import DashboardException

from ..settings import Settings
from ..security import Scope

import logging
logger = logging.getLogger('routes')


@APIRouter('/multicluster', Scope.CONFIG_OPT)
@APIDoc('Multi Cluster Route Management API', 'Multi Cluster Route')
class MultiClusterRoute(RESTController):
    def _proxy(self, method, path, api_name, params=None, payload=None, verify=False):

        config2 = Settings.REMOTE_CLUSTER_URLS
        matching_dict = None
        base_url = ''
        api_token = ''
        config = Settings.MULTICLUSTER_CONFIG
        if config2 is not None and config is not None:
            matching_dict = next((item for item in config2 if item.get('remoteClusterUrl') == config['current_url']), None)

        if matching_dict:
            api_token = matching_dict.get('apiToken')
            base_url = matching_dict.get('remoteClusterUrl')

        logger.info('base_url is %s', base_url)
        logger.info('token is %s', api_token)
        
        try:
            headers = {
                'Accept': 'application/vnd.ceph.api.v1.0+json',
                'Authorization': 'Bearer ' + api_token
            }
            response = requests.request(method, base_url + path, params=params,
                                        json=payload, verify=verify, headers=headers)
            logger.error("the response is %s", response)
        except Exception as e:
            raise DashboardException(
                "Could not reach {}'s API on {}".format(api_name, base_url),
                http_status_code=404,
                component='dashboard')
        import json
        try:
            content = json.loads(response.content, strict=False)
        except json.JSONDecodeError as e:
            raise DashboardException(
                "Error parsing Dashboard API response: {}".format(e.msg),
                component='dashboard')
        return content



    @Endpoint()
    @ReadPermission
    @EndpointDoc("Which route you want to go")
    def route(self, path: str = '', method: str = 'GET', params = None, payload = None):
        logger.info('params is %s', params)
        response = self._proxy(method, path, 'dashboard', params, payload=payload)
        return response

    @Endpoint('PUT')
    @CreatePermission
    def set_config(self, config: str):
        Settings.MULTICLUSTER_CONFIG['current_url'] = config
    
    @Endpoint()
    @ReadPermission
    def get_config(self):
        return Settings.MULTICLUSTER_CONFIG
