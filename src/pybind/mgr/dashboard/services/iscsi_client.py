# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

import json
import logging

from requests.auth import HTTPBasicAuth

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from ..rest_client import RestClient
from ..settings import Settings
from .iscsi_config import IscsiGatewaysConfig

logger = logging.getLogger('iscsi_client')


class IscsiClient(RestClient):
    _CLIENT_NAME = 'iscsi'
    _instances = {}  # type: dict

    service_url = None
    gateway_name = None

    @classmethod
    def instance(cls, gateway_name=None, service_url=None):
        if not service_url:
            if not gateway_name:
                gateway_name = list(IscsiGatewaysConfig.get_gateways_config()['gateways'].keys())[0]
            gateways_config = IscsiGatewaysConfig.get_gateway_config(gateway_name)
            service_url = gateways_config['service_url']

        instance = cls._instances.get(gateway_name)
        if not instance or service_url != instance.service_url or \
                instance.session.verify != Settings.ISCSI_API_SSL_VERIFICATION:
            url = urlparse(service_url)
            ssl = url.scheme == 'https'
            host = url.hostname
            port = url.port
            username = url.username
            password = url.password
            if not port:
                port = 443 if ssl else 80

            auth = HTTPBasicAuth(username, password)
            instance = IscsiClient(host, port, IscsiClient._CLIENT_NAME, ssl,
                                   auth, Settings.ISCSI_API_SSL_VERIFICATION)
            instance.service_url = service_url
            instance.gateway_name = gateway_name
            if gateway_name:
                cls._instances[gateway_name] = instance

        return instance

    @RestClient.api_get('/api/_ping')
    def ping(self, request=None):
        return request()

    @RestClient.api_get('/api/settings')
    def get_settings(self, request=None):
        return request()

    @RestClient.api_get('/api/sysinfo/ip_addresses')
    def get_ip_addresses(self, request=None):
        return request()

    @RestClient.api_get('/api/sysinfo/hostname')
    def get_hostname(self, request=None):
        return request()

    @RestClient.api_get('/api/config')
    def get_config(self, request=None):
        return request({
            'decrypt_passwords': True
        })

    @RestClient.api_put('/api/target/{target_iqn}')
    def create_target(self, target_iqn, target_controls, request=None):
        logger.debug("[%s] Creating target: %s", self.gateway_name, target_iqn)
        return request({
            'controls': json.dumps(target_controls)
        })

    @RestClient.api_delete('/api/target/{target_iqn}')
    def delete_target(self, target_iqn, request=None):
        logger.debug("[%s] Deleting target: %s", self.gateway_name, target_iqn)
        return request()

    @RestClient.api_put('/api/target/{target_iqn}')
    def reconfigure_target(self, target_iqn, target_controls, request=None):
        logger.debug("[%s] Reconfiguring target: %s", self.gateway_name, target_iqn)
        return request({
            'mode': 'reconfigure',
            'controls': json.dumps(target_controls)
        })

    @RestClient.api_put('/api/gateway/{target_iqn}/{gateway_name}')
    def create_gateway(self, target_iqn, gateway_name, ip_address, request=None):
        logger.debug("[%s] Creating gateway: %s/%s", self.gateway_name, target_iqn,
                     gateway_name)
        return request({
            'ip_address': ','.join(ip_address),
            'skipchecks': 'true'
        })

    @RestClient.api_get('/api/gatewayinfo')
    def get_gatewayinfo(self, request=None):
        return request()

    @RestClient.api_delete('/api/gateway/{target_iqn}/{gateway_name}')
    def delete_gateway(self, target_iqn, gateway_name, request=None):
        logger.debug("Deleting gateway: %s/%s", target_iqn, gateway_name)
        return request()

    @RestClient.api_put('/api/disk/{pool}/{image}')
    def create_disk(self, pool, image, backstore, wwn, request=None):
        logger.debug("[%s] Creating disk: %s/%s", self.gateway_name, pool, image)
        return request({
            'mode': 'create',
            'backstore': backstore,
            'wwn': wwn
        })

    @RestClient.api_delete('/api/disk/{pool}/{image}')
    def delete_disk(self, pool, image, request=None):
        logger.debug("[%s] Deleting disk: %s/%s", self.gateway_name, pool, image)
        return request({
            'preserve_image': 'true'
        })

    @RestClient.api_put('/api/disk/{pool}/{image}')
    def reconfigure_disk(self, pool, image, controls, request=None):
        logger.debug("[%s] Reconfiguring disk: %s/%s", self.gateway_name, pool, image)
        return request({
            'controls': json.dumps(controls),
            'mode': 'reconfigure'
        })

    @RestClient.api_put('/api/targetlun/{target_iqn}')
    def create_target_lun(self, target_iqn, image_id, lun, request=None):
        logger.debug("[%s] Creating target lun: %s/%s", self.gateway_name, target_iqn,
                     image_id)
        return request({
            'disk': image_id,
            'lun_id': lun
        })

    @RestClient.api_delete('/api/targetlun/{target_iqn}')
    def delete_target_lun(self, target_iqn, image_id, request=None):
        logger.debug("[%s] Deleting target lun: %s/%s", self.gateway_name, target_iqn,
                     image_id)
        return request({
            'disk': image_id
        })

    @RestClient.api_put('/api/client/{target_iqn}/{client_iqn}')
    def create_client(self, target_iqn, client_iqn, request=None):
        logger.debug("[%s] Creating client: %s/%s", self.gateway_name, target_iqn, client_iqn)
        return request()

    @RestClient.api_delete('/api/client/{target_iqn}/{client_iqn}')
    def delete_client(self, target_iqn, client_iqn, request=None):
        logger.debug("[%s] Deleting client: %s/%s", self.gateway_name, target_iqn, client_iqn)
        return request()

    @RestClient.api_put('/api/clientlun/{target_iqn}/{client_iqn}')
    def create_client_lun(self, target_iqn, client_iqn, image_id, request=None):
        logger.debug("[%s] Creating client lun: %s/%s", self.gateway_name, target_iqn,
                     client_iqn)
        return request({
            'disk': image_id
        })

    @RestClient.api_delete('/api/clientlun/{target_iqn}/{client_iqn}')
    def delete_client_lun(self, target_iqn, client_iqn, image_id, request=None):
        logger.debug("iSCSI[%s] Deleting client lun: %s/%s", self.gateway_name, target_iqn,
                     client_iqn)
        return request({
            'disk': image_id
        })

    @RestClient.api_put('/api/clientauth/{target_iqn}/{client_iqn}')
    def create_client_auth(self, target_iqn, client_iqn, username, password, mutual_username,
                           mutual_password, request=None):
        logger.debug("[%s] Creating client auth: %s/%s/%s/%s/%s/%s",
                     self.gateway_name, target_iqn, client_iqn, username, password, mutual_username,
                     mutual_password)
        return request({
            'username': username,
            'password': password,
            'mutual_username': mutual_username,
            'mutual_password': mutual_password
        })

    @RestClient.api_put('/api/hostgroup/{target_iqn}/{group_name}')
    def create_group(self, target_iqn, group_name, members, image_ids, request=None):
        logger.debug("[%s] Creating group: %s/%s", self.gateway_name, target_iqn, group_name)
        return request({
            'members': ','.join(members),
            'disks': ','.join(image_ids)
        })

    @RestClient.api_put('/api/hostgroup/{target_iqn}/{group_name}')
    def update_group(self, target_iqn, group_name, members, image_ids, request=None):
        logger.debug("iSCSI[%s] Updating group: %s/%s", self.gateway_name, target_iqn, group_name)
        return request({
            'action': 'remove',
            'members': ','.join(members),
            'disks': ','.join(image_ids)
        })

    @RestClient.api_delete('/api/hostgroup/{target_iqn}/{group_name}')
    def delete_group(self, target_iqn, group_name, request=None):
        logger.debug("[%s] Deleting group: %s/%s", self.gateway_name, target_iqn, group_name)
        return request()

    @RestClient.api_put('/api/discoveryauth')
    def update_discoveryauth(self, user, password, mutual_user, mutual_password, request=None):
        logger.debug("[%s] Updating discoveryauth: %s/%s/%s/%s", self.gateway_name, user,
                     password, mutual_user, mutual_password)
        return request({
            'username': user,
            'password': password,
            'mutual_username': mutual_user,
            'mutual_password': mutual_password
        })

    @RestClient.api_put('/api/targetauth/{target_iqn}')
    def update_targetacl(self, target_iqn, action, request=None):
        logger.debug("[%s] Updating targetacl: %s/%s", self.gateway_name, target_iqn, action)
        return request({
            'action': action
        })

    @RestClient.api_put('/api/targetauth/{target_iqn}')
    def update_targetauth(self, target_iqn, user, password, mutual_user, mutual_password,
                          request=None):
        logger.debug("[%s] Updating targetauth: %s/%s/%s/%s/%s", self.gateway_name,
                     target_iqn, user, password, mutual_user, mutual_password)
        return request({
            'username': user,
            'password': password,
            'mutual_username': mutual_user,
            'mutual_password': mutual_password
        })

    @RestClient.api_get('/api/targetinfo/{target_iqn}')
    def get_targetinfo(self, target_iqn, request=None):
        # pylint: disable=unused-argument
        return request()

    @RestClient.api_get('/api/clientinfo/{target_iqn}/{client_iqn}')
    def get_clientinfo(self, target_iqn, client_iqn, request=None):
        # pylint: disable=unused-argument
        return request()
