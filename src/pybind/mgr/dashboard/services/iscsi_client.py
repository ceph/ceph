# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods
from __future__ import absolute_import

import json

from requests.auth import HTTPBasicAuth

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from .iscsi_cli import IscsiGatewaysConfig
from .. import logger
from ..rest_client import RestClient


class IscsiClient(RestClient):
    _CLIENT_NAME = 'iscsi'
    _instances = {}

    service_url = None

    @classmethod
    def instance(cls, gateway_name=None):
        if not gateway_name:
            gateway_name = list(IscsiGatewaysConfig.get_gateways_config()['gateways'].keys())[0]
        gateways_config = IscsiGatewaysConfig.get_gateway_config(gateway_name)
        service_url = gateways_config['service_url']

        instance = cls._instances.get(gateway_name)
        if not instance or service_url != instance.service_url:
            url = urlparse(service_url)
            ssl = url.scheme == 'https'
            host = url.hostname
            port = url.port
            username = url.username
            password = url.password
            if not port:
                port = 443 if ssl else 80
            auth = HTTPBasicAuth(username, password)
            instance = IscsiClient(host, port, IscsiClient._CLIENT_NAME, ssl, auth)
            instance.service_url = service_url
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

    @RestClient.api_get('/api/config')
    def get_config(self, request=None):
        return request()

    @RestClient.api_put('/api/target/{target_iqn}')
    def create_target(self, target_iqn, target_controls, request=None):
        logger.debug("iSCSI: Creating target: %s", target_iqn)
        return request({
            'controls': json.dumps(target_controls)
        })

    @RestClient.api_delete('/api/target/{target_iqn}')
    def delete_target(self, target_iqn, request=None):
        logger.debug("iSCSI: Deleting target: %s", target_iqn)
        return request()

    @RestClient.api_put('/api/target/{target_iqn}')
    def reconfigure_target(self, target_iqn, target_controls, request=None):
        logger.debug("iSCSI: Reconfiguring target: %s", target_iqn)
        return request({
            'mode': 'reconfigure',
            'controls': json.dumps(target_controls)
        })

    @RestClient.api_put('/api/gateway/{target_iqn}/{gateway_name}')
    def create_gateway(self, target_iqn, gateway_name, ip_address, request=None):
        logger.debug("iSCSI: Creating gateway: %s/%s", target_iqn, gateway_name)
        return request({
            'ip_address': ','.join(ip_address),
            'skipchecks': 'true'
        })

    @RestClient.api_put('/api/disk/{image_id}')
    def create_disk(self, image_id, request=None):
        logger.debug("iSCSI: Creating disk: %s", image_id)
        return request({
            'mode': 'create'
        })

    @RestClient.api_delete('/api/disk/{image_id}')
    def delete_disk(self, image_id, request=None):
        logger.debug("iSCSI: Deleting disk: %s", image_id)
        return request({
            'preserve_image': 'true'
        })

    @RestClient.api_put('/api/disk/{image_id}')
    def reconfigure_disk(self, image_id, controls, request=None):
        logger.debug("iSCSI: Reconfiguring disk: %s", image_id)
        return request({
            'controls': json.dumps(controls),
            'mode': 'reconfigure'
        })

    @RestClient.api_put('/api/targetlun/{target_iqn}')
    def create_target_lun(self, target_iqn, image_id, request=None):
        logger.debug("iSCSI: Creating target lun: %s/%s", target_iqn, image_id)
        return request({
            'disk': image_id
        })

    @RestClient.api_delete('/api/targetlun/{target_iqn}')
    def delete_target_lun(self, target_iqn, image_id, request=None):
        logger.debug("iSCSI: Deleting target lun: %s/%s", target_iqn, image_id)
        return request({
            'disk': image_id
        })

    @RestClient.api_put('/api/client/{target_iqn}/{client_iqn}')
    def create_client(self, target_iqn, client_iqn, request=None):
        logger.debug("iSCSI: Creating client: %s/%s", target_iqn, client_iqn)
        return request()

    @RestClient.api_delete('/api/client/{target_iqn}/{client_iqn}')
    def delete_client(self, target_iqn, client_iqn, request=None):
        logger.debug("iSCSI: Deleting client: %s/%s", target_iqn, client_iqn)
        return request()

    @RestClient.api_put('/api/clientlun/{target_iqn}/{client_iqn}')
    def create_client_lun(self, target_iqn, client_iqn, image_id, request=None):
        logger.debug("iSCSI: Creating client lun: %s/%s", target_iqn, client_iqn)
        return request({
            'disk': image_id
        })

    @RestClient.api_put('/api/clientauth/{target_iqn}/{client_iqn}')
    def create_client_auth(self, target_iqn, client_iqn, chap, chap_mutual, request=None):
        logger.debug("iSCSI: Creating client auth: %s/%s/%s/%s",
                     target_iqn, client_iqn, chap, chap_mutual)
        return request({
            'chap': chap,
            'chap_mutual': chap_mutual
        })

    @RestClient.api_put('/api/hostgroup/{target_iqn}/{group_name}')
    def create_group(self, target_iqn, group_name, members, image_ids, request=None):
        logger.debug("iSCSI: Creating group: %s/%s", target_iqn, group_name)
        return request({
            'members': ','.join(members),
            'disks': ','.join(image_ids)
        })

    @RestClient.api_delete('/api/hostgroup/{target_iqn}/{group_name}')
    def delete_group(self, target_iqn, group_name, request=None):
        logger.debug("iSCSI: Deleting group: %s/%s", target_iqn, group_name)
        return request()

    @RestClient.api_put('/api/discoveryauth')
    def update_discoveryauth(self, user, password, mutual_user, mutual_password, request=None):
        logger.debug("iSCSI: Updating discoveryauth: %s/%s/%s/%s", user, password, mutual_user,
                     mutual_password)
        chap = ''
        if user and password:
            chap = '{}/{}'.format(user, password)
        chap_mutual = ''
        if mutual_user and mutual_password:
            chap_mutual = '{}/{}'.format(mutual_user, mutual_password)
        return request({
            'chap': chap,
            'chap_mutual': chap_mutual
        })
