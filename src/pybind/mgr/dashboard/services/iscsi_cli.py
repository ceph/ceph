# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import json

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from mgr_module import CLIReadCommand, CLIWriteCommand

from .orchestrator import OrchClient
from .. import mgr


class IscsiGatewayAlreadyExists(Exception):
    def __init__(self, gateway_name):
        super(IscsiGatewayAlreadyExists, self).__init__(
            "iSCSI gateway '{}' already exists".format(gateway_name))


class IscsiGatewayDoesNotExist(Exception):
    def __init__(self, hostname):
        super(IscsiGatewayDoesNotExist, self).__init__(
            "iSCSI gateway '{}' does not exist".format(hostname))


class InvalidServiceUrl(Exception):
    def __init__(self, service_url):
        super(InvalidServiceUrl, self).__init__(
            "Invalid service URL '{}'. "
            "Valid format: '<scheme>://<username>:<password>@<host>[:port]'.".format(service_url))


class ManagedByOrchestratorException(Exception):
    def __init__(self):
        super(ManagedByOrchestratorException, self).__init__(
            "iSCSI configuration is managed by the orchestrator")


_ISCSI_STORE_KEY = "_iscsi_config"


class IscsiGatewaysConfig(object):
    @classmethod
    def _load_config(cls):
        if OrchClient.instance().available():
            raise ManagedByOrchestratorException()
        json_db = mgr.get_store(_ISCSI_STORE_KEY,
                                '{"gateways": {}}')
        return json.loads(json_db)

    @classmethod
    def _save_config(cls, config):
        mgr.set_store(_ISCSI_STORE_KEY, json.dumps(config))

    @classmethod
    def add_gateway(cls, name, service_url):
        config = cls._load_config()
        if name in config:
            raise IscsiGatewayAlreadyExists(name)
        url = urlparse(service_url)
        if not url.scheme or not url.hostname or not url.username or not url.password:
            raise InvalidServiceUrl(service_url)
        config['gateways'][name] = {'service_url': service_url}
        cls._save_config(config)

    @classmethod
    def remove_gateway(cls, name):
        config = cls._load_config()
        if name not in config['gateways']:
            raise IscsiGatewayDoesNotExist(name)

        del config['gateways'][name]
        cls._save_config(config)

    @classmethod
    def get_gateways_config(cls):
        try:
            config = cls._load_config()
        except ManagedByOrchestratorException:
            config = {'gateways': {}}
            instances = OrchClient.instance().list_service_info("iscsi")
            for instance in instances:
                config['gateways'][instance.nodename] = {
                    'service_url': instance.service_url
                }
        return config

    @classmethod
    def get_gateway_config(cls, name):
        config = IscsiGatewaysConfig.get_gateways_config()
        if name not in config['gateways']:
            raise IscsiGatewayDoesNotExist(name)
        return config['gateways'][name]


@CLIReadCommand('dashboard iscsi-gateway-list', desc='List iSCSI gateways')
def list_iscsi_gateways(_):
    return 0, json.dumps(IscsiGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard iscsi-gateway-add',
                 'name=name,type=CephString '
                 'name=service_url,type=CephString',
                 'Add iSCSI gateway configuration')
def add_iscsi_gateway(_, name, service_url):
    try:
        IscsiGatewaysConfig.add_gateway(name, service_url)
        return 0, 'Success', ''
    except IscsiGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except InvalidServiceUrl as ex:
        return -errno.EINVAL, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)


@CLIWriteCommand('dashboard iscsi-gateway-rm',
                 'name=name,type=CephString',
                 'Remove iSCSI gateway configuration')
def remove_iscsi_gateway(_, name):
    try:
        IscsiGatewaysConfig.remove_gateway(name)
        return 0, 'Success', ''
    except IscsiGatewayDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
