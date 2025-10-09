# -*- coding: utf-8 -*-

import json

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

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
    def _load_config_from_store(cls):
        json_db = mgr.get_store(_ISCSI_STORE_KEY,
                                '{"gateways": {}}')
        config = json.loads(json_db)
        cls.update_iscsi_config(config)
        return config

    @classmethod
    def update_iscsi_config(cls, config):
        """
        Since `ceph-iscsi` config v10, gateway names were renamed from host short name to FQDN.
        If Ceph Dashboard were configured before v10, we try to update our internal gateways
        database automatically.
        """
        for gateway_name, gateway_config in list(config['gateways'].items()):
            if '.' not in gateway_name:
                from ..rest_client import RequestException
                from .iscsi_client import IscsiClient  # pylint: disable=cyclic-import
                try:
                    service_url = gateway_config['service_url']
                    new_gateway_name = IscsiClient.instance(
                        service_url=service_url).get_hostname()['data']
                    if gateway_name != new_gateway_name:
                        config['gateways'][new_gateway_name] = gateway_config
                        del config['gateways'][gateway_name]
                        cls._save_config(config)
                except RequestException:
                    # If gateway is not accessible, it should be removed manually
                    # or we will try to update automatically next time
                    continue

    @classmethod
    def _save_config(cls, config):
        mgr.set_store(_ISCSI_STORE_KEY, json.dumps(config))

    @classmethod
    def validate_service_url(cls, service_url):
        url = urlparse(service_url)
        if not url.scheme or not url.hostname or not url.username or not url.password:
            raise InvalidServiceUrl(service_url)

    @classmethod
    def add_gateway(cls, name, service_url):
        config = cls.get_gateways_config()
        if name in config:
            raise IscsiGatewayAlreadyExists(name)
        IscsiGatewaysConfig.validate_service_url(service_url)
        config['gateways'][name] = {'service_url': service_url}
        cls._save_config(config)

    @classmethod
    def remove_gateway(cls, name):
        config = cls._load_config_from_store()
        if name not in config['gateways']:
            raise IscsiGatewayDoesNotExist(name)

        del config['gateways'][name]
        cls._save_config(config)

    @classmethod
    def get_gateways_config(cls):
        return cls._load_config_from_store()

    @classmethod
    def get_gateway_config(cls, name):
        config = IscsiGatewaysConfig.get_gateways_config()
        if name not in config['gateways']:
            raise IscsiGatewayDoesNotExist(name)
        return config['gateways'][name]
