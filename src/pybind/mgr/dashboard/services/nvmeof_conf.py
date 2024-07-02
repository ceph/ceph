# -*- coding: utf-8 -*-

import json

from .. import mgr
from ..exceptions import DashboardException


class NvmeofGatewayAlreadyExists(Exception):
    def __init__(self, gateway_name):
        super(NvmeofGatewayAlreadyExists, self).__init__(
            "NVMe-oF gateway '{}' already exists".format(gateway_name))


class NvmeofGatewayDoesNotExist(Exception):
    def __init__(self, hostname):
        super(NvmeofGatewayDoesNotExist, self).__init__(
            "NVMe-oF gateway '{}' does not exist".format(hostname))


class ManagedByOrchestratorException(Exception):
    def __init__(self):
        super(ManagedByOrchestratorException, self).__init__(
            "NVMe-oF configuration is managed by the orchestrator")


_NVMEOF_STORE_KEY = "_nvmeof_config"


class NvmeofGatewaysConfig(object):
    @classmethod
    def _load_config_from_store(cls):
        json_db = mgr.get_store(_NVMEOF_STORE_KEY,
                                '{"gateways": {}}')
        config = json.loads(json_db)
        cls._save_config(config)
        return config

    @classmethod
    def _save_config(cls, config):
        mgr.set_store(_NVMEOF_STORE_KEY, json.dumps(config))

    @classmethod
    def get_gateways_config(cls):
        return cls._load_config_from_store()

    @classmethod
    def add_gateway(cls, name, service_url):
        config = cls.get_gateways_config()
        if name in config:
            raise NvmeofGatewayAlreadyExists(name)
        config['gateways'][name] = {'service_url': service_url}
        cls._save_config(config)

    @classmethod
    def remove_gateway(cls, name):
        config = cls.get_gateways_config()
        if name not in config['gateways']:
            raise NvmeofGatewayDoesNotExist(name)
        del config['gateways'][name]
        cls._save_config(config)

    @classmethod
    def get_service_info(cls):
        try:
            config = cls.get_gateways_config()
            service_name =  list(config['gateways'].keys())[0]
            addr = config['gateways'][service_name]['service_url']
            return service_name, addr
        except KeyError as e:
            raise DashboardException(
                msg=f'NVMe-oF configuration is not set: {e}',
            )

    @classmethod
    def get_client_cert(cls, service_name: str):
        client_cert = mgr.get_store(f'{service_name}/mtls_client_cert')
        if client_cert:
            return client_cert.encode()

    @classmethod
    def get_client_key(cls, service_name: str):
        client_key = mgr.get_store(f'{service_name}/mtls_client_key')
        if client_key:
            return client_key.encode()

    @classmethod
    def get_server_cert(cls, service_name: str):
        server_cert = mgr.get_store(f'{service_name}/mtls_server_cert')
        if server_cert:
            return server_cert.encode()
