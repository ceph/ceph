# -*- coding: utf-8 -*-

import json

from .. import mgr

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
