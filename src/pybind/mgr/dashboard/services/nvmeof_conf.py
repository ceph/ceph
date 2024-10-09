# -*- coding: utf-8 -*-

import json
import logging

from orchestrator import OrchestratorError

from .. import mgr
from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient

logger = logging.getLogger('nvmeof_conf')


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
    def add_gateway(cls, name, service_url, group, daemon_name):
        config = cls.get_gateways_config()

        if name in config.get('gateways', {}):
            existing_gateways = config['gateways'][name]
            for gateway in existing_gateways:
                if 'daemon_name' not in gateway:
                    gateway['daemon_name'] = daemon_name
                    break
                if gateway['service_url'] == service_url:
                    return

        new_gateway = {
            'service_url': service_url,
            'group': group,
            'daemon_name': daemon_name
        }

        if name in config.get('gateways', {}):
            config['gateways'][name].append(new_gateway)
        else:
            config['gateways'][name] = [new_gateway]

        cls._save_config(config)

    @classmethod
    def remove_gateway(cls, name):
        config = cls.get_gateways_config()
        if name not in config['gateways']:
            raise NvmeofGatewayDoesNotExist(name)
        del config['gateways'][name]
        cls._save_config(config)

    @classmethod
    def get_service_info(cls, group=None):
        try:
            config = cls.get_gateways_config()
            gateways = config.get('gateways', {})
            if not gateways:
                return None

            if group:
                return _get_name_url_for_group(gateways, group)

            return _get_default_service(gateways)

        except (KeyError, IndexError) as e:
            raise DashboardException(
                msg=f'NVMe-oF configuration is not set: {e}',
            )

    @classmethod
    def get_client_cert(cls, service_name: str):
        client_cert = cls.from_cert_store('nvmeof_client_cert', service_name)
        return client_cert.encode() if client_cert else None

    @classmethod
    def get_client_key(cls, service_name: str):
        client_key = cls.from_cert_store('nvmeof_client_key', service_name, key=True)
        return client_key.encode() if client_key else None

    @classmethod
    def get_root_ca_cert(cls, service_name: str):
        root_ca_cert = cls.from_cert_store('nvmeof_root_ca_cert', service_name)
        # If root_ca_cert is not set, use server_cert as root_ca_cert
        return root_ca_cert.encode() if root_ca_cert else cls.get_server_cert(service_name)

    @classmethod
    def get_server_cert(cls, service_name: str):
        server_cert = cls.from_cert_store('nvmeof_server_cert', service_name)
        return server_cert.encode() if server_cert else None

    @classmethod
    def from_cert_store(cls, entity: str, service_name: str, key=False):
        try:
            orch = OrchClient.instance()
            if orch.available():
                if key:
                    return orch.cert_store.get_key(entity, service_name,
                                                   ignore_missing_exception=True)
                return orch.cert_store.get_cert(entity, service_name,
                                                ignore_missing_exception=True)
            return None
        except OrchestratorError:
            # just return None if any orchestrator error is raised
            # otherwise nvmeof api will raise this error and doesn't proceed.
            return None


def _get_name_url_for_group(gateways, group):
    try:
        orch = OrchClient.instance()
        for service_name, svc_config in gateways.items():
            # get the group name of the service and match it against the
            # group name provided
            group_name_from_svc = orch.services.get(service_name)[0].spec.group
            if group == group_name_from_svc:
                running_daemons = _get_running_daemons(orch, service_name)
                config = _get_running_daemon_svc_config(svc_config, running_daemons)

                if config:
                    return service_name, config['service_url']
        return None

    except OrchestratorError:
        return _get_default_service(gateways)


def _get_running_daemons(orch, service_name):
    # get the running nvmeof daemons
    daemons = [d.to_dict()
               for d in orch.services.list_daemons(service_name=service_name)]
    return [d['daemon_name'] for d in daemons
            if d['status_desc'] == 'running']


def _get_running_daemon_svc_config(svc_config, running_daemons):
    try:
        return next(config for config in svc_config
                    if config['daemon_name'] in running_daemons)
    except StopIteration:
        return None


def _get_default_service(gateways):
    if gateways:
        gateway_keys = list(gateways.keys())
        # if there are more than 1 gateway, rather than chosing a random gateway
        # from any of the group, raise an exception to make it clear that we need
        # to specify the group name in the API request.
        if len(gateway_keys) > 1:
            raise DashboardException(
                msg=(
                    "Multiple NVMe-oF gateway groups are configured. "
                    "Please specify the 'gw_group' parameter in the request."
                ),
                component="nvmeof"
            )
        service_name = gateway_keys[0]
        return service_name, gateways[service_name][0]['service_url']
    return None
