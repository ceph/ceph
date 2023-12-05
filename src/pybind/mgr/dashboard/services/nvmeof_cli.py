# -*- coding: utf-8 -*-
import errno
import json

from mgr_module import CLICheckNonemptyFileInput, CLIReadCommand, CLIWriteCommand

from ..rest_client import RequestException
from .nvmeof_conf import NvmeofGatewaysConfig, NvmeofGatewayAlreadyExists, \
    ManagedByOrchestratorException

@CLIReadCommand('dashboard nvmeof-gateway-list')
def list_nvmeof_gateways(_):
    '''
    List NVMe-oF gateways
    '''
    return 0, json.dumps(NvmeofGatewaysConfig.get_gateways_config()), ''

@CLIWriteCommand('dashboard nvmeof-gateway-add')
@CLICheckNonemptyFileInput(desc='NVMe-oF gateway configuration')
def add_nvmeof_gateway(_, inbuf, name: str):
    '''
    Add NVMe-oF gateway configuration. Gateway URL read from -i <file>
    '''
    service_url = inbuf
    try:
        NvmeofGatewaysConfig.add_gateway(name, service_url)
        return 0, 'Success', ''
    except NvmeofGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
    except RequestException as ex:
        return -errno.EINVAL, '', str(ex)

@CLIWriteCommand('dashboard nvmeof-gateway-rm')
def remove_nvmeof_gateway(_, name: str):
    '''
    Remove NVMe-oF gateway configuration
    '''
    try:
        NvmeofGatewaysConfig.remove_gateway(name)
        return 0, 'Success', ''
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
