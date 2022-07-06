# -*- coding: utf-8 -*-

import errno
import json
from typing import Optional

from mgr_module import CLICheckNonemptyFileInput, CLIReadCommand, CLIWriteCommand

from ..rest_client import RequestException
from .iscsi_client import IscsiClient
from .iscsi_config import InvalidServiceUrl, IscsiGatewayAlreadyExists, \
    IscsiGatewayDoesNotExist, IscsiGatewaysConfig, \
    ManagedByOrchestratorException


@CLIReadCommand('dashboard iscsi-gateway-list')
def list_iscsi_gateways(_):
    '''
    List iSCSI gateways
    '''
    return 0, json.dumps(IscsiGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard iscsi-gateway-add')
@CLICheckNonemptyFileInput(desc='iSCSI gateway configuration')
def add_iscsi_gateway(_, inbuf, name: Optional[str] = None):
    '''
    Add iSCSI gateway configuration. Gateway URL read from -i <file>
    '''
    service_url = inbuf
    try:
        IscsiGatewaysConfig.validate_service_url(service_url)
        if name is None:
            name = IscsiClient.instance(service_url=service_url).get_hostname()['data']
        IscsiGatewaysConfig.add_gateway(name, service_url)
        return 0, 'Success', ''
    except IscsiGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except InvalidServiceUrl as ex:
        return -errno.EINVAL, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
    except RequestException as ex:
        return -errno.EINVAL, '', str(ex)


@CLIWriteCommand('dashboard iscsi-gateway-rm')
def remove_iscsi_gateway(_, name: str):
    '''
    Remove iSCSI gateway configuration
    '''
    try:
        IscsiGatewaysConfig.remove_gateway(name)
        return 0, 'Success', ''
    except IscsiGatewayDoesNotExist as ex:
        return -errno.ENOENT, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
