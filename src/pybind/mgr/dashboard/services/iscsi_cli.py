# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import json

from mgr_module import CLIReadCommand, CLIWriteCommand

from .iscsi_client import IscsiClient
from .iscsi_config import IscsiGatewaysConfig, IscsiGatewayAlreadyExists, InvalidServiceUrl, \
    ManagedByOrchestratorException, IscsiGatewayDoesNotExist
from ..rest_client import RequestException


@CLIReadCommand('dashboard iscsi-gateway-list', desc='List iSCSI gateways')
def list_iscsi_gateways(_):
    return 0, json.dumps(IscsiGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard iscsi-gateway-add',
                 'name=service_url,type=CephString',
                 'Add iSCSI gateway configuration')
def add_iscsi_gateway(_, service_url):
    try:
        IscsiGatewaysConfig.validate_service_url(service_url)
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
