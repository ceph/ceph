# -*- coding: utf-8 -*-
import errno
import json
from typing import Any, Dict, Optional, Union

import yaml
from mgr_module import CLICheckNonemptyFileInput, CLICommand, CLIReadCommand, \
    CLIWriteCommand, HandleCommandResult, HandlerFuncType

from ..exceptions import DashboardException
from ..rest_client import RequestException
from .nvmeof_conf import ManagedByOrchestratorException, \
    NvmeofGatewayAlreadyExists, NvmeofGatewaysConfig


@CLIReadCommand('dashboard nvmeof-gateway-list')
def list_nvmeof_gateways(_):
    '''
    List NVMe-oF gateways
    '''
    return 0, json.dumps(NvmeofGatewaysConfig.get_gateways_config()), ''


@CLIWriteCommand('dashboard nvmeof-gateway-add')
@CLICheckNonemptyFileInput(desc='NVMe-oF gateway configuration')
def add_nvmeof_gateway(_, inbuf, name: str, group: str, daemon_name: str):
    '''
    Add NVMe-oF gateway configuration. Gateway URL read from -i <file>
    '''
    service_url = inbuf
    try:
        NvmeofGatewaysConfig.add_gateway(name, service_url, group, daemon_name)
        return 0, 'Success', ''
    except NvmeofGatewayAlreadyExists as ex:
        return -errno.EEXIST, '', str(ex)
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)
    except RequestException as ex:
        return -errno.EINVAL, '', str(ex)


@CLIWriteCommand('dashboard nvmeof-gateway-rm')
def remove_nvmeof_gateway(_, name: str, daemon_name: str = ''):
    '''
    Remove NVMe-oF gateway configuration
    '''
    try:
        NvmeofGatewaysConfig.remove_gateway(name, daemon_name)
        return 0, 'Success', ''
    except ManagedByOrchestratorException as ex:
        return -errno.EINVAL, '', str(ex)


B = "B"
K, KB = "K", "KB"
M, MB = "M", "MB"
G, GB = "G", "GB"
T, TB = "T", "TB"
P, PB = "P", "PB"
UNITS = {
    B: 1,
    KB: 1024,
    K: 1024,
    MB: 1024**2,
    M: 1024**2,
    GB: 1024**3,
    G: 1024**3,
    TB: 1024**4,
    T: 1024**4,
    PB: 1024**5,
    P: 1024**5
}


def convert_to_bytes(size: Union[int, str], default_unit=None):
    if isinstance(size, int):
        number = size
        size = str(size)
    else:
        num_str = ''.join(filter(str.isdigit, size))
        number = int(num_str)
    unit_str = ''.join(filter(str.isalpha, size))
    if not unit_str:
        if not default_unit:
            raise ValueError("No size unit was provided")
        unit_str = default_unit

    if unit_str in UNITS:
        return number * UNITS[unit_str]
    raise ValueError(f"Invalid unit: {unit_str}")


class NvmeofCLICommand(CLICommand):
    def __call__(self, func) -> HandlerFuncType:  # type: ignore
        # pylint: disable=useless-super-delegation
        """
        This method is being overriden solely to be able to disable the linters checks for typing.
        The NvmeofCLICommand decorator assumes a different type returned from the
        function it wraps compared to CLICmmand, breaking a Liskov substitution principal,
        hence triggering linters alerts.
        """
        return super().__call__(func)

    def call(self,
             mgr: Any,
             cmd_dict: Dict[str, Any],
             inbuf: Optional[str] = None) -> HandleCommandResult:
        try:
            ret = super().call(mgr, cmd_dict, inbuf)
            out_format = cmd_dict.get('format')
            if out_format == 'json' or not out_format:
                if ret is None:
                    out = ''
                else:
                    out = json.dumps(ret)
            elif out_format == 'yaml':
                if ret is None:
                    out = ''
                else:
                    out = yaml.dump(ret)
            else:
                return HandleCommandResult(-errno.EINVAL, '',
                                           f"format '{out_format}' is not implemented")
            return HandleCommandResult(0, out, '')
        except DashboardException as e:
            return HandleCommandResult(-errno.EINVAL, '', str(e))
