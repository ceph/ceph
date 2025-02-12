# -*- coding: utf-8 -*-
import errno
import json
from typing import Any, Dict, Optional, Union, get_type_hints, get_origin, get_args, \
    Annotated, NewType

import yaml
from mgr_module import CLICheckNonemptyFileInput, CLICommand, CLIReadCommand, \
    CLIWriteCommand, HandleCommandResult, HandlerFuncType

from ..exceptions import DashboardException
from ..rest_client import RequestException
from .nvmeof_conf import ManagedByOrchestratorException, \
    NvmeofGatewayAlreadyExists, NvmeofGatewaysConfig

NvmeCliSize = NewType("NvmeCliSize", str)

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
K, KB, KIB = "K", "KB", "KiB"
M, MB, MIB = "M", "MB", "MiB"
G, GB, GIB = "G", "GB", "GiB"
T, TB, TIB = "T", "TB", "TiB"
P, PB, PIB = "P", "PB", "PiB"

MULTIPLES = ['', K, M, G, T, P]
UNITS = {
	f"{prefix}{suffix}": 1024**mult
	for mult, prefix in enumerate(MULTIPLES)
	for suffix in ['', 'B', 'iB']
	if not (prefix == '' and suffix == 'iB')
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

    def _convert_annotated_types(self, cmd_dict):
        for arg, hint in get_type_hints(self.func, include_extras=True).items():
            if get_origin(hint) is Annotated:
                annotated_args = get_args(hint)
                if len(annotated_args) < 2:
                    continue
                metadata_type = annotated_args[1]
                if metadata_type:
                    if metadata_type is NvmeCliSize:
                        cmd_dict[arg] = convert_to_bytes(cmd_dict[arg], B)
        
    def call(self,
             mgr: Any,
             cmd_dict: Dict[str, Any],
             inbuf: Optional[str] = None) -> HandleCommandResult:
        self._convert_annotated_types(cmd_dict)
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
