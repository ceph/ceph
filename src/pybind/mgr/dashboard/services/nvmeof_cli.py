# -*- coding: utf-8 -*-
import errno
import json
from typing import Any, Dict, Optional

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

from abc import ABC, abstractmethod

class OutputFormatter(ABC):
    @abstractmethod
    def format_output(self, data):
        """Format the given data for output."""
        pass

    @abstractmethod
    def get_type(self) -> str:
        """Return a string representing the output format type ('text', 'json', 'yaml')."""
        pass

class TextOutputFormatter(OutputFormatter):
    def get_type(self):
        return 'text'

class StatusTextOutputFormatter(TextOutputFormatter):
    def _convert_to_status_text_output(self, data):
        if data.get('status') == 0:
            return "Success"
        else:
            return data.get("error_message")
    
    def format_output(self, data):
        return self._convert_to_status_text_output(data)


class DataTextOutputFormatter(TextOutputFormatter):
    def _snake_case_to_title(self, s):
        return s.replace('_', ' ').title()

    def _create_table(self, field_names):
        from prettytable import PrettyTable
        table = PrettyTable(border=True)
        titles = [self._snake_case_to_title(field) for field in field_names]
        table.field_names = titles
        table.align = 'l'
        table.left_padding_width = 0
        table.right_padding_width = 2
        return table
    
    def _get_list_text_output(self, data):
        columns = list(dict.fromkeys([key for obj in data for key in obj.keys()]))
        table = self._create_table(columns)
        for d in data:
            row = []
            for col in columns:
                row.append(str(d.get(col)))
            table.add_row(row)
        return table.get_string()

    def _get_object_text_output(self, data):
        columns = [k for k in data.keys() if k not in ["status", "error_message"]]
        table = self._create_table(columns)
        row = []
        for col in columns:
            row.append(str(data.get(col)))
        table.add_row(row)
        return table.get_string()

    def _is_list_of_complex_type(self, value):
        if not isinstance(value, list):
            return False

        if not value:
            return None

        primitives = (int, float, str, bool, bytes)

        return not isinstance(value[0], primitives)

    def _select_list_field(self, data: Dict):
        for key, value in data.items():
            if self._is_list_of_complex_type(value):
                return key

    def _convert_to_text_output(self, data):
        data_field = self._select_list_field(data)
        if not data_field:
            return self._get_object_text_output(data)
        return self._get_list_text_output(data[data_field])
    
    def format_output(self, data):
        return self._convert_to_text_output(data)
    
class NvmeofCLICommand(CLICommand):
    def __init__(self, prefix, perm = 'rw', poll = False, output_formatter: TextOutputFormatter=None):
        super().__init__(prefix, perm, poll)
        if not output_formatter:
            output_formatter = DataTextOutputFormatter()
        self._output_formatter = output_formatter
        
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
            if ret is None:
                    out = ''
            if out_format == 'text' or not out_format:
                out = self._output_formatter.format_output(ret)
            elif out_format == 'json':
                out = json.dumps(ret)
            elif out_format == 'yaml':
                out = yaml.dump(ret)
            else:
                return HandleCommandResult(-errno.EINVAL, '',
                                           f"format '{out_format}' is not implemented")
            return HandleCommandResult(0, out, '')
        except DashboardException as e:
            return HandleCommandResult(-errno.EINVAL, '', str(e))
