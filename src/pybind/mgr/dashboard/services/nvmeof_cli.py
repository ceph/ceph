# -*- coding: utf-8 -*-
import errno
import json
from abc import ABC, abstractmethod
from typing import Annotated, Any, Dict, List, NamedTuple, Optional, Type, \
    Union, get_args, get_origin, get_type_hints

import yaml
from mgr_module import CLICheckNonemptyFileInput, CLICommand, CLIReadCommand, \
    CLIWriteCommand, HandleCommandResult, HandlerFuncType
from prettytable import PrettyTable

from ..exceptions import DashboardException
from ..model.nvmeof import CliFlags, CliHeader
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


MULTIPLES = ['', "K", "M", "G", "T", "P"]
UNITS = {
    f"{prefix}{suffix}": 1024 ** mult
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
            raise ValueError("default unit was not provided")
        unit_str = default_unit

    if unit_str in UNITS:
        return number * UNITS[unit_str]
    raise ValueError(f"Invalid unit: {unit_str}")


def convert_from_bytes(num_in_bytes):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(num_in_bytes)
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    # Round to no decimal if it's an integer, otherwise show 1 decimal place
    if size.is_integer():
        size_str = f"{int(size)}"
    else:
        size_str = f"{size:.1f}"

    return f"{size_str}{units[unit_index]}"


class OutputFormatter(ABC):
    @abstractmethod
    def format_output(self, data, model):
        """Format the given data for output."""
        raise NotImplementedError()


class AnnotatedDataTextOutputFormatter(OutputFormatter):
    def _snake_case_to_title(self, s):
        return s.replace('_', ' ').title()

    def _create_table(self, field_names):
        table = PrettyTable(border=True)
        titles = [self._snake_case_to_title(field) for field in field_names]
        table.field_names = titles
        table.align = 'l'
        table.padding_width = 0
        return table

    def _get_text_output(self, data):
        if isinstance(data, list):
            return self._get_list_text_output(data)
        return self._get_object_text_output(data)

    def _get_row(self, columns, data_obj):
        row = []
        for col in columns:
            col_val = data_obj.get(col)
            if col_val is None:
                col_val = ''
            row.append(str(col_val))
        return row

    def _get_list_text_output(self, data):
        columns = list(dict.fromkeys([key for obj in data for key in obj.keys()]))
        table = self._create_table(columns)
        for d in data:
            table.add_row(self._get_row(columns, d))
        return table.get_string()

    def _get_object_text_output(self, data):
        columns = [k for k in data.keys() if k not in ["status", "error_message"]]
        table = self._create_table(columns)
        table.add_row(self._get_row(columns, data))
        return table.get_string()

    def _is_list_of_complex_type(self, value):
        if not isinstance(value, list):
            return False

        if not value:
            return None

        primitives = (int, float, str, bool, bytes)

        return not isinstance(value[0], primitives)

    def _select_list_field(self, data: Dict) -> Optional[str]:
        for key, value in data.items():
            if self._is_list_of_complex_type(value):
                return key
        return None

    def is_namedtuple_type(self, obj):
        return isinstance(obj, type) and issubclass(obj, tuple) and hasattr(obj, '_fields')

    # pylint: disable=too-many-branches
    def process_dict(self, input_dict: dict,
                     nt_class: Type[NamedTuple],
                     is_top_level: bool) -> Union[Dict, str, List]:
        result: Dict = {}
        if not input_dict:
            return result
        hints = get_type_hints(nt_class, include_extras=True)

        for field, type_hint in hints.items():
            if field not in input_dict:
                continue

            value = input_dict[field]
            origin = get_origin(type_hint)

            actual_type = type_hint
            annotations = []
            output_name = field
            skip = False

            if origin is Annotated:
                actual_type, *annotations = get_args(type_hint)
                for annotation in annotations:
                    if annotation == CliFlags.DROP:
                        skip = True
                        break
                    if isinstance(annotation, CliHeader):
                        output_name = annotation.label
                    elif is_top_level and annotation == CliFlags.EXCLUSIVE_LIST:
                        assert get_origin(actual_type) == list
                        assert len(get_args(actual_type)) == 1
                        return [self.process_dict(item, get_args(actual_type)[0],
                                                  False) for item in value]
                    elif is_top_level and annotation == CliFlags.EXCLUSIVE_RESULT:
                        return f"Failure: {input_dict.get('error_message')}" if bool(
                            input_dict[field]) else "Success"
                    elif annotation == CliFlags.SIZE:
                        value = convert_from_bytes(int(input_dict[field]))

            if skip:
                continue

            # If it's a nested namedtuple and value is a dict, recurse
            if self.is_namedtuple_type(actual_type) and isinstance(value, dict):
                result[output_name] = self.process_dict(value, actual_type, False)
            else:
                result[output_name] = value

        return result

    def _convert_to_text_output(self, data, model):
        data = self.process_dict(data, model, True)
        if isinstance(data, str):
            return data
        return self._get_text_output(data)

    def format_output(self, data, model):
        return self._convert_to_text_output(data, model)


class NvmeofCLICommand(CLICommand):
    desc: str

    def __init__(self, prefix, model: Type[NamedTuple], alias=None, perm='rw', poll=False):
        super().__init__(prefix, perm, poll)
        self._output_formatter = AnnotatedDataTextOutputFormatter()
        self._model = model
        self._alias = alias

    def _use_api_endpoint_desc_if_available(self, func):
        if not self.desc and hasattr(func, 'doc_info'):
            self.desc = func.doc_info.get('summary', '')

    def __call__(self, func) -> HandlerFuncType:  # type: ignore
        if self._alias:
            NvmeofCLICommand(self._alias, model=self._model)._register_handler(func)

        resp = super().__call__(func)
        self._use_api_endpoint_desc_if_available(func)
        return resp

    def call(self,
             mgr: Any,
             cmd_dict: Dict[str, Any],
             inbuf: Optional[str] = None) -> HandleCommandResult:
        try:
            ret = super().call(mgr, cmd_dict, inbuf)
            out_format = cmd_dict.get('format')
            if ret is None:
                out = ''
            if out_format == 'plain' or not out_format:
                out = self._output_formatter.format_output(ret, self._model)
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
