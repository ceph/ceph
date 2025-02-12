import json
from typing import Annotated
import errno
from unittest.mock import MagicMock

import pytest
from mgr_module import CLICommand, HandleCommandResult

from ..services.nvmeof_cli import GB, KB, MB, PB, TB, B, G, K, M, \
    NvmeofCLICommand, P, T, convert_to_bytes, NvmeCliSize


@pytest.fixture(scope="class", name="command_with_size_annotation_name")
def fixture_command_with_size_annotation_name():
    return "test annotated size command"


@pytest.fixture(scope="class", name="command_with_size_annotation")
def fixture_command_with_size_annotation(command_with_size_annotation_name):
    print(NvmeofCLICommand.COMMANDS.keys())
    @NvmeofCLICommand(command_with_size_annotation_name)
    def func(_, param: Annotated[int, NvmeCliSize]): # noqa # pylint: disable=unused-variable
        return {'a': '1', 'param': param}
    print(NvmeofCLICommand.COMMANDS.keys())
    yield func 
    del NvmeofCLICommand.COMMANDS[command_with_size_annotation_name]
    assert command_with_size_annotation_name not in NvmeofCLICommand.COMMANDS


@pytest.fixture(scope="class", name="sample_command")
def fixture_sample_command():
    test_cmd = "test command"

    @NvmeofCLICommand(test_cmd)
    def func(_): # noqa # pylint: disable=unused-variable
        return {'a': '1', 'b': 2}
    yield test_cmd
    del NvmeofCLICommand.COMMANDS[test_cmd]
    assert test_cmd not in NvmeofCLICommand.COMMANDS


@pytest.fixture(name='base_call_mock')
def fixture_base_call_mock(monkeypatch):
    mock_result = {'a': 'b'}
    super_mock = MagicMock()
    super_mock.return_value = mock_result
    monkeypatch.setattr(CLICommand, 'call', super_mock)
    return super_mock


@pytest.fixture(name='base_call_return_none_mock')
def fixture_base_call_return_none_mock(monkeypatch):
    mock_result = None
    super_mock = MagicMock()
    super_mock.return_value = mock_result
    monkeypatch.setattr(CLICommand, 'call', super_mock)
    return super_mock


class TestNvmeofCLICommand:
    def test_command_being_added(self, sample_command):
        assert sample_command in NvmeofCLICommand.COMMANDS
        assert isinstance(NvmeofCLICommand.COMMANDS[sample_command], NvmeofCLICommand)

    def test_command_return_cmd_result_default_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == '{"a": "b"}'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_json_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'json'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == '{"a": "b"}'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_yaml_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'yaml'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == 'a: b\n'
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_invalid_format(self, base_call_mock, sample_command):
        mock_result = {'a': 'b'}
        super_mock = MagicMock()
        super_mock.call.return_value = mock_result

        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'invalid'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == -errno.EINVAL
        assert result.stdout == ''
        assert result.stderr
        base_call_mock.assert_called_once()

    def test_command_return_empty_cmd_result(self, base_call_return_none_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == ''
        assert result.stderr == ''
        base_call_return_none_mock.assert_called_once()
        
        
class TestConvertAnnotatedType:
    def test_command_convert_annotated_parameter(self, command_with_size_annotation, command_with_size_annotation_name):
        print(command_with_size_annotation)
        
        result = NvmeofCLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": f"{5 * 1024 ** 2}"})
        assert result.retval == 0
        assert json.loads(result.stdout)['param'] == 5 * 1024 ** 2
        assert result.stderr == ''
        
        result = NvmeofCLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": f"{5 * 1024}{KB}"})
        assert result.retval == 0
        assert json.loads(result.stdout)['param'] == 5 * 1024 ** 2
        assert result.stderr == ''
        
        result = NvmeofCLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": "5{MB}"})
        assert result.retval == 0
        assert json.loads(result.stdout)['param'] == 5 * 1024 ** 2
        assert result.stderr == ''
    


class TestConvertToBytes:
    def test_with_kb(self):
        assert convert_to_bytes(f"100{KB}") == 102400
        assert convert_to_bytes(f"100{K}") == 102400

    def test_with_mb(self):
        assert convert_to_bytes(f"2{MB}") == 2 * 1024 ** 2
        assert convert_to_bytes(f"2{M}") == 2 * 1024 ** 2

    def test_with_gb(self):
        assert convert_to_bytes(f"1{GB}") == 1024 ** 3
        assert convert_to_bytes(f"1{G}") == 1024 ** 3

    def test_with_tb(self):
        assert convert_to_bytes(f"1{TB}") == 1024 ** 4
        assert convert_to_bytes(f"1{T}") == 1024 ** 4

    def test_with_pb(self):
        assert convert_to_bytes(f"1{PB}") == 1024 ** 5
        assert convert_to_bytes(f"1{P}") == 1024 ** 5

    def test_with_integer(self):
        assert convert_to_bytes(50, default_unit=B) == 50

    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            convert_to_bytes("50XYZ")

    def test_b(self):
        assert convert_to_bytes(f"500{B}") == 500

    def test_with_large_number(self):
        assert convert_to_bytes(f"1000{GB}") == 1000 * 1024 ** 3

    def test_no_number(self):
        with pytest.raises(ValueError):
            convert_to_bytes(GB)

    def test_no_unit_with_default_unit_gb(self):
        assert convert_to_bytes("500", default_unit=GB) == 500 * 1024 ** 3

    def test_no_unit_with_no_default_unit_raises(self):
        with pytest.raises(ValueError):
            convert_to_bytes("500")

    def test_unit_in_input_overrides_default(self):
        assert convert_to_bytes("50", default_unit=KB) == 50 * 1024
        assert convert_to_bytes("50KB", default_unit=KB) == 50 * 1024
        assert convert_to_bytes("50MB", default_unit=KB) == 50 * 1024 ** 2
