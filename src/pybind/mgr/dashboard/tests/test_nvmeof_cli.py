import errno
import json
import unittest
from typing import Annotated, List, NamedTuple
from unittest.mock import MagicMock

import pytest
from mgr_module import CLICommand, HandleCommandResult

from ..controllers import EndpointDoc
from ..model.nvmeof import CliFlags, CliHeader
from ..services.nvmeof_cli import AnnotatedDataTextOutputFormatter, \
    NvmeofCLICommand, convert_from_bytes, convert_to_bytes
from ..tests import CLICommandTestMixin


@pytest.fixture(scope="class", name="sample_command")
def fixture_sample_command():
    test_cmd = "test command"

    class Model(NamedTuple):
        a: str
        b: int

    @NvmeofCLICommand(test_cmd, Model)
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
        assert result.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
        assert result.stderr == ''
        base_call_mock.assert_called_once()

    def test_command_return_cmd_result_plain_format(self, base_call_mock, sample_command):
        result = NvmeofCLICommand.COMMANDS[sample_command].call(MagicMock(), {'format': 'plain'})
        assert isinstance(result, HandleCommandResult)
        assert result.retval == 0
        assert result.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
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
        assert result.stdout == (
            "++\n"
            "||\n"
            "++\n"
            "\n"
            "++"
        )
        assert result.stderr == ''
        base_call_return_none_mock.assert_called_once()

    def test_command_empty_desc_by_default(self, sample_command):
        assert NvmeofCLICommand.COMMANDS[sample_command].desc == ''

    def test_command_with_endpointdoc_get_desc(self):
        test_cmd = "test command1"
        test_desc = 'test desc1'

        class Model(NamedTuple):
            a: str
            b: int

        @NvmeofCLICommand(test_cmd, Model)
        @EndpointDoc(test_desc)
        def func(_): # noqa # pylint: disable=unused-variable
            return {'a': '1', 'b': 2}

        assert NvmeofCLICommand.COMMANDS[test_cmd].desc == test_desc

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_command_with_endpointdoc_and_docstr_get_docstr(self):
        test_cmd = "test command1"
        test_desc = 'test desc1'
        test_docstr = 'test docstr'

        class Model(NamedTuple):
            a: str
            b: int

        @NvmeofCLICommand(test_cmd, Model)
        @EndpointDoc(test_desc)
        def func(_): # noqa # pylint: disable=unused-variable
            """test docstr"""
            return {'a': '1', 'b': 2}

        assert NvmeofCLICommand.COMMANDS[test_cmd].desc == test_docstr

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_command_alias_calls_command(self, base_call_mock):
        test_cmd = "test command1"
        test_alias = "test alias1"

        class Model(NamedTuple):
            a: str
            b: int

        @NvmeofCLICommand(test_cmd, Model, alias=test_alias)
        def func(_): # noqa # pylint: disable=unused-variable
            return {'a': '1', 'b': 2}

        assert test_cmd in NvmeofCLICommand.COMMANDS
        assert test_alias in NvmeofCLICommand.COMMANDS

        result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})
        assert result.retval == 0
        assert result.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
        assert result.stderr == ''
        base_call_mock.assert_called_once()

        result = NvmeofCLICommand.COMMANDS[test_alias].call(MagicMock(), {})
        assert result.retval == 0
        assert result.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
        assert result.stderr == ''
        assert base_call_mock.call_count == 2

        del NvmeofCLICommand.COMMANDS[test_cmd]
        del NvmeofCLICommand.COMMANDS[test_alias]
        assert test_cmd not in NvmeofCLICommand.COMMANDS
        assert test_alias not in NvmeofCLICommand.COMMANDS


class TestNVMeoFConfCLI(unittest.TestCase, CLICommandTestMixin):
    def setUp(self):
        self.mock_kv_store()

    def test_cli_add_gateway(self):

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool.group',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group='group'
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool.group': [{
                    'group': 'group',
                    'daemon_name': 'nvmeof_daemon',
                    'service_url': 'http://nvmf:port'
                }]
            }
        )

    def test_cli_migration_from_legacy_config(self):
        legacy_config = json.dumps({
            'gateways': {
                'nvmeof.pool': {
                    'service_url': 'http://nvmf:port'
                }
            }
        })
        self.set_key('_nvmeof_config', legacy_config)

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }]
            }
        )

    def test_cli_add_gw_to_existing(self):
        # add first gw
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        # add another daemon to the first gateway
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group=''
        )

        config = json.loads(self.get_key('_nvmeof_config'))

        # make sure its appended to the existing gateway
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }, {
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': '',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_add_new_gw(self):
        # add first config
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        # add another gateway
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof2.pool.group',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group='group'
        )

        config = json.loads(self.get_key('_nvmeof_config'))

        # make sure its added as a new entry
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon',
                    'group': '',
                    'service_url': 'http://nvmf:port'
                }],
                'nvmeof2.pool.group': [{
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': 'group',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_rm_gateway(self):
        self.test_cli_add_gateway()
        self.exec_cmd('nvmeof-gateway-rm', name='nvmeof.pool.group')

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {}
        )

    def test_cli_rm_daemon_from_gateway(self):
        self.test_cli_add_gw_to_existing()
        self.exec_cmd(
            'nvmeof-gateway-rm',
            name='nvmeof.pool',
            daemon_name='nvmeof_daemon'
        )

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {
                'nvmeof.pool': [{
                    'daemon_name': 'nvmeof_daemon_2',
                    'group': '',
                    'service_url': 'http://nvmf-2:port'
                }]
            }
        )

    def test_cli_legacy_config_rm(self):
        legacy_config = json.dumps({
            'gateways': {
                'nvmeof.pool': {
                    'service_url': 'http://nvmf:port'
                }
            }
        })
        self.set_key('_nvmeof_config', legacy_config)

        self.exec_cmd('nvmeof-gateway-rm', name='nvmeof.pool')

        config = json.loads(self.get_key('_nvmeof_config'))
        self.assertEqual(
            config['gateways'], {}
        )


class TestAnnotatedDataTextOutputFormatter():
    def test_no_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: int
            byte: int

        data = {'name': 'Alice', 'age': 30, "byte": 20971520}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+---+--------+\n'
            '|Name |Age|Byte    |\n'
            '+-----+---+--------+\n'
            '|Alice|30 |20971520|\n'
            '+-----+---+--------+'
        )

    def test_none_to_empty_str_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: int
            byte: int

        data = {'name': 'Alice', 'age': 30, "byte": None}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+---+----+\n'
            '|Name |Age|Byte|\n'
            '+-----+---+----+\n'
            '|Alice|30 |    |\n'
            '+-----+---+----+'
        )

    def test_size_bytes_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: int
            byte: Annotated[int, CliFlags.SIZE]

        data = {'name': 'Alice', 'age': 30, "byte": 20971520}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+---+----+\n'
            '|Name |Age|Byte|\n'
            '+-----+---+----+\n'
            '|Alice|30 |20MB|\n'
            '+-----+---+----+'
        )

    def test_drop_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: Annotated[int, CliFlags.DROP]

        data = {'name': 'Alice', 'age': 30}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+\n'
            '|Name |\n'
            '+-----+\n'
            '|Alice|\n'
            '+-----+'
        )

    def test_multiple_annotations(self):
        class Sample(NamedTuple):
            name: str
            age: Annotated[int, CliFlags.SIZE, CliHeader('test')]

        data = {'name': 'Alice', 'age': 1024*1024}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+----+\n'
            '|Name |Test|\n'
            '+-----+----+\n'
            '|Alice|1MB |\n'
            '+-----+----+'
        )

    def test_override_header_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: Annotated[int, CliHeader('test')]

        data = {'name': 'Alice', 'age': 30}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+----+\n'
            '|Name |Test|\n'
            '+-----+----+\n'
            '|Alice|30  |\n'
            '+-----+----+'
        )

    def test_override_exclusive_list_field_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: int

        class SampleList(NamedTuple):
            status: int
            error_message: str
            samples: Annotated[List[Sample], CliFlags.EXCLUSIVE_LIST]

        data = {"status": 0, "error_message": '',
                "samples": [{'name': 'Alice', 'age': 30},
                            {'name': 'Bob', 'age': 40}]}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, SampleList)
        assert output == (
            '+-----+---+\n'
            '|Name |Age|\n'
            '+-----+---+\n'
            '|Alice|30 |\n'
            '|Bob  |40 |\n'
            '+-----+---+'
        )

    def test_exclusive_result_indicator_annotation(self):
        class Sample(NamedTuple):
            name: str
            status: Annotated[int, CliFlags.EXCLUSIVE_RESULT]
            error_message: str

        data = {'name': 'Alice', 'status': 30, "error_message": 'bla'}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == 'Failure: bla'

        data = {'name': 'Alice', 'status': 0}
        output = formatter.format_output(data, Sample)

        assert output == 'Success'


class TestConverFromBytes:
    def test_valid_inputs(self):
        assert convert_from_bytes(209715200) == '200MB'
        assert convert_from_bytes(219715200) == '209.5MB'
        assert convert_from_bytes(1048576) == '1MB'
        assert convert_from_bytes(123) == '123B'
        assert convert_from_bytes(5368709120) == '5GB'


class TestConvertToBytes:
    def test_valid_inputs(self):
        assert convert_to_bytes('200MB') == 209715200
        assert convert_to_bytes('1MB') == 1048576
        assert convert_to_bytes('123B') == 123
        assert convert_to_bytes('5GB') == 5368709120

    def test_default_unit(self):
        with pytest.raises(ValueError):
            assert convert_to_bytes('5') == 5368709120
        assert convert_to_bytes('5', default_unit='GB') == 5368709120
