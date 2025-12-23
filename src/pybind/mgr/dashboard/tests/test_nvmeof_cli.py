import errno
import json
import unittest
from typing import Annotated, List, NamedTuple, Optional
from unittest.mock import MagicMock

import pytest
from mgr_module import CLICommand, HandleCommandResult

from ..controllers import EndpointDoc
from ..model.nvmeof import CliFieldTransformer, CliFlags, CliHeader
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
        assert json.loads(result.stdout) == {"a": "b"}
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


class TestNvmeofCLICommandSuccessMessage:

    def test_plain_output_uses_success_message_template(self):
        test_cmd = "nvmeof set_log_level"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="set log level to {log_level}"
        )
        def set_log_level(self, log_level: str, gw_group: Optional[str] = None, traddr: Optional[str] = None):  # noqa
            return {"status": 0}

        result_default = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"log_level": "info"}
        )
        assert isinstance(result_default, HandleCommandResult)
        assert result_default.retval == 0
        assert result_default.stdout == "set log level to info"
        assert result_default.stderr == ''

        result_plain = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "log_level": "info"}
        )
        assert isinstance(result_plain, HandleCommandResult)
        assert result_plain.retval == 0
        assert result_plain.stdout == "set log level to info"
        assert result_plain.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_plain_output_falls_back_when_template_unresolvable(self):
        test_cmd = "nvmeof gateway set_log_level_fallback"

        class Model(NamedTuple):
            a: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="set log level to {log_level}"
        )
        def set_log_level(self, a: str):  # noqa
            return {"a": "b"}

        result_plain = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain"}
        )
        assert isinstance(result_plain, HandleCommandResult)
        assert result_plain.retval == 0
        assert result_plain.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
        assert result_plain.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_default_output_falls_back_when_template_unresolvable(self):
        test_cmd = "nvmeof gateway set_log_level_fallback_default"

        class Model(NamedTuple):
            a: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="set log level to {log_level}"
        )
        def set_log_level(self, a: str):  # noqa
            return {"a": "b"}

        result_default = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})
        assert isinstance(result_default, HandleCommandResult)
        assert result_default.retval == 0
        assert result_default.stdout == (
            "+-+\n"
            "|A|\n"
            "+-+\n"
            "|b|\n"
            "+-+"
        )
        assert result_default.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_alias_inherits_success_message_template(self):
        test_cmd = "nvmeof gateway set_log_level_main"
        test_alias = "nvmeof gw set_log_level_alias"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            alias=test_alias,
            success_message_template="set log level to {log_level}"
        )
        def set_log_level(self, log_level: str):  # noqa
            return {"status": 0}

        result_main = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "log_level": "debug"}
        )
        assert result_main.retval == 0
        assert result_main.stdout == "set log level to debug"
        assert result_main.stderr == ''

        result_alias = NvmeofCLICommand.COMMANDS[test_alias].call(
            MagicMock(),
            {"format": "plain", "log_level": "warn"}
        )
        assert result_alias.retval == 0
        assert result_alias.stdout == "set log level to warn"
        assert result_alias.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        del NvmeofCLICommand.COMMANDS[test_alias]
        assert test_cmd not in NvmeofCLICommand.COMMANDS
        assert test_alias not in NvmeofCLICommand.COMMANDS

    def test_plain_uses_success_message_fn(self):
        test_cmd = "nvmeof gw set_log_level fn"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_fn=lambda args, response: (
                f"set log level to {args.get('log_level', '')}"
                + (" for all hosts" if args.get('all_hosts') else "")
            )
        )
        def fn(self, log_level: str, all_hosts: bool = False):  # noqa
            return {"status": 0}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "log_level": "info", "all_hosts": True}
        )
        assert res.retval == 0
        assert res.stdout == "set log level to info for all hosts"
        assert res.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_template_formats_int_and_list_without_failure(self):
        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            "nvmeof mixed params",
            Model,
            success_message_template="ns {nsid} hosts {host_nqn}"
        )
        def fn(self, nsid: int, host_nqn: list[str]):  # noqa
            return {"status": 1}

        res = NvmeofCLICommand.COMMANDS["nvmeof mixed params"].call(
            MagicMock(),
            {"format": "plain", "nsid": 42, "host_nqn": ["a", "b"]}
        )
        assert res.retval == 0
        assert res.stdout == "ns 42 hosts a,b"

        del NvmeofCLICommand.COMMANDS["nvmeof mixed params"]
        assert "nvmeof mixed params" not in NvmeofCLICommand.COMMANDS

    def test_success_message_uses_default_when_cli_omits_param(self):
        class Model(NamedTuple):
            status: str

        def create(mgr, nqn: str, host_name: str, traddr: str,
                   trsvcid: int = 4420, adrfam: int = 0, gw_group: Optional[str] = None):
            return dict(status=1)

        cmd = NvmeofCLICommand(
            "nvmeof listener add",
            model=Model,
            success_message_template="Adding {nqn} listener at {traddr}:{trsvcid}: Successful"
        )
        cmd(create)

        cmd_dict = {
            "nqn": "nqn.2014-08.org.nvmexpress:uuid:1234",
            "host_name": "nvme-host-1",
            "traddr": "10.0.0.5",
            # 'trsvcid' omitted
            # 'adrfam' omitted
        }

        result = cmd.call(mgr=None, cmd_dict=cmd_dict, inbuf=None)
        assert result.retval == 0
        assert result.stderr == ""
        assert result.stdout == (
            "Adding nqn.2014-08.org.nvmexpress:uuid:1234 listener at 10.0.0.5:4420: Successful"
        )

    def test_success_message_cli_value_overrides_default(self):
        class Model(NamedTuple):
            status: str

        def create(mgr, nqn: str, host_name: str, traddr: str,
                   trsvcid: int = 4420, adrfam: int = 0, gw_group: Optional[str] = None):
            return dict(status=1)

        cmd = NvmeofCLICommand(
            "nvmeof listener add",
            model=Model,
            success_message_template="Adding {nqn} listener at {traddr}:{trsvcid}: Successful"
        )
        cmd(create)

        cmd_dict = {
            "nqn": "nqn.2014-08.org.nvmexpress:uuid:abcd",
            "host_name": "nvme-host-2",
            "traddr": "192.168.1.10",
            "trsvcid": 8009,  # override default 4420
        }

        result = cmd.call(mgr=None, cmd_dict=cmd_dict, inbuf=None)
        assert result.retval == 0
        assert result.stderr == ""
        assert result.stdout == (
            "Adding nqn.2014-08.org.nvmexpress:uuid:abcd listener at 192.168.1.10:8009: Successful"
        )

    def test_defaults_allow_none_and_template_does_not_crash(self):
        class Model(NamedTuple):
            status: str

        def create_with_none(
            mgr,
            nqn: str,
            traddr: str,
            trsvcid: int = 4420,
            gw_group: Optional[str] = None,  # None default intentionally used
        ):
            return dict(status=1)

        cmd = NvmeofCLICommand(
            "nvmeof listener add",
            model=Model,
            success_message_template="Adding {nqn} listener at {traddr}:{trsvcid} gw={gw_group}: Successful",
        )
        cmd(create_with_none)

        cmd_dict = {
            "nqn": "nqn.none.test",
            "traddr": "127.0.0.1",
            # 'gw_group' omitted; None default should be injected
        }

        result = cmd.call(mgr=None, cmd_dict=cmd_dict, inbuf=None)
        assert result.retval == 0
        assert result.stderr == ""
        assert result.stdout == (
            "Adding nqn.none.test listener at 127.0.0.1:4420 gw=None: Successful"
        )

    def test_template_can_use_response_fields(self):
        test_cmd = "nvmeof show op status"

        class Model(NamedTuple):
            status: str
            message: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="operation {op} finished with status {message}"
        )
        def op(self, op: str):
            return {"status": 1, "message": "done"}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "op": "rebuild"}
        )
        assert res.retval == 0
        assert res.stdout == "operation rebuild finished with status done"
        assert res.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS



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

    def test_field_transformation_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: Annotated[int, CliFieldTransformer(lambda x: 5)]

        data = {'name': 'Alice', 'age': 30}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+---+\n'
            '|Name |Age|\n'
            '+-----+---+\n'
            '|Alice|5  |\n'
            '+-----+---+'
        )

    def test_field_transformation_with_override_header_annotation(self):
        class Sample(NamedTuple):
            name: str
            age: Annotated[int, CliFieldTransformer(lambda x: 5), CliHeader("bla")]

        data = {'name': 'Alice', 'age': 30}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+---+\n'
            '|Name |Bla|\n'
            '+-----+---+\n'
            '|Alice|5  |\n'
            '+-----+---+'
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

    def test_flatten_internal_fields_annotation(self):
        class SampleInternal(NamedTuple):
            surname: str
            height: int

        class Sample(NamedTuple):
            name: str
            age: int
            sample_internal: Annotated[SampleInternal, CliFlags.PROMOTE_INTERNAL_FIELDS]

        class SampleList(NamedTuple):
            status: int
            error_message: str
            samples: Annotated[List[Sample], CliFlags.EXCLUSIVE_LIST]

        data = {"status": 0, "error_message": '',
                "samples": [{'name': 'Alice', 'age': 30,
                             "sample_internal": {"surname": "cohen", "height": 170}},
                            {'name': 'Bob', 'age': 40,
                             "sample_internal": {"surname": "levi", "height": 182}}]}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, SampleList)
        assert output == (
            '+-----+---+-------+------+\n'
            '|Name |Age|Surname|Height|\n'
            '+-----+---+-------+------+\n'
            '|Alice|30 |cohen  |170   |\n'
            '|Bob  |40 |levi   |182   |\n'
            '+-----+---+-------+------+'
        )

    def test_enum_type(self):
        from enum import Enum

        class SampleEnum(Enum):
            test = 1
            bla = 2

        class Sample(NamedTuple):
            name: str
            state: SampleEnum

        data = {'name': 'Alice', 'state': 2}

        formatter = AnnotatedDataTextOutputFormatter()
        output = formatter.format_output(data, Sample)
        assert output == (
            '+-----+-----+\n'
            '|Name |State|\n'
            '+-----+-----+\n'
            '|Alice|bla  |\n'
            '+-----+-----+'
        )


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
