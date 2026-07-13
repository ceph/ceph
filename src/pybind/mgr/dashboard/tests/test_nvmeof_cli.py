# pylint: disable=too-many-lines
import errno
import json
import unittest
from typing import Annotated, List, NamedTuple, Optional
from unittest.mock import MagicMock

import pytest
from mgr_module import HandleCommandResult

from ..cli import DBCLICommand
from ..controllers import EndpointDoc
from ..exceptions import DashboardException
from ..model.nvmeof import CliEmptyMessage, CliFieldTransformer, CliFlags, CliHeader
from ..services.nvmeof_cli import AnnotatedDataTextOutputFormatter, \
    NvmeofCLICommand, convert_from_bytes, convert_to_bytes, \
    format_host_updates, resolve_nvmeof_server_address
from ..tests import CLICommandTestMixin


@pytest.fixture(scope="class", name="sample_command")
def fixture_sample_command():
    test_cmd = "test command"

    class Model(NamedTuple):
        a: str
        b: int

    @NvmeofCLICommand(test_cmd, Model)
    def func(_):  # pylint: disable=unused-argument, unused-variable
        return {'a': '1', 'b': 2}
    yield test_cmd
    del NvmeofCLICommand.COMMANDS[test_cmd]
    assert test_cmd not in NvmeofCLICommand.COMMANDS


@pytest.fixture(name='base_call_mock')
def fixture_base_call_mock(monkeypatch):
    mock_result = {'a': 'b'}
    super_mock = MagicMock()
    super_mock.return_value = mock_result
    monkeypatch.setattr(DBCLICommand, 'call', super_mock)
    return super_mock


@pytest.fixture(name='base_call_return_none_mock')
def fixture_base_call_return_none_mock(monkeypatch):
    mock_result = None
    super_mock = MagicMock()
    super_mock.return_value = mock_result
    monkeypatch.setattr(DBCLICommand, 'call', super_mock)
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
        assert result.stdout == ''
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
        def func(_):  # pylint: disable=unused-argument, unused-variable
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
        def func(_):  # pylint: disable=unused-argument, unused-variable
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
        def func(_):  # pylint: disable=unused-argument, unused-variable
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
    # pylint: disable=unused-argument, unused-variable

    def test_plain_output_uses_success_message_template(self):
        test_cmd = "nvmeof set_log_level"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="set log level to {log_level}"
        )
        def set_log_level(self, log_level: str, gw_group: Optional[str] = None,
                          traddr: Optional[str] = None):
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
        def set_log_level(self, a: str):
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
        def set_log_level(self, a: str):
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
        def set_log_level(self, log_level: str):
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

    def test_plain_uses_success_message_map_callable(self):
        test_cmd = "nvmeof gw set_log_level map callable"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="set log level to {log_level}{suffix}",
            success_message_map={
                "suffix": lambda _v, f: " for all hosts" if f.get("all_hosts") else ""
            }
        )
        def fn(self, log_level: str, all_hosts: bool = False):
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

    def test_success_message_map_dict_maps_exact_values(self):
        test_cmd = "nvmeof ns change_visibility map dict"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template='visibility "{auto_visible}": Successful',
            success_message_map={
                "auto_visible": {
                    True: "visible to all hosts",
                    False: "visible to selected hosts",
                }
            }
        )
        def fn(self, auto_visible: bool):
            return {"status": 0}

        res_true = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "auto_visible": True}
        )
        assert res_true.retval == 0
        assert res_true.stdout == 'visibility "visible to all hosts": Successful'

        res_false = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "auto_visible": False}
        )
        assert res_false.retval == 0
        assert res_false.stdout == 'visibility "visible to selected hosts": Successful'

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_success_message_map_dict_missing_key_leaves_raw_value(self):
        test_cmd = "nvmeof map dict missing key"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="val {x}",
            success_message_map={
                "x": {1: "one"}
            }
        )
        def fn(self, x: int):
            return {"status": 0}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "x": 2}
        )
        assert res.retval == 0
        assert res.stdout == "val 2"

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_success_message_map_dict_value_callable(self):
        test_cmd = "nvmeof map dict value callable"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="host {host_name}",
            success_message_map={
                "host_name": {
                    "*": "for all hosts",
                    "h1": (lambda v, _f: f"for host {v}"),
                }
            }
        )
        def fn(self, host_name: str):
            return {"status": 0}

        res_star = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "host_name": "*"}
        )
        assert res_star.stdout == "host for all hosts"

        res_h1 = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "host_name": "h1"}
        )
        assert res_h1.stdout == "host for host h1"

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_success_message_map_supports_derived_fields(self):
        test_cmd = "nvmeof map derived field"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="msg {derived}",
            success_message_map={
                "derived": lambda _v, f: f"nqn={f.get('nqn')}",
            }
        )
        def fn(self, nqn: str):
            return {"status": 0}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "nqn": "subsys1"}
        )
        assert res.retval == 0
        assert res.stdout == "msg nqn=subsys1"

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_alias_inherits_success_message_map(self):
        test_cmd = "nvmeof map alias main"
        test_alias = "nvmeof map alias alias"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            alias=test_alias,
            success_message_template="lvl {log_level}{suffix}",
            success_message_map={
                "suffix": lambda _v, f: "!" if f.get("urgent") else "."
            }
        )
        def fn(self, log_level: str, urgent: bool = False):
            return {"status": 0}

        res_main = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "log_level": "debug", "urgent": True}
        )
        assert isinstance(res_main, HandleCommandResult)
        assert res_main.retval == 0
        assert res_main.stdout == "lvl debug!"
        assert res_main.stderr == ''

        res_alias = NvmeofCLICommand.COMMANDS[test_alias].call(
            MagicMock(),
            {"format": "plain", "log_level": "warn", "urgent": False}
        )
        assert isinstance(res_alias, HandleCommandResult)
        assert res_alias.retval == 0
        assert res_alias.stdout == "lvl warn."
        assert res_alias.stderr == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        del NvmeofCLICommand.COMMANDS[test_alias]
        assert test_cmd not in NvmeofCLICommand.COMMANDS
        assert test_alias not in NvmeofCLICommand.COMMANDS

    def test_map_failure_does_not_break_template_rendering(self):
        test_cmd = "nvmeof map failure fallback"

        class Model(NamedTuple):
            a: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="value {a}",
            success_message_map={
                "a": lambda _v, _f: 1 / 0,  # force exception
            }
        )
        def fn(self, a: str):
            return {"a": "b"}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "a": "ignored"}
        )
        assert res.retval == 0
        assert res.stdout == "value b"
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
        def fn(self, nsid: int, host_nqn: list[str]):
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
            "trsvcid": 8009,
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
            gw_group: Optional[str] = None,
        ):
            return dict(status=1)

        cmd = NvmeofCLICommand(
            "nvmeof listener add",
            model=Model,
            success_message_template=(
                "Adding {nqn} listener at {traddr}:{trsvcid} "
                "gw={gw_group}: Successful"
            ),
        )
        cmd(create_with_none)

        cmd_dict = {
            "nqn": "nqn.none.test",
            "traddr": "127.0.0.1",
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

    def test_success_message_fn_overrides_template(self):
        test_cmd = "nvmeof success fn overrides template"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="TEMPLATE {nqn}",
            success_message_fn=lambda f: f"FN {f.get('nqn')}"
        )
        def fn(self, nqn: str):
            return {"status": 0}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "nqn": "subsysA"}
        )
        assert res.retval == 0
        assert res.stderr == ''
        assert res.stdout == "FN subsysA"

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_success_message_fn_can_use_response_fields(self):
        test_cmd = "nvmeof success fn response fields"

        class Model(NamedTuple):
            status: str
            message: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="TEMPLATE {message}",
            success_message_fn=lambda f: f"FN message={f.get('message')}"
        )
        def fn(self, op: str):
            return {"status": 0, "message": "done"}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "op": "ignored"}
        )
        assert res.retval == 0
        assert res.stderr == ''
        assert res.stdout == "FN message=done"

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS

    def test_success_message_fn_failure_falls_back_to_default_formatting(self):
        test_cmd = "nvmeof success fn failure fallback"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="TEMPLATE {nqn}",
            success_message_fn=lambda _f: 1 / 0
        )
        def fn(self, nqn: str):
            return {"status": 0}

        res = NvmeofCLICommand.COMMANDS[test_cmd].call(
            MagicMock(),
            {"format": "plain", "nqn": "subsysB"}
        )
        assert res.retval == 0
        assert res.stderr == ""
        assert res.stdout == ''

        del NvmeofCLICommand.COMMANDS[test_cmd]
        assert test_cmd not in NvmeofCLICommand.COMMANDS


class TestNvmeofCLICommandDeprecatedParams:
    @staticmethod
    def _make_cmd(test_cmd, deprecated_params=None, alias=None):
        class Model(NamedTuple):
            status: str

        kwargs = {}
        if deprecated_params is not None:
            kwargs['deprecated_params'] = deprecated_params
        if alias is not None:
            kwargs['alias'] = alias

        @NvmeofCLICommand(test_cmd, Model, **kwargs)
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            return {'status': 'ok'}

        return test_cmd

    @staticmethod
    def _cleanup(*cmds):
        for cmd in cmds:
            NvmeofCLICommand.COMMANDS.pop(cmd, None)

    def test_deprecated_params_stored_on_instance(self):
        test_cmd = "test deprecated store"
        mapping = {"old_param": "old_param is deprecated, use new_param"}
        self._make_cmd(test_cmd, deprecated_params=mapping)
        try:
            cmd = NvmeofCLICommand.COMMANDS[test_cmd]
            assert cmd._deprecated_params == mapping
        finally:
            self._cleanup(test_cmd)

    def test_no_deprecated_params_defaults_to_empty_dict(self):
        test_cmd = "test deprecated empty"
        self._make_cmd(test_cmd)
        try:
            cmd = NvmeofCLICommand.COMMANDS[test_cmd]
            assert cmd._deprecated_params == {}
        finally:
            self._cleanup(test_cmd)

    def test_none_deprecated_params_defaults_to_empty_dict(self):
        test_cmd = "test deprecated none"
        self._make_cmd(test_cmd, deprecated_params=None)
        try:
            cmd = NvmeofCLICommand.COMMANDS[test_cmd]
            assert cmd._deprecated_params == {}
        finally:
            self._cleanup(test_cmd)

    def test_alias_inherits_deprecated_params(self):
        test_cmd = "test deprecated alias main"
        test_alias = "test deprecated alias alias"
        mapping = {"old_param": "old_param is deprecated, use new_param"}
        self._make_cmd(test_cmd, deprecated_params=mapping, alias=test_alias)
        try:
            assert NvmeofCLICommand.COMMANDS[test_alias]._deprecated_params == mapping
        finally:
            self._cleanup(test_cmd, test_alias)

    def test_alias_without_deprecated_params_has_empty_dict(self):
        test_cmd = "test no deprecated alias main"
        test_alias = "test no deprecated alias alias"
        self._make_cmd(test_cmd, alias=test_alias)
        try:
            assert NvmeofCLICommand.COMMANDS[test_alias]._deprecated_params == {}
        finally:
            self._cleanup(test_cmd, test_alias)

    def test_warning_emitted_when_deprecated_param_is_supplied(self):
        test_cmd = "test deprecated warn supplied"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_no_warning_when_deprecated_param_is_absent(self):
        test_cmd = "test deprecated warn absent"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo"}
            )
            assert result.retval == 0
            assert "Warning" not in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_no_warning_when_deprecated_params_is_empty(self):
        test_cmd = "test no deprecated params"
        self._make_cmd(test_cmd)
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "Warning" not in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_multiple_deprecated_params_each_emit_warning(self):
        test_cmd = "test deprecated multi warn"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={
                "old_a": "--old-a is deprecated, use --new-a",
                "old_b": "--old-b is deprecated, use --new-b",
            }
        )
        def fn(self, new_a: str, old_a: Optional[str] = None,  # pylint: disable=unused-variable
               old_b: Optional[str] = None):
            return {'status': 'ok'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_a": "x", "old_a": "y", "old_b": "z"}
            )
            assert result.retval == 0
            assert "\nWarning: --old-a is deprecated, use --new-a" in result.stdout
            assert "\nWarning: --old-b is deprecated, use --new-b" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_only_supplied_deprecated_param_warns(self):
        test_cmd = "test deprecated partial warn"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={
                "old_a": "--old-a is deprecated, use --new-a",
                "old_b": "--old-b is deprecated, use --new-b",
            }
        )
        def fn(self, new_a: str, old_a: Optional[str] = None,  # pylint: disable=unused-variable
               old_b: Optional[str] = None):
            return {'status': 'ok'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_a": "x", "old_a": "y"}
            )
            assert result.retval == 0
            assert "\nWarning: --old-a is deprecated, use --new-a" in result.stdout
            assert "--old-b" not in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_warning_not_emitted_for_json_format(self):
        test_cmd = "test deprecated json no warn"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"format": "json", "new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "Warning" not in result.stdout
            parsed = json.loads(result.stdout)
            assert "Warning" not in str(parsed)
        finally:
            self._cleanup(test_cmd)

    def test_warning_not_emitted_for_yaml_format(self):
        test_cmd = "test deprecated yaml no warn"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"format": "yaml", "new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "Warning" not in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_warning_emitted_for_default_format(self):
        """Default format (no 'format' key) also emits warnings."""
        test_cmd = "test deprecated default format warn"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_warning_emitted_for_explicit_plain_format(self):
        test_cmd = "test deprecated plain format warn"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"format": "plain", "new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_deprecated_warning_and_error_message_warning_both_appear(self):
        test_cmd = "test deprecated coexist error message"

        class Model(NamedTuple):
            status: str
            error_message: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            return {'status': 'ok', 'error_message': 'something to note from gRPC'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert "\nWarning: something to note from gRPC" in result.stdout
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_error_message_warning_without_deprecated_param(self):
        test_cmd = "test error message no deprecated"

        class Model(NamedTuple):
            status: str
            error_message: str

        @NvmeofCLICommand(test_cmd, Model)
        def fn(self, new_param: str):  # pylint: disable=unused-variable
            return {'status': 'ok', 'error_message': 'note from gRPC'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo"}
            )
            assert result.retval == 0
            assert "\nWarning: note from gRPC" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_alias_emits_same_warning_as_main_command(self):
        test_cmd = "test deprecated alias warn main"
        test_alias = "test deprecated alias warn alias"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"},
            alias=test_alias
        )
        try:
            for cmd_key in (test_cmd, test_alias):
                result = NvmeofCLICommand.COMMANDS[cmd_key].call(
                    MagicMock(),
                    {"new_param": "foo", "old_param": "bar"}
                )
                assert result.retval == 0, f"failed for {cmd_key}"
                assert "\nWarning: --old-param is deprecated, please use --new-param" \
                    in result.stdout, f"warning missing for {cmd_key}"
        finally:
            self._cleanup(test_cmd, test_alias)

    def test_alias_also_suppresses_warning_when_param_absent(self):
        test_cmd = "test deprecated alias no warn main"
        test_alias = "test deprecated alias no warn alias"
        self._make_cmd(
            test_cmd,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"},
            alias=test_alias
        )
        try:
            result = NvmeofCLICommand.COMMANDS[test_alias].call(
                MagicMock(),
                {"new_param": "foo"}
            )
            assert result.retval == 0
            assert "Warning" not in result.stdout
        finally:
            self._cleanup(test_cmd, test_alias)

    def test_warning_emitted_on_failure_when_deprecated_param_supplied(self):
        test_cmd = "test deprecated warn on failure"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            raise DashboardException(msg="something went wrong", component="nvmeof",
                                     http_status_code=500)

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == -errno.EINVAL
            assert result.stdout == ''
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stderr
        finally:
            self._cleanup(test_cmd)

    def test_no_warning_on_failure_when_deprecated_param_absent(self):
        test_cmd = "test deprecated no warn on failure absent"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            raise DashboardException(msg="something went wrong", component="nvmeof",
                                     http_status_code=500)

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo"}
            )
            assert result.retval == -errno.EINVAL
            assert "Warning" not in result.stderr
        finally:
            self._cleanup(test_cmd)

    def test_warning_in_stdout_on_success_and_stderr_on_failure(self):
        test_cmd_ok = "test deprecated path ok"
        test_cmd_fail = "test deprecated path fail"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd_ok,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn_ok(self, new_param: str,  # pylint: disable=unused-variable
                  old_param: Optional[str] = None):
            return {'status': 'ok'}

        @NvmeofCLICommand(
            test_cmd_fail,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn_fail(self, new_param: str,  # pylint: disable=unused-variable
                    old_param: Optional[str] = None):
            raise DashboardException(msg="boom", component="nvmeof", http_status_code=500)

        try:
            ok_result = NvmeofCLICommand.COMMANDS[test_cmd_ok].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert ok_result.retval == 0
            assert "\nWarning: --old-param is deprecated, please use --new-param" \
                in ok_result.stdout
            assert ok_result.stderr == ''

            fail_result = NvmeofCLICommand.COMMANDS[test_cmd_fail].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert fail_result.retval == -errno.EINVAL
            assert fail_result.stdout == ''
            assert "\nWarning: --old-param is deprecated, please use --new-param" \
                in fail_result.stderr
        finally:
            self._cleanup(test_cmd_ok, test_cmd_fail)

    def test_deprecated_warning_appended_after_success_message(self):
        test_cmd = "test deprecated with success msg"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="Done: {new_param}",
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            return {'status': 'ok'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo", "old_param": "bar"}
            )
            assert result.retval == 0
            assert result.stdout.startswith("Done: foo")
            assert "\nWarning: --old-param is deprecated, please use --new-param" in result.stdout
        finally:
            self._cleanup(test_cmd)

    def test_no_warning_appended_to_success_message_when_param_absent(self):
        test_cmd = "test deprecated with success msg no warn"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            success_message_template="Done: {new_param}",
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, new_param: str,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            return {'status': 'ok'}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"new_param": "foo"}
            )
            assert result.retval == 0
            assert result.stdout == "Done: foo"
        finally:
            self._cleanup(test_cmd)

    def test_malformed_arg_in_args_map_returns_handle_command_result_not_uncaught_exception(self):
        test_cmd = "test argspec throws inside try"

        class Model(NamedTuple):
            status: str

        @NvmeofCLICommand(
            test_cmd,
            Model,
            deprecated_params={"old_param": "--old-param is deprecated, please use --new-param"}
        )
        def fn(self, count: int,  # pylint: disable=unused-variable
               old_param: Optional[str] = None):
            return {'status': 'ok'}

        try:
            # 'count' is typed as int but we pass a string value to it so
            # CephArgtype.cast_to raises ValueError inside _args_map_from_argspec
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(
                MagicMock(),
                {"count": "not-a-number", "old_param": "bar"}
            )
            assert isinstance(result, HandleCommandResult)
            assert result.retval == -errno.EINVAL
            assert result.stdout == ''
            assert result.stderr != ''
        finally:
            self._cleanup(test_cmd)


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
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group=''
        )

        config = json.loads(self.get_key('_nvmeof_config'))

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
        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof.pool',
            inbuf='http://nvmf:port',
            daemon_name='nvmeof_daemon',
            group=''
        )

        self.exec_cmd(
            'nvmeof-gateway-add',
            name='nvmeof2.pool.group',
            inbuf='http://nvmf-2:port',
            daemon_name='nvmeof_daemon_2',
            group='group'
        )

        config = json.loads(self.get_key('_nvmeof_config'))

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


class TestAnnotatedDataTextOutputFormatter:
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


class TestFormatHostUpdates:
    def test_single_wildcard(self):
        args = {"nqn": "subsys1", "host_nqn": "*"}
        out = format_host_updates(
            args,
            template_wildcard="Allowing open host access to {nqn}: Successful",
            template_item="Adding host {host_nqn} to {nqn}: Successful",
        )
        assert out == "Allowing open host access to subsys1: Successful"

    def test_single_host(self):
        args = {"nqn": "subsys1", "host_nqn": "hostA"}
        out = format_host_updates(
            args,
            template_wildcard="Allowing open host access to {nqn}: Successful",
            template_item="Adding host {host_nqn} to {nqn}: Successful",
        )
        assert out == "Adding host hostA to subsys1: Successful"

    def test_multiple_hosts_mixed_including_wildcard(self):
        args = {"nqn": "subsys1", "host_nqn": ["hostA", "*", "hostB"]}
        out = format_host_updates(
            args,
            template_wildcard="Disabling open host access to {nqn}: Successful",
            template_item="Removing host {host_nqn} access from {nqn}: Successful",
        )
        assert out == (
            "Removing host hostA access from subsys1: Successful\n"
            "Disabling open host access to subsys1: Successful\n"
            "Removing host hostB access from subsys1: Successful"
        )

    def test_none_host_arg_returns_empty_string(self):
        args = {"nqn": "subsys1", "host_nqn": None}
        out = format_host_updates(
            args,
            template_wildcard="Allowing open host access to {nqn}: Successful",
            template_item="Adding host {host_nqn} to {nqn}: Successful",
        )
        assert out == ""

    def test_missing_host_arg_returns_empty_string(self):
        args = {"nqn": "subsys1"}
        out = format_host_updates(
            args,
            template_wildcard="Allowing open host access to {nqn}: Successful",
            template_item="Adding host {host_nqn} to {nqn}: Successful",
        )
        assert out == ""

    def test_missing_nqn_renders_as_none(self):
        args = {"host_nqn": "*"}
        out = format_host_updates(
            args,
            template_wildcard="Allowing open host access to {nqn}: Successful",
            template_item="Adding host {host_nqn} to {nqn}: Successful",
        )
        assert out == "Allowing open host access to None: Successful"

    def test_custom_arg_names(self):
        args = {"subsystem": "nqn.test", "host": ["h1", "*"]}
        out = format_host_updates(
            args,
            nqn_arg="subsystem",
            host_arg="host",
            template_wildcard="W {nqn}",
            template_item="H {host_nqn} {nqn}",
        )
        assert out == "H h1 nqn.test\nW nqn.test"


class TestResolveNvmeofServerAddress:
    def test_resolve_returns_server_address_when_set(self):
        assert resolve_nvmeof_server_address(
            server_address="10.0.0.1",
            traddr=None,
            require=False,
        ) == "10.0.0.1"

    def test_resolve_uses_traddr_fallback_when_server_address_missing(self):
        assert resolve_nvmeof_server_address(
            server_address=None,
            traddr="10.0.0.2",
            require=False,
        ) == "10.0.0.2"

    def test_resolve_strips_whitespace_and_treats_empty_as_none(self):
        assert resolve_nvmeof_server_address(
            server_address="  ",
            traddr=" 10.0.0.2 ",
            require=False,
        ) == "10.0.0.2"

    def test_resolve_rejects_both_server_address_and_traddr(self):
        with pytest.raises(DashboardException) as excinfo:
            resolve_nvmeof_server_address(
                server_address="10.0.0.1",
                traddr="10.0.0.2",
                require=False,
            )

        err = excinfo.value
        assert err.status == 400
        assert err.code == "server_address_and_traddr_mutually_exclusive"

    def test_resolve_require_true_rejects_missing(self):
        with pytest.raises(DashboardException) as excinfo:
            resolve_nvmeof_server_address(
                server_address=None,
                traddr=None,
                require=True,
            )

        err = excinfo.value
        assert err.status == 400
        assert err.code == "missing_server_address"

    def test_resolve_require_false_allows_missing(self):
        assert resolve_nvmeof_server_address(
            server_address=None,
            traddr=None,
            require=False,
        ) is None


class TestCliEmptyMessage:
    def test_empty_list_with_message_returns_custom_text(self):
        test_cmd = "nvmeof test empty list message"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST, CliEmptyMessage("No items found")]

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'items': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No items found"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_empty_list_with_template_substitution(self):
        test_cmd = "nvmeof test empty list template"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST,
                             CliEmptyMessage("No items for {context}")]
            context: Annotated[str, CliFlags.DROP] = ""

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'items': [], 'context': 'test-subsystem'}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No items for test-subsystem"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_non_empty_list_returns_table(self):
        test_cmd = "nvmeof test non empty list"

        class Item(NamedTuple):
            name: str
            value: int

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST, CliEmptyMessage("No items found")]

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'items': [{'name': 'item1', 'value': 10}, {'name': 'item2', 'value': 20}]
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert 'Name' in result.stdout
            assert 'Value' in result.stdout
            assert 'item1' in result.stdout
            assert 'item2' in result.stdout
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_empty_message_with_missing_template_var_fallback(self):
        test_cmd = "nvmeof test missing template var"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST,
                             CliEmptyMessage("No items for {missing_field}")]

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'items': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert "No items for {missing_field}" in result.stdout
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_empty_message_json_format_returns_empty_list(self):
        test_cmd = "nvmeof test empty json"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST, CliEmptyMessage("No items found")]

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'items': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {'format': 'json'})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            data = json.loads(result.stdout)
            assert data == {'status': 0, 'error_message': '', 'items': []}
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_empty_message_yaml_format_returns_empty_list(self):
        test_cmd = "nvmeof test empty yaml"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST, CliEmptyMessage("No items found")]

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'items': []}

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {'format': 'yaml'})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert 'items: []' in result.stdout
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_multiple_template_variables(self):
        test_cmd = "nvmeof test multiple vars"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST,
                             CliEmptyMessage("No {item_type} for {subsystem} in {location}")]
            item_type: Annotated[str, CliFlags.DROP] = ""
            subsystem: Annotated[str, CliFlags.DROP] = ""
            location: Annotated[str, CliFlags.DROP] = ""

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'items': [],
                'item_type': 'listeners',
                'subsystem': 'nqn.test',
                'location': 'gateway1'
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No listeners for nqn.test in gateway1"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_empty_message_with_special_characters(self):
        test_cmd = "nvmeof test special chars"

        class Item(NamedTuple):
            name: str

        class TestModel(NamedTuple):
            status: int
            error_message: str
            items: Annotated[List[Item], CliFlags.EXCLUSIVE_LIST,
                             CliEmptyMessage("No items found! (subsystem: {nqn})")]
            nqn: Annotated[str, CliFlags.DROP] = ""

        @NvmeofCLICommand(test_cmd, TestModel)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'items': [],
                'nqn': 'test-nqn'
            }

        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})
            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert "No items found! (subsystem: test-nqn)" in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]


class TestCliEmptyMessageForAllListCommands:
    def test_subsystem_list_empty_message(self):
        from ..model.nvmeof import SubsystemList

        test_cmd = "nvmeof test subsystem list empty"

        @NvmeofCLICommand(test_cmd, SubsystemList)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'subsystems': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No subsystems"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_subsystem_list_non_empty_returns_table(self):
        from ..model.nvmeof import SubsystemList

        test_cmd = "nvmeof test subsystem list non empty"

        @NvmeofCLICommand(test_cmd, SubsystemList)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'subsystems': [{
                    'nqn': 'nqn.2016-06.io.spdk:cnode1',
                    'enable_ha': True,
                    'serial_number': 'SPDK00000000000001',
                    'model_number': 'SPDK bdev Controller',
                    'min_cntlid': 1,
                    'max_cntlid': 65519,
                    'namespace_count': 0,
                    'subtype': 'NVMe',
                    'max_namespaces': 256,
                    'has_dhchap_key': False,
                    'allow_any_host': False,
                    'created_without_key': False,
                    'network_mask': []
                }]
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert 'nqn.2016-06.io.spdk:cnode1' in result.stdout
            assert 'SPDK00000000000001' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_connection_list_empty_message_with_subsystem(self):
        from ..model.nvmeof import ConnectionList

        test_cmd = "nvmeof test connection list empty"

        @NvmeofCLICommand(test_cmd, ConnectionList)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'subsystem_nqn': 'nqn.2016-06.io.spdk:cnode1',
                'connections': []
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No connections for nqn.2016-06.io.spdk:cnode1"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_connection_list_non_empty_returns_table(self):
        from ..model.nvmeof import ConnectionList

        test_cmd = "nvmeof test connection list non empty"

        @NvmeofCLICommand(test_cmd, ConnectionList)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'subsystem_nqn': 'nqn.2016-06.io.spdk:cnode1',
                'connections': [{
                    'nqn': 'nqn.2014-08.org.nvmexpress:uuid:12345',
                    'traddr': '192.168.1.100',
                    'trsvcid': 4420,
                    'trtype': 'TCP',
                    'adrfam': 0,
                    'connected': True,
                    'qpairs_count': 2,
                    'controller_id': 1,
                    'use_psk': False,
                    'use_dhchap': False,
                    'dhchap_controller_origin': None,
                    'subsystem': 'nqn.2016-06.io.spdk:cnode1',
                    'disconnected_due_to_keepalive_timeout': False
                }]
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert 'nqn.2014-08.org.nvmexpress:uuid:12345' in result.stdout
            assert '192.168.1.100' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_hosts_info_empty_message_with_subsystem(self):
        from ..model.nvmeof import HostsInfo

        test_cmd = "nvmeof test hosts info empty"

        @NvmeofCLICommand(test_cmd, HostsInfo)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'allow_any_host': False,
                'subsystem_nqn': 'nqn.2016-06.io.spdk:cnode1',
                'hosts': []
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert result.stdout == "No hosts are allowed to access nqn.2016-06.io.spdk:cnode1"
            assert result.stderr == ''
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_hosts_info_non_empty_returns_table(self):
        from ..model.nvmeof import HostsInfo

        test_cmd = "nvmeof test hosts info non empty"

        @NvmeofCLICommand(test_cmd, HostsInfo)
        def func(_):  # pylint: disable=unused-argument, unused-variable
            return {
                'status': 0,
                'error_message': '',
                'allow_any_host': False,
                'subsystem_nqn': 'nqn.2016-06.io.spdk:cnode1',
                'hosts': [{
                    'nqn': 'nqn.2014-08.org.nvmexpress:uuid:host1',
                    'use_psk': False,
                    'use_dhchap': True,
                    'dhchap_controller_origin': 'controller',
                    'disconnected_due_to_keepalive_timeout': False
                }]
            }
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd].call(MagicMock(), {})

            assert isinstance(result, HandleCommandResult)
            assert result.retval == 0
            assert 'nqn.2014-08.org.nvmexpress:uuid:host1' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd]

    def test_all_empty_messages_work_with_json_format(self):
        from ..model.nvmeof import ConnectionList, HostsInfo, SubsystemList

        test_cmd_subsys = "nvmeof test subsystem json"

        @NvmeofCLICommand(test_cmd_subsys, SubsystemList)
        def func_subsys(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'subsystems': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd_subsys].call(
                MagicMock(), {'format': 'json'})
            assert '"subsystems": []' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd_subsys]

        test_cmd_conn = "nvmeof test connection json"

        @NvmeofCLICommand(test_cmd_conn, ConnectionList)
        def func_conn(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '',
                    'subsystem_nqn': 'test', 'connections': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd_conn].call(MagicMock(), {'format': 'json'})
            assert '"connections": []' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd_conn]

        test_cmd_hosts = "nvmeof test hosts json"

        @NvmeofCLICommand(test_cmd_hosts, HostsInfo)
        def func_hosts(_):  # pylint: disable=unused-argument, unused-variable
            return {'status': 0, 'error_message': '', 'allow_any_host': False,
                    'subsystem_nqn': 'test', 'hosts': []}
        try:
            result = NvmeofCLICommand.COMMANDS[test_cmd_hosts].call(MagicMock(), {'format': 'json'})
            assert '"hosts": []' in result.stdout
        finally:
            del NvmeofCLICommand.COMMANDS[test_cmd_hosts]
