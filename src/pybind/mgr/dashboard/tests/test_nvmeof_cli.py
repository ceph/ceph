import errno
import json
import unittest
from unittest.mock import MagicMock

import pytest
from mgr_module import CLICommand, HandleCommandResult

from ..services.nvmeof_cli import NvmeofCLICommand
from ..tests import CLICommandTestMixin


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
