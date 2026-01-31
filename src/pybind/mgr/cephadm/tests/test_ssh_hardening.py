#!/usr/bin/env python3
"""Tests for SSH hardening functionality in cephadm."""

import pytest
from unittest import mock
try:
    from unittest.mock import AsyncMock
except ImportError:
    from asyncmock import AsyncMock

from orchestrator import OrchestratorError
from cephadm.ssh import SSHManager, RemoteCommand, Executables
from cephadm.tests.fixtures import with_host


class TestSudoHardening:
    """Test SSH hardening functionality."""

    @pytest.fixture
    def setup_sudo_hardening(self, cephadm_module):
        """Common setup for SSH hardening tests."""
        cephadm_module.sudo_hardening = True
        cephadm_module.ssh_user = 'cephadm'
        cephadm_module.cephadm_binary_path = '/var/lib/ceph/fsid/cephadm.abc123'
        cephadm_module.invoker_path = '/usr/libexec/cephadm_invoker.py'
        return cephadm_module

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SSH connection."""
        mock_conn = AsyncMock()
        mock_conn.run.return_value = mock.Mock(
            stdout='', stderr='', returncode=0
        )
        return mock_conn

    @pytest.mark.parametrize('command', [
        lambda: RemoteCommand(RemoteCommand('python3'),
                              ['/var/lib/ceph/fsid/cephadm.abc123',
                               'check-host', '--expect-hostname', 'test-host']),
        lambda: RemoteCommand(Executables.LS, ['-la', '/tmp']),
        lambda: RemoteCommand(RemoteCommand('/var/lib/ceph/fsid/cephadm.abc123'),
                              ['version']),
    ])
    def test_unwrapped_command_errors(self, setup_sudo_hardening,
                                      mock_connection, command):
        """Test that unwrapped commands error out when SSH hardening
        is enabled."""
        ssh_manager = SSHManager(setup_sudo_hardening)

        with mock.patch.object(ssh_manager, '_remote_connection',
                               return_value=mock_connection):
            cmd = command()
            with pytest.raises(OrchestratorError,
                               match='command is not wrapped with invoker'):
                setup_sudo_hardening.wait_async(
                    ssh_manager._execute_command('test-host', cmd)
                )

    def test_sudo_hardening_disabled_no_wrapping(
        self,
        cephadm_module,
        mock_connection
    ):
        """Test that commands are not wrapped when SSH hardening
        is disabled."""
        cephadm_module.sudo_hardening = False
        cephadm_module.ssh_user = 'cephadm'
        cephadm_module.cephadm_binary_path = '/var/lib/ceph/fsid/cephadm.abc123'
        ssh_manager = SSHManager(cephadm_module)

        with mock.patch.object(ssh_manager, '_remote_connection',
                               return_value=mock_connection):
            cmd = RemoteCommand(Executables.LS, ['-la', '/tmp'])
            cephadm_module.wait_async(
                ssh_manager._execute_command('test-host', cmd)
            )

            mock_connection.run.assert_called_once()
            called_args = mock_connection.run.call_args[0][0]
            assert called_args == 'sudo ls -la /tmp'

    def test_wrapped_command_succeeds(self, setup_sudo_hardening,
                                      mock_connection):
        """Test that wrapped commands succeed when SSH hardening
        is enabled."""
        ssh_manager = SSHManager(setup_sudo_hardening)

        with mock.patch.object(ssh_manager, '_remote_connection',
                               return_value=mock_connection):
            cmd = RemoteCommand(
                Executables.INVOKER,
                ['run', '/var/lib/ceph/fsid/cephadm.abc123', '--help']
            )
            setup_sudo_hardening.wait_async(
                ssh_manager._execute_command('test-host', cmd)
            )

            mock_connection.run.assert_called()
            called_args = mock_connection.run.call_args[0][0]
            assert called_args == ('sudo /usr/libexec/cephadm_invoker.py '
                                   'run /var/lib/ceph/fsid/cephadm.abc123 --help')

    def test_check_host_with_sudo_hardening_integration(
            self, setup_sudo_hardening, mock_connection):
        """Integration test for check_host with SSH hardening enabled."""
        with mock.patch.object(setup_sudo_hardening,
                               '_prepare_new_host_for_sudo_hardening'):
            with mock.patch.object(setup_sudo_hardening.ssh, '_remote_connection',
                                   return_value=mock_connection):
                with with_host(setup_sudo_hardening, 'test-host'):
                    code, _, err = setup_sudo_hardening.check_host('test-host')
                    assert code == 0
                    assert err == ''

                    called_args = mock_connection.run.call_args[0][0]
                    assert '/usr/libexec/cephadm_invoker.py' in called_args
                    assert 'sudo' in called_args
                    assert 'run' in called_args
                    assert 'check-host' in called_args
                    assert '--expect-hostname test-host' in called_args

    def test_check_host_without_sudo_hardening(
        self,
        cephadm_module,
        mock_connection
    ):
        """Integration test for check_host with SSH hardening disabled."""
        cephadm_module.sudo_hardening = False
        cephadm_module.ssh_user = 'root'
        cephadm_module.cephadm_binary_path = '/var/lib/ceph/fsid/cephadm.abc123'

        with mock.patch.object(cephadm_module.ssh, '_remote_connection',
                               return_value=mock_connection):
            with with_host(cephadm_module, 'test-host'):
                code, _, err = cephadm_module.check_host('test-host')
                assert code == 0
                assert err == ''

                called_args = mock_connection.run.call_args[0][0]
                assert '/usr/libexec/cephadm_invoker.py' not in called_args
                assert 'sudo' not in called_args
                assert 'check-host' in called_args
                assert '--expect-hostname test-host' in called_args

    def test_execute_cephadm_exec_with_hardening(self, setup_sudo_hardening):
        """Test cephadm exec with SSH hardening enabled."""
        ssh_manager = SSHManager(setup_sudo_hardening)
        mock_conn = AsyncMock()
        mock_conn.run.return_value = mock.Mock(
            stdout='success', stderr='', returncode=0
        )

        with mock.patch.object(setup_sudo_hardening.ssh, '_remote_connection',
                               return_value=mock_conn):
            cmd = RemoteCommand(Executables.LS, ['-la', '/tmp'])
            out, err, code = ssh_manager.execute_cephadm_exec('test-host', cmd)

            called_args = mock_conn.run.call_args[0][0]
            assert called_args.startswith('sudo /usr/libexec/cephadm_invoker.py')
            assert setup_sudo_hardening.cephadm_binary_path in called_args
            assert 'exec' in called_args and '--command' in called_args
            assert (out, err, code) == ('success', '', 0)

    def test_execute_cephadm_exec_without_hardening(self, cephadm_module):
        """Test cephadm exec with SSH hardening disabled."""
        cephadm_module.sudo_hardening = False
        cephadm_module.ssh_user = 'cephadm'
        cephadm_module.cephadm_binary_path = '/var/lib/ceph/fsid/cephadm.abc123'
        ssh_manager = SSHManager(cephadm_module)

        mock_conn = AsyncMock()
        mock_conn.run.return_value = mock.Mock(
            stdout='success', stderr='', returncode=0
        )

        with mock.patch.object(cephadm_module.ssh, '_remote_connection',
                               return_value=mock_conn):
            cmd = RemoteCommand(Executables.LS, ['-la', '/tmp'])
            out, err, code = ssh_manager.execute_cephadm_exec('test-host', cmd)

            called_args = mock_conn.run.call_args[0][0]
            # When hardening is disabled, cephadm exec should use cephadm binary directly (not invoker)
            assert cephadm_module.cephadm_binary_path in called_args
            assert '/usr/libexec/cephadm_invoker.py' not in called_args
            assert 'exec' in called_args and '--command' in called_args
            assert (out, err, code) == ('success', '', 0)
