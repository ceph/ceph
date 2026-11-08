import asyncio

import asyncssh
from asyncssh.process import SSHCompletedProcess
from unittest import mock
try:
    # AsyncMock was not added until python 3.8
    from unittest.mock import AsyncMock
except ImportError:
    from asyncmock import AsyncMock
except ImportError:
    AsyncMock = None
import pytest


try:
    from asyncssh.misc import ConnectionLost
except ImportError:
    ConnectionLost = None

from ceph.deployment.hostspec import HostSpec

from cephadm import CephadmOrchestrator
from cephadm.serve import CephadmServe
from cephadm.tests.fixtures import with_host, wait, async_side_effect
from cephadm.utils import cephadmNoImage
from orchestrator import OrchestratorError


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestWithSSH:
    @mock.patch("cephadm.ssh.SSHManager._execute_command")
    @mock.patch("cephadm.ssh.SSHManager._check_execute_command")
    def test_offline(self, check_execute_command, execute_command, cephadm_module):
        check_execute_command.side_effect = async_side_effect('')
        execute_command.side_effect = async_side_effect(('', '', 0))

        if not AsyncMock:
            # can't run this test if we could not import AsyncMock
            return
        mock_connect = AsyncMock(return_value='')
        with mock.patch("asyncssh.connect", new=mock_connect) as asyncssh_connect:
            with with_host(cephadm_module, 'test'):
                asyncssh_connect.side_effect = ConnectionLost('reason')
                code, out, err = cephadm_module.check_host('test')
                assert out == ''
                assert "Failed to connect to test at address (1::4)" in err

                out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
                assert out == HostSpec('test', '1::4', status='Offline').to_json()

                asyncssh_connect.return_value = mock.MagicMock()
                asyncssh_connect.side_effect = None
                assert CephadmServe(cephadm_module)._check_host('test') is None
                out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
                assert out == HostSpec('test', '1::4').to_json()

    def test_ssh_remote_cmds_execution(self, cephadm_module):

        if not AsyncMock:
            # can't run this test if we could not import AsyncMock
            return

        class FakeConn:
            def __init__(self, exception=None, returncode=0):
                self.exception = exception
                self.returncode = returncode

            async def run(self, *args, **kwargs):
                if self.exception:
                    raise self.exception
                else:
                    return SSHCompletedProcess(returncode=self.returncode, stdout="", stderr="")

            async def close(self):
                pass

        def run_test(host, conn, expected_error):
            mock_connect = AsyncMock(return_value=conn)
            with pytest.raises(OrchestratorError, match=expected_error):
                with mock.patch("asyncssh.connect", new=mock_connect):
                    with with_host(cephadm_module, host):
                        CephadmServe(cephadm_module)._check_host(host)

        # Test case 1: connection error
        run_test('test1', FakeConn(exception=asyncssh.ChannelOpenError(1, "", "")), "Unable to reach remote host test1")

        # Test case 2: asyncssh ProcessError
        stderr = "my-process-stderr"
        run_test('test2', FakeConn(exception=asyncssh.ProcessError(returncode=3,
                                                                   env="",
                                                                   command="",
                                                                   subsystem="",
                                                                   exit_status="",
                                                                   exit_signal="",
                                                                   stderr=stderr,
                                                                   stdout="")), f"cannot execute the command.+{stderr}")
        # Test case 4: generic error
        run_test('test4', FakeConn(exception=Exception), "Generic error Exception while executing command.+")


@pytest.mark.skipif(ConnectionLost is not None, reason='asyncssh')
class TestWithoutSSH:
    def test_can_run(self, cephadm_module: CephadmOrchestrator):
        assert cephadm_module.can_run() == (False, "loading asyncssh library:No module named 'asyncssh'")


def test_remote_command():
    from cephadm.ssh import RemoteCommand, Executables

    assert list(RemoteCommand(Executables.TRUE)) == ['true']
    assert list(RemoteCommand(Executables.RM, ['-rf', '/tmp/blat'])) == [
        'rm',
        '-rf',
        '/tmp/blat',
    ]


@mock.patch("cephadm.offline_watcher.CephadmServe._run_cephadm")
def test_offline_watcher_uses_cephadm_check_online(run_cephadm, cephadm_module):
    run_cephadm.side_effect = async_side_effect(([''], [''], 0))

    with with_host(cephadm_module, 'test'):
        run_cephadm.reset_mock()
        cephadm_module.offline_watcher.check_host('test')

    run_cephadm.assert_called_once_with(
        'test', cephadmNoImage, ['_orch', 'check-online'], [],
        no_fsid=True, log_output=cephadm_module.log_refresh_metadata
    )


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestAsyncSSHLogLevel:
    """Test that asyncssh log verbosity is controlled by the module options
    asyncssh_log_level and asyncssh_debug_level rather than being hardcoded.
    """

    @mock.patch("asyncssh.set_debug_level")
    @mock.patch("asyncssh.set_log_level")
    @mock.patch("asyncssh.connect")
    def test_default_options_reset_debug_level(
        self, mock_connect, mock_set_log_level, mock_set_debug_level, cephadm_module
    ):
        """With the shipped defaults (INFO, debug_level=0) set_log_level must
        be called with INFO and set_debug_level must be called with asyncssh's
        own default (1) so the global debug level is not left stuck at any
        previously-set value."""
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())

        defaults = {o['name']: o.get('default') for o in cephadm_module.MODULE_OPTIONS}
        assert defaults['asyncssh_log_level'] == 'INFO'
        assert defaults['asyncssh_debug_level'] == 0
        cephadm_module.asyncssh_log_level = defaults['asyncssh_log_level']
        cephadm_module.asyncssh_debug_level = defaults['asyncssh_debug_level']

        cephadm_module.wait_async(cephadm_module.ssh._remote_connection('test', '1::4'))

        mock_set_log_level.assert_called_with('INFO')
        # 0 is not a valid asyncssh debug level; we pass 1 (asyncssh default)
        # so the global state is always explicitly reset on each connection.
        mock_set_debug_level.assert_called_with(1)

    @pytest.mark.parametrize("log_level", ['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    @mock.patch("asyncssh.set_debug_level")
    @mock.patch("asyncssh.set_log_level")
    @mock.patch("asyncssh.connect")
    def test_log_level_option_is_forwarded(
        self, mock_connect, mock_set_log_level, mock_set_debug_level, cephadm_module, log_level
    ):
        """asyncssh.set_log_level must be called with whatever value the
        module option asyncssh_log_level holds. With debug_level=0,
        set_debug_level must be called with 1 (asyncssh's own default)."""
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())

        cephadm_module.asyncssh_log_level = log_level
        cephadm_module.asyncssh_debug_level = 0

        cephadm_module.wait_async(cephadm_module.ssh._remote_connection('test', '1::4'))

        mock_set_log_level.assert_called_with(log_level)
        mock_set_debug_level.assert_called_with(1)

    @pytest.mark.parametrize("debug_level", [1, 2, 3])
    @mock.patch("asyncssh.set_debug_level")
    @mock.patch("asyncssh.set_log_level")
    @mock.patch("asyncssh.connect")
    def test_debug_level_option_is_forwarded(
        self, mock_connect, mock_set_log_level, mock_set_debug_level, cephadm_module, debug_level
    ):
        """asyncssh.set_debug_level must be called with the exact integer
        value from asyncssh_debug_level when it is 1-3."""
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())

        cephadm_module.asyncssh_log_level = 'DEBUG'
        cephadm_module.asyncssh_debug_level = debug_level

        cephadm_module.wait_async(cephadm_module.ssh._remote_connection('test', '1::4'))

        mock_set_log_level.assert_called_with('DEBUG')
        mock_set_debug_level.assert_called_with(debug_level)


class AddressConnection:
    def __init__(self, addr):
        self.addr = addr
        self.closed = False

    def is_closed(self):
        return self.closed

    def close(self):
        self.closed = True

    async def run(self, *args, **kwargs):
        if self.closed:
            raise asyncssh.ConnectionLost('connection closed')
        return SSHCompletedProcess(stdout=self.addr, stderr='', returncode=0)


@pytest.mark.parametrize('connect_fails', [True, False])
def test_address_change_ignores_old_connection_result(cephadm_module, connect_fails):
    from cephadm.ssh import Executables, HostConnectionError, RemoteCommand

    mgr = cephadm_module
    mgr.inventory.add_host(HostSpec('test', '192.0.2.1'))

    async def scenario():
        started = asyncio.Event()
        finish = asyncio.Event()
        old_conn = AddressConnection('192.0.2.1')

        async def connect(addr, **kwargs):
            if addr == '192.0.2.1':
                started.set()
                await finish.wait()
                if connect_fails:
                    raise OSError('old address unreachable')
                return old_conn
            assert addr == '192.0.2.2'
            return AddressConnection(addr)

        with mock.patch('asyncssh.connect', side_effect=connect):
            pending = asyncio.create_task(mgr.ssh._remote_connection('test'))
            await started.wait()
            mgr.inventory.set_addr('test', '192.0.2.2')
            await mgr.ssh._reset_con('test')
            await mgr.ssh._remote_connection('test')
            finish.set()
            with pytest.raises(HostConnectionError):
                await pending
            assert not mgr.offline_hosts
            out, _, _ = await mgr.ssh._execute_command('test', RemoteCommand(Executables.TRUE))
            assert out == '192.0.2.2'
            if not connect_fails:
                assert old_conn.closed

    mgr.wait_async(scenario())


@pytest.mark.parametrize('error', [RuntimeError, OSError])
def test_address_change_ignores_old_command_failure(cephadm_module, error):
    from cephadm.ssh import Executables, HostConnectionError, RemoteCommand

    mgr = cephadm_module
    mgr.inventory.add_host(HostSpec('test', '192.0.2.1'))

    async def scenario():
        started = asyncio.Event()
        finish = asyncio.Event()

        async def fail_command(*args, **kwargs):
            started.set()
            await finish.wait()
            raise error('old command failed')

        async def connect(addr, **kwargs):
            conn = AddressConnection(addr)
            if addr == '192.0.2.1':
                conn.run = fail_command
            return conn

        with mock.patch('asyncssh.connect', side_effect=connect):
            pending = asyncio.create_task(
                mgr.ssh._execute_command('test', RemoteCommand(Executables.TRUE)))
            await started.wait()
            mgr.inventory.set_addr('test', '192.0.2.2')
            await mgr.ssh._reset_con('test')
            current = await mgr.ssh._remote_connection('test')
            finish.set()
            if error is OSError:
                assert (await pending)[0] == '192.0.2.2'
            else:
                with pytest.raises(HostConnectionError, match='old command failed'):
                    await pending
            assert not current.closed
            assert not mgr.offline_hosts
            out, _, _ = await mgr.ssh._execute_command('test', RemoteCommand(Executables.TRUE))
            assert out == '192.0.2.2'

    mgr.wait_async(scenario())


@mock.patch('cephadm.utils.resolve_ip', return_value='192.0.2.2')
def test_address_change_clears_offline_from_validation(resolve_ip, cephadm_module):
    mgr = cephadm_module
    mgr.inventory.add_host(HostSpec('test', '192.0.2.1'))

    async def check_host(*args, **kwargs):
        assert kwargs['addr'] == '192.0.2.2'
        # An old SSH failure arrives just before validation finishes.
        mgr.offline_hosts.add('test')
        return [], [], 0

    with mock.patch('cephadm.serve.CephadmServe._run_cephadm', side_effect=check_host):
        wait(mgr, mgr.update_host_addr('test', '192.0.2.2'))
    host = wait(mgr, mgr.get_hosts())[0]
    assert (host.addr, host.status) == ('192.0.2.2', '')
