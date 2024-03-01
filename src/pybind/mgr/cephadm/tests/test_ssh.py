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

        # Test case 1: command failure
        run_test('test1', FakeConn(returncode=1), "Command .+ failed")

        # Test case 2: connection error
        run_test('test2', FakeConn(exception=asyncssh.ChannelOpenError(1, "", "")), "Unable to reach remote host test2.")

        # Test case 3: asyncssh ProcessError
        stderr = "my-process-stderr"
        run_test('test3', FakeConn(exception=asyncssh.ProcessError(returncode=3,
                                                                   env="",
                                                                   command="",
                                                                   subsystem="",
                                                                   exit_status="",
                                                                   exit_signal="",
                                                                   stderr=stderr,
                                                                   stdout="")), f"Cannot execute the command.+{stderr}")
        # Test case 4: generic error
        run_test('test4', FakeConn(exception=Exception), "Generic error while executing command.+")


@pytest.mark.skipif(ConnectionLost is not None, reason='asyncssh')
class TestWithoutSSH:
    def test_can_run(self, cephadm_module: CephadmOrchestrator):
        assert cephadm_module.can_run() == (False, "loading asyncssh library:No module named 'asyncssh'")
