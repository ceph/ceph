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


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestSSHIdentity:
    """The cephadm SSH identity is loaded once, by _reconfig_ssh(), not by
    asyncssh.connect() for every new connection."""

    @staticmethod
    def _set_identity(cephadm_module, key, cert=None):
        cephadm_module.set_store('ssh_identity_key', key.export_private_key().decode())
        cephadm_module.set_store('ssh_identity_pub', key.export_public_key().decode())
        cephadm_module.set_store('ssh_identity_cert',
                                 cert.export_certificate().decode() if cert else None)
        cephadm_module.ssh._reconfig_ssh()

    @mock.patch("asyncssh.connect")
    def test_identity_loaded_once(self, mock_connect, cephadm_module):
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())
        key = asyncssh.generate_private_key('ssh-ed25519')
        self._set_identity(cephadm_module, key)

        client_keys = cephadm_module.ssh._client_keys
        assert client_keys is not None
        assert [k.key_public_data for k in client_keys] == [key.public_data]

        with mock.patch("asyncssh.load_keypairs", wraps=asyncssh.load_keypairs) as load:
            for host in ('host1', 'host2', 'host3'):
                cephadm_module.wait_async(
                    cephadm_module.ssh._remote_connection(host, '1::4'))
            load.assert_not_called()
        assert mock_connect.call_count == 3
        for call in mock_connect.call_args_list:
            assert call.kwargs['client_keys'] is client_keys

    def test_identity_with_certificate(self, cephadm_module):
        key = asyncssh.generate_private_key('ssh-ed25519')
        ca = asyncssh.generate_private_key('ssh-ed25519')
        cert = ca.generate_user_certificate(key, 'root', principals=['root'])
        self._set_identity(cephadm_module, key, cert)

        # as client_keys=[key file]: the key with its certificate, and alone
        client_keys = cephadm_module.ssh._client_keys
        assert client_keys is not None
        assert [k.algorithm for k in client_keys] == [
            b'ssh-ed25519-cert-v01@openssh.com', b'ssh-ed25519']
        assert all(k.key_public_data == key.public_data for k in client_keys)

    @mock.patch("asyncssh.connect")
    def test_identity_follows_key_changes(self, mock_connect, cephadm_module):
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())
        key1 = asyncssh.generate_private_key('ssh-ed25519')
        key2 = asyncssh.generate_private_key('ssh-ed25519')

        self._set_identity(cephadm_module, key1)
        self._set_identity(cephadm_module, key2)
        cephadm_module.wait_async(cephadm_module.ssh._remote_connection('host1', '1::4'))
        client_keys = mock_connect.call_args.kwargs['client_keys']
        assert [k.key_public_data for k in client_keys] == [key2.public_data]

        # key removed: back to the key file, as before
        cephadm_module.set_store('ssh_identity_key', None)
        cephadm_module.set_store('ssh_identity_pub', None)
        cephadm_module.ssh._reconfig_ssh()
        assert cephadm_module.ssh._client_keys is None

    @mock.patch("asyncssh.connect")
    def test_unloadable_identity_uses_key_file(self, mock_connect, cephadm_module, caplog):
        if not AsyncMock:
            return
        mock_connect.side_effect = async_side_effect(mock.MagicMock())
        cephadm_module.set_store('ssh_identity_key', 'not a key')
        cephadm_module.ssh._reconfig_ssh()

        assert cephadm_module.ssh._client_keys is None
        assert 'Unable to load the cephadm SSH identity' in caplog.text
        # the connection reads the key file and reports the error, as before
        cephadm_module.wait_async(cephadm_module.ssh._remote_connection('host1', '1::4'))
        assert mock_connect.call_args.kwargs['client_keys'] == [cephadm_module.tkey.name]


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestSSHConnectionReset:
    """A failure closes the connection it happened on, not a connection
    opened since then and maybe used by other commands for the host."""

    @staticmethod
    def _conn(run=None):
        conn = mock.MagicMock()
        if run is not None:
            conn.run.side_effect = run
        return conn

    def test_reset_con_given_connection(self, cephadm_module):
        ssh = cephadm_module.ssh
        failed, cached = self._conn(), self._conn()
        ssh.cons['host1'] = cached

        cephadm_module.wait_async(ssh._reset_con('host1', failed))
        failed.close.assert_called_once_with()
        cached.close.assert_not_called()
        assert ssh.cons['host1'] is cached

        cephadm_module.wait_async(ssh._reset_con('host1', cached))
        cached.close.assert_called_once_with()
        assert 'host1' not in ssh.cons

    def test_reset_con_cached_connection(self, cephadm_module):
        ssh = cephadm_module.ssh
        cached = self._conn()
        ssh.cons['host1'] = cached
        cephadm_module.wait_async(ssh._reset_con('host1'))
        cached.close.assert_called_once_with()
        assert 'host1' not in ssh.cons
        # nothing cached: nothing to do
        cephadm_module.wait_async(ssh._reset_con('host1'))

    def test_retry_closes_only_failed_connection(self, cephadm_module):
        from cephadm.ssh import RemoteCommand, Executables
        ssh = cephadm_module.ssh
        other = self._conn()

        async def fail(*args, **kwargs):
            # meanwhile, another command opened a new connection to the host
            ssh.cons['host1'] = other
            raise ConnectionLost('reason')

        failed = self._conn(run=fail)
        retry = self._conn(run=async_side_effect(
            SSHCompletedProcess(returncode=0, stdout='ok', stderr='')))
        conns = iter([failed, retry])

        async def remote_connection(host, addr=None):
            return next(conns)

        with mock.patch.object(ssh, '_remote_connection', side_effect=remote_connection):
            out, err, code = cephadm_module.wait_async(
                ssh._execute_command('host1', RemoteCommand(Executables.TRUE), addr='1::4'))
        assert (out, code) == ('ok', 0)
        failed.close.assert_called_once_with()
        other.close.assert_not_called()
        assert ssh.cons['host1'] is other

    @pytest.mark.parametrize('exc', [
        asyncssh.ChannelOpenError(1, '', ''),  # not recoverable
        asyncssh.ProcessError(env='', command='', subsystem='', exit_status=1,
                              exit_signal='', returncode=1, stdout='', stderr=''),
        Exception('boom'),
    ])
    def test_failure_closes_only_failed_connection(self, cephadm_module, exc):
        from cephadm.ssh import RemoteCommand, Executables, HostConnectionError
        ssh = cephadm_module.ssh
        other = self._conn()

        async def fail(*args, **kwargs):
            ssh.cons['host1'] = other
            raise exc

        failed = self._conn(run=fail)

        async def remote_connection(host, addr=None):
            return failed

        with mock.patch.object(ssh, '_remote_connection', side_effect=remote_connection):
            with pytest.raises(HostConnectionError):
                cephadm_module.wait_async(
                    ssh._execute_command('host1', RemoteCommand(Executables.TRUE), addr='1::4'))
        failed.close.assert_called_once_with()
        other.close.assert_not_called()
        assert ssh.cons['host1'] is other

    @mock.patch("cephadm.ssh.SSHManager._execute_command")
    def test_run_cephadm_error_keeps_connection(self, execute_command, cephadm_module):
        ssh = cephadm_module.ssh
        cached = self._conn()
        ssh.cons['host1'] = cached

        async def remote_connection(host, addr=None):
            return cached

        # an error that is not about the connection (the connection
        # errors are handled by _execute_command())
        execute_command.side_effect = OrchestratorError('not wrapped with invoker')
        with mock.patch.object(ssh, '_remote_connection', side_effect=remote_connection):
            out, err, code = cephadm_module.wait_async(CephadmServe(cephadm_module)._run_cephadm(
                'host1', cephadmNoImage, 'check-host', [], error_ok=True, no_fsid=True))
        assert code == 1
        cached.close.assert_not_called()
        assert ssh.cons['host1'] is cached


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestSSHConnectionReuse:
    """One connection per host and address, also for hosts not in the
    inventory yet."""

    @staticmethod
    def _new_conn(*args, **kwargs):
        conn = mock.MagicMock()
        conn.is_connected.return_value = True
        conn.is_closing.return_value = False
        conn.is_closed.return_value = False
        return conn

    @pytest.fixture
    def connect(self):
        conns = []

        async def _connect(*args, **kwargs):
            await asyncio.sleep(0)  # let concurrent callers interleave
            conn = self._new_conn()
            conn.addr = args[0]
            conns.append(conn)
            return conn

        with mock.patch("asyncssh.connect", side_effect=_connect) as m:
            m.conns = conns
            yield m

    def test_new_host_connection_reused(self, cephadm_module, connect):
        ssh = cephadm_module.ssh
        assert 'new1' not in cephadm_module.inventory
        c1 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        c2 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        assert c1 is c2
        assert connect.call_count == 1
        assert ssh.cons['new1'] is c1

    def test_inventory_host_connection_reused(self, cephadm_module, connect):
        ssh = cephadm_module.ssh
        cephadm_module.inventory.add_host(HostSpec('test', '1::4'))
        # no address given: the inventory one
        c1 = cephadm_module.wait_async(ssh._remote_connection('test'))
        c2 = cephadm_module.wait_async(ssh._remote_connection('test'))
        c3 = cephadm_module.wait_async(ssh._remote_connection('test', '1::4'))
        assert c1 is c2 is c3
        assert c1.addr == '1::4'
        assert connect.call_count == 1

    def test_other_address_replaces_connection(self, cephadm_module, connect):
        ssh = cephadm_module.ssh
        c1 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        c2 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.2'))
        assert c1 is not c2
        assert c2.addr == '10.0.0.2'
        c1.close.assert_called_once_with()
        c2.close.assert_not_called()
        assert ssh.cons['new1'] is c2

    def test_closed_connection_replaced(self, cephadm_module, connect):
        ssh = cephadm_module.ssh
        c1 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        c1.is_closing.return_value = True
        c2 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        assert c1 is not c2
        c1.close.assert_called_once_with()
        assert ssh.cons['new1'] is c2

    def test_concurrent_callers_share_one_connection(self, cephadm_module, connect):
        ssh = cephadm_module.ssh

        async def many():
            return await asyncio.gather(*(ssh._remote_connection('new1', '10.0.0.1')
                                          for _ in range(5)))

        conns = cephadm_module.wait_async(many())
        assert connect.call_count == 1
        assert all(c is conns[0] for c in conns)

    def test_reset_forgets_address(self, cephadm_module, connect):
        ssh = cephadm_module.ssh
        c1 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        ssh.reset_con('new1')
        c1.close.assert_called_once_with()
        assert 'new1' not in ssh.cons
        assert 'new1' not in ssh._con_addrs
        c2 = cephadm_module.wait_async(ssh._remote_connection('new1', '10.0.0.1'))
        assert c2 is not c1

    def test_new_host_check_uses_one_connection(self, cephadm_module, connect):
        # check-host on a new host without the cephadm binary: check-host
        # (127), ls (2), copy of the binary (several commands and SFTP),
        # check-host again. All on one connection.
        results = iter([127, 2])
        commands = []

        async def run(cmd, **kwargs):
            commands.append(cmd)
            return SSHCompletedProcess(returncode=next(results, 0), stdout='', stderr='')

        def new_conn(*args, **kwargs):
            conn = TestSSHConnectionReuse._new_conn()
            conn.run.side_effect = run
            sftp = conn.start_sftp_client.return_value.__aenter__.return_value
            sftp.put = AsyncMock()
            return conn

        async def _connect(*args, **kwargs):
            conn = new_conn()
            connect.conns.append(conn)
            return conn

        connect.side_effect = _connect
        out, err, code = cephadm_module.wait_async(CephadmServe(cephadm_module)._run_cephadm(
            'new1', cephadmNoImage, 'check-host', ['--expect-hostname', 'new1'],
            addr='10.0.0.1', error_ok=True, no_fsid=True))
        assert code == 0
        assert len(commands) > 5
        assert connect.call_count == 1


@pytest.mark.skipif(ConnectionLost is None, reason='no asyncssh')
class TestSSHResetWhileConnecting:
    """A connection being opened when the connections to the host are reset
    is not kept: the call that opened it gets a new connection, as if it
    came after the reset."""

    @staticmethod
    def _run_reset_during_connect(cephadm_module, reset):
        ssh = cephadm_module.ssh
        conns = []

        async def scenario():
            started, release = asyncio.Event(), asyncio.Event()

            async def connect(*args, **kwargs):
                started.set()
                await release.wait()
                conn = TestSSHConnectionReuse._new_conn()
                conns.append(conn)
                return conn

            with mock.patch("asyncssh.connect", side_effect=connect):
                task = asyncio.ensure_future(ssh._remote_connection('host1', '10.0.0.1'))
                await started.wait()
                await reset()
                release.set()
                return await task

        conn = cephadm_module.wait_async(scenario())
        return conn, conns

    def test_host_reset_while_connecting(self, cephadm_module):
        ssh = cephadm_module.ssh
        conn, conns = self._run_reset_during_connect(
            cephadm_module, lambda: ssh._reset_con('host1'))
        assert len(conns) == 2
        conns[0].close.assert_called_once_with()
        assert conn is conns[1]
        assert ssh.cons['host1'] is conns[1]
        conns[1].close.assert_not_called()

    def test_all_reset_while_connecting(self, cephadm_module):
        ssh = cephadm_module.ssh

        async def reset_all():
            ssh._reset_cons()

        conn, conns = self._run_reset_during_connect(cephadm_module, reset_all)
        assert len(conns) == 2
        conns[0].close.assert_called_once_with()
        assert conn is conns[1]
        assert ssh.cons['host1'] is conns[1]

    def test_reset_of_other_host_while_connecting(self, cephadm_module):
        ssh = cephadm_module.ssh
        conn, conns = self._run_reset_during_connect(
            cephadm_module, lambda: ssh._reset_con('host2'))
        assert len(conns) == 1
        assert ssh.cons['host1'] is conn
        conn.close.assert_not_called()

    def test_failed_connection_reset_while_connecting(self, cephadm_module):
        # closing one failed connection is not a reset of the host
        ssh = cephadm_module.ssh
        failed = TestSSHConnectionReuse._new_conn()
        conn, conns = self._run_reset_during_connect(
            cephadm_module, lambda: ssh._reset_con('host1', failed))
        assert len(conns) == 1
        assert ssh.cons['host1'] is conn

    def test_reset_cons_closes_snapshot(self, cephadm_module):
        ssh = cephadm_module.ssh
        c1, c2 = TestSSHConnectionReuse._new_conn(), TestSSHConnectionReuse._new_conn()
        ssh.cons.update({'host1': c1, 'host2': c2})
        ssh._con_addrs.update({'host1': '10.0.0.1', 'host2': '10.0.0.2'})
        generation = ssh._cons_generation
        ssh._reset_cons()
        c1.close.assert_called_once_with()
        c2.close.assert_called_once_with()
        assert ssh.cons == {} and ssh._con_addrs == {}
        assert ssh._cons_generation == generation + 1

    def _reset_inventory_host_during_connect(self, cephadm_module, change):
        # connect to an inventory host without giving its address, while
        # the host is changed in the inventory and its connections reset
        ssh = cephadm_module.ssh
        cephadm_module.inventory.add_host(HostSpec('host1', '10.0.0.1'))
        conns = []

        async def scenario():
            started, release = asyncio.Event(), asyncio.Event()

            async def connect(*args, **kwargs):
                started.set()
                await release.wait()
                conn = TestSSHConnectionReuse._new_conn()
                conn.addr = args[0]
                conns.append(conn)
                return conn

            with mock.patch("asyncssh.connect", side_effect=connect):
                task = asyncio.ensure_future(ssh._remote_connection('host1'))
                await started.wait()
                change()
                await ssh._reset_con('host1')
                release.set()
                return await task

        return scenario, conns

    def test_host_removed_while_connecting(self, cephadm_module):
        ssh = cephadm_module.ssh
        scenario, conns = self._reset_inventory_host_during_connect(
            cephadm_module, lambda: cephadm_module.inventory.rm_host('host1'))
        with pytest.raises(OrchestratorError, match='host address is empty'):
            cephadm_module.wait_async(scenario())
        assert len(conns) == 1
        conns[0].close.assert_called_once_with()
        assert 'host1' not in ssh.cons

    def test_host_addr_changed_while_connecting(self, cephadm_module):
        ssh = cephadm_module.ssh
        scenario, conns = self._reset_inventory_host_during_connect(
            cephadm_module, lambda: cephadm_module.inventory.set_addr('host1', '10.0.0.2'))
        conn = cephadm_module.wait_async(scenario())
        assert [c.addr for c in conns] == ['10.0.0.1', '10.0.0.2']
        conns[0].close.assert_called_once_with()
        assert conn is conns[1]
        assert ssh.cons['host1'] is conn and ssh._con_addrs['host1'] == '10.0.0.2'
