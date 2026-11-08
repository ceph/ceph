import enum
import logging
import os
import asyncio
import concurrent
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from threading import Thread
from contextlib import contextmanager
from io import StringIO
from shlex import quote
from typing import TYPE_CHECKING, Optional, List, Tuple, Dict, Iterator, TypeVar, Awaitable, Union, Any
from ceph.deployment.hostspec import normalize_hostname
from orchestrator import OrchestratorError
from .utils import is_fips_enabled, get_default_ssh_config

try:
    import asyncssh
except ImportError:
    asyncssh = None  # type: ignore

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from asyncssh.connection import SSHClientConnection

T = TypeVar('T')


logger = logging.getLogger(__name__)

asyncssh_logger = logging.getLogger('asyncssh')
asyncssh_logger.propagate = False


class HostConnectionError(OrchestratorError):
    def __init__(self, message: str, hostname: str, addr: str) -> None:
        super().__init__(message)
        self.hostname = hostname
        self.addr = addr


class RemoteExecutable(str):
    pass


class RemoteCommand:
    exe: RemoteExecutable
    args: List[str]

    def __init__(self, exe: RemoteExecutable, args: Optional[List[str]] = None) -> None:
        self.exe = exe
        self.args = args or []

    def __iter__(self) -> Iterator[str]:
        yield str(self.exe)
        for arg in self.args:
            yield arg

    def quoted(self) -> Iterator[str]:
        return (quote(a) for a in self)

    def __str__(self) -> str:
        return " ".join(self.quoted())

    def __repr__(self) -> str:
        # handy when debugging tests
        return f'<RemoteCommand>({self.exe!r}, {self.args!r})'

    def __eq__(self, other: object) -> bool:
        # handy when working with unit tests
        if not isinstance(other, self.__class__):
            return NotImplemented
        return other.exe == self.exe and other.args == self.args


class RemoteSudoCommand(RemoteCommand):
    use_sudo: bool = True

    def __init__(
        self, exe: RemoteExecutable, args: List[str], use_sudo: bool = True
    ) -> None:
        super().__init__(exe, args)
        self.use_sudo = use_sudo

    def __iter__(self) -> Iterator[str]:
        if self.use_sudo:
            yield 'sudo'
        for a in super().__iter__():
            yield a

    @classmethod
    def wrap(
        cls, other: RemoteCommand, use_sudo: bool = True
    ) -> 'RemoteSudoCommand':
        return cls(other.exe, other.args, use_sudo)


class Executables(RemoteExecutable, enum.Enum):
    CHMOD = RemoteExecutable('chmod')
    CHOWN = RemoteExecutable('chown')
    LS = RemoteExecutable('ls')
    MKDIR = RemoteExecutable('mkdir')
    MV = RemoteExecutable('mv')
    RM = RemoteExecutable('rm')
    SYSCTL = RemoteExecutable('sysctl')
    TOUCH = RemoteExecutable('touch')
    TRUE = RemoteExecutable('true')
    INVOKER = RemoteExecutable('/usr/libexec/cephadm_invoker.py')

    def __str__(self) -> str:
        return self.value


class EventLoopThread(Thread):

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        super().__init__(target=self._loop.run_forever)
        self.start()

    def get_result(self, coro: Awaitable[T], timeout: Optional[int] = None) -> T:
        # useful to note: This "run_coroutine_threadsafe" returns a
        # concurrent.futures.Future, rather than an asyncio.Future. They are
        # fairly similar but have a few differences, notably in our case
        # that the result function of a concurrent.futures.Future accepts
        # a timeout argument
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout)
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            # try to cancel the task before raising the exception further up
            future.cancel()
            raise


@dataclass
class _ConnectionEntry:
    task: 'asyncio.Task[SSHClientConnection]'
    waiters: int = 0


class SSHManager:

    # Retry count for connection/channel errors - easy to change
    SSH_RETRY_COUNT = 3

    # SSH Channel Open Error codes (from RFC 4254)
    # Only retry on transient/recoverable errors
    CHANNEL_OPEN_RECOVERABLE_CODES = {
        2,  # OPEN_CONNECT_FAILED - connection to target failed (may be transient)
        4,  # OPEN_RESOURCE_SHORTAGE - server lacks resources (may clear up)
    }

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self._connections: Dict[Tuple[str, str], _ConnectionEntry] = {}

    def _is_conn_valid(self, conn: "SSHClientConnection") -> bool:
        """Safely check if an AsyncSSH connection is still valid and usable."""
        try:
            if conn is None:
                return False
            if hasattr(conn, "is_connected") and not conn.is_connected():
                return False
            if hasattr(conn, "is_closing") and conn.is_closing():
                return False
            if hasattr(conn, "is_closed") and callable(conn.is_closed) and conn.is_closed():
                return False
            return True
        except Exception:
            return False

    async def _remote_connection(self,
                                 host: str,
                                 addr: Optional[str] = None,
                                 ) -> "SSHClientConnection":
        if not addr and host in self.mgr.inventory:
            addr = self.mgr.inventory.get_addr(host)
        if not addr:
            raise OrchestratorError("host address is empty")

        key = (normalize_hostname(host), addr)
        entry = self._connections.get(key)
        if entry and entry.task.done() and not self._is_conn_valid(entry.task.result()):
            self._discard_connection(key)
            entry = None
        if entry is None:
            entry = _ConnectionEntry(asyncio.create_task(self._connect(*key)))
            self._connections[key] = entry

        entry.waiters += 1
        try:
            conn = await asyncio.shield(entry.task)
            if self._connections.get(key) is not entry:
                raise HostConnectionError('SSH connection reset while connecting', host, addr)
            if self._is_inventory_addr(host, addr):
                self.mgr.offline_hosts_remove(host)
            return conn
        finally:
            entry.waiters -= 1
            if not entry.waiters and not entry.task.done():
                # The last caller timed out. Do not leave an unbounded connect running.
                if self._connections.get(key) is entry:
                    del self._connections[key]
                entry.task.cancel()

    async def _connect(self, host: str, addr: str) -> "SSHClientConnection":
        key = (host, addr)
        try:
            conn = await self._open_connection(host, addr)
        except BaseException:
            entry = self._connections.get(key)
            if entry and entry.task is asyncio.current_task():
                del self._connections[key]
            raise
        entry = self._connections.get(key)
        if entry is None or entry.task is not asyncio.current_task():
            conn.close()
            raise HostConnectionError('SSH connection reset while connecting', host, addr)
        return conn

    async def _open_connection(self, host: str, addr: str) -> "SSHClientConnection":
        local_fips = is_fips_enabled()
        target_fips = self.mgr.cache.get_host_fips_enabled(host)
        logger.debug(
            'FIPS state for SSH connection to %s: local=%s, target=%s',
            host,
            local_fips,
            target_fips,
        )

        if local_fips and self.mgr.ssh_pub:
            key_parts = self.mgr.ssh_pub.strip().split(maxsplit=1)

            if key_parts and key_parts[0] == 'ssh-ed25519':
                raise HostConnectionError(
                    'The configured cephadm SSH identity uses ED25519, '
                    'which is not supported when FIPS mode is enabled. '
                    'Replace the cephadm SSH identity with an RSA key.',
                    host,
                    addr,
                )

        assert self.mgr.ssh_user
        n = self.mgr.ssh_user + '@' + addr
        logger.debug("Opening connection to {} with ssh options '{}'".format(
            n, self.mgr._ssh_options))

        asyncssh.set_log_level(self.mgr.asyncssh_log_level)
        # asyncssh.set_debug_level() only accepts 1-3, so 0 (the "no extra
        # verbosity" default) maps to asyncssh's minimum of 1. Debug output
        # is turned off via asyncssh_log_level (INFO/WARNING/ERROR)

        asyncssh.set_debug_level(self.mgr.asyncssh_debug_level
                                 if self.mgr.asyncssh_debug_level > 0 else 1)

        with self.redirect_log(host, addr):
            ssh_options = asyncssh.SSHClientConnectionOptions(
                keepalive_interval=self.mgr.ssh_keepalive_interval,
                keepalive_count_max=self.mgr.ssh_keepalive_count_max
            )
            conn = await asyncssh.connect(addr, username=self.mgr.ssh_user, client_keys=[self.mgr.tkey.name],
                                          known_hosts=None, config=[self.mgr.ssh_config_fname],
                                          preferred_auth=['publickey'], options=ssh_options)
        return conn

    @contextmanager
    def redirect_log(self, host: str, addr: str) -> Iterator[None]:
        log_string = StringIO()
        ch = logging.StreamHandler(log_string)
        # No handler-level filter: asyncssh_log_level governs capture via the logger.
        # Do not add ch.setLevel() here; it would be a second, tighter gate that
        # silently drops records the logger already allowed.
        asyncssh_logger.addHandler(ch)

        try:
            yield
        except Exception as e:
            entry = self._connections.get((host, addr))
            if entry and entry.task is asyncio.current_task() and self._is_inventory_addr(host, addr):
                self.mgr.offline_hosts.add(host)
            msg = f'Failed to connect to {host} ({addr}): {e}'
            logger.exception(msg)
            log_content = log_string.getvalue()
            if log_content:
                logger.debug(f'SSH log for {host} ({addr}): {log_content}')
            raise HostConnectionError(msg, host, addr) from e
        finally:
            log_string.flush()
            asyncssh_logger.removeHandler(ch)

    def remote_connection(self,
                          host: str,
                          addr: Optional[str] = None,
                          ) -> "SSHClientConnection":
        with self.mgr.async_timeout_handler(host, f'ssh {host} (addr {addr})'):
            return self.mgr.wait_async(self._remote_connection(host, addr))

    def _enforce_sudo_hardening(
        self,
        host: str,
        cmd_components: RemoteCommand
    ) -> None:
        """
        Enforce that commands are wrapped with invoker when SSH hardening
        is enabled.
        """
        if not self.mgr.sudo_hardening:
            return

        is_wrapped = (
            isinstance(cmd_components.exe, RemoteExecutable)
            and str(cmd_components.exe) == str(Executables.INVOKER)
        )
        if not is_wrapped:
            msg = (f'SSH hardening is enabled but command is not '
                   f'wrapped with invoker for host {host}. '
                   f'Command: {cmd_components}')
            logger.error(msg)
            raise OrchestratorError(msg)

    async def _execute_command(self,
                               host: str,
                               cmd_components: RemoteCommand,
                               stdin: Optional[Union[str, bytes]] = None,
                               addr: Optional[str] = None,
                               log_command: Optional[bool] = True,
                               ) -> Tuple[str, str, int]:

        try:
            address = addr or self.mgr.inventory.get_addr(host)
        except Exception:
            address = host
        conn = await self._remote_connection(host, addr)
        # Enforce invoker usage if SSH hardening is enabled
        self._enforce_sudo_hardening(host, cmd_components)

        use_sudo = (self.mgr.ssh_user != 'root')
        rcmd = RemoteSudoCommand.wrap(cmd_components, use_sudo=use_sudo)
        if log_command:
            logger.debug(f'[{host}] Running command: {rcmd}')

        # Retry logic for transient connection/channel errors
        for attempt in range(self.SSH_RETRY_COUNT):
            try:
                run_kw: Dict[str, Any] = {}
                if stdin is not None:
                    run_kw['input'] = stdin
                    # Bytes stdin: use encoding=None (else asyncssh expects str).
                    if isinstance(stdin, bytes):
                        run_kw['encoding'] = None
                r = await conn.run(str(rcmd), **run_kw)
                break  # Success, exit retry loop
            # Handle retryable exceptions (connection/channel errors)
            # Note: handle these Exceptions otherwise you might get a weird error like
            # TypeError: __init__() missing 1 required positional argument: 'reason'
            # (due to the asyncssh error interacting with raise_if_exception)

            # Retryable exception types for SSH command execution
            except (
                asyncssh.ChannelOpenError,
                asyncssh.ConnectionLost,
                asyncssh.DisconnectError,
                asyncio.TimeoutError,
                OSError,
            ) as e:
                error_type = type(e).__name__
                logger.exception('Command exection failed with %s', error_type)
                # For ChannelOpenError, check if the error code is recoverable
                if isinstance(e, asyncssh.ChannelOpenError):
                    error_code = getattr(e, 'code', None)
                    logger.debug(
                        f'{error_type} (code={error_code}) on attempt '
                        f'{attempt + 1}/{self.SSH_RETRY_COUNT} '
                        f'for host {host}: {str(e)}')
                    # Check if this error code is recoverable/retryable
                    if error_code not in self.CHANNEL_OPEN_RECOVERABLE_CODES:
                        # Non-recoverable error code, don't retry
                        logger.debug(
                            f'ChannelOpenError code {error_code} is not recoverable, '
                            f'not retrying for host {host}')
                        self._discard_failed_connection(host, address, conn, mark_offline=True)
                        raise HostConnectionError(
                            f'Unable to reach remote host {host}. {str(e)}',
                            host, address)
                else:
                    logger.debug(
                        f'{error_type} on attempt {attempt + 1}/{self.SSH_RETRY_COUNT} '
                        f'for host {host}: {str(e)}')

                # Reset connection and get a new one for retry
                if attempt < self.SSH_RETRY_COUNT - 1:
                    self._discard_failed_connection(host, address, conn, mark_offline=False)
                    # Not the last attempt, try to get a new connection
                    try:
                        address = addr or self.mgr.inventory.get_addr(host)
                        conn = await self._remote_connection(host, addr)
                    except Exception as conn_e:
                        logger.debug(
                            f'Failed to re-establish connection to {host} '
                            f'on retry: {str(conn_e)}')
                        # Continue to next attempt, connection will be retried
                        continue
                else:
                    # Last attempt failed, raise the error
                    self._discard_failed_connection(host, address, conn, mark_offline=True)
                    raise HostConnectionError(
                        f'Unable to reach remote host {host} after '
                        f'{self.SSH_RETRY_COUNT} attempts. {str(e)}',
                        host, address)
            except asyncssh.ProcessError as e:
                msg = f"ProcessError cannot execute the command '{rcmd}' on the {host}. {str(e.stderr)}."
                logger.exception(msg)
                self._discard_failed_connection(host, address, conn, mark_offline=True)
                raise HostConnectionError(msg, host, address)
            except Exception as e:
                error_type = type(e).__name__
                msg = (f"Generic error {error_type} while executing command '{rcmd}' "
                       f"on the host {host}. {str(e)}.")
                logger.exception(msg)
                self._discard_failed_connection(host, address, conn, mark_offline=True)
                raise HostConnectionError(msg, host, address)

        def _rstrip(v: Union[bytes, str, None]) -> str:
            if not v:
                return ''
            if isinstance(v, str):
                return v.rstrip('\n')
            if isinstance(v, bytes):
                return v.decode().rstrip('\n')
            raise OrchestratorError(
                f'Unable to parse ssh output with type {type(v)} from remote host {host}')

        out = _rstrip(r.stdout)
        err = _rstrip(r.stderr)
        rc = r.returncode if r.returncode else 0

        return out, err, rc

    def execute_command(self,
                        host: str,
                        cmd: RemoteCommand,
                        stdin: Optional[Union[str, bytes]] = None,
                        addr: Optional[str] = None,
                        log_command: Optional[bool] = True
                        ) -> Tuple[str, str, int]:
        with self.mgr.async_timeout_handler(host, " ".join(cmd)):
            return self.mgr.wait_async(self._execute_command(host, cmd, stdin, addr, log_command))

    async def _check_execute_command(self,
                                     host: str,
                                     cmd: RemoteCommand,
                                     stdin: Optional[Union[str, bytes]] = None,
                                     addr: Optional[str] = None,
                                     log_command: Optional[bool] = True
                                     ) -> str:
        out, err, code = await self._execute_command(host, cmd, stdin, addr, log_command)
        if code != 0:
            msg = f'Command {cmd} failed. {err}'
            logger.debug(msg)
            raise OrchestratorError(msg)
        return out

    def check_execute_command(self,
                              host: str,
                              cmd: RemoteCommand,
                              stdin: Optional[Union[str, bytes]] = None,
                              addr: Optional[str] = None,
                              log_command: Optional[bool] = True,
                              ) -> str:
        with self.mgr.async_timeout_handler(host, " ".join(cmd)):
            return self.mgr.wait_async(self._check_execute_command(host, cmd, stdin, addr, log_command))

    async def _write_remote_file(self,
                                 host: str,
                                 path: str,
                                 content: bytes,
                                 mode: Optional[int] = None,
                                 uid: Optional[int] = None,
                                 gid: Optional[int] = None,
                                 addr: Optional[str] = None,
                                 ) -> None:
        """This method will be used to only write cephadm binary when sudo_hardening is disbaled.
        Other host files are written via ``cephadm deploy-file`` on the target host.
        """
        try:
            cephadm_tmp_dir = f"/tmp/cephadm-{self.mgr._cluster_fsid}"
            dirname = os.path.dirname(path)
            mkdir = RemoteCommand(Executables.MKDIR, ['-p', dirname])
            await self._check_execute_command(host, mkdir, addr=addr)
            mkdir2 = RemoteCommand(Executables.MKDIR, ['-p', cephadm_tmp_dir + dirname])
            await self._check_execute_command(host, mkdir2, addr=addr)
            tmp_path = cephadm_tmp_dir + path + '.new'
            touch = RemoteCommand(Executables.TOUCH, [tmp_path])
            await self._check_execute_command(host, touch, addr=addr)
            if self.mgr.ssh_user != 'root':
                assert self.mgr.ssh_user
                chown = RemoteCommand(
                    Executables.CHOWN,
                    ['-R', self.mgr.ssh_user, cephadm_tmp_dir]
                )
                await self._check_execute_command(host, chown, addr=addr)
                chmod = RemoteCommand(Executables.CHMOD, [str(644), tmp_path])
                await self._check_execute_command(host, chmod, addr=addr)
            with NamedTemporaryFile(prefix='cephadm-write-remote-file-') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write(content)
                f.flush()
                conn = await self._remote_connection(host, addr)
                async with conn.start_sftp_client() as sftp:
                    await sftp.put(f.name, tmp_path)
            if uid is not None and gid is not None:
                # shlex quote takes str or byte object, not int
                chown = RemoteCommand(
                    Executables.CHOWN,
                    ['-R', str(uid) + ':' + str(gid), tmp_path]
                )
                await self._check_execute_command(host, chown, addr=addr)
            if mode is not None:
                chmod = RemoteCommand(Executables.CHMOD, [oct(mode)[2:], tmp_path])
                await self._check_execute_command(host, chmod, addr=addr)
            mv = RemoteCommand(Executables.MV, ['-Z', tmp_path, path])
            await self._check_execute_command(host, mv, addr=addr)
        except Exception as e:
            msg = f"Unable to write {host}:{path}: {e}"
            logger.exception(msg)
            raise OrchestratorError(msg)

    async def _deploy_cephadm_binary_via_invoker(
        self,
        host: str,
        cephadm_path: str,
        cephadm_content: bytes,
        addr: Optional[str] = None
    ) -> None:
        """
        Deploy cephadm binary using the invoker for secure operations.
        This creates a temp file locally, copies it to remote host, then
        calls the invoker to perform all deployment operations.
        """
        with NamedTemporaryFile(prefix='cephadm-deploy-', delete=False) as local_tmp:
            local_tmp.write(cephadm_content)
            local_tmp.flush()
            local_tmp_path = local_tmp.name

        try:
            remote_tmp_path = f'/tmp/cephadm-{self.mgr._cluster_fsid}.new'

            conn = await self._remote_connection(host, addr)
            async with conn.start_sftp_client() as sftp:
                await sftp.put(local_tmp_path, remote_tmp_path)

            invoker_cmd = RemoteCommand(
                Executables.INVOKER,
                ['deploy_binary', remote_tmp_path, cephadm_path]
            )
            await self._execute_command(host, invoker_cmd, addr=addr)

        finally:
            try:
                os.unlink(local_tmp_path)
            except OSError:
                pass

    def _is_inventory_addr(self, host: str, addr: str) -> bool:
        return host in self.mgr.inventory and self.mgr.inventory.get_addr(host) == addr

    def _discard_failed_connection(self, host: str, addr: str, conn: "SSHClientConnection",
                                   mark_offline: bool) -> None:
        key = (normalize_hostname(host), addr)
        entry = self._connections.get(key)
        if entry and entry.task.done() and entry.task.result() is conn:
            if mark_offline and self._is_inventory_addr(host, addr):
                self.mgr.offline_hosts.add(key[0])
            self._discard_connection(key)

    def _discard_connection(self, key: Tuple[str, str]) -> None:
        entry = self._connections.pop(key, None)
        if entry and entry.task.done():
            entry.task.result().close()
        # An in-flight _connect closes its own result when it sees it was retired.

    async def _reset_con(self, host: str, addr: Optional[str] = None) -> None:
        host = normalize_hostname(host)
        for key in list(self._connections):
            if key[0] == host and (addr is None or key[1] == addr):
                self._discard_connection(key)

    def reset_con(self, host: str, addr: Optional[str] = None) -> None:
        with self.mgr.async_timeout_handler(cmd=f'resetting ssh connection to {host}'):
            self.mgr.wait_async(self._reset_con(host, addr))

    async def commit_host_addr(self, host: str, addr: str) -> None:
        old_addr = self.mgr.inventory.get_addr(host)
        self.mgr.inventory.set_addr(host, addr)
        if old_addr != addr:
            self._discard_connection((normalize_hostname(host), old_addr))
        self.mgr.offline_hosts_remove(host)

    async def _reset_cons(self) -> None:
        for key in list(self._connections):
            self._discard_connection(key)

    def _reconfig_ssh(self) -> None:
        temp_files = []  # type: list
        ssh_options = []  # type: List[str]

        # ssh_config
        self.mgr.ssh_config_fname = self.mgr.ssh_config_file
        ssh_config = self.mgr.get_store("ssh_config")
        if ssh_config is not None or self.mgr.ssh_config_fname is None:
            if not ssh_config:
                ssh_config = get_default_ssh_config()
            f = NamedTemporaryFile(prefix='cephadm-conf-')
            os.fchmod(f.fileno(), 0o600)
            f.write(ssh_config.encode('utf-8'))
            f.flush()  # make visible to other processes
            temp_files += [f]
            self.mgr.ssh_config_fname = f.name
        if self.mgr.ssh_config_fname:
            self.mgr.validate_ssh_config_fname(self.mgr.ssh_config_fname)
            ssh_options += ['-F', self.mgr.ssh_config_fname]
        self.mgr.ssh_config = ssh_config

        # identity
        ssh_key = self.mgr.get_store("ssh_identity_key")
        ssh_pub = self.mgr.get_store("ssh_identity_pub")
        ssh_cert = self.mgr.get_store("ssh_identity_cert")
        self.mgr.ssh_pub = ssh_pub
        self.mgr.ssh_key = ssh_key
        self.mgr.ssh_cert = ssh_cert
        if ssh_key:
            self.mgr.tkey = NamedTemporaryFile(prefix='cephadm-identity-')
            self.mgr.tkey.write(ssh_key.encode('utf-8'))
            os.fchmod(self.mgr.tkey.fileno(), 0o600)
            self.mgr.tkey.flush()  # make visible to other processes
            temp_files += [self.mgr.tkey]
            if ssh_pub:
                tpub = open(self.mgr.tkey.name + '.pub', 'w')
                os.fchmod(tpub.fileno(), 0o600)
                tpub.write(ssh_pub)
                tpub.flush()  # make visible to other processes
                temp_files += [tpub]
            if ssh_cert:
                tcert = open(self.mgr.tkey.name + '-cert.pub', 'w')
                os.fchmod(tcert.fileno(), 0o600)
                tcert.write(ssh_cert)
                tcert.flush()  # make visible to other processes
                temp_files += [tcert]
            ssh_options += ['-i', self.mgr.tkey.name]

        self.mgr._temp_files = temp_files
        if ssh_options:
            self.mgr._ssh_options = ' '.join(ssh_options)
        else:
            self.mgr._ssh_options = None

        if self.mgr.mode == 'root':
            self.mgr.ssh_user = self.mgr.get_store('ssh_user', default='root')
        elif self.mgr.mode == 'cephadm-package':
            self.mgr.ssh_user = 'cephadm'

        if hasattr(self.mgr, 'event_loop'):
            self.mgr.wait_async(self._reset_cons())
