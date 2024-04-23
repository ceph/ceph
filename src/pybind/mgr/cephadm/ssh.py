import enum
import logging
import os
import asyncio
import concurrent
from tempfile import NamedTemporaryFile
from threading import Thread
from contextlib import contextmanager
from io import StringIO
from shlex import quote
from typing import TYPE_CHECKING, Optional, List, Tuple, Dict, Iterator, TypeVar, Awaitable, Union
from orchestrator import OrchestratorError

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


DEFAULT_SSH_CONFIG = """
Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ConnectTimeout=30
"""


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


class SSHManager:

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.cons: Dict[str, "SSHClientConnection"] = {}

    async def _remote_connection(self,
                                 host: str,
                                 addr: Optional[str] = None,
                                 ) -> "SSHClientConnection":
        if not self.cons.get(host) or host not in self.mgr.inventory:
            if not addr and host in self.mgr.inventory:
                addr = self.mgr.inventory.get_addr(host)

            if not addr:
                raise OrchestratorError("host address is empty")

            assert self.mgr.ssh_user
            n = self.mgr.ssh_user + '@' + addr
            logger.debug("Opening connection to {} with ssh options '{}'".format(
                n, self.mgr._ssh_options))

            asyncssh.set_log_level('DEBUG')
            asyncssh.set_debug_level(3)

            with self.redirect_log(host, addr):
                try:
                    ssh_options = asyncssh.SSHClientConnectionOptions(
                        keepalive_interval=7, keepalive_count_max=3)
                    conn = await asyncssh.connect(addr, username=self.mgr.ssh_user, client_keys=[self.mgr.tkey.name],
                                                  known_hosts=None, config=[self.mgr.ssh_config_fname],
                                                  preferred_auth=['publickey'], options=ssh_options)
                except OSError:
                    raise
                except asyncssh.Error:
                    raise
                except Exception:
                    raise
            self.cons[host] = conn

        self.mgr.offline_hosts_remove(host)

        return self.cons[host]

    @contextmanager
    def redirect_log(self, host: str, addr: str) -> Iterator[None]:
        log_string = StringIO()
        ch = logging.StreamHandler(log_string)
        ch.setLevel(logging.INFO)
        asyncssh_logger.addHandler(ch)

        try:
            yield
        except OSError as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            msg = f"Can't communicate with remote host `{addr}`, possibly because the host is not reachable or python3 is not installed on the host. {str(e)}"
            logger.exception(msg)
            raise HostConnectionError(msg, host, addr)
        except asyncssh.Error as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            msg = f'Failed to connect to {host} ({addr}). {str(e)}' + '\n' + f'Log: {log_content}'
            logger.debug(msg)
            raise HostConnectionError(msg, host, addr)
        except Exception as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            logger.exception(str(e))
            raise HostConnectionError(
                f'Failed to connect to {host} ({addr}): {repr(e)}' + '\n' f'Log: {log_content}', host, addr)
        finally:
            log_string.flush()
            asyncssh_logger.removeHandler(ch)

    def remote_connection(self,
                          host: str,
                          addr: Optional[str] = None,
                          ) -> "SSHClientConnection":
        with self.mgr.async_timeout_handler(host, f'ssh {host} (addr {addr})'):
            return self.mgr.wait_async(self._remote_connection(host, addr))

    async def _execute_command(self,
                               host: str,
                               cmd_components: RemoteCommand,
                               stdin: Optional[str] = None,
                               addr: Optional[str] = None,
                               log_command: Optional[bool] = True,
                               ) -> Tuple[str, str, int]:

        conn = await self._remote_connection(host, addr)
        use_sudo = (self.mgr.ssh_user != 'root')
        rcmd = RemoteSudoCommand.wrap(cmd_components, use_sudo=use_sudo)
        try:
            address = addr or self.mgr.inventory.get_addr(host)
        except Exception:
            address = host
        if log_command:
            logger.debug(f'Running command: {rcmd}')
        try:
            test_cmd = RemoteSudoCommand(
                Executables.TRUE, [], use_sudo=use_sudo
            )
            r = await conn.run(str(test_cmd), check=True, timeout=5)  # host quick check
            r = await conn.run(str(rcmd), input=stdin)
        # handle these Exceptions otherwise you might get a weird error like
        # TypeError: __init__() missing 1 required positional argument: 'reason' (due to the asyncssh error interacting with raise_if_exception)
        except asyncssh.ChannelOpenError as e:
            # SSH connection closed or broken, will create new connection next call
            logger.debug(f'Connection to {host} failed. {str(e)}')
            await self._reset_con(host)
            self.mgr.offline_hosts.add(host)
            raise HostConnectionError(f'Unable to reach remote host {host}. {str(e)}', host, address)
        except asyncssh.ProcessError as e:
            msg = f"Cannot execute the command '{rcmd}' on the {host}. {str(e.stderr)}."
            logger.debug(msg)
            await self._reset_con(host)
            self.mgr.offline_hosts.add(host)
            raise HostConnectionError(msg, host, address)
        except Exception as e:
            msg = f"Generic error while executing command '{rcmd}' on the host {host}. {str(e)}."
            logger.debug(msg)
            await self._reset_con(host)
            self.mgr.offline_hosts.add(host)
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
                        stdin: Optional[str] = None,
                        addr: Optional[str] = None,
                        log_command: Optional[bool] = True
                        ) -> Tuple[str, str, int]:
        with self.mgr.async_timeout_handler(host, " ".join(cmd)):
            return self.mgr.wait_async(self._execute_command(host, cmd, stdin, addr, log_command))

    async def _check_execute_command(self,
                                     host: str,
                                     cmd: RemoteCommand,
                                     stdin: Optional[str] = None,
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
                              stdin: Optional[str] = None,
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
            if uid is not None and gid is not None and mode is not None:
                # shlex quote takes str or byte object, not int
                chown = RemoteCommand(
                    Executables.CHOWN,
                    ['-R', str(uid) + ':' + str(gid), tmp_path]
                )
                await self._check_execute_command(host, chown, addr=addr)
                chmod = RemoteCommand(Executables.CHMOD, [oct(mode)[2:], tmp_path])
                await self._check_execute_command(host, chmod, addr=addr)
            mv = RemoteCommand(Executables.MV, [tmp_path, path])
            await self._check_execute_command(host, mv, addr=addr)
        except Exception as e:
            msg = f"Unable to write {host}:{path}: {e}"
            logger.exception(msg)
            raise OrchestratorError(msg)

    def write_remote_file(self,
                          host: str,
                          path: str,
                          content: bytes,
                          mode: Optional[int] = None,
                          uid: Optional[int] = None,
                          gid: Optional[int] = None,
                          addr: Optional[str] = None,
                          ) -> None:
        with self.mgr.async_timeout_handler(host, f'writing file {path}'):
            self.mgr.wait_async(self._write_remote_file(
                host, path, content, mode, uid, gid, addr))

    async def _reset_con(self, host: str) -> None:
        conn = self.cons.get(host)
        if conn:
            logger.debug(f'_reset_con close {host}')
            conn.close()
            del self.cons[host]

    def reset_con(self, host: str) -> None:
        with self.mgr.async_timeout_handler(cmd=f'resetting ssh connection to {host}'):
            self.mgr.wait_async(self._reset_con(host))

    def _reset_cons(self) -> None:
        for host, conn in self.cons.items():
            logger.debug(f'_reset_cons close {host}')
            conn.close()
        self.cons = {}

    def _reconfig_ssh(self) -> None:
        temp_files = []  # type: list
        ssh_options = []  # type: List[str]

        # ssh_config
        self.mgr.ssh_config_fname = self.mgr.ssh_config_file
        ssh_config = self.mgr.get_store("ssh_config")
        if ssh_config is not None or self.mgr.ssh_config_fname is None:
            if not ssh_config:
                ssh_config = DEFAULT_SSH_CONFIG
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

        self._reset_cons()
