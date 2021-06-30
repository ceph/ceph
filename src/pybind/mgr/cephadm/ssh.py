import asyncssh
import asyncssh.logging
import logging
import os
import tempfile
from contextlib import contextmanager
from io import StringIO
from shlex import quote
from typing import TYPE_CHECKING, Optional, List, Tuple, Dict, Any
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from asyncssh.connection import SSHClientConnection

logger = logging.getLogger(__name__)

DEFAULT_SSH_CONFIG = """
Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ConnectTimeout=30
"""


class SSHManager:

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.cons: Dict[str, "SSHClientConnection"] = {}

    async def _remote_connection(self,
                                 host: str,
                                 addr: Optional[str] = None,
                                 ) -> "SSHClientConnection":
        if not self.cons.get(host):
            if not addr and host in self.mgr.inventory:
                addr = self.mgr.inventory.get_addr(host)

            self.mgr.offline_hosts_remove(host)

            if not addr:
                raise OrchestratorError("host address is empty")

            assert self.mgr.ssh_user
            n = self.mgr.ssh_user + '@' + addr
            logger.debug("Opening connection to {} with ssh options '{}'".format(
                n, self.mgr._ssh_options))

            with self.redirect_log(host, addr):
                try:
                    conn = await asyncssh.connect(addr, username=self.mgr.ssh_user, client_keys=[self.mgr.tkey.name], known_hosts=None, config=[self.mgr.ssh_config_fname])
                    self.cons[host] = conn
                except OSError:
                    raise
                except asyncssh.Error:
                    raise

        conn = self.cons.get(host)
        return conn

    @contextmanager
    def redirect_log(self, host, addr):
        ssh_logger = logging.getLogger()
        asyncssh.logging.set_log_level(logging.DEBUG)
        log_string = StringIO()
        ch = logging.StreamHandler(log_string)
        ch.setLevel(logging.DEBUG)
        ssh_logger.addHandler(ch)

        try:
            yield
        except OSError as e:
            log_content = log_string.getvalue()
            log_string.flush()
            logger.debug(log_content)
            msg = f"Can't communicate with remote host `{addr}`, possibly because python3 is not installed there. {str(e)}" + '\n' + f'Log: {log_content}'
            raise OrchestratorError(msg)
        except asyncssh.Error as e:
            log_content = log_string.getvalue()
            log_string.flush()
            logger.debug(log_content)
            msg = f'Failed to connect to {host} ({addr}). {str(e)}' + '\n' + f'Log: {log_content}'
            raise OrchestratorError(msg)
        finally:
            ssh_logger.removeHandler(ch)

    def remote_connection(self, *args: Any) -> "SSHClientConnection":
        return self.mgr.loop.run_until_complete(self._remote_connection(*args))

    async def _execute_command(self,
                               host: str,
                               cmd: List[str],
                               stdin: Optional[bytes] = b"",
                               addr: Optional[str] = None,
                               **kwargs: Any,
                               ) -> Tuple[str, str, int]:
        conn = await self._remote_connection(host, addr)
        cmd = " ".join(quote(x) for x in cmd)
        r = await conn.run(cmd, input=stdin.decode() if stdin else None)
        out = r.stdout.rstrip('\n')
        err = r.stderr.rstrip('\n')
        return out, err, r.returncode

    def execute_command(self, *args: Any, **kwargs: Any) -> Tuple[str, str, int]:
        return self.mgr.loop.run_until_complete(self._execute_command(*args, **kwargs))

    async def _check_execute_command(self,
                                     host: str,
                                     cmd: List[str],
                                     stdin: Optional[bytes] = b"",
                                     addr: Optional[str] = None,
                                     **kwargs: Any,
                                     ) -> str:
        out, err, code = await self._execute_command(host, cmd, stdin, addr)
        if code != 0:
            raise OrchestratorError(f'Command {cmd} failed. {err}')
        return out

    def check_execute_command(self, *args: Any, **kwargs: Any) -> str:
        return self.mgr.loop.run_until_complete(self._check_execute_command(*args, **kwargs))

    async def _write_remote_file(self,
                                 host: str,
                                 path: str,
                                 content: bytes,
                                 mode: Optional[int] = None,
                                 uid: Optional[int] = None,
                                 gid: Optional[int] = None,
                                 addr: Optional[str] = None,
                                 **kwargs: Any,
                                 ) -> None:
        try:
            dirname = os.path.dirname(path)
            await self._check_execute_command(host, ['mkdir', '-p', dirname], addr=addr)
            tmp_path = path + '.new'
            await self._check_execute_command(host, ['touch', tmp_path], addr=addr)
            if uid is not None and gid is not None and mode is not None:
                # shlex quote takes str or byte object, not int
                await self._check_execute_command(host, ['sudo', 'chown', '-R', str(uid) + ':' + str(gid), tmp_path], addr=addr)
                await self._check_execute_command(host, ['sudo', 'chmod', oct(mode)[2:], tmp_path], addr=addr)
            await self._check_execute_command(host, ['tee', '-', tmp_path], stdin=content, addr=addr)
            await self._check_execute_command(host, ['mv', tmp_path, path], addr=addr)
        except Exception as e:
            msg = f"Unable to write {host}:{path}: {e}"
            logger.exception(msg)
            raise OrchestratorError(msg)

    def write_remote_file(self, *args: Any, **kwargs: Any) -> None:
        return self.mgr.loop.run_until_complete(self._write_remote_file(*args, **kwargs))

    async def _reset_con(self, host: str) -> None:
        conn = self.cons.get(host)
        if conn:
            logger.debug(f'_reset_con close {host}')
            conn.close()
            await conn.wait_closed()
            del self.cons[host]

    async def _reset_cons(self) -> None:
        for host, conn in self.cons.items():
            logger.debug(f'_reset_cons close {host}')
            conn.close()
            await conn.wait_closed()
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
            f = tempfile.NamedTemporaryFile(prefix='cephadm-conf-')
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
        self.mgr.ssh_pub = ssh_pub
        self.mgr.ssh_key = ssh_key
        if ssh_key and ssh_pub:
            self.mgr.tkey = tempfile.NamedTemporaryFile(prefix='cephadm-identity-')
            self.mgr.tkey.write(ssh_key.encode('utf-8'))
            os.fchmod(self.mgr.tkey.fileno(), 0o600)
            self.mgr.tkey.flush()  # make visible to other processes
            tpub = open(self.mgr.tkey.name + '.pub', 'w')
            os.fchmod(tpub.fileno(), 0o600)
            tpub.write(ssh_pub)
            tpub.flush()  # make visible to other processes
            temp_files += [self.mgr.tkey, tpub]
            ssh_options += ['-i', self.mgr.tkey.name]

        self.mgr._temp_files = temp_files
        if ssh_options:
            self.mgr._ssh_options = ' '.join(ssh_options)  # type: Optional[str]
        else:
            self.mgr._ssh_options = None

        if self.mgr.mode == 'root':
            self.mgr.ssh_user = self.mgr.get_store('ssh_user', default='root')
        elif self.mgr.mode == 'cephadm-package':
            self.mgr.ssh_user = 'cephadm'

        self._reset_cons()
