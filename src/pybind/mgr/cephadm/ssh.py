import asyncssh
import asyncssh.logging
import logging
import os
from io import StringIO
from shlex import quote
from typing import TYPE_CHECKING, Optional, List, Tuple
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from asyncssh import SSHClientConnection

logger = logging.getLogger(__name__)


class SSHConnection:

    cons = {}

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    async def _remote_connection(self,
                                 host: str,
                                 addr: Optional[str] = None,
                                 ) -> "SSHClientConnection":
        if not SSHConnection.cons.get(host):
            if not addr and host in self.mgr.inventory:
                addr = self.mgr.inventory.get_addr(host)

            self.mgr.offline_hosts_remove(host)

            if not addr:
                raise OrchestratorError("host address is empty")

            assert self.mgr.ssh_user
            n = self.mgr.ssh_user + '@' + addr
            logger.debug("Opening connection to {} with ssh options '{}'".format(
                n, self.mgr._ssh_options))

            ssh_logger = logging.getLogger()
            asyncssh.logging.set_log_level(logging.DEBUG)
            log_string = StringIO()
            ch = logging.StreamHandler(log_string)
            ch.setLevel(logging.DEBUG)
            ssh_logger.addHandler(ch)

            try:
                conn = await asyncssh.connect(addr, username=self.mgr.ssh_user, client_keys=[self.mgr.tkey.name], known_hosts=None, config=[self.mgr.ssh_config_fname])
                SSHConnection.cons[host] = conn
            except OSError as e:
                log_content = log_string.getvalue()
                log_string.flush()
                logger.debug(log_content)
                msg = f"Can't communicate with remote host `{addr}`, possibly because python3 is not installed there. {str(e)}" + '\n' + f'Log: {log_content}'
                ssh_logger.removeHandler(ch)
                raise OrchestratorError(msg)
            except asyncssh.Error as e:
                o = self._check_host(host)
                if o is not None:
                    log_content = log_string.getvalue()
                    log_string.flush()
                    logger.debug(log_content)
                    msg = f'Failed to connect to {host} ({addr}) due to: {str(e)}' + '\n' + f'Log: {log_content}'
                else:
                    msg = f'Failed to connect to {host} ({addr}) due to: {str(e)}'
                ssh_logger.removeHandler(ch)
                raise OrchestratorError(msg)

        conn = SSHConnection.cons.get(host)
        return conn

    async def execute_command(self,
                              host: str,
                              cmd: List[str],
                              stdin: Optional[bytes] = b"",
                              addr: Optional[str] = None,
                              ) -> Tuple[str, str, int]:
        conn = await self._remote_connection(host, addr)
        cmd = " ".join(quote(x) for x in cmd)
        r = await conn.run(cmd, input=stdin.decode() if stdin else None)
        out = r.stdout.rstrip('\n')
        err = r.stderr.rstrip('\n')
        return out, err, r.returncode

    async def check_execute_command(self,
                                    host: str,
                                    cmd: List[str],
                                    stdin: Optional[bytes] = b"",
                                    addr: Optional[str] = None
                                    ) -> str:
        out, err, code = await self.execute_command(host, cmd, stdin, addr)
        if code != 0:
            print(f'Command {cmd} failed. {err}')
        return out

    async def _write_remote_file(self,
                                 host: str,
                                 path: str,
                                 content: bytes,
                                 mode: Optional[int] = None,
                                 uid: Optional[int] = None,
                                 gid: Optional[int] = None,
                                 addr: Optional[str] = None
                                 ) -> None:
        try:
            dirname = os.path.dirname(path)
            await self.check_execute_command(host, ['mkdir', '-p', dirname], addr=addr)
            tmp_path = path + '.new'
            await self.check_execute_command(host, ['touch', tmp_path], addr=addr)
            if uid is not None and gid is not None and mode is not None:
                # shlex quote takes str or byte object, not int
                await self.check_execute_command(host, ['sudo', 'chown', '-R', str(uid) + ':' + str(gid), tmp_path], addr=addr)
                await self.check_execute_command(host, ['sudo', 'chmod', oct(mode)[2:], tmp_path], addr=addr)
            await self.check_execute_command(host, ['tee', '-', tmp_path], stdin=content, addr=addr)
            await self.check_execute_command(host, ['mv', tmp_path, path], addr=addr)
        except Exception as e:
            msg = f"Unable to write {host}:{path}: {e}"
            logger.exception(msg)
            raise OrchestratorError(msg)

    async def _reset_con(self, host: str) -> None:
        conn = SSHConnection.cons.get(host)
        if conn:
            self.log.debug(f'_reset_con close {host}')
            conn.close()
            await conn.wait_closed()
            del SSHConnection.cons[host]

    async def _reset_cons(self) -> None:
        for host, conn in SSHConnection.cons.items():
            self.log.debug(f'_reset_cons close {host}')
            conn.close()
            await conn.wait_closed()
        SSHConnection.cons = {}
