import logging
from contextlib import contextmanager
from io import StringIO
from shlex import quote
from typing import TYPE_CHECKING, Optional, List, Tuple, Dict, Any, Iterator
from orchestrator import OrchestratorError

try:
    import asyncssh
except ImportError:
    asyncssh = None

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from asyncssh.connection import SSHClientConnection

logger = logging.getLogger(__name__)

asyncssh_logger = logging.getLogger('asyncssh')
asyncssh_logger.propagate = False

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
                    conn = await asyncssh.connect(addr, username=self.mgr.ssh_user, client_keys=[self.mgr.tkey.name], known_hosts=None, config=[self.mgr.ssh_config_fname], preferred_auth=['publickey'])
                except OSError:
                    raise
                except asyncssh.Error:
                    raise
                except Exception:
                    raise
            self.cons[host] = conn

        self.mgr.offline_hosts_remove(host)
        conn = self.cons.get(host)
        return conn

    @contextmanager
    def redirect_log(self, host: str, addr: str) -> Iterator[None]:
        log_string = StringIO()
        ch = logging.StreamHandler(log_string)
        ch.setLevel(logging.DEBUG)
        asyncssh_logger.addHandler(ch)

        try:
            yield
        except OSError as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            msg = f"Can't communicate with remote host `{addr}`, possibly because python3 is not installed there. {str(e)}" + '\n' + f'Log: {log_content}'
            logger.exception(msg)
            raise OrchestratorError(msg)
        except asyncssh.Error as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            msg = f'Failed to connect to {host} ({addr}). {str(e)}' + '\n' + f'Log: {log_content}'
            logger.debug(msg)
            raise OrchestratorError(msg)
        except Exception as e:
            self.mgr.offline_hosts.add(host)
            log_content = log_string.getvalue()
            logger.exception(str(e))
            raise OrchestratorError(f'Failed to connect to {host} ({addr}): {repr(e)}' + '\n' f'Log: {log_content}')
        finally:
            log_string.flush()
            asyncssh_logger.removeHandler(ch)

    async def _execute_command(self,
                               host: str,
                               cmd: List[str],
                               stdin: Optional[bytes] = b"",
                               addr: Optional[str] = None,
                               **kwargs: Any,
                               ) -> Tuple[str, str, int]:
        conn = await self._remote_connection(host, addr)
        cmd = "sudo " + " ".join(quote(x) for x in cmd)
        logger.debug(f'Running command: {cmd}')
        try:
            r = await conn.run(cmd, input=stdin.decode() if stdin else None)
        # handle these Exceptions otherwise you might get a weird error like TypeError: __init__() missing 1 required positional argument: 'reason' (due to the asyncssh error interacting with raise_if_exception)
        except (asyncssh.ChannelOpenError, Exception) as e:
            # SSH connection closed or broken, will create new connection next call
            logger.debug(f'Connection to {host} failed. {str(e)}')
            self._reset_con(host)
            self.mgr.offline_hosts.add(host)
            raise OrchestratorError(f'Unable to reach remote host {host}. {str(e)}')
        out = r.stdout.rstrip('\n')
        err = r.stderr.rstrip('\n')
        return out, err, r.returncode

    async def _check_execute_command(self,
                                     host: str,
                                     cmd: List[str],
                                     stdin: Optional[bytes] = b"",
                                     addr: Optional[str] = None,
                                     **kwargs: Any,
                                     ) -> str:
        out, err, code = await self._execute_command(host, cmd, stdin, addr)
        if code != 0:
            msg = f'Command {cmd} failed. {err}'
            logger.debug(msg)
            raise OrchestratorError(msg)
        return out

