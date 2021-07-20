import logging
from contextlib import contextmanager
from io import StringIO
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

