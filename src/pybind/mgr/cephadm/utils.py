import logging
from functools import wraps
from typing import Optional, Callable, TypeVar, List

from orchestrator import OrchestratorError


T = TypeVar('T')
logger = logging.getLogger(__name__)


def name_to_config_section(name: str) -> str:
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    daemon_type = name.split('.', 1)[0]
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', 'crash', 'iscsi']:
        return 'client.' + name
    elif daemon_type in ['mon', 'osd', 'mds', 'mgr', 'client']:
        return name
    else:
        return 'mon'

def name_to_auth_entity(daemon_type,  # type: str
                        daemon_id,    # type: str
                        host = ""     # type  Optional[str] = ""
                        ):
    """
    Map from daemon names/host to ceph entity names (as seen in config)
    """
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', "iscsi"]:
        return 'client.' + daemon_type + "." + daemon_id
    elif daemon_type == 'crash':
        if host == "":
            raise OrchestratorError("Host not provided to generate <crash> auth entity name")
        return 'client.' + daemon_type + "." + host
    elif daemon_type == 'mon':
        return 'mon.'
    elif daemon_type == 'mgr':
        return daemon_type + "." + daemon_id
    elif daemon_type in ['osd', 'mds', 'client']:
        return daemon_type + "." + daemon_id
    else:
        raise OrchestratorError("unknown auth entity name")


def forall_hosts(f: Callable[..., T]) -> Callable[..., List[T]]:
    @wraps(f)
    def forall_hosts_wrapper(*args) -> List[T]:
        from cephadm.module import CephadmOrchestrator

        # Some weired logic to make calling functions with multiple arguments work.
        if len(args) == 1:
            vals = args[0]
            self = None
        elif len(args) == 2:
            self, vals = args
        else:
            assert 'either f([...]) or self.f([...])'

        def do_work(arg):
            if not isinstance(arg, tuple):
                arg = (arg, )
            try:
                if self:
                    return f(self, *arg)
                return f(*arg)
            except Exception as e:
                logger.exception(f'executing {f.__name__}({args}) failed.')
                raise

        assert CephadmOrchestrator.instance is not None
        return CephadmOrchestrator.instance._worker_pool.map(do_work, vals)


    return forall_hosts_wrapper