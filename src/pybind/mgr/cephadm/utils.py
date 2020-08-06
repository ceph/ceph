import logging
import re
import json
from enum import Enum
from functools import wraps
from typing import Optional, Callable, TypeVar, List, NewType, TYPE_CHECKING
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm import CephadmOrchestrator

T = TypeVar('T')
logger = logging.getLogger(__name__)

ConfEntity = NewType('ConfEntity', str)
AuthEntity = NewType('AuthEntity', str)


class CephadmNoImage(Enum):
    token = 1


# Used for _run_cephadm used for check-host etc that don't require an --image parameter
cephadmNoImage = CephadmNoImage.token


def name_to_config_section(name: str) -> ConfEntity:
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    daemon_type = name.split('.', 1)[0]
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', 'crash', 'iscsi']:
        return ConfEntity('client.' + name)
    elif daemon_type in ['mon', 'osd', 'mds', 'mgr', 'client']:
        return ConfEntity(name)
    else:
        return ConfEntity('mon')


def name_to_auth_entity(daemon_type: str,
                        daemon_id: str,
                        host: str = "",
                        ) -> AuthEntity:
    """
    Map from daemon names/host to ceph entity names (as seen in config)
    """
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', "iscsi"]:
        return AuthEntity('client.' + daemon_type + "." + daemon_id)
    elif daemon_type == 'crash':
        if host == "":
            raise OrchestratorError("Host not provided to generate <crash> auth entity name")
        return AuthEntity('client.' + daemon_type + "." + host)
    elif daemon_type == 'mon':
        return AuthEntity('mon.')
    elif daemon_type == 'mgr':
        return AuthEntity(daemon_type + "." + daemon_id)
    elif daemon_type in ['osd', 'mds', 'client']:
        return AuthEntity(daemon_type + "." + daemon_id)
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


def get_cluster_health(mgr: 'CephadmOrchestrator') -> str:
    # check cluster health
    ret, out, err = mgr.check_mon_command({
        'prefix': 'health',
        'format': 'json',
    })
    try:
        j = json.loads(out)
    except Exception as e:
        raise OrchestratorError('failed to parse health status')

    return j['status']
