import logging
import json
import socket
from enum import Enum
from functools import wraps
from typing import Optional, Callable, TypeVar, List, NewType, TYPE_CHECKING, Any, NamedTuple
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm import CephadmOrchestrator

T = TypeVar('T')
logger = logging.getLogger(__name__)

ConfEntity = NewType('ConfEntity', str)


class CephadmNoImage(Enum):
    token = 1


# ceph daemon types that use the ceph container image.
# NOTE: order important here as these are used for upgrade order
CEPH_TYPES = ['mgr', 'mon', 'crash', 'osd', 'mds', 'rgw',
              'rbd-mirror', 'cephfs-mirror', 'ceph-exporter']
GATEWAY_TYPES = ['iscsi', 'nfs', 'nvmeof']
MONITORING_STACK_TYPES = ['node-exporter', 'prometheus',
                          'alertmanager', 'grafana', 'loki', 'promtail']
RESCHEDULE_FROM_OFFLINE_HOSTS_TYPES = ['haproxy', 'nfs']

CEPH_UPGRADE_ORDER = CEPH_TYPES + GATEWAY_TYPES + MONITORING_STACK_TYPES

# these daemon types use the ceph container image
CEPH_IMAGE_TYPES = CEPH_TYPES + ['iscsi', 'nfs', 'node-proxy']

# these daemons do not use the ceph image. There are other daemons
# that also don't use the ceph image, but we only care about those
# that are part of the upgrade order here
NON_CEPH_IMAGE_TYPES = MONITORING_STACK_TYPES + ['nvmeof']

# Used for _run_cephadm used for check-host etc that don't require an --image parameter
cephadmNoImage = CephadmNoImage.token


class ContainerInspectInfo(NamedTuple):
    image_id: str
    ceph_version: Optional[str]
    repo_digests: Optional[List[str]]


class SpecialHostLabels(str, Enum):
    ADMIN: str = '_admin'
    NO_MEMORY_AUTOTUNE: str = '_no_autotune_memory'
    DRAIN_DAEMONS: str = '_no_schedule'
    DRAIN_CONF_KEYRING: str = '_no_conf_keyring'

    def to_json(self) -> str:
        return self.value


def name_to_config_section(name: str) -> ConfEntity:
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    daemon_type = name.split('.', 1)[0]
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', 'crash', 'iscsi', 'ceph-exporter', 'nvmeof']:
        return ConfEntity('client.' + name)
    elif daemon_type in ['mon', 'osd', 'mds', 'mgr', 'client']:
        return ConfEntity(name)
    else:
        return ConfEntity('mon')


def forall_hosts(f: Callable[..., T]) -> Callable[..., List[T]]:
    @wraps(f)
    def forall_hosts_wrapper(*args: Any) -> List[T]:
        from cephadm.module import CephadmOrchestrator

        # Some weird logic to make calling functions with multiple arguments work.
        if len(args) == 1:
            vals = args[0]
            self = None
        elif len(args) == 2:
            self, vals = args
        else:
            assert 'either f([...]) or self.f([...])'

        def do_work(arg: Any) -> T:
            if not isinstance(arg, tuple):
                arg = (arg, )
            try:
                if self:
                    return f(self, *arg)
                return f(*arg)
            except Exception:
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
    except ValueError:
        msg = 'Failed to parse health status: Cannot decode JSON'
        logger.exception('%s: \'%s\'' % (msg, out))
        raise OrchestratorError('failed to parse health status')

    return j['status']


def is_repo_digest(image_name: str) -> bool:
    """
    repo digest are something like "ceph/ceph@sha256:blablabla"
    """
    return '@' in image_name


def resolve_ip(hostname: str) -> str:
    try:
        r = socket.getaddrinfo(hostname, None, flags=socket.AI_CANONNAME,
                               type=socket.SOCK_STREAM)
        # pick first v4 IP, if present
        for a in r:
            if a[0] == socket.AF_INET:
                return a[4][0]
        return r[0][4][0]
    except socket.gaierror as e:
        raise OrchestratorError(f"Cannot resolve ip for host {hostname}: {e}")


def ceph_release_to_major(release: str) -> int:
    return ord(release[0]) - ord('a') + 1


def file_mode_to_str(mode: int) -> str:
    r = ''
    for shift in range(0, 9, 3):
        r = (
            f'{"r" if (mode >> shift) & 4 else "-"}'
            f'{"w" if (mode >> shift) & 2 else "-"}'
            f'{"x" if (mode >> shift) & 1 else "-"}'
        ) + r
    return r
