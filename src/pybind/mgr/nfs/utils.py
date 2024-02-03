import functools
import logging
import stat
from typing import List, Tuple, TYPE_CHECKING

from object_format import ErrorResponseBase
import orchestrator
import cephfs
from mgr_util import CephfsClient, open_filesystem

if TYPE_CHECKING:
    from nfs.module import Module

EXPORT_PREFIX: str = "export-"
CONF_PREFIX: str = "conf-nfs."
USER_CONF_PREFIX: str = "userconf-nfs."

log = logging.getLogger(__name__)


class NonFatalError(ErrorResponseBase):
    """Raise this exception when you want to interrupt the flow of a function
    and return an informative message to the user. In certain situations the
    NFS MGR module wants to indicate an action was or was not taken but still
    return a success code so that non-interactive scripts continue as if the
    overall action was completed.
    """
    def __init__(self, msg: str) -> None:
        super().__init__(msg)
        self.msg = msg

    def format_response(self) -> Tuple[int, str, str]:
        return 0, "", self.msg


class ManualRestartRequired(NonFatalError):
    """Raise this exception type if all other changes were successful but
    user needs to manually restart nfs services.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(" ".join((msg, "(Manual Restart of NFS Pods required)")))


def export_obj_name(export_id: int) -> str:
    """Return a rados object name for the export."""
    return f"{EXPORT_PREFIX}{export_id}"


def conf_obj_name(cluster_id: str) -> str:
    """Return a rados object name for the config."""
    return f"{CONF_PREFIX}{cluster_id}"


def user_conf_obj_name(cluster_id: str) -> str:
    """Returna a rados object name for the user config."""
    return f"{USER_CONF_PREFIX}{cluster_id}"


def available_clusters(mgr: 'Module') -> List[str]:
    '''
    This method returns list of available cluster ids.
    Service name is service_type.service_id
    Example:
    completion.result value:
    <ServiceDescription of <NFSServiceSpec for service_name=nfs.vstart>>
    return value: ['vstart']
    '''
    # TODO check cephadm cluster list with rados pool conf objects
    completion = mgr.describe_service(service_type='nfs')
    orchestrator.raise_if_exception(completion)
    assert completion.result is not None
    return [cluster.spec.service_id for cluster in completion.result
            if cluster.spec.service_id]


def restart_nfs_service(mgr: 'Module', cluster_id: str) -> None:
    '''
    This methods restarts the nfs daemons
    '''
    completion = mgr.service_action(action='restart',
                                    service_name='nfs.' + cluster_id)
    orchestrator.raise_if_exception(completion)


def check_fs(mgr: 'Module', fs_name: str) -> bool:
    '''
    This method checks if given fs is valid
    '''
    fs_map = mgr.get('fs_map')
    return fs_name in [fs['mdsmap']['fs_name'] for fs in fs_map['filesystems']]


def cephfs_path_is_dir(mgr: 'Module', fs: str, path: str) -> None:
    @functools.lru_cache(maxsize=1)
    def _get_cephfs_client() -> CephfsClient:
        return CephfsClient(mgr)
    cephfs_client = _get_cephfs_client()

    with open_filesystem(cephfs_client, fs) as fs_handle:
        stx = fs_handle.statx(path.encode('utf-8'), cephfs.CEPH_STATX_MODE,
                              cephfs.AT_SYMLINK_NOFOLLOW)
        if not stat.S_ISDIR(stx.get('mode')):
            raise NotADirectoryError()
