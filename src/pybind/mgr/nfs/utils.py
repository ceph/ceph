from typing import List, TYPE_CHECKING

import orchestrator

if TYPE_CHECKING:
    from nfs.module import Module

POOL_NAME = 'nfs-ganesha'


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
