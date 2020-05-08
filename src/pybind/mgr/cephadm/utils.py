import re

from orchestrator import OrchestratorError

def name_to_config_section(name):
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


def name_to_auth_entity(name) -> str:
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    daemon_type = name.split('.', 1)[0]
    if daemon_type in ['rgw', 'rbd-mirror', 'nfs', 'crash', 'iscsi']:
        return 'client.' + name
    elif daemon_type == 'mon':
        return 'mon.'
    elif daemon_type in ['osd', 'mds', 'mgr', 'client']:
        return name
    else:
        raise OrchestratorError("unknown auth entity name")
