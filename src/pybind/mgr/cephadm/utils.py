import re

from orchestrator import OrchestratorError

from typing import Optional

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
