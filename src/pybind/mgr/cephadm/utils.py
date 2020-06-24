import logging
from typing import List, Dict, Iterable, Tuple, TYPE_CHECKING

import yaml

from ceph.deployment.service_spec import PlacementSpec, HostPlacementSpec
from mgr_util import to_yaml_or_json
from orchestrator import OrchestratorError, DaemonDescription

if TYPE_CHECKING:
    from cephadm import CephadmOrchestrator

logger = logging.getLogger(__name__)

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


def generate_specs_for_daemons(mgr: "CephadmOrchestrator", all_dds: List[DaemonDescription], known_dds: List[DaemonDescription]) -> Iterable[str]:

    def dd_to_hpl(dd: DaemonDescription) -> HostPlacementSpec:
        return HostPlacementSpec(
            hostname=dd.hostname,
            name=dd.daemon_id,
            network=''
        )

    placements: Dict[Tuple[str, str], PlacementSpec] = {}
    for dd in all_dds:
        if dd in known_dds:
            continue
        try:
            # This is going to be problematic, as it is impossible to
            # implement service_id() correctly for all cases.
            service_id = dd.service_id()
            service_name = dd.service_name()
        except Exception:
            logger.warning(f"Failed to generate service spec for {dd.name()}")
            continue

        if (dd.daemon_type, service_id) not in placements:
            placements[(dd.daemon_type, service_id)] = PlacementSpec(hosts=[dd_to_hpl(dd)])
        else:
            placements[(dd.daemon_type, service_id)].hosts.append(dd_to_hpl(dd))

    for key, placement in placements.items():
        service_type, service_id = key
        context = {
            'placement': to_yaml_or_json(placement.to_json(), 'yaml', many=False).strip(),
            'service_id': service_id
        }

        yml = mgr.template.render(f'spec_templates/{service_type}.yaml.j2', context)
        yield yml
