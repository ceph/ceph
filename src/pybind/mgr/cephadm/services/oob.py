"""
The oob section of RGW and OSD specs: what the containers of a service need
for out-of-band (S3 over RDMA) transfers. See ceph.cephadm.oob_types.
"""
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ceph.cephadm.oob_types import OOBSpec
from ceph.deployment.service_spec import ServiceSpec
from cephadm import utils

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

DEP_PREFIX = 'oob:'


def _oob_memory(mgr: "CephadmOrchestrator", entity: str) -> Optional[int]:
    value = mgr.get_foreign_ceph_option(entity, 'oob_memory')
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def oob_container_config(
    mgr: "CephadmOrchestrator",
    spec: Optional[ServiceSpec],
    entity: str,
) -> Optional[Dict[str, Any]]:
    """The oob config to send with the deployment of a daemon whose config
    entity is the given one (osd.3, client.rgw.foo.host.abcdef), or of the
    daemons of a config section (osd, client.rgw.foo). None when the spec
    has no oob section.
    """
    oob = getattr(spec, 'oob', None)
    if not oob:
        return None
    return OOBSpec.from_json(oob).container_config(_oob_memory(mgr, entity))


def oob_dependencies(
    mgr: "CephadmOrchestrator",
    spec: Optional[ServiceSpec],
    section: str,
) -> List[str]:
    """A dependency that follows the oob config of a service's daemons, so
    that a change redeploys them (see oob_changed()).
    """
    config = oob_container_config(mgr, spec, section)
    if config is None:
        return []
    return [DEP_PREFIX + utils.config_hash(json.dumps(config, sort_keys=True))]


def oob_changed(curr_deps: List[str], last_deps: List[str]) -> bool:
    """Whether the oob config changed between two dependency lists. The
    container arguments follow from it, so only a redeploy applies it.
    """
    changed = set(curr_deps).symmetric_difference(last_deps)
    return any(d.startswith(DEP_PREFIX) for d in changed)
