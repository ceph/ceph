from typing import Dict, Iterable, List

from ceph.deployment.service_spec import ServiceSpec
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext
from ..schedule import DaemonPlacement


@register_cephadm_service
class FcmDedupService(CephService):
    TYPE = 'fcm-dedup'

    def filter_host_candidates(
        self,
        spec: ServiceSpec,
        candidates: Iterable[DaemonPlacement],
    ) -> List[DaemonPlacement]:
        """One fcm-dedup daemon per 'in' OSD, placed on the host that
        runs the OSD. The daemon id is the OSD id.
        """
        osd_ids_by_host: Dict[str, List[str]] = {}
        osd_metadata = self.mgr.get('osd_metadata')
        for osd in self.mgr.get('osd_map')['osds']:
            if osd.get('in') != 1:
                continue
            osd_id = str(osd['osd'])
            hostname = osd_metadata.get(osd_id, {}).get('hostname', '')
            if hostname:
                osd_ids_by_host.setdefault(hostname.split('.')[0], []).append(osd_id)

        return [
            c._replace(name=osd_id)
            for c in candidates
            for osd_id in sorted(osd_ids_by_host.get(c.hostname.split('.')[0], []))
        ]

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.keyring = self.get_keyring_with_caps(
            self.get_auth_entity(daemon_spec.daemon_id),
            ['mon', 'allow r',
             'osd', 'allow r',
             'mgr', 'allow r'])
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        return daemon_spec
