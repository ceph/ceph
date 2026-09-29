import logging
from typing import List

from ceph.deployment.service_spec import FcmDedupSpec
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .service_registry import register_cephadm_service
from orchestrator import DaemonDescription

logger = logging.getLogger(__name__)


@register_cephadm_service
class FcmDedupService(CephService):
    TYPE = 'fcm-dedup'

    def prepare_create(
        self, daemon_spec: CephadmDaemonDeploySpec
    ) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        keyring = self.get_keyring_with_caps(
            self.get_auth_entity(daemon_spec.daemon_id),
            ['mon', 'allow r',
             'osd', 'allow r',
             'mgr', 'allow r'],
        )
        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def allow_colo(self) -> bool:
        return True

    def get_daemons_to_upgrade(
        self,
        spec: FcmDedupSpec,  # type: ignore[override]
        daemons: List[DaemonDescription],
    ) -> List[DaemonDescription]:
        return daemons
