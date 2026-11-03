from typing import TYPE_CHECKING
import json
from typing import List, Optional, Dict, Any, Tuple
from ceph.deployment.service_spec import ServiceSpec
from orchestrator import OrchestratorError
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


@register_cephadm_service
class CephadmAgent(CephService):
    TYPE = 'agent'

    @classmethod
    def _get_ceph_volume_image(cls, mgr: "CephadmOrchestrator") -> str:
        return mgr.get_container_image('osd') or ''

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """Return service-specific dependencies.

        Protected override hook for subclasses to provide dependencies specific to
        the service. This method should not be called directly; callers should use
        ``get_dependencies()``, which is the public entry point and computes the
        complete dependency set.
        """
        agent = mgr.http_server.agent
        container_image = cls._get_ceph_volume_image(mgr)
        return sorted(
            [
                str(mgr.get_mgr_ip()),
                str(agent.server_port),
                mgr.cert_mgr.get_root_ca(),
                str(mgr.get_module_option("device_enhanced_scan")),
                container_image,
            ]
        )

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        super().prepare_create(deploy_ctx)
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host
        daemon_spec.ports = [self.mgr.agent_starting_port]

        if not self.mgr.http_server.agent:
            raise OrchestratorError('Cannot deploy agent before creating cephadm endpoint')

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host), [])
        daemon_spec.keyring = keyring
        self.mgr.agent_cache.agent_keys[host] = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        agent = self.mgr.http_server.agent
        try:
            assert agent
            assert agent.server_port
        except Exception:
            raise OrchestratorError(
                'Cannot deploy agent daemons until cephadm endpoint has finished generating certs')

        container_image = self._get_ceph_volume_image(self.mgr)
        cfg = {'target_ip': self.mgr.get_mgr_ip(),
               'target_port': agent.server_port,
               'refresh_period': self.mgr.agent_refresh_rate,
               'listener_port': self.mgr.agent_starting_port,
               'host': daemon_spec.host,
               'container_image': container_image,
               'device_enhanced_scan': str(self.mgr.device_enhanced_scan)}

        tls_creds = self.get_certificates(daemon_spec)
        config = {
            'agent.json': json.dumps(cfg),
            'keyring': daemon_spec.keyring,
            'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
            'listener.crt': tls_creds.cert,
            'listener.key': tls_creds.key,
        }

        return config, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)
