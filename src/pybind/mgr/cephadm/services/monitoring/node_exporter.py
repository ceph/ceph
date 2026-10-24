from typing import TYPE_CHECKING
from typing import List, Any, Tuple, Dict, Optional
from mgr_module import HandleCommandResult
from ..service_registry import register_cephadm_service
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import ServiceSpec
from cephadm.services.cephadmservice import CephadmService, DaemonDeployContext, next_action_for_mgmt_stack_service
from ... import utils


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
class NodeExporterService(CephadmService):
    TYPE = 'node-exporter'
    DEFAULT_SERVICE_PORT = 9100

    @property
    def needs_monitoring(self) -> bool:
        return True

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        deps = []
        deps.append(f'secure_monitoring_stack:{mgr.secure_monitoring_stack}')
        deps += mgr.cache.get_daemons_by_types(['mgmt-gateway'])
        return sorted(deps)

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()
        if security_enabled:
            tls_pair = self.get_certificates(daemon_spec)
            r = {
                'files': {
                    'web.yml': self.mgr.template.render('services/node-exporter/web.yml.j2',
                                                        {'enable_mtls': mgmt_gw_enabled}),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
                    'node_exporter.crt': tls_pair.cert,
                    'node_exporter.key': tls_pair.key,
                },
                'web_config': '/etc/node-exporter/web.yml'
            }
        else:
            r = {}

        return r, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # since node exporter runs on each host and cannot compromise data, no extra checks required
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        out = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, out, '')

    def choose_next_action(
        self,
        scheduled_action: utils.Action,
        daemon_type: Optional[str],
        spec: Optional[ServiceSpec],
        curr_deps: List[str],
        last_deps: List[str],
        daemon: Optional[DaemonDescription] = None,
    ) -> utils.NextDaemonStep:
        """Given the scheduled_action, service spec, daemon_type, and
        current and previous dependency lists return the next action that
        this service would prefer cephadm take.
        """
        return next_action_for_mgmt_stack_service(
            scheduled_action, daemon_type, spec, curr_deps, last_deps
        )
