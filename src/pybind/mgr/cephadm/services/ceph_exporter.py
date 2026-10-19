from typing import TYPE_CHECKING
from typing import List, Optional, Dict, Any, cast
from ceph.deployment.service_spec import CephExporterSpec, ServiceSpec
from mgr_util import merge_dicts
from orchestrator import DaemonDescription
from cephadm import utils
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext, next_action_for_mgmt_stack_service


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


@register_cephadm_service
class CephExporterService(CephService):
    TYPE = 'ceph-exporter'
    DEFAULT_SERVICE_PORT = 9926

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

        deps = [f'secure_monitoring_stack:{mgr.secure_monitoring_stack}']
        deps += mgr.cache.get_daemons_by_types(['mgmt-gateway'])
        return sorted(deps)

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        super().prepare_certificates(daemon_spec)
        spec = cast(CephExporterSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        deploy_ctx.service_spec = spec
        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_spec.daemon_id),
                                             ['mon', 'profile ceph-exporter',
                                              'mon', 'allow r',
                                              'mgr', 'allow r',
                                              'osd', 'allow r'])
        exporter_config: Dict[str, Any] = {}
        if spec.sock_dir:
            exporter_config.update({'sock-dir': spec.sock_dir})
        if spec.port:
            exporter_config.update({'port': f'{spec.port}'})
        if spec.prio_limit is not None:
            exporter_config.update({'prio-limit': f'{spec.prio_limit}'})
        if spec.stats_period:
            exporter_config.update({'stats-period': f'{spec.stats_period}'})

        security_enabled, _, _ = self.mgr._get_security_config()
        if security_enabled:
            exporter_config.update({'https_enabled': True})
            tls_creds = self.get_certificates(daemon_spec)
            exporter_config['files'] = {
                'ceph-exporter.crt': tls_creds.cert,
                'ceph-exporter.key': tls_creds.key
            }

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        daemon_spec.final_config = merge_dicts(daemon_spec.final_config, exporter_config)

        return daemon_spec

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
