import logging
from typing import List, Any, Tuple, Dict, cast

from ceph.deployment.service_spec import CustomContainerSpec
from .service_registry import register_cephadm_service

from .cephadmservice import CephadmService, CephadmDaemonDeploySpec, DaemonDeployContext

logger = logging.getLogger(__name__)


@register_cephadm_service
class CustomContainerService(CephadmService):
    TYPE = 'container'

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        return daemon_spec

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        spec = cast(CustomContainerSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        config: Dict[str, Any] = spec.config_json()
        deps = self.get_dependencies(self.mgr, spec, daemon_spec.daemon_type)
        logger.debug(
            'Generated configuration for \'%s\' service: config-json=%s, dependencies=%s' %
            (self.TYPE, config, deps))
        return config, deps
