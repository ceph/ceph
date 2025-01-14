import logging
from typing import List, Any, Tuple, Dict, cast

from ceph.deployment.service_spec import CustomContainerSpec
from .service_registry import register_cephadm_service

from .cephadmservice import CephadmService, CephadmDaemonDeploySpec

logger = logging.getLogger(__name__)


@register_cephadm_service
class CustomContainerService(CephadmService):
    TYPE = 'container'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) \
            -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) \
            -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []
        spec = cast(CustomContainerSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        config: Dict[str, Any] = spec.config_json()
        logger.debug(
            'Generated configuration for \'%s\' service: config-json=%s, dependencies=%s' %
            (self.TYPE, config, deps))
        return config, deps
