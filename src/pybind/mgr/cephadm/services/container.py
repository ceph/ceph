import logging
from typing import List, Any, Tuple, Dict

from ceph.deployment.service_spec import CustomContainerSpec

from .cephadmservice import CephadmService, CephadmDaemonSpec

logger = logging.getLogger(__name__)


class CustomContainerService(CephadmService):
    TYPE = 'container'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec[CustomContainerSpec]) \
            -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonSpec[CustomContainerSpec]) \
            -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec
        deps: List[str] = []
        spec: CustomContainerSpec = daemon_spec.spec
        config: Dict[str, Any] = spec.config_json()
        logger.debug(
            'Generated configuration for \'%s\' service: config-json=%s, dependencies=%s' %
            (self.TYPE, config, deps))
        return config, deps
