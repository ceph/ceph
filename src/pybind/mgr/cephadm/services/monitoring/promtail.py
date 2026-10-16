from typing import TYPE_CHECKING
from typing import List, Any, Tuple, Dict, Optional
from ..service_registry import register_cephadm_service
from ceph.deployment.service_spec import ServiceSpec
from cephadm.services.cephadmservice import CephadmService, DaemonDeployContext


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
class PromtailService(CephadmService):
    TYPE = 'promtail'
    DEFAULT_SERVICE_PORT = 9080

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        return sorted(mgr.cache.get_daemons_by_types(['loki']))

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)
        daemons = self.mgr.cache.get_daemons_by_service('loki')
        loki_host = ''
        for i, dd in enumerate(daemons):
            assert dd.hostname is not None
            if i == 0:
                loki_host = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)

        context = {
            'client_hostname': loki_host,
        }

        yml = self.mgr.template.render('services/promtail.yml.j2', context)
        return {
            "files": {
                "promtail.yml": yml
            }
        }, deps
