from dataclasses import replace
from typing import List, cast, Optional, TYPE_CHECKING
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from ceph.deployment.service_spec import TracingSpec, ServiceSpec
from orchestrator import DaemonDescription
from .service_registry import register_cephadm_service
from mgr_util import build_url
from cephadm import utils

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator


@register_cephadm_service
class ElasticSearchService(CephadmService):
    TYPE = 'elasticsearch'
    DEFAULT_SERVICE_PORT = 9200

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        return daemon_spec


@register_cephadm_service
class JaegerService(CephadmService):
    TYPE = 'jaeger'

    @classmethod
    def get_dependencies(cls, mgr: "CephadmOrchestrator",
                         spec: Optional[ServiceSpec] = None,
                         daemon_type: Optional[str] = None) -> List[str]:
        """Return list of elasticsearch daemon names that jaeger depends on"""
        return sorted(mgr.cache.get_daemons_by_types(['elasticsearch']))

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        elasticsearch_nodes = get_elasticsearch_nodes(self, daemon_spec)
        # For Jaeger v2 all-in-one, we need both elasticsearch nodes and OTLP receiver config
        # Get the jaeger_agent_host and jaeger_agent_port from Ceph config
        jaeger_agent_host = self.mgr.get_ceph_option('jaeger_agent_host')
        jaeger_agent_port = self.mgr.get_ceph_option('jaeger_agent_port')
        daemon_spec.final_config = {
            'elasticsearch_nodes': ",".join(elasticsearch_nodes),
            'jaeger_agent_host': str(jaeger_agent_host),
            'jaeger_agent_port': str(jaeger_agent_port),
        }
        # Set dependencies on elasticsearch nodes so jaeger waits for them to be deployed
        daemon_spec.deps = self.get_dependencies(self.mgr)
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
        action = super().choose_next_action(
            scheduled_action, daemon_type, spec, curr_deps, last_deps
        )
        # changes to jaeger deps (elasticsearch nodes) affect the way the unit.run for
        # the daemon is written, which we rewrite on redeploy, but not on reconfig.
        # However, only redeploy if dependencies actually changed to avoid continuous restarts
        if action.action is utils.Action.RECONFIG:
            if curr_deps != last_deps:
                action = utils.NextDaemonStep(utils.Action.REDEPLOY)
        return action


def get_elasticsearch_nodes(service: CephadmService, daemon_spec: CephadmDaemonDeploySpec) -> List[str]:
    elasticsearch_nodes = []
    for dd in service.mgr.cache.get_daemons_by_type(ElasticSearchService.TYPE):
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else service.mgr.inventory.get_addr(dd.hostname)
        port = dd.ports[0] if dd.ports else ElasticSearchService.DEFAULT_SERVICE_PORT
        url = build_url(host=addr, port=port).lstrip('/')
        elasticsearch_nodes.append(f'http://{url}')

    if len(elasticsearch_nodes) == 0:
        # takes elasticsearch address from TracingSpec if provided
        spec: TracingSpec = cast(
            TracingSpec, service.mgr.spec_store.active_specs[daemon_spec.service_name])
        if spec.es_nodes is not None:
            urls = spec.es_nodes.split(",")
            for url in urls:
                elasticsearch_nodes.append(f'http://{url}')

    return elasticsearch_nodes
