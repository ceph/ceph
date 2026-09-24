from typing import Dict, Any, List, Tuple, cast, Optional, TYPE_CHECKING
from cephadm.services.cephadmservice import (
    CephadmService,
    CephadmDaemonDeploySpec,
)
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

    def generate_config(
        self, daemon_spec: CephadmDaemonDeploySpec
    ) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        return {}, []


@register_cephadm_service
class JaegerService(CephadmService):
    TYPE = 'jaeger'

    @classmethod
    def get_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """Return list of elasticsearch daemon names that jaeger depends on"""
        return sorted(mgr.cache.get_daemons_by_types(['elasticsearch']))

    def generate_config(
        self, daemon_spec: CephadmDaemonDeploySpec
    ) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        elasticsearch_nodes = get_elasticsearch_nodes(self, daemon_spec)
        otlp_port = str(self.mgr.get_ceph_option('jaeger_agent_port'))
        es_urls = ','.join(elasticsearch_nodes)

        config_yaml = (
            'service:\n'
            '  extensions: [jaeger_storage, jaeger_query]\n'
            '  pipelines:\n'
            '    traces:\n'
            '      receivers: [otlp]\n'
            '      exporters: [jaeger_storage_exporter]\n'
            'extensions:\n'
            '  jaeger_storage:\n'
            '    backends:\n'
            '      some_storage:\n'
            '        elasticsearch:\n'
            f'          server_urls: [{es_urls}]\n'
            '  jaeger_query:\n'
            '    storage:\n'
            '      traces: some_storage\n'
            'receivers:\n'
            '  otlp:\n'
            '    protocols:\n'
            '      grpc:\n'
            f'        endpoint: "0.0.0.0:{otlp_port}"\n'
            'exporters:\n'
            '  jaeger_storage_exporter:\n'
            '    trace_storage: some_storage\n'
        )
        deps = self.get_dependencies(self.mgr)
        return {'files': {'config.yaml': config_yaml}}, deps

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]) -> None:
        """Set jaeger_agent_host in the cluster config so Ceph daemons
        (OSD, RGW) know which host to send OTLP traces to."""
        if not daemon_descrs:
            return
        dd = daemon_descrs[0]
        assert dd.hostname is not None
        host_ip = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
        self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': 'global',
            'name': 'jaeger_agent_host',
            'value': host_ip,
        })

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


def get_elasticsearch_nodes(
    service: CephadmService, daemon_spec: CephadmDaemonDeploySpec
) -> List[str]:
    elasticsearch_nodes = []
    for dd in service.mgr.cache.get_daemons_by_type(
        ElasticSearchService.TYPE
    ):
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else service.mgr.inventory.get_addr(dd.hostname)
        port = (
            dd.ports[0]
            if dd.ports
            else ElasticSearchService.DEFAULT_SERVICE_PORT
        )
        url = build_url(host=addr, port=port).lstrip('/')
        elasticsearch_nodes.append(f'http://{url}')

    if len(elasticsearch_nodes) == 0:
        # takes elasticsearch address from TracingSpec if provided
        spec: TracingSpec = cast(
            TracingSpec,
            service.mgr.spec_store.active_specs[daemon_spec.service_name],
        )
        if spec.es_nodes is not None:
            urls = spec.es_nodes.split(",")
            for url in urls:
                elasticsearch_nodes.append(f'http://{url}')

    return elasticsearch_nodes
