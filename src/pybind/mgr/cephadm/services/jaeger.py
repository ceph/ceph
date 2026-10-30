from typing import Dict, Any, List, Tuple, cast, Optional, TYPE_CHECKING
from cephadm.services.cephadmservice import (
    CephadmService,
    CephadmDaemonDeploySpec,
    DaemonDeployContext
)
from ceph.deployment.service_spec import TracingSpec, ServiceSpec
from orchestrator import DaemonDescription, OrchestratorError
from .service_registry import register_cephadm_service
from mgr_util import build_url

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator


@register_cephadm_service
class ElasticSearchService(CephadmService):
    TYPE = 'elasticsearch'
    DEFAULT_SERVICE_PORT = 9200

    def generate_config(
        self, deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == deploy_ctx.daemon_spec.daemon_type
        return {}, []


@register_cephadm_service
class JaegerService(CephadmService):
    TYPE = 'jaeger'

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """Return dependencies that affect jaeger's generated config.

        Includes elasticsearch daemon identities (name + addr:port) so that
        changes to ES daemon addresses or ports trigger a reconfig/redeploy,
        the es_nodes spec field for external-backend changes, and the
        jaeger_agent_port ceph option so that OTLP port changes also trigger
        a reconfig.
        """
        deps = []

        # Include each ES daemon's name AND its resolved addr:port so that
        # changes to a daemon's IP or port (without renaming it) are detected.
        for dd in mgr.cache.get_daemons_by_type(ElasticSearchService.TYPE):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else mgr.inventory.get_addr(dd.hostname)
            port = (
                dd.ports[0]
                if dd.ports
                else ElasticSearchService.DEFAULT_SERVICE_PORT
            )
            deps.append(f'{dd.name()}:{addr}:{port}')

        # When no managed ES daemons are present, es_nodes drives the URLs.
        if spec is not None and getattr(spec, 'es_nodes', None):
            tracing_spec = cast(TracingSpec, spec)
            deps.append(f'es_nodes:{tracing_spec.es_nodes}')

        # The OTLP port is read from this config option and written into the
        # generated config, so a change here must trigger a reconfig.
        deps.append(f'jaeger_agent_port:{mgr.get_ceph_option("jaeger_agent_port")}')

        return deps

    def generate_config(
        self, deploy_ctx: DaemonDeployContext
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec

        assert self.TYPE == daemon_spec.daemon_type
        elasticsearch_nodes = get_elasticsearch_nodes(self, daemon_spec, spec)
        otlp_port = str(self.mgr.get_ceph_option('jaeger_agent_port'))

        config_yaml = self.mgr.template.render(
            'services/jaeger.config.yaml.j2',
            {'es_urls': elasticsearch_nodes, 'otlp_port': otlp_port}
        )
        deps = self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type
        )
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
            'force': True,
        })


def get_elasticsearch_nodes(
    service: CephadmService,
    daemon_spec: CephadmDaemonDeploySpec,
    spec: Optional[ServiceSpec],
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
        if spec is not None and getattr(spec, 'es_nodes', None):
            tracing_spec = cast(TracingSpec, spec)
            urls = tracing_spec.es_nodes.split(",")
            for url in urls:
                url = url.strip()
                if not url.startswith('http://') and not url.startswith('https://'):
                    url = f'http://{url}'
                elasticsearch_nodes.append(url)

    if not elasticsearch_nodes:
        raise OrchestratorError(
            f'{daemon_spec.service_name}: no Elasticsearch backend available: '
            'no elasticsearch daemon is deployed and no es_nodes are set in the spec')

    return elasticsearch_nodes
