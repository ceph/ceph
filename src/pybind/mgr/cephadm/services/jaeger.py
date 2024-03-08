from typing import List, cast
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from ceph.deployment.service_spec import TracingSpec
from mgr_util import build_url


class ElasticSearchService(CephadmService):
    TYPE = 'elasticsearch'
    DEFAULT_SERVICE_PORT = 9200

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        return daemon_spec


class JaegerAgentService(CephadmService):
    TYPE = 'jaeger-agent'
    DEFAULT_SERVICE_PORT = 6799

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        collectors = []
        deps: List[str] = []
        for dd in self.mgr.cache.get_daemons_by_type(JaegerCollectorService.TYPE):
            # scrape jaeger-collector nodes
            assert dd.hostname is not None
            port = dd.ports[0] if dd.ports else JaegerCollectorService.DEFAULT_SERVICE_PORT
            url = build_url(host=dd.hostname, port=port).lstrip('/')
            collectors.append(url)
            deps.append(url)
        daemon_spec.final_config = {'collector_nodes': ",".join(collectors)}
        daemon_spec.deps = sorted(deps)
        return daemon_spec


class JaegerCollectorService(CephadmService):
    TYPE = 'jaeger-collector'
    DEFAULT_SERVICE_PORT = 14250

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        elasticsearch_nodes = get_elasticsearch_nodes(self, daemon_spec)
        daemon_spec.final_config = {'elasticsearch_nodes': ",".join(elasticsearch_nodes)}
        return daemon_spec


class JaegerQueryService(CephadmService):
    TYPE = 'jaeger-query'
    DEFAULT_SERVICE_PORT = 16686

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        elasticsearch_nodes = get_elasticsearch_nodes(self, daemon_spec)
        daemon_spec.final_config = {'elasticsearch_nodes': ",".join(elasticsearch_nodes)}
        return daemon_spec


def get_elasticsearch_nodes(service: CephadmService, daemon_spec: CephadmDaemonDeploySpec) -> List[str]:
    elasticsearch_nodes = []
    for dd in service.mgr.cache.get_daemons_by_type(ElasticSearchService.TYPE):
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else service.mgr.inventory.get_addr(dd.hostname)
        port = dd.ports[0] if dd.ports else ElasticSearchService.DEFAULT_SERVICE_PORT
        url = build_url(host=addr, port=port).lstrip('/')
        elasticsearch_nodes.append(f'http://{url}')

    if len(elasticsearch_nodes) == 0:
        # takes elasticsearch address from TracingSpec
        spec: TracingSpec = cast(
            TracingSpec, service.mgr.spec_store.active_specs[daemon_spec.service_name])
        assert spec.es_nodes is not None
        urls = spec.es_nodes.split(",")
        for url in urls:
            elasticsearch_nodes.append(f'http://{url}')

    return elasticsearch_nodes
