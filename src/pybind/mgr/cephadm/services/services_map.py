# cephadm related services
from .cephadmservice import CephadmAgent
from .cephadmservice import CephExporterService
from .node_proxy import NodeProxy
from .iscsi import IscsiService
from .jaeger import JaegerAgentService
from .mgmt_gateway import MgmtGatewayService
# monitoring
from .monitoring import PrometheusService
from .monitoring import GrafanaService
from .monitoring import AlertmanagerService
from .monitoring import PromtailService
from .monitoring import NodeExporterService

# Define services mapping dictionary
ServiceMap = {
    CephadmAgent.TYPE: CephadmAgent,
    NodeProxy.TYPE: NodeProxy,
    IscsiService.TYPE: IscsiService,
    PrometheusService.TYPE: PrometheusService,
    GrafanaService.TYPE: GrafanaService,
    AlertmanagerService.TYPE: AlertmanagerService,
    PromtailService.TYPE: PromtailService,
    CephExporterService.TYPE: CephExporterService,
    NodeExporterService.TYPE: NodeExporterService,
    JaegerAgentService.TYPE: JaegerAgentService,
    MgmtGatewayService.TYPE: MgmtGatewayService
}
