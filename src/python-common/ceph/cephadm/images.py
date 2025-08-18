# Default container images -----------------------------------------------------

from typing import NamedTuple
from enum import Enum


class ContainerImage(NamedTuple):
    image_ref: str  # reference to default container image
    key: str  # image key
    desc: str  # description of image

    def __repr__(self) -> str:
        return self.image_ref


def _create_image(image_ref: str, key: str) -> ContainerImage:
    _img_prefix = 'container_image_'
    description = key.replace('_', ' ').capitalize()
    return ContainerImage(
        image_ref,
        f'{_img_prefix}{key}',
        f'{description} container image'
    )


class DefaultImages(Enum):
    PROMETHEUS = _create_image('quay.io/prometheus/prometheus:v2.51.0', 'prometheus')
    LOKI = _create_image('docker.io/grafana/loki:3.0.0', 'loki')
    PROMTAIL = _create_image('docker.io/grafana/promtail:3.0.0', 'promtail')
    NODE_EXPORTER = _create_image('quay.io/prometheus/node-exporter:v1.7.0', 'node_exporter')
    ALERTMANAGER = _create_image('quay.io/prometheus/alertmanager:v0.27.0', 'alertmanager')
    GRAFANA = _create_image('quay.io/ceph/grafana:11.6.0', 'grafana')
    HAPROXY = _create_image('quay.io/ceph/haproxy:2.3', 'haproxy')
    KEEPALIVED = _create_image('quay.io/ceph/keepalived:2.2.4', 'keepalived')
    NVMEOF = _create_image('quay.io/ceph/nvmeof:1.5', 'nvmeof')
    SNMP_GATEWAY = _create_image('docker.io/maxwo/snmp-notifier:v1.2.1', 'snmp_gateway')
    ELASTICSEARCH = _create_image('quay.io/omrizeneva/elasticsearch:6.8.23', 'elasticsearch')
    JAEGER_COLLECTOR = _create_image('quay.io/jaegertracing/jaeger-collector:1.29',
                                     'jaeger_collector')
    JAEGER_AGENT = _create_image('quay.io/jaegertracing/jaeger-agent:1.29', 'jaeger_agent')
    JAEGER_QUERY = _create_image('quay.io/jaegertracing/jaeger-query:1.29', 'jaeger_query')
    SAMBA = _create_image('quay.io/samba.org/samba-server:ceph20-centos-amd64', 'samba')
    SAMBA_METRICS = _create_image('quay.io/samba.org/samba-metrics:ceph20-centos-amd64', 'samba_metrics')
    NGINX = _create_image('quay.io/ceph/nginx:sclorg-nginx-126', 'nginx')
    OAUTH2_PROXY = _create_image('quay.io/oauth2-proxy/oauth2-proxy:v7.6.0', 'oauth2_proxy')

    @property
    def image_ref(self) -> str:
        return self.value.image_ref

    @property
    def key(self) -> str:
        return self.value.key

    @property
    def desc(self) -> str:
        return self.value.desc


class NonCephImageServiceTypes(Enum):
    prometheus = 'prometheus'
    loki = 'loki'
    promtail = 'promtail'
    node_exporter = 'node-exporter'
    alertmanager = 'alertmanager'
    grafana = 'grafana'
    nvmeof = 'nvmeof'
    snmp_gateway = 'snmp-gateway'
    elasticsearch = 'elasticsearch'
    jaeger_collector = 'jaeger-collector'
    jaeger_query = 'jaeger-query'
    jaeger_agent = 'jaeger-agent'
    samba = 'smb'
    oauth2_proxy = 'oauth2-proxy'
