# Default container images -----------------------------------------------------

from typing import NamedTuple
from enum import Enum


class ContainerImage(NamedTuple):
    name: str
    mgr_image_key: str


_mgr_prefix = 'mgr/cephadm/container_image_'


class DefaultImages(Enum):
    PROMETHEUS = ContainerImage(
        'quay.io/prometheus/prometheus:v2.51.0', f'{_mgr_prefix}prometheus'
    )
    LOKI = ContainerImage(
        'docker.io/grafana/loki:3.0.0', f'{_mgr_prefix}loki'
    )
    PROMTAIL = ContainerImage(
        'docker.io/grafana/promtail:3.0.0', f'{_mgr_prefix}promtail'
    )
    NODE_EXPORTER = ContainerImage(
        'quay.io/prometheus/node-exporter:v1.7.0',
        f'{_mgr_prefix}node_exporter',
    )
    ALERT_MANAGER = ContainerImage(
        'quay.io/prometheus/alertmanager:v0.27.0',
        f'{_mgr_prefix}alertmanager',
    )
    GRAFANA = ContainerImage(
        'quay.io/ceph/grafana:10.4.8', f'{_mgr_prefix}grafana'
    )
    HAPROXY = ContainerImage(
        'quay.io/ceph/haproxy:2.3', f'{_mgr_prefix}haproxy'
    )
    KEEPALIVED = ContainerImage(
        'quay.io/ceph/keepalived:2.2.4', f'{_mgr_prefix}keepalived'
    )
    NVMEOF = ContainerImage(
        'quay.io/ceph/nvmeof:1.2.17', f'{_mgr_prefix}nvmeof'
    )
    SNMP_GATEWAY = ContainerImage(
        'docker.io/maxwo/snmp-notifier:v1.2.1', f'{_mgr_prefix}snmp_notifier'
    )
    ELASTICSEARCH = ContainerImage(
        'quay.io/omrizeneva/elasticsearch:6.8.23',
        f'{_mgr_prefix}elasticsearch',
    )
    JAEGER_COLLECTOR = ContainerImage(
        'quay.io/jaegertracing/jaeger-collector:1.29',
        f'{_mgr_prefix}jaeger_collector',
    )
    JAEGER_AGENT = ContainerImage(
        'quay.io/jaegertracing/jaeger-agent:1.29',
        f'{_mgr_prefix}jaeger_agent',
    )
    JAEGER_QUERY = ContainerImage(
        'quay.io/jaegertracing/jaeger-query:1.29',
        f'{_mgr_prefix}jaeger_query',
    )
    SAMBA = ContainerImage(
        'quay.io/samba.org/samba-server:devbuilds-centos-amd64',
        f'{_mgr_prefix}smb',
    )
    SAMBA_METRICS = ContainerImage(
        'quay.io/samba.org/samba-metrics:latest',
        f'{_mgr_prefix}samba_metrics'
    )
    NGINX = ContainerImage(
        'quay.io/ceph/nginx:sclorg-nginx-126', f'{_mgr_prefix}nginx'
    )
    OAUTH2_PROXY = ContainerImage(
        'quay.io/oauth2-proxy/oauth2-proxy:v7.6.0',
        f'{_mgr_prefix}oauth2_proxy',
    )
