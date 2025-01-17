# constants.py - constant values used throughout the cephadm sources

# Default container images -----------------------------------------------------
DEFAULT_IMAGE = 'quay.ceph.io/ceph-ci/ceph:main'
DEFAULT_IMAGE_IS_MAIN = True
DEFAULT_IMAGE_RELEASE = 'squid'
DEFAULT_PROMETHEUS_IMAGE = 'quay.io/prometheus/prometheus:v2.51.0'
DEFAULT_LOKI_IMAGE = 'quay.io/ceph/loki:3.0.0'
DEFAULT_PROMTAIL_IMAGE = 'quay.io/ceph/promtail:3.0.0'
DEFAULT_NODE_EXPORTER_IMAGE = 'quay.io/prometheus/node-exporter:v1.7.0'
DEFAULT_ALERT_MANAGER_IMAGE = 'quay.io/prometheus/alertmanager:v0.27.0'
DEFAULT_GRAFANA_IMAGE = 'quay.io/ceph/grafana:10.4.8'
DEFAULT_HAPROXY_IMAGE = 'quay.io/ceph/haproxy:2.3'
DEFAULT_KEEPALIVED_IMAGE = 'quay.io/ceph/keepalived:2.2.4'
DEFAULT_NVMEOF_IMAGE = 'quay.io/ceph/nvmeof:1.2.17'
DEFAULT_SNMP_GATEWAY_IMAGE = 'quay.io/ceph/snmp-notifier:v1.2.1'
DEFAULT_ELASTICSEARCH_IMAGE = 'quay.io/omrizeneva/elasticsearch:6.8.23'
DEFAULT_JAEGER_COLLECTOR_IMAGE = 'quay.io/jaegertracing/jaeger-collector:1.29'
DEFAULT_JAEGER_AGENT_IMAGE = 'quay.io/jaegertracing/jaeger-agent:1.29'
DEFAULT_JAEGER_QUERY_IMAGE = 'quay.io/jaegertracing/jaeger-query:1.29'
DEFAULT_SMB_IMAGE = 'quay.io/samba.org/samba-server:devbuilds-centos-amd64'
DEFAULT_SMBMETRICS_IMAGE = 'quay.io/samba.org/samba-metrics:latest'
DEFAULT_NGINX_IMAGE = 'quay.io/ceph/nginx:sclorg-nginx-126'
DEFAULT_OAUTH2_PROXY_IMAGE = 'quay.io/oauth2-proxy/oauth2-proxy:v7.6.0'
DEFAULT_REGISTRY = 'quay.io'  # normalize unqualified digests to this
# ------------------------------------------------------------------------------

LATEST_STABLE_RELEASE = 'squid'
DATA_DIR = '/var/lib/ceph'
LOG_DIR = '/var/log/ceph'
LOCK_DIR = '/run/cephadm'
LOGROTATE_DIR = '/etc/logrotate.d'
SYSCTL_DIR = '/etc/sysctl.d'
UNIT_DIR = '/etc/systemd/system'
CEPH_CONF_DIR = 'config'
CEPH_CONF = 'ceph.conf'
CEPH_PUBKEY = 'ceph.pub'
CEPH_KEYRING = 'ceph.client.admin.keyring'
CEPH_DEFAULT_CONF = f'/etc/ceph/{CEPH_CONF}'
CEPH_DEFAULT_KEYRING = f'/etc/ceph/{CEPH_KEYRING}'
CEPH_DEFAULT_PUBKEY = f'/etc/ceph/{CEPH_PUBKEY}'
LOG_DIR_MODE = 0o770
DATA_DIR_MODE = 0o700
DEFAULT_MODE = 0o600
CONTAINER_INIT = True
MIN_PODMAN_VERSION = (2, 0, 2)
CGROUPS_SPLIT_PODMAN_VERSION = (2, 1, 0)
PIDS_LIMIT_UNLIMITED_PODMAN_VERSION = (3, 4, 1)
CUSTOM_PS1 = r'[ceph: \u@\h \W]\$ '
DEFAULT_TIMEOUT = None  # in seconds
DEFAULT_RETRY = 15
DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'
QUIET_LOG_LEVEL = 9  # DEBUG is 10, so using 9 to be lower level than DEBUG
NO_DEPRECATED = False
UID_NOBODY = 65534
GID_NOGROUP = 65534
