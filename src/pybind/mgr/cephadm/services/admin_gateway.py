import errno
import ipaddress
import logging
import os
import socket
from typing import List, Any, Tuple, Dict, Optional, cast
from urllib.parse import urlparse

from mgr_module import HandleCommandResult
from mgr_module import ServiceInfoT
from ceph.deployment.utils import wrap_ipv6
from mgr_util import build_url

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import AdminGatewaySpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from cephadm.services.monitoring import AlertmanagerService, PrometheusService

logger = logging.getLogger(__name__)


class AdminGatewayService(CephadmService):
    TYPE = 'admin-gateway'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def get_service_endpoints(self, service_name, scheme):
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service(service_name):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else AlertmanagerService.DEFAULT_SERVICE_PORT
            srv_entries.append('{}'.format(build_url(scheme=scheme, host=addr, port=port).lstrip('/')))
        return srv_entries

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []
        prometheus_eps = self.get_service_endpoints('prometheus', 'http')
        alertmanager_eps = self.get_service_endpoints('alertmanager', 'http')
        grafana_eps = self.get_service_endpoints('grafana', 'https')
        context = {
            'mgr_addr': f"http://{wrap_ipv6(self.mgr.get_mgr_ip())}:8080",
            'prometheus_url': f"{prometheus_eps[0]}"  if prometheus_eps else None,
            'alertmanager_url': f"{alertmanager_eps[0]}"  if alertmanager_eps else None,
            'grafana_url': f"{grafana_eps[0]}" if grafana_eps else None
        }
        conf = self.mgr.template.render('services/admin-gateway/nginx.conf.j2', context)
        return {
            "files": {
                "nginx.conf": conf
            }
        }, sorted(deps)
