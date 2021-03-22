import logging
from typing import List, cast, Tuple, Dict, Any

from ceph.deployment.service_spec import HA_RGWSpec

from .cephadmservice import CephadmDaemonDeploySpec, CephService
from ..utils import resolve_ip

logger = logging.getLogger(__name__)


class HA_RGWService(CephService):
    TYPE = 'ha-rgw'

    class rgw_server():
        def __init__(self, hostname: str, address: str, port: int):
            self.name = hostname
            self.ip = address
            self.port = port

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'haproxy' or daemon_spec.daemon_type == 'keepalived'
        if daemon_spec.daemon_type == 'haproxy':
            return self.haproxy_prepare_create(daemon_spec)
        else:
            return self.keepalived_prepare_create(daemon_spec)

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert daemon_spec.daemon_type == 'haproxy' or daemon_spec.daemon_type == 'keepalived'

        if daemon_spec.daemon_type == 'haproxy':
            return self.haproxy_generate_config(daemon_spec)
        else:
            return self.keepalived_generate_config(daemon_spec)

    def haproxy_prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'haproxy'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(HA_RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.haproxy_generate_config(daemon_spec)

        return daemon_spec

    def keepalived_prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'keepalived'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(HA_RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.keepalived_generate_config(daemon_spec)

        return daemon_spec

    def haproxy_generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        spec = cast(HA_RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        rgw_daemons = self.mgr.cache.get_daemons_by_type('rgw')
        rgw_servers = []
        for daemon in rgw_daemons:
            assert daemon.hostname is not None
            rgw_servers.append(self.rgw_server(
                daemon.name(),
                resolve_ip(daemon.hostname),
                daemon.ports[0] if daemon.ports else 80
            ))

        # virtual ip address cannot have netmask attached when passed to haproxy config
        # since the port is added to the end and something like 123.123.123.10/24:8080 is invalid
        virtual_ip_address = spec.virtual_ip_address
        if "/" in str(spec.virtual_ip_address):
            just_ip = str(spec.virtual_ip_address).split('/')[0]
            virtual_ip_address = just_ip

        ha_context = {'spec': spec, 'rgw_servers': rgw_servers,
                      'virtual_ip_address': virtual_ip_address}

        haproxy_conf = self.mgr.template.render('services/haproxy/haproxy.cfg.j2', ha_context)

        config_files = {
            'files': {
                "haproxy.cfg": haproxy_conf,
            }
        }
        if spec.ha_proxy_frontend_ssl_certificate:
            ssl_cert = spec.ha_proxy_frontend_ssl_certificate
            if isinstance(ssl_cert, list):
                ssl_cert = '\n'.join(ssl_cert)
            config_files['files']['haproxy.pem'] = ssl_cert

        return config_files, []

    def keepalived_generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        host = daemon_spec.host

        spec = cast(HA_RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        all_hosts = spec.definitive_host_list

        # set state. first host in placement is master all others backups
        state = 'BACKUP'
        if all_hosts[0] == host:
            state = 'MASTER'

        # remove host, daemon is being deployed on from all_hosts list for
        # other_ips in conf file and converter to ips
        all_hosts.remove(host)
        other_ips = [resolve_ip(h) for h in all_hosts]

        ka_context = {'spec': spec, 'state': state,
                      'other_ips': other_ips,
                      'host_ip': resolve_ip(host)}

        keepalived_conf = self.mgr.template.render(
            'services/keepalived/keepalived.conf.j2', ka_context)

        config_file = {
            'files': {
                "keepalived.conf": keepalived_conf,
            }
        }

        return config_file, []
