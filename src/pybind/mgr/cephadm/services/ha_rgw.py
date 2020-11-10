import json
import logging
from typing import List, cast, Tuple, Dict, Any

from ceph.deployment.service_spec import HA_RGWSpec

from orchestrator import DaemonDescription, OrchestratorError
from .cephadmservice import CephadmDaemonSpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class HA_RGWService(CephService):
    TYPE = 'HA_RGW'


class HAproxyService(CephService):
    TYPE = 'haproxy'

    class rgw_server():
        def __init__(self, hostname: str, address: str):
            self.name = hostname
            self.ip = address

    def prepare_create(self, daemon_spec: CephadmDaemonSpec[HA_RGWSpec]) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = daemon_spec.spec

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id),
            'caps': [],
        })

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonSpec) -> Tuple[Dict[str, Any], List[str]]:

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host

        service_name: str = "HA_RGW." + daemon_id.split('.')[0]
        if service_name in self.mgr.spec_store.specs:
            spec = cast(HA_RGWSpec, self.mgr.spec_store.specs[service_name])

            rgw_daemons = self.mgr.cache.get_daemons_by_type('rgw')
            rgw_servers = []
            for daemon in rgw_daemons:
                rgw_servers.append(self.rgw_server(
                    daemon.name(), utils.resolve_ip(daemon.hostname)))

            # virtual ip address cannot have netmask attached when passed to haproxy config
            # since the port is added to the end and something like 123.123.123.10/24:8080 is invalid
            virtual_ip_address = spec.virtual_ip_address
            if "/" in str(spec.virtual_ip_address):
                just_ip = str(spec.virtual_ip_address).split('/')[0]
                virtual_ip_address = just_ip

            ha_context = {'spec': spec, 'rgw_servers': rgw_servers,
                          'virtual_ip_address': virtual_ip_address}

            haproxy_conf = self.mgr.template.render('services/haproxy/haproxy.cfg.j2', ha_context)

            config_file = {
                'files': {
                    "haproxy.cfg": haproxy_conf,
                }
            }

            return config_file, []
        else:
            config_file = {'files': {}}
            return config_file, []


class KeepAlivedService(CephService):
    TYPE = 'keepalived'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec[HA_RGWSpec]) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = daemon_spec.spec

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id),
            'caps': [],
        })

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonSpec) -> Tuple[Dict[str, Any], List[str]]:

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host

        service_name: str = "HA_RGW." + daemon_id.split('.')[0]
        if service_name in self.mgr.spec_store.specs:
            spec = cast(HA_RGWSpec, self.mgr.spec_store.specs[service_name])

            all_hosts = []
            for h, network, name in spec.definitive_host_list:
                all_hosts.append(h)

            # set state. first host in placement is master all others backups
            state = 'BACKUP'
            if all_hosts[0] == host:
                state = 'MASTER'

            # remove host, daemon is being deployed on from all_hosts list for
            # other_ips in conf file and converter to ips
            all_hosts.remove(host)
            other_ips = [utils.resolve_ip(h) for h in all_hosts]

            ka_context = {'spec': spec, 'state': state,
                          'other_ips': other_ips,
                          'host_ip': utils.resolve_ip(host)}

            keepalived_conf = self.mgr.template.render(
                'services/keepalived/keepalived.conf.j2', ka_context)

            config_file = {
                'files': {
                    "keepalived.conf": keepalived_conf,
                }
            }

            return config_file, []
        else:
            config_file = {'files': {}}
            return config_file, []
