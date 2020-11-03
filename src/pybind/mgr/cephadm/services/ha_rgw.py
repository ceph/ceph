import json
import logging
from typing import List, cast, Tuple

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

        rgw_daemons = self.mgr.cache.get_daemons_by_service('rgw')
        rgw_servers = []
        for daemon in rgw_daemons:
            rgw_servers.append(self.rgw_server(daemon.hostname, self.mgr.inventory.get_addr(daemon.hostname)))

        ha_context = {'spec': spec, 'rgw_servers': rgw_servers}

        haproxy_conf = self.mgr.template.render('services/haproxy/haproxy.cfg.j2', ha_context)

        daemon_spec.extra_files = {'haproxy.cfg': haproxy_conf}

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id),
            'caps': [],
        })

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))
        return daemon_spec

class KeepAlivedService(CephService):
    TYPE = 'keepalived'

    def prepare_create(self, daemon_spec: CephadmDaemonSpec[HA_RGWSpec]) -> CephadmDaemonSpec:
        assert self.TYPE == daemon_spec.daemon_type
        assert daemon_spec.spec

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = daemon_spec.spec

        ka_context = {'spec': spec, 'state': 'MASTER',
                      'other_ips': [],
                      'host_ip': self.mgr.inventory.get_addr(host)}

        keepalive_conf = self.mgr.template.render(
            'services/keepalived/keepalived.conf.j2', ka_context)

        daemon_spec.extra_files = {'keepalived.conf': keepalive_conf}

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.get_auth_entity(daemon_id),
            'caps': [],
        })

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))
        return daemon_spec
