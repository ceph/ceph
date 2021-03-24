import ipaddress
import logging
import random
import string
from typing import List, cast, Tuple, Dict, Any

from ceph.deployment.service_spec import VIPSpec

from .cephadmservice import CephadmDaemonDeploySpec, CephService
from ..utils import resolve_ip

logger = logging.getLogger(__name__)


class VIPService(CephService):
    TYPE = 'vip'

    def prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'keepalived'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(VIPSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> Tuple[Dict[str, Any], List[str]]:
        spec = cast(VIPSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        assert spec.backend_service
        backend_spec = self.mgr.spec_store[spec.backend_service].spec
        assert backend_spec

        # generate password?
        pw_key = f'{spec.service_name()}/password'
        password = self.mgr.get_store(pw_key)
        if password is None:
            password = ''.join(random.choice(string.ascii_lowercase) for _ in range(20))
            self.mgr.set_store(pw_key, password)
        else:
            if spec.password:
                self.mgr.set_store(pw_key, None)

        daemons = self.mgr.cache.get_daemons_by_service(spec.backend_service)
        deps = sorted([d.name() for d in daemons])

        host = daemon_spec.host
        hosts = sorted(list(set([str(d.hostname) for d in daemons])))

        # interface
        interface = 'eth0'
        for subnet, ifaces in self.mgr.cache.networks.get(host, {}).items():
            logger.info(f'subnet {subnet} ifaces {ifaces} vip {spec.vip}')
            if ifaces and ipaddress.ip_address(spec.vip) in ipaddress.ip_network(subnet):
                logger.info(f'{spec.vip} is in {subnet}')
                interface = list(ifaces.keys())[0]
                break

        # script to monitor health
        uri = '/'
        if backend_spec.service_type == 'haproxy':
            uri = '/health'

        script = None
        for d in daemons:
            if d.hostname == host:
                if d.daemon_type in ['rgw', 'haproxy']:
                    assert d.ports
                    port = d.ports[0]
                    script = f'/usr/bin/curl http://{d.ip or "localhost"}:{port}{uri}'
        assert script

        # set state. first host in placement is master all others backups
        state = 'BACKUP'
        if hosts[0] == host:
            state = 'MASTER'

        # remove host, daemon is being deployed on from hosts list for
        # other_ips in conf file and converter to ips
        hosts.remove(host)
        other_ips = [resolve_ip(h) for h in hosts]

        keepalived_conf = self.mgr.template.render(
            'services/vip/keepalived.conf.j2',
            {
                'spec': spec,
                'script': script,
                'password': password,
                'interface': interface,
                'state': state,
                'other_ips': other_ips,
                'host_ip': resolve_ip(host),
            }
        )

        config_file = {
            'files': {
                "keepalived.conf": keepalived_conf,
            }
        }

        return config_file, deps
