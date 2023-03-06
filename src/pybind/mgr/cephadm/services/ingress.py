import ipaddress
import logging
import random
import string
from typing import List, Dict, Any, Tuple, cast, Optional

from ceph.deployment.service_spec import IngressSpec
from mgr_util import build_url
from cephadm import utils
from orchestrator import OrchestratorError, DaemonDescription
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, CephService

logger = logging.getLogger(__name__)


class IngressService(CephService):
    TYPE = 'ingress'
    MAX_KEEPALIVED_PASS_LEN = 8

    def primary_daemon_type(self) -> str:
        return 'haproxy'

    def per_host_daemon_type(self) -> Optional[str]:
        return 'keepalived'

    def prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        if daemon_spec.daemon_type == 'haproxy':
            return self.haproxy_prepare_create(daemon_spec)
        if daemon_spec.daemon_type == 'keepalived':
            return self.keepalived_prepare_create(daemon_spec)
        assert False, "unexpected daemon type"

    def generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec
    ) -> Tuple[Dict[str, Any], List[str]]:
        if daemon_spec.daemon_type == 'haproxy':
            return self.haproxy_generate_config(daemon_spec)
        else:
            return self.keepalived_generate_config(daemon_spec)
        assert False, "unexpected daemon type"

    def haproxy_prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'haproxy'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(IngressSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.debug('prepare_create haproxy.%s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.haproxy_generate_config(daemon_spec)

        return daemon_spec

    def haproxy_generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> Tuple[Dict[str, Any], List[str]]:
        spec = cast(IngressSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        assert spec.backend_service
        if spec.backend_service not in self.mgr.spec_store:
            raise RuntimeError(
                f'{spec.service_name()} backend service {spec.backend_service} does not exist')
        backend_spec = self.mgr.spec_store[spec.backend_service].spec
        daemons = self.mgr.cache.get_daemons_by_service(spec.backend_service)
        deps = [d.name() for d in daemons]

        # generate password?
        pw_key = f'{spec.service_name()}/monitor_password'
        password = self.mgr.get_store(pw_key)
        if password is None:
            if not spec.monitor_password:
                password = ''.join(random.choice(string.ascii_lowercase)
                                   for _ in range(self.MAX_KEEPALIVED_PASS_LEN))
                self.mgr.set_store(pw_key, password)
        else:
            if spec.monitor_password:
                self.mgr.set_store(pw_key, None)
        if spec.monitor_password:
            password = spec.monitor_password

        if backend_spec.service_type == 'nfs':
            mode = 'tcp'
            # we need to get the nfs daemon with the highest rank_generation for
            # each rank we are currently deploying for the haproxy config
            # for example if we had three (rank, rank_generation) pairs of
            # (0, 0), (0, 1), (1, 0) we would want the nfs daemon corresponding
            # to (0, 1) and (1, 0) because those are the two with the highest
            # rank_generation for the existing ranks (0 and 1, with the highest
            # rank_generation for rank 0 being 1 and highest rank_generation for
            # rank 1 being 0)
            ranked_daemons = [d for d in daemons if (d.rank is not None and d.rank_generation is not None)]
            by_rank: Dict[int, DaemonDescription] = {}
            for d in ranked_daemons:
                # It doesn't seem like mypy can figure out that rank
                # and rank_generation for both the daemon we're looping on
                # and all those in by_rank cannot be None due to the filtering
                # when creating the ranked_daemons list, which is why these
                # seemingly unnecessary assertions are here.
                assert d.rank is not None
                if d.rank not in by_rank:
                    by_rank[d.rank] = d
                else:
                    same_rank_nfs = by_rank[d.rank]
                    assert d.rank_generation is not None
                    assert same_rank_nfs.rank_generation is not None
                    # if we have multiple of the same rank. take the one
                    # with the highesr rank generation
                    if d.rank_generation > same_rank_nfs.rank_generation:
                        by_rank[d.rank] = d
            servers = []

            # try to establish how many ranks we *should* have
            num_ranks = backend_spec.placement.count
            if not num_ranks:
                num_ranks = 1 + max(by_rank.keys())

            for rank in range(num_ranks):
                if rank in by_rank:
                    d = by_rank[rank]
                    assert d.ports
                    servers.append({
                        'name': f"{spec.backend_service}.{rank}",
                        'ip': d.ip or utils.resolve_ip(self.mgr.inventory.get_addr(str(d.hostname))),
                        'port': d.ports[0],
                    })
                else:
                    # offline/missing server; leave rank in place
                    servers.append({
                        'name': f"{spec.backend_service}.{rank}",
                        'ip': '0.0.0.0',
                        'port': 0,
                    })
        else:
            mode = 'http'
            servers = [
                {
                    'name': d.name(),
                    'ip': d.ip or utils.resolve_ip(self.mgr.inventory.get_addr(str(d.hostname))),
                    'port': d.ports[0],
                } for d in daemons if d.ports
            ]

        host_ip = daemon_spec.ip or self.mgr.inventory.get_addr(daemon_spec.host)
        haproxy_conf = self.mgr.template.render(
            'services/ingress/haproxy.cfg.j2',
            {
                'spec': spec,
                'backend_spec': backend_spec,
                'mode': mode,
                'servers': servers,
                'user': spec.monitor_user or 'admin',
                'password': password,
                'ip': "*" if spec.virtual_ips_list else str(spec.virtual_ip).split('/')[0] or daemon_spec.ip or '*',
                'frontend_port': daemon_spec.ports[0] if daemon_spec.ports else spec.frontend_port,
                'monitor_port': daemon_spec.ports[1] if daemon_spec.ports else spec.monitor_port,
                'local_host_ip': host_ip
            }
        )
        config_files = {
            'files': {
                "haproxy.cfg": haproxy_conf,
            }
        }
        if spec.ssl_cert:
            ssl_cert = spec.ssl_cert
            if isinstance(ssl_cert, list):
                ssl_cert = '\n'.join(ssl_cert)
            config_files['files']['haproxy.pem'] = ssl_cert

        return config_files, sorted(deps)

    def keepalived_prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'keepalived'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(IngressSpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.debug('prepare_create keepalived.%s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.keepalived_generate_config(daemon_spec)

        return daemon_spec

    def keepalived_generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> Tuple[Dict[str, Any], List[str]]:
        spec = cast(IngressSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        assert spec.backend_service

        # generate password?
        pw_key = f'{spec.service_name()}/keepalived_password'
        password = self.mgr.get_store(pw_key)
        if password is None:
            if not spec.keepalived_password:
                password = ''.join(random.choice(string.ascii_lowercase)
                                   for _ in range(self.MAX_KEEPALIVED_PASS_LEN))
                self.mgr.set_store(pw_key, password)
        else:
            if spec.keepalived_password:
                self.mgr.set_store(pw_key, None)
        if spec.keepalived_password:
            password = spec.keepalived_password

        daemons = self.mgr.cache.get_daemons_by_service(spec.service_name())

        if not daemons:
            raise OrchestratorError(
                f'Failed to generate keepalived.conf: No daemons deployed for {spec.service_name()}')

        deps = sorted([d.name() for d in daemons if d.daemon_type == 'haproxy'])

        host = daemon_spec.host
        hosts = sorted(list(set([host] + [str(d.hostname) for d in daemons])))

        # interface
        bare_ips = []
        if spec.virtual_ip:
            bare_ips.append(str(spec.virtual_ip).split('/')[0])
        elif spec.virtual_ips_list:
            bare_ips = [str(vip).split('/')[0] for vip in spec.virtual_ips_list]
        interface = None
        for bare_ip in bare_ips:
            for subnet, ifaces in self.mgr.cache.networks.get(host, {}).items():
                if ifaces and ipaddress.ip_address(bare_ip) in ipaddress.ip_network(subnet):
                    interface = list(ifaces.keys())[0]
                    logger.info(
                        f'{bare_ip} is in {subnet} on {host} interface {interface}'
                    )
                    break
            else:  # nobreak
                continue
            break
        # try to find interface by matching spec.virtual_interface_networks
        if not interface and spec.virtual_interface_networks:
            for subnet, ifaces in self.mgr.cache.networks.get(host, {}).items():
                if subnet in spec.virtual_interface_networks:
                    interface = list(ifaces.keys())[0]
                    logger.info(
                        f'{spec.virtual_ip} will be configured on {host} interface '
                        f'{interface} (which has guiding subnet {subnet})'
                    )
                    break
        if not interface:
            raise OrchestratorError(
                f"Unable to identify interface for {spec.virtual_ip} on {host}"
            )

        # script to monitor health
        script = '/usr/bin/false'
        for d in daemons:
            if d.hostname == host:
                if d.daemon_type == 'haproxy':
                    assert d.ports
                    port = d.ports[1]   # monitoring port
                    host_ip = d.ip or self.mgr.inventory.get_addr(d.hostname)
                    script = f'/usr/bin/curl {build_url(scheme="http", host=host_ip, port=port)}/health'
        assert script

        states = []
        priorities = []
        virtual_ips = []

        # Set state and priority. Have one master for each VIP. Or at least the first one as master if only one VIP.
        if spec.virtual_ip:
            virtual_ips.append(spec.virtual_ip)
            if hosts[0] == host:
                states.append('MASTER')
                priorities.append(100)
            else:
                states.append('BACKUP')
                priorities.append(90)

        elif spec.virtual_ips_list:
            virtual_ips = spec.virtual_ips_list
            if len(virtual_ips) > len(hosts):
                raise OrchestratorError(
                    "Number of virtual IPs for ingress is greater than number of available hosts"
                )
            for x in range(len(virtual_ips)):
                if hosts[x] == host:
                    states.append('MASTER')
                    priorities.append(100)
                else:
                    states.append('BACKUP')
                    priorities.append(90)

        # remove host, daemon is being deployed on from hosts list for
        # other_ips in conf file and converter to ips
        if host in hosts:
            hosts.remove(host)
        other_ips = [utils.resolve_ip(self.mgr.inventory.get_addr(h)) for h in hosts]

        keepalived_conf = self.mgr.template.render(
            'services/ingress/keepalived.conf.j2',
            {
                'spec': spec,
                'script': script,
                'password': password,
                'interface': interface,
                'virtual_ips': virtual_ips,
                'states': states,
                'priorities': priorities,
                'other_ips': other_ips,
                'host_ip': utils.resolve_ip(self.mgr.inventory.get_addr(host)),
            }
        )

        config_file = {
            'files': {
                "keepalived.conf": keepalived_conf,
            }
        }

        return config_file, deps
