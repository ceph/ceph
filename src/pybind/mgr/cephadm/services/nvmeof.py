import errno
import logging
import json
from typing import List, cast, Optional
from ipaddress import ip_address, IPv6Address

from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import NvmeofServiceSpec

from orchestrator import OrchestratorError, DaemonDescription, DaemonDescriptionStatus
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class NvmeofService(CephService):
    TYPE = 'nvmeof'
    PROMETHEUS_PORT = 10008

    def config(self, spec: NvmeofServiceSpec) -> None:  # type: ignore
        assert self.TYPE == spec.service_type
        # Looking at src/pybind/mgr/cephadm/services/iscsi.py
        # asserting spec.pool/spec.group might be appropriate
        if not spec.pool:
            raise OrchestratorError("pool should be in the spec")
        if spec.group is None:
            raise OrchestratorError("group should be in the spec")
        # unlike some other config funcs, if this fails we can't
        # go forward deploying the daemon and then retry later. For
        # that reason we make no attempt to catch the OrchestratorError
        # this may raise
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        nvmeof_gw_id = daemon_spec.daemon_id
        host_ip = self.mgr.inventory.get_addr(daemon_spec.host)

        keyring = self.get_keyring_with_caps(self.get_auth_entity(nvmeof_gw_id),
                                             ['mon', 'profile rbd',
                                              'osd', 'profile rbd'])

        # TODO: check if we can force jinja2 to generate dicts with double quotes instead of using json.dumps
        transport_tcp_options = json.dumps(spec.transport_tcp_options) if spec.transport_tcp_options else None
        name = '{}.{}'.format(utils.name_to_config_section('nvmeof'), nvmeof_gw_id)
        rados_id = name[len('client.'):] if name.startswith('client.') else name
        addr = spec.addr or host_ip
        discovery_addr = spec.discovery_addr or host_ip
        context = {
            'spec': spec,
            'name': name,
            'addr': addr,
            'discovery_addr': discovery_addr,
            'port': spec.port,
            'spdk_log_level': '',
            'rpc_socket_dir': '/var/tmp/',
            'rpc_socket_name': 'spdk.sock',
            'transport_tcp_options': transport_tcp_options,
            'rados_id': rados_id
        }
        gw_conf = self.mgr.template.render('services/nvmeof/ceph-nvmeof.conf.j2', context)

        daemon_spec.keyring = keyring
        daemon_spec.extra_files = {'ceph-nvmeof.conf': gw_conf}

        # Indicate to the daemon whether to utilize huge pages
        if spec.spdk_mem_size:
            daemon_spec.extra_files['spdk_mem_size'] = str(spec.spdk_mem_size)

        if spec.enable_auth:
            if (
                not spec.client_cert
                or not spec.client_key
                or not spec.server_cert
                or not spec.server_key
                or not spec.root_ca_cert
            ):
                err_msg = 'enable_auth is true but '
                for cert_key_attr in ['server_key', 'server_cert', 'client_key', 'client_cert', 'root_ca_cert']:
                    if not hasattr(spec, cert_key_attr):
                        err_msg += f'{cert_key_attr}, '
                err_msg += 'attribute(s) missing from nvmeof spec'
                self.mgr.log.error(err_msg)
            else:
                daemon_spec.extra_files['server_cert'] = spec.server_cert
                daemon_spec.extra_files['client_cert'] = spec.client_cert
                daemon_spec.extra_files['server_key'] = spec.server_key
                daemon_spec.extra_files['client_key'] = spec.client_key
                daemon_spec.extra_files['root_ca_cert'] = spec.root_ca_cert

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        daemon_spec.deps = []
        return daemon_spec

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]) -> None:
        """ Overrides the daemon_check_post to add nvmeof gateways safely
        """
        self.mgr.log.info(f"nvmeof daemon_check_post {daemon_descrs}")
        for dd in daemon_descrs:
            spec = cast(NvmeofServiceSpec,
                        self.mgr.spec_store.all_specs.get(dd.service_name(), None))
            if not spec:
                self.mgr.log.error(f'Failed to find spec for {dd.name()}')
                return
            pool = spec.pool
            group = spec.group
            # Notify monitor about this gateway creation
            cmd = {
                'prefix': 'nvme-gw create',
                'id': f'{utils.name_to_config_section("nvmeof")}.{dd.daemon_id}',
                'group': group,
                'pool': pool
            }
            self.mgr.log.info(f"create gateway: monitor command {cmd}")
            _, _, err = self.mgr.mon_command(cmd)
            if err:
                err_msg = (f"Unable to send monitor command {cmd}, error {err}")
                logger.error(err_msg)
                raise OrchestratorError(err_msg)
        super().daemon_check_post(daemon_descrs)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        def get_set_cmd_dicts(out: str) -> List[dict]:
            gateways = json.loads(out)['gateways']
            cmd_dicts = []

            for dd in daemon_descrs:
                spec = cast(NvmeofServiceSpec,
                            self.mgr.spec_store.all_specs.get(dd.service_name(), None))
                service_name = dd.service_name()
                if dd.hostname is None:
                    err_msg = ('Trying to config_dashboard nvmeof but no hostname is defined')
                    logger.error(err_msg)
                    raise OrchestratorError(err_msg)

                if not spec:
                    logger.warning(f'No ServiceSpec found for {service_name}')
                    continue

                ip = utils.resolve_ip(self.mgr.inventory.get_addr(dd.hostname))
                if type(ip_address(ip)) is IPv6Address:
                    ip = f'[{ip}]'
                service_url = '{}:{}'.format(ip, spec.port or '5500')
                gw = gateways.get(dd.hostname)
                if not gw or gw['service_url'] != service_url:
                    logger.info(f'Adding NVMeoF gateway {service_url} to Dashboard')
                    cmd_dicts.append({
                        'prefix': 'dashboard nvmeof-gateway-add',
                        'inbuf': service_url,
                        'name': service_name,
                        'group': spec.group,
                        'daemon_name': dd.name()
                    })
            return cmd_dicts

        self._check_and_set_dashboard(
            service_name='nvmeof',
            get_cmd='dashboard nvmeof-gateway-list',
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 nvmeof, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Nvmeof', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nvmeof daemon. make sure none are down
        warn_message = ('ALERT: 1 nvmeof daemon is already down. Please bring it back up before stopping this one')
        nvmeof_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        for i in nvmeof_daemons:
            if i.status != DaemonDescriptionStatus.running:
                return HandleCommandResult(-errno.EBUSY, '', warn_message)

        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        warn_message = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, warn_message, '')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """
        # to clean the keyring up
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        service_name = daemon.service_name()
        daemon_name = daemon.name()

        # remove config for dashboard nvmeof gateways if any
        ret, _, err = self.mgr.mon_command({
            'prefix': 'dashboard nvmeof-gateway-rm',
            'name': service_name,
            'daemon_name': daemon_name
        })
        if not ret:
            logger.info(f'{daemon_name} removed from nvmeof gateways dashboard config')

        spec = cast(NvmeofServiceSpec,
                    self.mgr.spec_store.all_specs.get(daemon.service_name(), None))
        if not spec:
            self.mgr.log.error(f'Failed to find spec for {daemon_name}')
            return
        pool = spec.pool
        group = spec.group

        # Notify monitor about this gateway deletion
        cmd = {
            'prefix': 'nvme-gw delete',
            'id': f'{utils.name_to_config_section("nvmeof")}.{daemon.daemon_id}',
            'group': group,
            'pool': pool
        }
        self.mgr.log.info(f"delete gateway: monitor command {cmd}")
        _, _, err = self.mgr.mon_command(cmd)
        if err:
            self.mgr.log.error(f"Unable to send monitor command {cmd}, error {err}")
