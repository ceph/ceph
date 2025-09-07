import errno
import logging
import json
from typing import List, cast, Optional
from ipaddress import ip_address, IPv6Address

from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import NvmeofServiceSpec, CertificateSource

from orchestrator import (
    OrchestratorError,
    DaemonDescription,
    DaemonDescriptionStatus,
    HostSpec,
)
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .service_registry import register_cephadm_service
from .. import utils

logger = logging.getLogger(__name__)
NVMEOF_CLIENT_CERT_LABEL = 'client'


@register_cephadm_service
class NvmeofService(CephService):
    TYPE = 'nvmeof'
    PROMETHEUS_PORT = 10008

    @property
    def needs_monitoring(self) -> bool:
        return True

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

    def configure_tls(self, spec: NvmeofServiceSpec, daemon_spec: CephadmDaemonDeploySpec) -> None:
        """
        Configure TLS and mTLS files for the NVMeoF daemon.

        - Always attaches server_cert/server_key if TLS is enabled.
        - If mTLS (enable_auth) is enabled, also attaches client_cert, client_key, and root_ca_cert.
        - Supports both cephadm-signed and user-provided certificates.
        """
        svc_name = spec.service_name()

        if not spec.ssl:
            self.mgr.log.info(f"TLS for nvmeof service {svc_name} is disabled.")
            return

        host = daemon_spec.host

        # Attach server-side certificates
        tls_pair = self.get_certificates(daemon_spec)
        daemon_spec.extra_files.update({
            'server_cert': tls_pair.cert,
            'server_key': tls_pair.key,
        })

        # If mTLS is not enabled, we're done
        if not spec.enable_auth:
            return

        client_cert = client_key = root_ca_cert = None

        if spec.certificate_source == CertificateSource.CEPHADM_SIGNED.value:
            client_tls_pair = self.get_self_signed_certificates_with_label(
                spec, daemon_spec, NVMEOF_CLIENT_CERT_LABEL
            )
            client_cert = client_tls_pair.cert
            client_key = client_tls_pair.key
            root_ca_cert = self.mgr.cert_mgr.get_root_ca()

        elif spec.certificate_source == CertificateSource.REFERENCE.value:
            client_cert = self.mgr.cert_mgr.get_cert('nvmeof_client_cert', service_name=svc_name, host=host)
            client_key = self.mgr.cert_mgr.get_key('nvmeof_client_key', service_name=svc_name, host=host)
            root_ca_cert = self.mgr.cert_mgr.get_cert('nvmeof_root_ca_cert', service_name=svc_name, host=host)

        elif spec.certificate_source == CertificateSource.INLINE.value:
            assert spec.client_cert and spec.client_key and spec.root_ca_cert  # for mypy
            client_cert, client_key, root_ca_cert = spec.client_cert, spec.client_key, spec.root_ca_cert
            self.mgr.cert_mgr.save_cert('nvmeof_client_cert', client_cert, svc_name, daemon_spec.host, user_made=True)
            self.mgr.cert_mgr.save_key('nvmeof_client_key', client_key, svc_name, daemon_spec.host, user_made=True)
            self.mgr.cert_mgr.save_cert('nvmeof_root_ca_cert', root_ca_cert, svc_name, daemon_spec.host, user_made=True)

        if not all([client_cert, client_key, root_ca_cert]):
            raise OrchestratorError("mTLS is enabled, but or more of client_cert, client_key, or root_ca_cert is missing or was not set correctly.")

        daemon_spec.extra_files.update({
            'client_cert': client_cert,
            'client_key': client_key,
            'root_ca_cert': root_ca_cert,
        })

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        nvmeof_gw_id = daemon_spec.daemon_id
        host_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        map_addr = spec.addr_map.get(daemon_spec.host) if spec.addr_map else None
        map_discovery_addr = spec.discovery_addr_map.get(daemon_spec.host) if spec.discovery_addr_map else None
        keyring = self.get_keyring_with_caps(self.get_auth_entity(nvmeof_gw_id),
                                             ['mon', 'profile rbd',
                                              'osd', 'profile rbd'])

        super().register_for_certificates(daemon_spec)
        self.mgr.cert_mgr.register_self_signed_cert_key_pair(spec.service_name(), NVMEOF_CLIENT_CERT_LABEL)
        self.configure_tls(spec, daemon_spec)

        # TODO: check if we can force jinja2 to generate dicts with double quotes instead of using json.dumps
        transport_tcp_options = json.dumps(spec.transport_tcp_options) if spec.transport_tcp_options else None
        iobuf_options = json.dumps(spec.iobuf_options) if spec.iobuf_options else None
        name = f'{utils.name_to_config_section(self.TYPE)}.{nvmeof_gw_id}'
        rados_id = name[len('client.'):] if name.startswith('client.') else name

        # The address is first searched in the per node address map,
        # then in the spec address configuration.
        # If neither is defined, the host IP is used as a fallback.
        addr = map_addr or spec.addr or host_ip
        self.mgr.log.info(f"gateway address: {addr} from {map_addr=} {spec.addr=} {host_ip=}")
        discovery_addr = map_discovery_addr or spec.discovery_addr or host_ip
        self.mgr.log.info(f"discovery address: {discovery_addr} from {map_discovery_addr=} {spec.discovery_addr=} {host_ip=}")
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
            'iobuf_options': iobuf_options,
            'rados_id': rados_id
        }
        gw_conf = self.mgr.template.render('services/nvmeof/ceph-nvmeof.conf.j2', context)

        daemon_spec.keyring = keyring
        daemon_spec.extra_files.update({'ceph-nvmeof.conf': gw_conf})

        # Indicate to the daemon whether to utilize huge pages
        if spec.spdk_mem_size:
            daemon_spec.extra_files['spdk_mem_size'] = str(spec.spdk_mem_size)
        elif spec.spdk_huge_pages:
            try:
                huge_pages_value = int(spec.spdk_huge_pages)
                daemon_spec.extra_files['spdk_huge_pages'] = str(huge_pages_value)
            except ValueError:
                logger.error(f"Invalid value for SPDK huge pages: {spec.spdk_huge_pages}")

        # Enable DSA probing
        if spec.enable_dsa_acceleration:
            daemon_spec.extra_files['enable_dsa_acceleration'] = str(spec.enable_dsa_acceleration)

        if spec.encryption_key:
            daemon_spec.extra_files['encryption_key'] = spec.encryption_key

        daemon_spec.final_config, _ = self.generate_config(daemon_spec)
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

            try:
                gateways = json.loads(out).get('gateways', [])
            except json.decoder.JSONDecodeError as e:
                logger.error(f'Error while trying to parse gateways JSON: {e}')
                return []

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
                        'group': spec.group if spec.group else '',
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
        if not force:
            warn_message = ('WARNING: Only one nvmeof daemon is running. Please bring another nvmeof daemon up before stopping the current one.')
            unreachable_hosts = [h.hostname for h in self.mgr.cache.get_unreachable_hosts()]
            running_nvmeof_daemons = [
                d for d in self.mgr.cache.get_daemons_by_type(self.TYPE)
                if d.status == DaemonDescriptionStatus.running and d.hostname not in unreachable_hosts
            ]
            if len(running_nvmeof_daemons) < 2:
                return HandleCommandResult(-errno.EBUSY, '', warn_message)

        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        warn_message = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, warn_message, '')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """
        # to clean the keyring up
        assert daemon.hostname

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

        self.mgr.cert_mgr.rm_self_signed_cert_key_pair(service_name, daemon.hostname, label=NVMEOF_CLIENT_CERT_LABEL)
        if spec.enable_auth and spec.certificate_source == CertificateSource.INLINE.value:
            for entry in ['nvmeof_client_cert', 'nvmeof_client_key', 'nvmeof_root_ca_cert']:
                if 'cert' in entry:
                    self.mgr.cert_mgr.rm_cert(entry, spec.service_name(), daemon.hostname)
                elif 'key' in entry:
                    self.mgr.cert_mgr.rm_key(entry, spec.service_name(), daemon.hostname)

    def get_blocking_daemon_hosts(self, service_name: str) -> List[HostSpec]:
        # we should not deploy nvmeof daemons on hosts that already have nvmeof daemons
        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[service_name].spec)
        blocking_daemons: List[DaemonDescription] = []
        other_nvmeof_services = [
            nspec for nspec in self.mgr.spec_store.get_specs_by_type('nvmeof').values()
            if nspec.service_name() != spec.service_name()
        ]
        for other_nvmeof_service in other_nvmeof_services:
            blocking_daemons += self.mgr.cache.get_daemons_by_service(other_nvmeof_service.service_name())
        blocking_daemon_hosts = [
            HostSpec(hostname=blocking_daemon.hostname)
            for blocking_daemon in blocking_daemons if blocking_daemon.hostname is not None
        ]
        return blocking_daemon_hosts
