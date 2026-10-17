from typing import TYPE_CHECKING
import errno
from typing import List, Optional, Dict, Any, Tuple, cast
from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import RGWSpec, ServiceSpec
from mgr_util import build_url
from orchestrator import OrchestratorError, DaemonDescription
from cephadm import utils
from ceph.cephadm.d3n_types import D3NCache, D3NCacheError, D3NCacheSpec, d3n_get_host_devs, d3n_paths
from cephadm.services.rgw_d3n import D3NDevicePlanner
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext, RGW_PROFILE_RELEASE
import logging

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


@register_cephadm_service
class RgwService(CephService):
    TYPE = 'rgw'

    def allow_colo(self) -> bool:
        return True

    def choose_next_action(
        self,
        scheduled_action: utils.Action,
        daemon_type: Optional[str],
        spec: Optional[ServiceSpec],
        curr_deps: List[str],
        last_deps: List[str],
        daemon: Optional[DaemonDescription] = None,
    ) -> utils.NextDaemonStep:
        step = super().choose_next_action(
            scheduled_action, daemon_type, spec, curr_deps, last_deps
        )
        # Only REDEPLOY when frontend config changed, as rgw_frontends
        # can't be updated at runtime other changes like ssl
        # certs can be applied via reconfig without a restart.
        if step.action is utils.Action.RECONFIG:
            changed = set(curr_deps).symmetric_difference(last_deps)
            if any(d.startswith('frontend:') for d in changed):
                return utils.NextDaemonStep(utils.Action.REDEPLOY)
        return step

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        deps = []
        # we keep the following deps calculation for backward compatibility
        # as old RGW specs use rgw_frontend_ssl_certificate instead of modern
        # ssl_cert/ssl_key fields
        rgw_spec = cast(RGWSpec, spec)
        ssl_cert = getattr(rgw_spec, 'rgw_frontend_ssl_certificate', None)
        if ssl_cert:
            if isinstance(ssl_cert, list):
                ssl_cert = '\n'.join(ssl_cert)
            deps.append(f'ssl-cert:{utils.config_hash(ssl_cert)}')

        if rgw_spec:
            frontend_parts = []
            if rgw_spec.rgw_frontend_type:
                frontend_parts.append(f'type={rgw_spec.rgw_frontend_type}')
            if rgw_spec.rgw_frontend_extra_args:
                frontend_parts.append(
                    f'extra_args={sorted(rgw_spec.rgw_frontend_extra_args)}')
            allow_reuse = getattr(rgw_spec, 'allow_port_reuse', False)
            frontend_parts.append(f'allow_port_reuse={allow_reuse}')
            deps.append(f'frontend:{utils.config_hash(str(sorted(frontend_parts)))}')

        return sorted(deps)

    def set_realm_zg_zone(self, spec: RGWSpec) -> None:
        assert self.TYPE == spec.service_type

        if spec.rgw_realm:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_realm',
                'value': spec.rgw_realm,
            })
        if spec.rgw_zonegroup:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_zonegroup',
                'value': spec.rgw_zonegroup,
            })
        if spec.rgw_zone:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': f"{utils.name_to_config_section('rgw')}.{spec.service_id}",
                'name': 'rgw_zone',
                'value': spec.rgw_zone,
            })

    def config(self, spec: RGWSpec) -> None:  # type: ignore
        assert self.TYPE == spec.service_type

        # set rgw_realm rgw_zonegroup and rgw_zone, if present
        self.set_realm_zg_zone(spec)

        if spec.zonegroup_hostnames:
            san_list = spec.zonegroup_hostnames or []
            hostnames = san_list + [f"*.{h}" for h in san_list] if spec.wildcard_enabled else san_list

            # zonegroup modify requires explicit realm/zonegroup/zone identifiers.
            # on single-site deployments, these values may be unset, avoid calling.
            if not (spec.rgw_realm and spec.rgw_zonegroup and spec.rgw_zone):
                logger.warning(
                    f"Skipping 'rgw zonegroup modify' for {spec.service_name()}: "
                    "zonegroup_hostnames is set but rgw_realm/rgw_zonegroup/rgw_zone "
                    "were not provided in the spec."
                )
            else:
                zg_update_cmd = {
                    'prefix': 'rgw zonegroup modify',
                    'realm_name': spec.rgw_realm,
                    'zonegroup_name': spec.rgw_zonegroup,
                    'zone_name': spec.rgw_zone,
                    'hostnames': hostnames,
                }
                logger.debug(f'rgw cmd: {zg_update_cmd}')
                ret, out, err = self.mgr.check_mon_command(zg_update_cmd)

        # TODO: fail, if we don't have a spec
        logger.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.mgr.spec_store.save(spec)
        self.mgr.trigger_connect_dashboard_rgw()

    def _compute_d3n_cache_for_daemon(
        self,
        daemon_spec: CephadmDaemonDeploySpec,
        spec: RGWSpec,
    ) -> Optional[D3NCache]:

        alloc = D3NDevicePlanner(self.mgr)
        d3n_raw = getattr(spec, 'd3n_cache', None)

        if not d3n_raw:
            return None

        try:
            d3n = D3NCacheSpec.from_json(d3n_raw)
        except D3NCacheError as e:
            raise OrchestratorError(str(e))

        host = daemon_spec.host
        if not host:
            raise OrchestratorError("missing host in daemon_spec")

        service_name = daemon_spec.service_name
        daemon_details = [
            dd for dd in self.mgr.cache.get_daemons_by_service(service_name)
            if dd.hostname == host
        ]

        fs_type, size_bytes, devs = d3n_get_host_devs(d3n, host)
        device = alloc.plan_device_for_daemon(service_name, host, devs, daemon_spec.daemon_id, daemon_details)

        logger.info(
            f"[D3N][alloc] service={service_name} host={host} daemon={daemon_spec.daemon_id} "
            f"chosen={device} used=? devs={devs}"

        )

        if daemon_spec.daemon_id:
            # warn if sharing is unavoidable
            alloc.warn_if_sharing_unavoidable(service_name, host, devs)

        mountpoint, cache_path = d3n_paths(self.mgr._cluster_fsid, device, daemon_spec.daemon_id)

        return D3NCache(
            device=device,
            filesystem=fs_type,
            size_bytes=size_bytes,
            mountpoint=mountpoint,
            cache_path=cache_path,
        )

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        super().prepare_certificates(daemon_spec)
        rgw_id, _ = daemon_spec.daemon_id, daemon_spec.host
        spec = cast(RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        deploy_ctx.service_spec = spec

        keyring = self.get_keyring(rgw_id)

        if daemon_spec.ports:
            port = daemon_spec.ports[0]
        else:
            # this is a redeploy of older instance that doesn't have an explicitly
            # assigned port, in which case we can assume there is only 1 per host
            # and it matches the spec.
            ports = spec.get_port()
            if spec.ssl:
                port = ports[1] if len(ports) > 1 else ports[0]
            else:
                port = ports[0]

        if spec.ssl:

            legacy_pem = spec.rgw_frontend_ssl_certificate
            if legacy_pem and not spec.ssl_cert:
                if isinstance(legacy_pem, list):
                    pem = '\n'.join(legacy_pem)
                else:
                    pem = legacy_pem
            else:
                san_list = spec.zonegroup_hostnames or []
                custom_sans = san_list + [f"*.{h}" for h in san_list] if spec.wildcard_enabled else san_list
                tls_creds = self.get_certificates(daemon_spec, custom_sans=custom_sans)
                pem = f'{tls_creds.key.rstrip()}\n{tls_creds.cert.lstrip()}'

            rgw_cert_name = daemon_spec.name() if spec.generate_cert else spec.service_name()
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': f'rgw/cert/{rgw_cert_name}',
                'val': pem,
            })

        # configure frontend
        args = []
        ftype = spec.rgw_frontend_type or "beast"

        # if an ssl_certificate arg was passed as part of rgw_frontend_extra_args
        # then we shouldn't add it automatically else the rgw won't start
        extra_ssl_cert_provided = any(
            arg.startswith("ssl_certificate=")
            for arg in (spec.rgw_frontend_extra_args or [])
        )

        if extra_ssl_cert_provided and spec.generate_cert:
            raise OrchestratorError("Cannot provide ssl_certificate in combination with generate_cert")

        # pick ip RGW should bind to
        ip_to_bind_to = ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec) or ''
            if ip_to_bind_to:
                # Tell the host-side port-in-use precheck (fetch_endpoints /
                # port_in_use) to probe this IP. Without port_ips it falls
                # back to 0.0.0.0 and can false-conflict with listeners on
                # other addresses.
                daemon_spec.port_ips.update({str(port): ip_to_bind_to})
            else:
                logger.warning(
                    f'Failed to find ip in {spec.networks} for host {daemon_spec.host}. '
                    f'{daemon_spec.name()} will bind to all IPs'
                )
        elif daemon_spec.ip:
            ip_to_bind_to = daemon_spec.ip
            # Same as only_bind_port_on_networks: keep precheck in sync with
            # the narrow endpoint=<ip>:<port> / port=<ip>:<port> bind below.
            daemon_spec.port_ips.update({str(port): ip_to_bind_to})

        if ftype == 'beast':
            if spec.ssl:
                if ip_to_bind_to:
                    args.append(
                        f"ssl_endpoint={build_url(host=ip_to_bind_to, port=port).lstrip('/')}")
                else:
                    args.append(f"ssl_port={port}")
                if spec.generate_cert:
                    args.append(f"ssl_certificate=config://rgw/cert/{daemon_spec.name()}")
                elif not extra_ssl_cert_provided:
                    args.append(f"ssl_certificate=config://rgw/cert/{spec.service_name()}")
            else:
                if ip_to_bind_to:
                    args.append(f"endpoint={build_url(host=ip_to_bind_to, port=port).lstrip('/')}")
                else:
                    args.append(f"port={port}")
        elif ftype == 'civetweb':
            if spec.ssl:
                if ip_to_bind_to:
                    # note the 's' suffix on port
                    args.append(f"port={build_url(host=ip_to_bind_to, port=port).lstrip('/')}s")
                else:
                    args.append(f"port={port}s")  # note the 's' suffix on port
                if spec.generate_cert:
                    args.append(f"ssl_certificate=config://rgw/cert/{daemon_spec.name()}")
                elif not extra_ssl_cert_provided:
                    args.append(f"ssl_certificate=config://rgw/cert/{spec.service_name()}")
            else:
                if ip_to_bind_to:
                    args.append(f"port={build_url(host=ip_to_bind_to, port=port).lstrip('/')}")
                else:
                    args.append(f"port={port}")
        else:
            raise OrchestratorError(f'Invalid rgw_frontend_type parameter: {ftype}. Valid values are: beast, civetweb.')

        if spec.rgw_frontend_extra_args is not None:
            args.extend(spec.rgw_frontend_extra_args)

        frontend = f'{ftype} {" ".join(args)}'
        daemon_name = utils.name_to_config_section(daemon_spec.name())

        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': daemon_name,
            'name': 'rgw_frontends',
            'value': frontend
        })

        if spec.rgw_user_counters_cache:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_user_counters_cache',
                'value': 'true',
            })
        if spec.rgw_bucket_counters_cache:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_bucket_counters_cache',
                'value': 'true',
            })

        if spec.rgw_user_counters_cache_size:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_user_counters_cache_size',
                'value': str(spec.rgw_user_counters_cache_size),
            })

        if spec.rgw_bucket_counters_cache_size:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_bucket_counters_cache_size',
                'value': str(spec.rgw_bucket_counters_cache_size),
            })

        if getattr(spec, 'disable_multisite_sync_traffic', None) is not None:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_run_sync_thread',
                'value': 'false' if spec.disable_multisite_sync_traffic else 'true',
            })

        qat_mode = spec.qat.get('compression') if spec.qat else None
        if qat_mode in ('sw', 'hw'):
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'qat_compressor_enabled',
                'value': 'true',
            })

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        d3n_cache = daemon_spec.final_config.get('d3n_cache')

        if d3n_cache:
            try:
                d3n = D3NCache.from_json(d3n_cache)
            except D3NCacheError as e:
                raise OrchestratorError(str(e))

            cache_path = d3n.cache_path
            size = d3n.size_bytes

            self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_d3n_l1_local_datacache_enabled',
                'value': 'true',
            })
            self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_d3n_l1_datacache_persistent_path',
                'value': cache_path,
            })
            self.mgr.check_mon_command({
                'prefix': 'config set',
                'who': daemon_name,
                'name': 'rgw_d3n_l1_datacache_size',
                'value': str(size),
            })

        return daemon_spec

    def get_keyring(self, rgw_id: str) -> str:
        """
        SMB and NFS services embed librgw. If the RGW service capabilities are
        updated, verify whether the same changes are required for these
        services as well.
        """
        # a mon or osd from before the profile reads it as granting nothing.
        # require_osd_release only moves up once all of them are upgraded
        osdmap = self.mgr.get('osd_map')
        release = osdmap.get('require_osd_release', 'argonaut')
        if utils.ceph_release_to_major(release) >= RGW_PROFILE_RELEASE:
            caps = ['mon', 'profile rgw',
                    'mgr', 'profile rgw',
                    'osd', 'profile rgw']
        else:
            caps = ['mon', 'allow *',
                    'mgr', 'allow rw',
                    'osd', 'allow rwx tag rgw *=*']
        return self.get_keyring_with_caps(self.get_auth_entity(rgw_id), caps)

    def purge(self, service_name: str) -> None:
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(service_name),
            'name': 'rgw_realm',
        })
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(service_name),
            'name': 'rgw_zone',
        })
        self.mgr.check_mon_command({
            'prefix': 'config-key rm',
            'key': f'rgw/cert/{service_name}',
        })
        self.mgr.trigger_connect_dashboard_rgw()

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'rgw_frontends',
        })

        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'rgw_d3n_l1_local_datacache_enabled',
        })
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'rgw_d3n_l1_datacache_persistent_path',
        })
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'rgw_d3n_l1_datacache_size',
        })
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': utils.name_to_config_section(daemon.name()),
            'name': 'qat_compressor_enabled'
        })

        self.mgr.check_mon_command({
            'prefix': 'config-key rm',
            'key': f'rgw/cert/{daemon.name()}',
        })

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # if load balancer (ingress) is present block if only 1 daemon up otherwise ok
        # if no load balancer, warn if > 1 daemon, block if only 1 daemon
        def ingress_present() -> bool:
            running_ingress_daemons = [
                daemon for daemon in self.mgr.cache.get_daemons_by_type('ingress') if daemon.status == 1]
            running_haproxy_daemons = [
                daemon for daemon in running_ingress_daemons if daemon.daemon_type == 'haproxy']
            running_keepalived_daemons = [
                daemon for daemon in running_ingress_daemons if daemon.daemon_type == 'keepalived']
            # check that there is at least one haproxy and keepalived daemon running
            if running_haproxy_daemons and running_keepalived_daemons:
                return True
            return False

        # if only 1 rgw, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'RGW', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 rgw daemon.
        # Say okay if load balancer present or force flag set
        if ingress_present() or force:
            return HandleCommandResult(0, warn_message, '')

        # if reached here, > 1 RGW daemon, no load balancer and no force flag.
        # Provide warning
        warn_message = "WARNING: Removing RGW daemons can cause clients to lose connectivity. "
        return HandleCommandResult(-errno.EBUSY, '', warn_message)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        self.mgr.trigger_connect_dashboard_rgw()

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        svc_spec = cast(RGWSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        deploy_ctx.service_spec = svc_spec
        config, deps = super().generate_config(deploy_ctx)

        if hasattr(svc_spec, 'rgw_exit_timeout_secs') and svc_spec.rgw_exit_timeout_secs:
            config['rgw_exit_timeout_secs'] = svc_spec.rgw_exit_timeout_secs

        if svc_spec.qat:
            config['qat'] = svc_spec.qat

        d3n_cache = self._compute_d3n_cache_for_daemon(daemon_spec, svc_spec)
        if d3n_cache:
            config['d3n_cache'] = d3n_cache.to_json()

        return config, deps

    def get_active_ports(self, service_name: str) -> List[int]:
        """
        Fetch active RGW frontend ports by parsing config entries for a given service.
        """
        ports = set()
        daemons = self.mgr.cache.get_daemons_by_service(service_name)
        for d in daemons:
            who = f"client.{d.name()}"
            try:
                ret, out, err = self.mgr.check_mon_command({
                    'prefix': 'config get',
                    'who': who,
                    'name': 'rgw_frontends'
                })

                if ret == 0 and out:
                    logger.debug(f"who: {who}, out: {out}")
                    ports.update(self._extract_ports_from_frontend(out.strip()))
            except Exception as e:
                logger.warning(f"Failed to get the config details for {who}: {e}")
                continue

        return sorted(ports)

    def _extract_ports_from_frontend(self, frontend_str: str) -> set[int]:

        ports = set()
        for val in frontend_str.split():
            if val.startswith("port="):
                ports.add(int(val.split("=")[1]))

        return ports
