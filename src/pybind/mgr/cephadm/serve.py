import hashlib
import json
import logging
import uuid
from collections import defaultdict
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional, List, cast, Dict, Any, Union, Tuple, Iterator, \
    DefaultDict

from cephadm import remotes

try:
    import remoto
    import execnet.gateway_bootstrap
except ImportError:
    remoto = None

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import ServiceSpec, CustomContainerSpec, PlacementSpec
from ceph.utils import str_to_datetime, datetime_now

import orchestrator
from orchestrator import OrchestratorError, set_exception_subject, OrchestratorEvent, \
    DaemonDescriptionStatus, daemon_type_to_service
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.schedule import HostAssignment
from cephadm.autotune import MemoryAutotuner
from cephadm.utils import forall_hosts, cephadmNoImage, is_repo_digest, \
    CephadmNoImage, CEPH_TYPES, ContainerInspectInfo
from mgr_module import MonCommandFailed
from mgr_util import format_bytes

from . import utils

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from remoto.backends import BaseConnection

logger = logging.getLogger(__name__)

REQUIRES_POST_ACTIONS = ['grafana', 'iscsi', 'prometheus', 'alertmanager', 'rgw']


class CephadmServe:
    """
    This module contains functions that are executed in the
    serve() thread. Thus they don't block the CLI.

    Please see the `Note regarding network calls from CLI handlers`
    chapter in the cephadm developer guide.

    On the other hand, These function should *not* be called form
    CLI handlers, to avoid blocking the CLI
    """

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.log = logger

    def serve(self) -> None:
        """
        The main loop of cephadm.

        A command handler will typically change the declarative state
        of cephadm. This loop will then attempt to apply this new state.
        """
        self.log.debug("serve starting")
        self.mgr.config_checker.load_network_config()

        while self.mgr.run:

            try:

                self.convert_tags_to_repo_digest()

                # refresh daemons
                self.log.debug('refreshing hosts and daemons')
                self._refresh_hosts_and_daemons()

                self._check_for_strays()

                self._update_paused_health()

                if self.mgr.need_connect_dashboard_rgw and self.mgr.config_dashboard:
                    self.mgr.need_connect_dashboard_rgw = False
                    if 'dashboard' in self.mgr.get('mgr_map')['modules']:
                        self.log.info('Checking dashboard <-> RGW credentials')
                        self.mgr.remote('dashboard', 'set_rgw_credentials')

                if not self.mgr.paused:
                    self.mgr.to_remove_osds.process_removal_queue()

                    self.mgr.migration.migrate()
                    if self.mgr.migration.is_migration_ongoing():
                        continue

                    if self._apply_all_services():
                        continue  # did something, refresh

                    self._check_daemons()

                    self._purge_deleted_services()

                    self._check_for_moved_osds()

                    if self.mgr.upgrade.continue_upgrade():
                        continue

            except OrchestratorError as e:
                if e.event_subject:
                    self.mgr.events.from_orch_error(e)

            self._serve_sleep()
        self.log.debug("serve exit")

    def _serve_sleep(self) -> None:
        sleep_interval = max(
            30,
            min(
                self.mgr.host_check_interval,
                self.mgr.facts_cache_timeout,
                self.mgr.daemon_cache_timeout,
                self.mgr.device_cache_timeout,
            )
        )
        self.log.debug('Sleeping for %d seconds', sleep_interval)
        self.mgr.event.wait(sleep_interval)
        self.mgr.event.clear()

    def _update_paused_health(self) -> None:
        if self.mgr.paused:
            self.mgr.set_health_warning('CEPHADM_PAUSED', 'cephadm background work is paused', 1, ["'ceph orch resume' to resume"])
        else:
            self.mgr.remove_health_warning('CEPHADM_PAUSED')

    def _autotune_host_memory(self, host: str) -> None:
        total_mem = self.mgr.cache.get_facts(host).get('memory_total_kb', 0)
        if not total_mem:
            val = None
        else:
            total_mem *= 1024   # kb -> bytes
            total_mem *= self.mgr.autotune_memory_target_ratio
            a = MemoryAutotuner(
                daemons=self.mgr.cache.get_daemons_by_host(host),
                config_get=self.mgr.get_foreign_ceph_option,
                total_mem=total_mem,
            )
            val, osds = a.tune()
            any_changed = False
            for o in osds:
                if self.mgr.get_foreign_ceph_option(o, 'osd_memory_target') != val:
                    self.mgr.check_mon_command({
                        'prefix': 'config rm',
                        'who': o,
                        'name': 'osd_memory_target',
                    })
                    any_changed = True
        if val is not None:
            if any_changed:
                self.mgr.log.info(
                    f'Adjusting osd_memory_target on {host} to {format_bytes(val, 6)}'
                )
                ret, out, err = self.mgr.mon_command({
                    'prefix': 'config set',
                    'who': f'osd/host:{host}',
                    'name': 'osd_memory_target',
                    'value': str(val),
                })
                if ret:
                    self.log.warning(
                        f'Unable to set osd_memory_target on {host} to {val}: {err}'
                    )
        else:
            self.mgr.check_mon_command({
                'prefix': 'config rm',
                'who': f'osd/host:{host}',
                'name': 'osd_memory_target',
            })
        self.mgr.cache.update_autotune(host)

    def _refresh_hosts_and_daemons(self) -> None:
        bad_hosts = []
        failures = []

        if self.mgr.manage_etc_ceph_ceph_conf or self.mgr.keys.keys:
            client_files = self._calc_client_files()
        else:
            client_files = {}

        @forall_hosts
        def refresh(host: str) -> None:

            # skip hosts that are in maintenance - they could be powered off
            if self.mgr.inventory._inventory[host].get("status", "").lower() == "maintenance":
                return

            if self.mgr.cache.host_needs_check(host):
                r = self._check_host(host)
                if r is not None:
                    bad_hosts.append(r)
            if self.mgr.cache.host_needs_daemon_refresh(host):
                self.log.debug('refreshing %s daemons' % host)
                r = self._refresh_host_daemons(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_registry_login(host) and self.mgr.get_store('registry_credentials'):
                self.log.debug(f"Logging `{host}` into custom registry")
                r = self._registry_login(
                    host, json.loads(str(self.mgr.get_store('registry_credentials'))))
                if r:
                    bad_hosts.append(r)

            if self.mgr.cache.host_needs_device_refresh(host):
                self.log.debug('refreshing %s devices' % host)
                r = self._refresh_host_devices(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_facts_refresh(host):
                self.log.debug(('Refreshing %s facts' % host))
                r = self._refresh_facts(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_osdspec_preview_refresh(host):
                self.log.debug(f"refreshing OSDSpec previews for {host}")
                r = self._refresh_host_osdspec_previews(host)
                if r:
                    failures.append(r)

            if (
                    self.mgr.cache.host_needs_autotune_memory(host)
                    and not self.mgr.inventory.has_label(host, '_no_autotune_memory')
            ):
                self.log.debug(f"autotuning memory for {host}")
                self._autotune_host_memory(host)

            self._write_client_files(client_files, host)

        refresh(self.mgr.cache.get_hosts())

        self.mgr.config_checker.run_checks()

        for k in [
                'CEPHADM_HOST_CHECK_FAILED',
                'CEPHADM_FAILED_DAEMON',
                'CEPHADM_REFRESH_FAILED',
        ]:
            self.mgr.remove_health_warning(k)
        if bad_hosts:
            self.mgr.set_health_warning('CEPHADM_HOST_CHECK_FAILED', f'{len(bad_hosts)} hosts fail cephadm check', len(bad_hosts), bad_hosts)
        if failures:
            self.mgr.set_health_warning('CEPHADM_REFRESH_FAILED', 'failed to probe daemons or devices', len(failures), failures)
        failed_daemons = []
        for dd in self.mgr.cache.get_daemons():
            if dd.status is not None and dd.status == DaemonDescriptionStatus.error:
                failed_daemons.append('daemon %s on %s is in %s state' % (
                    dd.name(), dd.hostname, dd.status_desc
                ))
        if failed_daemons:
            self.mgr.set_health_warning('CEPHADM_FAILED_DAEMON', f'{len(failed_daemons)} failed cephadm daemon(s)', len(failed_daemons), failed_daemons)

    def _check_host(self, host: str) -> Optional[str]:
        if host not in self.mgr.inventory:
            return None
        self.log.debug(' checking %s' % host)
        try:
            addr = self.mgr.inventory.get_addr(host) if host in self.mgr.inventory else host
            out, err, code = self._run_cephadm(
                host, cephadmNoImage, 'check-host', [],
                error_ok=True, no_fsid=True)
            self.mgr.cache.update_last_host_check(host)
            self.mgr.cache.save_host(host)
            if code:
                self.log.debug(' host %s (%s) failed check' % (host, addr))
                if self.mgr.warn_on_failed_host_check:
                    return 'host %s (%s) failed check: %s' % (host, addr, err)
            else:
                self.log.debug(' host %s (%s) ok' % (host, addr))
        except Exception as e:
            self.log.debug(' host %s (%s) failed check' % (host, addr))
            return 'host %s (%s) failed check: %s' % (host, addr, e)
        return None

    def _refresh_host_daemons(self, host: str) -> Optional[str]:
        try:
            ls = self._run_cephadm_json(host, 'mon', 'ls', [], no_fsid=True)
        except OrchestratorError as e:
            return str(e)
        dm = {}
        for d in ls:
            if not d['style'].startswith('cephadm'):
                continue
            if d['fsid'] != self.mgr._cluster_fsid:
                continue
            if '.' not in d['name']:
                continue
            sd = orchestrator.DaemonDescription()
            sd.last_refresh = datetime_now()
            for k in ['created', 'started', 'last_configured', 'last_deployed']:
                v = d.get(k, None)
                if v:
                    setattr(sd, k, str_to_datetime(d[k]))
            sd.daemon_type = d['name'].split('.')[0]
            if sd.daemon_type not in orchestrator.KNOWN_DAEMON_TYPES:
                logger.warning(f"Found unknown daemon type {sd.daemon_type} on host {host}")
                continue

            sd.daemon_id = '.'.join(d['name'].split('.')[1:])
            sd.hostname = host
            sd.container_id = d.get('container_id')
            if sd.container_id:
                # shorten the hash
                sd.container_id = sd.container_id[0:12]
            sd.container_image_name = d.get('container_image_name')
            sd.container_image_id = d.get('container_image_id')
            sd.container_image_digests = d.get('container_image_digests')
            sd.memory_usage = d.get('memory_usage')
            sd.memory_request = d.get('memory_request')
            sd.memory_limit = d.get('memory_limit')
            sd._service_name = d.get('service_name')
            sd.deployed_by = d.get('deployed_by')
            sd.version = d.get('version')
            sd.ports = d.get('ports')
            sd.ip = d.get('ip')
            sd.rank = int(d['rank']) if d.get('rank') is not None else None
            sd.rank_generation = int(d['rank_generation']) if d.get(
                'rank_generation') is not None else None
            sd.extra_container_args = d.get('extra_container_args')
            if 'state' in d:
                sd.status_desc = d['state']
                sd.status = {
                    'running': DaemonDescriptionStatus.running,
                    'stopped': DaemonDescriptionStatus.stopped,
                    'error': DaemonDescriptionStatus.error,
                    'unknown': DaemonDescriptionStatus.error,
                }[d['state']]
            else:
                sd.status_desc = 'unknown'
                sd.status = None
            dm[sd.name()] = sd
        self.log.debug('Refreshed host %s daemons (%d)' % (host, len(dm)))
        self.mgr.cache.update_host_daemons(host, dm)
        self.mgr.cache.save_host(host)
        return None

    def _refresh_facts(self, host: str) -> Optional[str]:
        try:
            val = self._run_cephadm_json(host, cephadmNoImage, 'gather-facts', [], no_fsid=True)
        except OrchestratorError as e:
            return str(e)

        self.mgr.cache.update_host_facts(host, val)

        return None

    def _refresh_host_devices(self, host: str) -> Optional[str]:
        with_lsm = self.mgr.device_enhanced_scan
        inventory_args = ['--', 'inventory',
                          '--format=json-pretty',
                          '--filter-for-batch']
        if with_lsm:
            inventory_args.insert(-1, "--with-lsm")

        try:
            try:
                devices = self._run_cephadm_json(host, 'osd', 'ceph-volume',
                                                 inventory_args)
            except OrchestratorError as e:
                if 'unrecognized arguments: --filter-for-batch' in str(e):
                    rerun_args = inventory_args.copy()
                    rerun_args.remove('--filter-for-batch')
                    devices = self._run_cephadm_json(host, 'osd', 'ceph-volume',
                                                     rerun_args)
                else:
                    raise

            networks = self._run_cephadm_json(host, 'mon', 'list-networks', [], no_fsid=True)
        except OrchestratorError as e:
            return str(e)

        self.log.debug('Refreshed host %s devices (%d) networks (%s)' % (
            host, len(devices), len(networks)))
        ret = inventory.Devices.from_json(devices)
        self.mgr.cache.update_host_devices_networks(host, ret.devices, networks)
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        return None

    def _refresh_host_osdspec_previews(self, host: str) -> Optional[str]:
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        self.log.debug(f'Refreshed OSDSpec previews for host <{host}>')
        return None

    def update_osdspec_previews(self, search_host: str = '') -> None:
        # Set global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.add(search_host)
        previews = []
        # query OSDSpecs for host <search host> and generate/get the preview
        # There can be multiple previews for one host due to multiple OSDSpecs.
        previews.extend(self.mgr.osd_service.get_previews(search_host))
        self.log.debug(f'Loading OSDSpec previews to HostCache for host <{search_host}>')
        self.mgr.cache.osdspec_previews[search_host] = previews
        # Unset global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.remove(search_host)

    def _check_for_strays(self) -> None:
        self.log.debug('_check_for_strays')
        for k in ['CEPHADM_STRAY_HOST',
                  'CEPHADM_STRAY_DAEMON']:
            self.mgr.remove_health_warning(k)
        if self.mgr.warn_on_stray_hosts or self.mgr.warn_on_stray_daemons:
            ls = self.mgr.list_servers()
            self.log.debug(ls)
            managed = self.mgr.cache.get_daemon_names()
            host_detail = []     # type: List[str]
            host_num_daemons = 0
            daemon_detail = []  # type: List[str]
            for item in ls:
                host = item.get('hostname')
                assert isinstance(host, str)
                daemons = item.get('services')  # misnomer!
                assert isinstance(daemons, list)
                missing_names = []
                for s in daemons:
                    daemon_id = s.get('id')
                    assert daemon_id
                    name = '%s.%s' % (s.get('type'), daemon_id)
                    if s.get('type') in ['rbd-mirror', 'cephfs-mirror', 'rgw', 'rgw-nfs']:
                        metadata = self.mgr.get_metadata(
                            cast(str, s.get('type')), daemon_id, {})
                        assert metadata is not None
                        try:
                            if s.get('type') == 'rgw-nfs':
                                # https://tracker.ceph.com/issues/49573
                                name = metadata['id'][:-4]
                            else:
                                name = '%s.%s' % (s.get('type'), metadata['id'])
                        except (KeyError, TypeError):
                            self.log.debug(
                                "Failed to find daemon id for %s service %s" % (
                                    s.get('type'), s.get('id')
                                )
                            )
                    if s.get('type') == 'tcmu-runner':
                        # because we don't track tcmu-runner daemons in the host cache
                        # and don't have a way to check if the daemon is part of iscsi service
                        # we assume that all tcmu-runner daemons are managed by cephadm
                        managed.append(name)
                    if host not in self.mgr.inventory:
                        missing_names.append(name)
                        host_num_daemons += 1
                    if name not in managed:
                        daemon_detail.append(
                            'stray daemon %s on host %s not managed by cephadm' % (name, host))
                if missing_names:
                    host_detail.append(
                        'stray host %s has %d stray daemons: %s' % (
                            host, len(missing_names), missing_names))
            if self.mgr.warn_on_stray_hosts and host_detail:
                self.mgr.set_health_warning(
                    'CEPHADM_STRAY_HOST', f'{len(host_detail)} stray host(s) with {host_num_daemons} daemon(s) not managed by cephadm', len(host_detail), host_detail)
            if self.mgr.warn_on_stray_daemons and daemon_detail:
                self.mgr.set_health_warning(
                    'CEPHADM_STRAY_DAEMON', f'{len(daemon_detail)} stray daemon(s) not managed by cephadm', len(daemon_detail), daemon_detail)

    def _check_for_moved_osds(self) -> None:
        all_osds: DefaultDict[int, List[orchestrator.DaemonDescription]] = defaultdict(list)
        for dd in self.mgr.cache.get_daemons_by_type('osd'):
            assert dd.daemon_id
            all_osds[int(dd.daemon_id)].append(dd)
        for osd_id, dds in all_osds.items():
            if len(dds) <= 1:
                continue
            running = [dd for dd in dds if dd.status == DaemonDescriptionStatus.running]
            error = [dd for dd in dds if dd.status == DaemonDescriptionStatus.error]
            msg = f'Found duplicate OSDs: {", ".join(str(dd) for dd in dds)}'
            logger.info(msg)
            if len(running) != 1:
                continue
            osd = self.mgr.get_osd_by_id(osd_id)
            if not osd or not osd['up']:
                continue
            for e in error:
                assert e.hostname
                try:
                    self._remove_daemon(e.name(), e.hostname, no_post_remove=True)
                    self.mgr.events.for_daemon(
                        e.name(), 'INFO', f"Removed duplicated daemon on host '{e.hostname}'")
                except OrchestratorError as ex:
                    self.mgr.events.from_orch_error(ex)
                    logger.exception(f'failed to remove duplicated daemon {e}')

    def _apply_all_services(self) -> bool:
        r = False
        specs = []  # type: List[ServiceSpec]
        for sn, spec in self.mgr.spec_store.active_specs.items():
            specs.append(spec)
        for name in ['CEPHADM_APPLY_SPEC_FAIL', 'CEPHADM_DAEMON_PLACE_FAIL']:
            self.mgr.remove_health_warning(name)
        self.mgr.apply_spec_fails = []
        for spec in specs:
            try:
                if self._apply_service(spec):
                    r = True
            except Exception as e:
                msg = f'Failed to apply {spec.service_name()} spec {spec}: {str(e)}'
                self.log.exception(msg)
                self.mgr.events.for_service(spec, 'ERROR', 'Failed to apply: ' + str(e))
                self.mgr.apply_spec_fails.append((spec.service_name(), str(e)))
                warnings = []
                for x in self.mgr.apply_spec_fails:
                    warnings.append(f'{x[0]}: {x[1]}')
                self.mgr.set_health_warning('CEPHADM_APPLY_SPEC_FAIL',
                                            f"Failed to apply {len(self.mgr.apply_spec_fails)} service(s): {','.join(x[0] for x in self.mgr.apply_spec_fails)}",
                                            len(self.mgr.apply_spec_fails),
                                            warnings)

        return r

    def _apply_service_config(self, spec: ServiceSpec) -> None:
        if spec.config:
            section = utils.name_to_config_section(spec.service_name())
            for name in ['CEPHADM_INVALID_CONFIG_OPTION', 'CEPHADM_FAILED_SET_OPTION']:
                self.mgr.remove_health_warning(name)
            invalid_config_options = []
            options_failed_to_set = []
            for k, v in spec.config.items():
                try:
                    current = self.mgr.get_foreign_ceph_option(section, k)
                except KeyError:
                    msg = f'Ignoring invalid {spec.service_name()} config option {k}'
                    self.log.warning(msg)
                    self.mgr.events.for_service(
                        spec, OrchestratorEvent.ERROR, f'Invalid config option {k}'
                    )
                    invalid_config_options.append(msg)
                    continue
                if current != v:
                    self.log.debug(f'setting [{section}] {k} = {v}')
                    try:
                        self.mgr.check_mon_command({
                            'prefix': 'config set',
                            'name': k,
                            'value': str(v),
                            'who': section,
                        })
                    except MonCommandFailed as e:
                        msg = f'Failed to set {spec.service_name()} option {k}: {e}'
                        self.log.warning(msg)
                        options_failed_to_set.append(msg)

            if invalid_config_options:
                self.mgr.set_health_warning('CEPHADM_INVALID_CONFIG_OPTION', f'Ignoring {len(invalid_config_options)} invalid config option(s)', len(invalid_config_options), invalid_config_options)
            if options_failed_to_set:
                self.mgr.set_health_warning('CEPHADM_FAILED_SET_OPTION', f'Failed to set {len(options_failed_to_set)} option(s)', len(options_failed_to_set), options_failed_to_set)

    def _apply_service(self, spec: ServiceSpec) -> bool:
        """
        Schedule a service.  Deploy new daemons or remove old ones, depending
        on the target label and count specified in the placement.
        """
        self.mgr.migration.verify_no_migration()

        service_type = spec.service_type
        service_name = spec.service_name()
        if spec.unmanaged:
            self.log.debug('Skipping unmanaged service %s' % service_name)
            return False
        if spec.preview_only:
            self.log.debug('Skipping preview_only service %s' % service_name)
            return False
        self.log.debug('Applying service %s spec' % service_name)

        self._apply_service_config(spec)

        if service_type == 'osd':
            self.mgr.osd_service.create_from_spec(cast(DriveGroupSpec, spec))
            # TODO: return True would result in a busy loop
            # can't know if daemon count changed; create_from_spec doesn't
            # return a solid indication
            return False

        svc = self.mgr.cephadm_services[service_type]
        daemons = self.mgr.cache.get_daemons_by_service(service_name)

        public_networks: List[str] = []
        if service_type == 'mon':
            out = str(self.mgr.get_foreign_ceph_option('mon', 'public_network'))
            if '/' in out:
                public_networks = [x.strip() for x in out.split(',')]
                self.log.debug('mon public_network(s) is %s' % public_networks)

        def matches_network(host):
            # type: (str) -> bool
            # make sure we have 1 or more IPs for any of those networks on that
            # host
            for network in public_networks:
                if len(self.mgr.cache.networks[host].get(network, [])) > 0:
                    return True
            self.log.info(
                f"Filtered out host {host}: does not belong to mon public_network"
                f" ({','.join(public_networks)})"
            )
            return False

        rank_map = None
        if svc.ranked():
            rank_map = self.mgr.spec_store[spec.service_name()].rank_map or {}
        ha = HostAssignment(
            spec=spec,
            hosts=self.mgr._schedulable_hosts(),
            unreachable_hosts=self.mgr._unreachable_hosts(),
            daemons=daemons,
            networks=self.mgr.cache.networks,
            filter_new_host=(
                matches_network if service_type == 'mon'
                else None
            ),
            allow_colo=svc.allow_colo(),
            primary_daemon_type=svc.primary_daemon_type(),
            per_host_daemon_type=svc.per_host_daemon_type(),
            rank_map=rank_map,
        )

        try:
            all_slots, slots_to_add, daemons_to_remove = ha.place()
            daemons_to_remove = [d for d in daemons_to_remove if (d.hostname and self.mgr.inventory._inventory[d.hostname].get(
                'status', '').lower() not in ['maintenance', 'offline'] and d.hostname not in self.mgr.offline_hosts)]
            self.log.debug('Add %s, remove %s' % (slots_to_add, daemons_to_remove))
        except OrchestratorError as e:
            msg = f'Failed to apply {spec.service_name()} spec {spec}: {str(e)}'
            self.log.error(msg)
            self.mgr.events.for_service(spec, 'ERROR', 'Failed to apply: ' + str(e))
            self.mgr.apply_spec_fails.append((spec.service_name(), str(e)))
            warnings = []
            for x in self.mgr.apply_spec_fails:
                warnings.append(f'{x[0]}: {x[1]}')
            self.mgr.set_health_warning('CEPHADM_APPLY_SPEC_FAIL',
                                        f"Failed to apply {len(self.mgr.apply_spec_fails)} service(s): {','.join(x[0] for x in self.mgr.apply_spec_fails)}",
                                        len(self.mgr.apply_spec_fails),
                                        warnings)
            return False

        r = None

        # sanity check
        final_count = len(daemons) + len(slots_to_add) - len(daemons_to_remove)
        if service_type in ['mon', 'mgr'] and final_count < 1:
            self.log.debug('cannot scale mon|mgr below 1)')
            return False

        # progress
        progress_id = str(uuid.uuid4())
        delta: List[str] = []
        if slots_to_add:
            delta += [f'+{len(slots_to_add)}']
        if daemons_to_remove:
            delta += [f'-{len(daemons_to_remove)}']
        progress_title = f'Updating {spec.service_name()} deployment ({" ".join(delta)} -> {len(all_slots)})'
        progress_total = len(slots_to_add) + len(daemons_to_remove)
        progress_done = 0

        def update_progress() -> None:
            self.mgr.remote(
                'progress', 'update', progress_id,
                ev_msg=progress_title,
                ev_progress=(progress_done / progress_total),
                add_to_ceph_s=True,
            )

        if progress_total:
            update_progress()

        # add any?
        did_config = False

        self.log.debug('Hosts that will receive new daemons: %s' % slots_to_add)
        self.log.debug('Daemons that will be removed: %s' % daemons_to_remove)

        try:
            # assign names
            for i in range(len(slots_to_add)):
                slot = slots_to_add[i]
                slot = slot.assign_name(self.mgr.get_unique_name(
                    slot.daemon_type,
                    slot.hostname,
                    daemons,
                    prefix=spec.service_id,
                    forcename=slot.name,
                    rank=slot.rank,
                    rank_generation=slot.rank_generation,
                ))
                slots_to_add[i] = slot
                if rank_map is not None:
                    assert slot.rank is not None
                    assert slot.rank_generation is not None
                    assert rank_map[slot.rank][slot.rank_generation] is None
                    rank_map[slot.rank][slot.rank_generation] = slot.name

            if rank_map:
                # record the rank_map before we make changes so that if we fail the
                # next mgr will clean up.
                self.mgr.spec_store.save_rank_map(spec.service_name(), rank_map)

                # remove daemons now, since we are going to fence them anyway
                for d in daemons_to_remove:
                    assert d.hostname is not None
                    self._remove_daemon(d.name(), d.hostname)
                daemons_to_remove = []

                # fence them
                svc.fence_old_ranks(spec, rank_map, len(all_slots))

            # create daemons
            daemon_place_fails = []
            for slot in slots_to_add:
                # first remove daemon on conflicting port?
                if slot.ports:
                    for d in daemons_to_remove:
                        if d.hostname != slot.hostname:
                            continue
                        if not (set(d.ports or []) & set(slot.ports)):
                            continue
                        if d.ip and slot.ip and d.ip != slot.ip:
                            continue
                        self.log.info(
                            f'Removing {d.name()} before deploying to {slot} to avoid a port conflict'
                        )
                        # NOTE: we don't check ok-to-stop here to avoid starvation if
                        # there is only 1 gateway.
                        self._remove_daemon(d.name(), d.hostname)
                        daemons_to_remove.remove(d)
                        progress_done += 1
                        break

                # deploy new daemon
                daemon_id = slot.name
                if not did_config:
                    svc.config(spec)
                    did_config = True

                daemon_spec = svc.make_daemon_spec(
                    slot.hostname, daemon_id, slot.network, spec,
                    daemon_type=slot.daemon_type,
                    ports=slot.ports,
                    ip=slot.ip,
                    rank=slot.rank,
                    rank_generation=slot.rank_generation,
                )
                self.log.debug('Placing %s.%s on host %s' % (
                    slot.daemon_type, daemon_id, slot.hostname))

                try:
                    daemon_spec = svc.prepare_create(daemon_spec)
                    self._create_daemon(daemon_spec)
                    r = True
                    progress_done += 1
                    update_progress()
                except (RuntimeError, OrchestratorError) as e:
                    msg = (f"Failed while placing {slot.daemon_type}.{daemon_id} "
                           f"on {slot.hostname}: {e}")
                    self.mgr.events.for_service(spec, 'ERROR', msg)
                    self.mgr.log.error(msg)
                    daemon_place_fails.append(msg)
                    # only return "no change" if no one else has already succeeded.
                    # later successes will also change to True
                    if r is None:
                        r = False
                    progress_done += 1
                    update_progress()
                    continue

                # add to daemon list so next name(s) will also be unique
                sd = orchestrator.DaemonDescription(
                    hostname=slot.hostname,
                    daemon_type=slot.daemon_type,
                    daemon_id=daemon_id,
                )
                daemons.append(sd)

            if daemon_place_fails:
                self.mgr.set_health_warning('CEPHADM_DAEMON_PLACE_FAIL', f'Failed to place {len(daemon_place_fails)} daemon(s)', len(daemon_place_fails), daemon_place_fails)

            # remove any?
            def _ok_to_stop(remove_daemons: List[orchestrator.DaemonDescription]) -> bool:
                daemon_ids = [d.daemon_id for d in remove_daemons]
                assert None not in daemon_ids
                # setting force flag retains previous behavior
                r = svc.ok_to_stop(cast(List[str], daemon_ids), force=True)
                return not r.retval

            while daemons_to_remove and not _ok_to_stop(daemons_to_remove):
                # let's find a subset that is ok-to-stop
                daemons_to_remove.pop()
            for d in daemons_to_remove:
                r = True
                assert d.hostname is not None
                self._remove_daemon(d.name(), d.hostname)

                progress_done += 1
                update_progress()

            self.mgr.remote('progress', 'complete', progress_id)
        except Exception as e:
            self.mgr.remote('progress', 'fail', progress_id, str(e))
            raise

        if r is None:
            r = False
        return r

    def _check_daemons(self) -> None:

        daemons = self.mgr.cache.get_daemons()
        daemons_post: Dict[str, List[orchestrator.DaemonDescription]] = defaultdict(list)
        for dd in daemons:
            # orphan?
            spec = self.mgr.spec_store.active_specs.get(dd.service_name(), None)
            assert dd.hostname is not None
            assert dd.daemon_type is not None
            assert dd.daemon_id is not None
            if not spec and dd.daemon_type not in ['mon', 'mgr', 'osd']:
                # (mon and mgr specs should always exist; osds aren't matched
                # to a service spec)
                self.log.info('Removing orphan daemon %s...' % dd.name())
                self._remove_daemon(dd.name(), dd.hostname)

            # ignore unmanaged services
            if spec and spec.unmanaged:
                continue

            # ignore daemons for deleted services
            if dd.service_name() in self.mgr.spec_store.spec_deleted:
                continue

            # These daemon types require additional configs after creation
            if dd.daemon_type in REQUIRES_POST_ACTIONS:
                daemons_post[dd.daemon_type].append(dd)

            if self.mgr.cephadm_services[daemon_type_to_service(dd.daemon_type)].get_active_daemon(
               self.mgr.cache.get_daemons_by_service(dd.service_name())).daemon_id == dd.daemon_id:
                dd.is_active = True
            else:
                dd.is_active = False

            deps = self.mgr._calc_daemon_deps(spec, dd.daemon_type, dd.daemon_id)
            last_deps, last_config = self.mgr.cache.get_daemon_last_config_deps(
                dd.hostname, dd.name())
            if last_deps is None:
                last_deps = []
            action = self.mgr.cache.get_scheduled_daemon_action(dd.hostname, dd.name())
            if not last_config:
                self.log.info('Reconfiguring %s (unknown last config time)...' % (
                    dd.name()))
                action = 'reconfig'
            elif last_deps != deps:
                self.log.debug('%s deps %s -> %s' % (dd.name(), last_deps,
                                                     deps))
                self.log.info('Reconfiguring %s (dependencies changed)...' % (
                    dd.name()))
                action = 'reconfig'
            elif spec is not None and hasattr(spec, 'extra_container_args') and dd.extra_container_args != spec.extra_container_args:
                self.log.debug(
                    f'{dd.name()} container cli args {dd.extra_container_args} -> {spec.extra_container_args}')
                self.log.info(f'Redeploying {dd.name()}, (container cli args changed) . . .')
                dd.extra_container_args = spec.extra_container_args
                action = 'redeploy'
            elif self.mgr.last_monmap and \
                    self.mgr.last_monmap > last_config and \
                    dd.daemon_type in CEPH_TYPES:
                self.log.info('Reconfiguring %s (monmap changed)...' % dd.name())
                action = 'reconfig'
            elif self.mgr.extra_ceph_conf_is_newer(last_config) and \
                    dd.daemon_type in CEPH_TYPES:
                self.log.info('Reconfiguring %s (extra config changed)...' % dd.name())
                action = 'reconfig'
            if action:
                if self.mgr.cache.get_scheduled_daemon_action(dd.hostname, dd.name()) == 'redeploy' \
                        and action == 'reconfig':
                    action = 'redeploy'
                try:
                    daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(dd)
                    self.mgr._daemon_action(daemon_spec, action=action)
                    if self.mgr.cache.rm_scheduled_daemon_action(dd.hostname, dd.name()):
                        self.mgr.cache.save_host(dd.hostname)
                except OrchestratorError as e:
                    self.mgr.events.from_orch_error(e)
                    if dd.daemon_type in daemons_post:
                        del daemons_post[dd.daemon_type]
                    # continue...
                except Exception as e:
                    self.mgr.events.for_daemon_from_exception(dd.name(), e)
                    if dd.daemon_type in daemons_post:
                        del daemons_post[dd.daemon_type]
                    # continue...

        # do daemon post actions
        for daemon_type, daemon_descs in daemons_post.items():
            run_post = False
            for d in daemon_descs:
                if d.name() in self.mgr.requires_post_actions:
                    self.mgr.requires_post_actions.remove(d.name())
                    run_post = True
            if run_post:
                self.mgr._get_cephadm_service(daemon_type_to_service(
                    daemon_type)).daemon_check_post(daemon_descs)

    def _purge_deleted_services(self) -> None:
        existing_services = self.mgr.spec_store.all_specs.items()
        for service_name, spec in list(existing_services):
            if service_name not in self.mgr.spec_store.spec_deleted:
                continue
            if self.mgr.cache.get_daemons_by_service(service_name):
                continue
            if spec.service_type in ['mon', 'mgr']:
                continue

            logger.info(f'Purge service {service_name}')

            self.mgr.cephadm_services[spec.service_type].purge(service_name)
            self.mgr.spec_store.finally_rm(service_name)

    def convert_tags_to_repo_digest(self) -> None:
        if not self.mgr.use_repo_digest:
            return
        settings = self.mgr.upgrade.get_distinct_container_image_settings()
        digests: Dict[str, ContainerInspectInfo] = {}
        for container_image_ref in set(settings.values()):
            if not is_repo_digest(container_image_ref):
                image_info = self._get_container_image_info(container_image_ref)
                if image_info.repo_digests:
                    # FIXME: we assume the first digest here is the best
                    assert is_repo_digest(image_info.repo_digests[0]), image_info
                digests[container_image_ref] = image_info

        for entity, container_image_ref in settings.items():
            if not is_repo_digest(container_image_ref):
                image_info = digests[container_image_ref]
                if image_info.repo_digests:
                    # FIXME: we assume the first digest here is the best
                    self.mgr.set_container_image(entity, image_info.repo_digests[0])

    def _calc_client_files(self) -> Dict[str, Dict[str, Tuple[int, int, int, bytes, str]]]:
        # host -> path -> (mode, uid, gid, content, digest)
        client_files: Dict[str, Dict[str, Tuple[int, int, int, bytes, str]]] = {}

        # ceph.conf
        config = self.mgr.get_minimal_ceph_conf().encode('utf-8')
        config_digest = ''.join('%02x' % c for c in hashlib.sha256(config).digest())

        if self.mgr.manage_etc_ceph_ceph_conf:
            try:
                pspec = PlacementSpec.from_string(self.mgr.manage_etc_ceph_ceph_conf_hosts)
                ha = HostAssignment(
                    spec=ServiceSpec('mon', placement=pspec),
                    hosts=self.mgr._schedulable_hosts(),
                    unreachable_hosts=self.mgr._unreachable_hosts(),
                    daemons=[],
                    networks=self.mgr.cache.networks,
                )
                all_slots, _, _ = ha.place()
                for host in {s.hostname for s in all_slots}:
                    if host not in client_files:
                        client_files[host] = {}
                    client_files[host]['/etc/ceph/ceph.conf'] = (
                        0o644, 0, 0, bytes(config), str(config_digest)
                    )
            except Exception as e:
                self.mgr.log.warning(
                    f'unable to calc conf hosts: {self.mgr.manage_etc_ceph_ceph_conf_hosts}: {e}')

        # client keyrings
        for ks in self.mgr.keys.keys.values():
            try:
                ret, keyring, err = self.mgr.mon_command({
                    'prefix': 'auth get',
                    'entity': ks.entity,
                })
                if ret:
                    self.log.warning(f'unable to fetch keyring for {ks.entity}')
                    continue
                digest = ''.join('%02x' % c for c in hashlib.sha256(
                    keyring.encode('utf-8')).digest())
                ha = HostAssignment(
                    spec=ServiceSpec('mon', placement=ks.placement),
                    hosts=self.mgr._schedulable_hosts(),
                    unreachable_hosts=self.mgr._unreachable_hosts(),
                    daemons=[],
                    networks=self.mgr.cache.networks,
                )
                all_slots, _, _ = ha.place()
                for host in {s.hostname for s in all_slots}:
                    if host not in client_files:
                        client_files[host] = {}
                    client_files[host]['/etc/ceph/ceph.conf'] = (
                        0o644, 0, 0, bytes(config), str(config_digest)
                    )
                    client_files[host][ks.path] = (
                        ks.mode, ks.uid, ks.gid, keyring.encode('utf-8'), digest
                    )
            except Exception as e:
                self.log.warning(
                    f'unable to calc client keyring {ks.entity} placement {ks.placement}: {e}')
        return client_files

    def _write_client_files(self,
                            client_files: Dict[str, Dict[str, Tuple[int, int, int, bytes, str]]],
                            host: str) -> None:
        updated_files = False
        old_files = self.mgr.cache.get_host_client_files(host).copy()
        for path, m in client_files.get(host, {}).items():
            mode, uid, gid, content, digest = m
            if path in old_files:
                match = old_files[path] == (digest, mode, uid, gid)
                del old_files[path]
                if match:
                    continue
            self.log.info(f'Updating {host}:{path}')
            self._write_remote_file(host, path, content, mode, uid, gid)
            self.mgr.cache.update_client_file(host, path, digest, mode, uid, gid)
            updated_files = True
        for path in old_files.keys():
            self.log.info(f'Removing {host}:{path}')
            with self._remote_connection(host) as tpl:
                conn, connr = tpl
                out, err, code = remoto.process.check(
                    conn,
                    ['rm', '-f', path])
            updated_files = True
            self.mgr.cache.removed_client_file(host, path)
        if updated_files:
            self.mgr.cache.save_host(host)

    def _create_daemon(self,
                       daemon_spec: CephadmDaemonDeploySpec,
                       reconfig: bool = False,
                       osd_uuid_map: Optional[Dict[str, Any]] = None,
                       ) -> str:

        with set_exception_subject('service', orchestrator.DaemonDescription(
                daemon_type=daemon_spec.daemon_type,
                daemon_id=daemon_spec.daemon_id,
                hostname=daemon_spec.host,
        ).service_id(), overwrite=True):

            try:
                image = ''
                start_time = datetime_now()
                ports: List[int] = daemon_spec.ports if daemon_spec.ports else []

                if daemon_spec.daemon_type == 'container':
                    spec = cast(CustomContainerSpec,
                                self.mgr.spec_store[daemon_spec.service_name].spec)
                    image = spec.image
                    if spec.ports:
                        ports.extend(spec.ports)

                if daemon_spec.daemon_type == 'cephadm-exporter':
                    if not reconfig:
                        assert daemon_spec.host
                        self._deploy_cephadm_binary(daemon_spec.host)

                # TCP port to open in the host firewall
                if len(ports) > 0:
                    daemon_spec.extra_args.extend([
                        '--tcp-ports', ' '.join(map(str, ports))
                    ])

                # osd deployments needs an --osd-uuid arg
                if daemon_spec.daemon_type == 'osd':
                    if not osd_uuid_map:
                        osd_uuid_map = self.mgr.get_osd_uuid_map()
                    osd_uuid = osd_uuid_map.get(daemon_spec.daemon_id)
                    if not osd_uuid:
                        raise OrchestratorError('osd.%s not in osdmap' % daemon_spec.daemon_id)
                    daemon_spec.extra_args.extend(['--osd-fsid', osd_uuid])

                if reconfig:
                    daemon_spec.extra_args.append('--reconfig')
                if self.mgr.allow_ptrace:
                    daemon_spec.extra_args.append('--allow-ptrace')

                try:
                    eca = daemon_spec.extra_container_args
                    if eca:
                        for a in eca:
                            daemon_spec.extra_args.append(f'--extra-container-args={a}')
                except AttributeError:
                    eca = None

                if self.mgr.cache.host_needs_registry_login(daemon_spec.host) and self.mgr.registry_url:
                    self._registry_login(daemon_spec.host,
                                         json.loads(str(self.mgr.get_store('registry_credentials'))))

                self.log.info('%s daemon %s on %s' % (
                    'Reconfiguring' if reconfig else 'Deploying',
                    daemon_spec.name(), daemon_spec.host))

                out, err, code = self._run_cephadm(
                    daemon_spec.host, daemon_spec.name(), 'deploy',
                    [
                        '--name', daemon_spec.name(),
                        '--meta-json', json.dumps({
                            'service_name': daemon_spec.service_name,
                            'ports': daemon_spec.ports,
                            'ip': daemon_spec.ip,
                            'deployed_by': self.mgr.get_active_mgr_digests(),
                            'rank': daemon_spec.rank,
                            'rank_generation': daemon_spec.rank_generation,
                            'extra_container_args': eca
                        }),
                        '--config-json', '-',
                    ] + daemon_spec.extra_args,
                    stdin=json.dumps(daemon_spec.final_config),
                    image=image,
                )

                # refresh daemon state?  (ceph daemon reconfig does not need it)
                if not reconfig or daemon_spec.daemon_type not in CEPH_TYPES:
                    if not code and daemon_spec.host in self.mgr.cache.daemons:
                        # prime cached service state with what we (should have)
                        # just created
                        sd = daemon_spec.to_daemon_description(
                            DaemonDescriptionStatus.starting, 'starting')
                        self.mgr.cache.add_daemon(daemon_spec.host, sd)
                        if daemon_spec.daemon_type in REQUIRES_POST_ACTIONS:
                            self.mgr.requires_post_actions.add(daemon_spec.name())
                    self.mgr.cache.invalidate_host_daemons(daemon_spec.host)

                self.mgr.cache.update_daemon_config_deps(
                    daemon_spec.host, daemon_spec.name(), daemon_spec.deps, start_time)
                self.mgr.cache.save_host(daemon_spec.host)
                msg = "{} {} on host '{}'".format(
                    'Reconfigured' if reconfig else 'Deployed', daemon_spec.name(), daemon_spec.host)
                if not code:
                    self.mgr.events.for_daemon(daemon_spec.name(), OrchestratorEvent.INFO, msg)
                else:
                    what = 'reconfigure' if reconfig else 'deploy'
                    self.mgr.events.for_daemon(
                        daemon_spec.name(), OrchestratorEvent.ERROR, f'Failed to {what}: {err}')
                return msg
            except OrchestratorError:
                redeploy = daemon_spec.name() in self.mgr.cache.get_daemon_names()
                if not reconfig and not redeploy:
                    # we have to clean up the daemon. E.g. keyrings.
                    servict_type = daemon_type_to_service(daemon_spec.daemon_type)
                    dd = daemon_spec.to_daemon_description(DaemonDescriptionStatus.error, 'failed')
                    self.mgr.cephadm_services[servict_type].post_remove(dd, is_failed_deploy=True)
                raise

    def _remove_daemon(self, name: str, host: str, no_post_remove: bool = False) -> str:
        """
        Remove a daemon
        """
        (daemon_type, daemon_id) = name.split('.', 1)
        daemon = orchestrator.DaemonDescription(
            daemon_type=daemon_type,
            daemon_id=daemon_id,
            hostname=host)

        with set_exception_subject('service', daemon.service_id(), overwrite=True):

            self.mgr.cephadm_services[daemon_type_to_service(daemon_type)].pre_remove(daemon)

            # NOTE: we are passing the 'force' flag here, which means
            # we can delete a mon instances data.
            args = ['--name', name, '--force']
            self.log.info('Removing daemon %s from %s' % (name, host))
            out, err, code = self._run_cephadm(
                host, name, 'rm-daemon', args)
            if not code:
                # remove item from cache
                self.mgr.cache.rm_daemon(host, name)
            self.mgr.cache.invalidate_host_daemons(host)

            if not no_post_remove:
                self.mgr.cephadm_services[daemon_type_to_service(
                    daemon_type)].post_remove(daemon, is_failed_deploy=False)

            return "Removed {} from host '{}'".format(name, host)

    def _run_cephadm_json(self,
                          host: str,
                          entity: Union[CephadmNoImage, str],
                          command: str,
                          args: List[str],
                          no_fsid: Optional[bool] = False,
                          image: Optional[str] = "",
                          ) -> Any:
        try:
            out, err, code = self._run_cephadm(
                host, entity, command, args, no_fsid=no_fsid, image=image)
            if code:
                raise OrchestratorError(f'host {host} `cephadm {command}` returned {code}: {err}')
        except Exception as e:
            raise OrchestratorError(f'host {host} `cephadm {command}` failed: {e}')
        try:
            return json.loads(''.join(out))
        except (ValueError, KeyError):
            msg = f'host {host} `cephadm {command}` failed: Cannot decode JSON'
            self.log.exception(f'{msg}: {"".join(out)}')
            raise OrchestratorError(msg)

    def _run_cephadm(self,
                     host: str,
                     entity: Union[CephadmNoImage, str],
                     command: str,
                     args: List[str],
                     addr: Optional[str] = "",
                     stdin: Optional[str] = "",
                     no_fsid: Optional[bool] = False,
                     error_ok: Optional[bool] = False,
                     image: Optional[str] = "",
                     env_vars: Optional[List[str]] = None,
                     ) -> Tuple[List[str], List[str], int]:
        """
        Run cephadm on the remote host with the given command + args

        Important: You probably don't want to run _run_cephadm from CLI handlers

        :env_vars: in format -> [KEY=VALUE, ..]
        """
        self.log.debug(f"_run_cephadm : command = {command}")
        self.log.debug(f"_run_cephadm : args = {args}")

        bypass_image = ('cephadm-exporter',)

        with self._remote_connection(host, addr) as tpl:
            conn, connr = tpl
            assert image or entity
            # Skip the image check for daemons deployed that are not ceph containers
            if not str(entity).startswith(bypass_image):
                if not image and entity is not cephadmNoImage:
                    image = self.mgr._get_container_image(entity)

            final_args = []

            # global args
            if env_vars:
                for env_var_pair in env_vars:
                    final_args.extend(['--env', env_var_pair])

            if image:
                final_args.extend(['--image', image])

            if not self.mgr.container_init:
                final_args += ['--no-container-init']

            # subcommand
            final_args.append(command)

            # subcommand args
            if not no_fsid:
                final_args += ['--fsid', self.mgr._cluster_fsid]

            final_args += args

            # exec
            self.log.debug('args: %s' % (' '.join(final_args)))
            if self.mgr.mode == 'root':
                if stdin:
                    self.log.debug('stdin: %s' % stdin)

                python = connr.choose_python()
                if not python:
                    raise RuntimeError(
                        'unable to find python on %s (tried %s in %s)' % (
                            host, remotes.PYTHONS, remotes.PATH))
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        [python, self.mgr.cephadm_binary_path] + final_args,
                        stdin=stdin.encode('utf-8') if stdin is not None else None)
                    if code == 2:
                        out_ls, err_ls, code_ls = remoto.process.check(
                            conn, ['ls', self.mgr.cephadm_binary_path])
                        if code_ls == 2:
                            self._deploy_cephadm_binary_conn(conn, host)
                            out, err, code = remoto.process.check(
                                conn,
                                [python, self.mgr.cephadm_binary_path] + final_args,
                                stdin=stdin.encode('utf-8') if stdin is not None else None)

                except RuntimeError as e:
                    self.mgr._reset_con(host)
                    if error_ok:
                        return [], [str(e)], 1
                    raise

            elif self.mgr.mode == 'cephadm-package':
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        ['sudo', '/usr/bin/cephadm'] + final_args,
                        stdin=stdin)
                except RuntimeError as e:
                    self.mgr._reset_con(host)
                    if error_ok:
                        return [], [str(e)], 1
                    raise
            else:
                assert False, 'unsupported mode'

            self.log.debug('code: %d' % code)
            if out:
                self.log.debug('out: %s' % '\n'.join(out))
            if err:
                self.log.debug('err: %s' % '\n'.join(err))
            if code and not error_ok:
                raise OrchestratorError(
                    'cephadm exited with an error code: %d, stderr:%s' % (
                        code, '\n'.join(err)))
            return out, err, code

    def _get_container_image_info(self, image_name: str) -> ContainerInspectInfo:
        # pick a random host...
        host = None
        for host_name in self.mgr.inventory.keys():
            host = host_name
            break
        if not host:
            raise OrchestratorError('no hosts defined')
        if self.mgr.cache.host_needs_registry_login(host) and self.mgr.registry_url:
            self._registry_login(host,
                                 json.loads(str(self.mgr.get_store('registry_credentials'))))

        pullargs: List[str] = []
        if self.mgr.registry_insecure:
            pullargs.append("--insecure")

        j = self._run_cephadm_json(host, '', 'pull', pullargs, image=image_name, no_fsid=True)

        r = ContainerInspectInfo(
            j['image_id'],
            j.get('ceph_version'),
            j.get('repo_digests')
        )
        self.log.debug(f'image {image_name} -> {r}')
        return r

    # function responsible for logging single host into custom registry
    def _registry_login(self, host: str, registry_json: Dict[str, str]) -> Optional[str]:
        self.log.debug(
            f"Attempting to log host {host} into custom registry @ {registry_json['url']}")
        # want to pass info over stdin rather than through normal list of args
        out, err, code = self._run_cephadm(
            host, 'mon', 'registry-login',
            ['--registry-json', '-'], stdin=json.dumps(registry_json), error_ok=True)
        if code:
            return f"Host {host} failed to login to {registry_json['url']} as {registry_json['username']} with given password"
        return None

    def _deploy_cephadm_binary(self, host: str) -> None:
        # Use tee (from coreutils) to create a copy of cephadm on the target machine
        self.log.info(f"Deploying cephadm binary to {host}")
        with self._remote_connection(host) as tpl:
            conn, _connr = tpl
            return self._deploy_cephadm_binary_conn(conn, host)

    def _deploy_cephadm_binary_conn(self, conn: "BaseConnection", host: str) -> None:
        _out, _err, code = remoto.process.check(
            conn,
            ['mkdir', '-p', f'/var/lib/ceph/{self.mgr._cluster_fsid}'])
        if code:
            msg = f"Unable to deploy the cephadm binary to {host}: {_err}"
            self.log.warning(msg)
            raise OrchestratorError(msg)
        _out, _err, code = remoto.process.check(
            conn,
            ['tee', '-', self.mgr.cephadm_binary_path],
            stdin=self.mgr._cephadm.encode('utf-8'))
        if code:
            msg = f"Unable to deploy the cephadm binary to {host}: {_err}"
            self.log.warning(msg)
            raise OrchestratorError(msg)

    def _write_remote_file(self,
                           host: str,
                           path: str,
                           content: bytes,
                           mode: int,
                           uid: int,
                           gid: int) -> None:
        with self._remote_connection(host) as tpl:
            conn, connr = tpl
            try:
                errmsg = connr.write_file(path, content, mode, uid, gid)
                if errmsg is not None:
                    raise OrchestratorError(errmsg)
            except Exception as e:
                msg = f"Unable to write {host}:{path}: {e}"
                self.log.warning(msg)
                raise OrchestratorError(msg)

    @contextmanager
    def _remote_connection(self,
                           host: str,
                           addr: Optional[str] = None,
                           ) -> Iterator[Tuple["BaseConnection", Any]]:
        if not addr and host in self.mgr.inventory:
            addr = self.mgr.inventory.get_addr(host)

        self.mgr.offline_hosts_remove(host)

        try:
            try:
                if not addr:
                    raise OrchestratorError("host address is empty")
                conn, connr = self.mgr._get_connection(addr)
            except OSError as e:
                self.mgr._reset_con(host)
                msg = f"Can't communicate with remote host `{addr}`, possibly because python3 is not installed there: {str(e)}"
                raise execnet.gateway_bootstrap.HostNotFound(msg)

            yield (conn, connr)

        except execnet.gateway_bootstrap.HostNotFound as e:
            # this is a misleading exception as it seems to be thrown for
            # any sort of connection failure, even those having nothing to
            # do with "host not found" (e.g., ssh key permission denied).
            self.mgr.offline_hosts.add(host)
            self.mgr._reset_con(host)

            user = self.mgr.ssh_user if self.mgr.mode == 'root' else 'cephadm'
            if str(e).startswith("Can't communicate"):
                msg = str(e)
            else:
                msg = f'''Failed to connect to {host} ({addr}).
Please make sure that the host is reachable and accepts connections using the cephadm SSH key

To add the cephadm SSH key to the host:
> ceph cephadm get-pub-key > ~/ceph.pub
> ssh-copy-id -f -i ~/ceph.pub {user}@{addr}

To check that the host is reachable open a new shell with the --no-hosts flag:
> cephadm shell --no-hosts

Then run the following:
> ceph cephadm get-ssh-config > ssh_config
> ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
> chmod 0600 ~/cephadm_private_key
> ssh -F ssh_config -i ~/cephadm_private_key {user}@{addr}'''
            raise OrchestratorError(msg) from e
        except Exception as ex:
            self.log.exception(ex)
            raise
