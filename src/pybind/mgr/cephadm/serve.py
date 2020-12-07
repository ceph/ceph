import datetime
import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Optional, List, Callable, cast, Set, Dict

try:
    import remoto
except ImportError:
    remoto = None

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import ServiceSpec, HostPlacementSpec, RGWSpec

import orchestrator
from cephadm.schedule import HostAssignment
from cephadm.upgrade import CEPH_UPGRADE_ORDER
from cephadm.utils import forall_hosts, cephadmNoImage, str_to_datetime, is_repo_digest
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator, ContainerInspectInfo

logger = logging.getLogger(__name__)

CEPH_TYPES = set(CEPH_UPGRADE_ORDER)


class CephadmServe:
    """
    This module contains functions that are executed in the
    serve() thread. Thus they don't block the CLI.

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
        while self.mgr.run:

            try:

                self.convert_tags_to_repo_digest()

                # refresh daemons
                self.log.debug('refreshing hosts and daemons')
                self._refresh_hosts_and_daemons()

                self._check_for_strays()

                self._update_paused_health()

                if not self.mgr.paused:
                    self.mgr.rm_util.process_removal_queue()

                    self.mgr.migration.migrate()
                    if self.mgr.migration.is_migration_ongoing():
                        continue

                    if self._apply_all_services():
                        continue  # did something, refresh

                    self._check_daemons()

                    if self.mgr.upgrade.continue_upgrade():
                        continue

            except OrchestratorError as e:
                if e.event_subject:
                    self.mgr.events.from_orch_error(e)

            self._serve_sleep()
        self.log.debug("serve exit")

    def _serve_sleep(self):
        sleep_interval = 600
        self.log.debug('Sleeping for %d seconds', sleep_interval)
        ret = self.mgr.event.wait(sleep_interval)
        self.mgr.event.clear()

    def _update_paused_health(self):
        if self.mgr.paused:
            self.mgr.health_checks['CEPHADM_PAUSED'] = {
                'severity': 'warning',
                'summary': 'cephadm background work is paused',
                'count': 1,
                'detail': ["'ceph orch resume' to resume"],
            }
            self.mgr.set_health_checks(self.mgr.health_checks)
        else:
            if 'CEPHADM_PAUSED' in self.mgr.health_checks:
                del self.mgr.health_checks['CEPHADM_PAUSED']
                self.mgr.set_health_checks(self.mgr.health_checks)

    def _refresh_hosts_and_daemons(self) -> None:
        bad_hosts = []
        failures = []

        @forall_hosts
        def refresh(host):
            if self.mgr.cache.host_needs_check(host):
                r = self._check_host(host)
                if r is not None:
                    bad_hosts.append(r)
            if self.mgr.cache.host_needs_daemon_refresh(host):
                self.log.debug('refreshing %s daemons' % host)
                r = self._refresh_host_daemons(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_registry_login(host) and self.mgr.registry_url:
                self.log.debug(f"Logging `{host}` into custom registry")
                r = self.mgr._registry_login(host, self.mgr.registry_url,
                                             self.mgr.registry_username, self.mgr.registry_password)
                if r:
                    bad_hosts.append(r)

            if self.mgr.cache.host_needs_device_refresh(host):
                self.log.debug('refreshing %s devices' % host)
                r = self._refresh_host_devices(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_osdspec_preview_refresh(host):
                self.log.debug(f"refreshing OSDSpec previews for {host}")
                r = self._refresh_host_osdspec_previews(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_new_etc_ceph_ceph_conf(host):
                self.log.debug(f"deploying new /etc/ceph/ceph.conf on `{host}`")
                r = self._deploy_etc_ceph_ceph_conf(host)
                if r:
                    bad_hosts.append(r)

        refresh(self.mgr.cache.get_hosts())

        health_changed = False
        if 'CEPHADM_HOST_CHECK_FAILED' in self.mgr.health_checks:
            del self.mgr.health_checks['CEPHADM_HOST_CHECK_FAILED']
            health_changed = True
        if bad_hosts:
            self.mgr.health_checks['CEPHADM_HOST_CHECK_FAILED'] = {
                'severity': 'warning',
                'summary': '%d hosts fail cephadm check' % len(bad_hosts),
                'count': len(bad_hosts),
                'detail': bad_hosts,
            }
            health_changed = True
        if failures:
            self.mgr.health_checks['CEPHADM_REFRESH_FAILED'] = {
                'severity': 'warning',
                'summary': 'failed to probe daemons or devices',
                'count': len(failures),
                'detail': failures,
            }
            health_changed = True
        elif 'CEPHADM_REFRESH_FAILED' in self.mgr.health_checks:
            del self.mgr.health_checks['CEPHADM_REFRESH_FAILED']
            health_changed = True
        if health_changed:
            self.mgr.set_health_checks(self.mgr.health_checks)

    def _check_host(self, host):
        if host not in self.mgr.inventory:
            return
        self.log.debug(' checking %s' % host)
        try:
            out, err, code = self.mgr._run_cephadm(
                host, cephadmNoImage, 'check-host', [],
                error_ok=True, no_fsid=True)
            self.mgr.cache.update_last_host_check(host)
            self.mgr.cache.save_host(host)
            if code:
                self.log.debug(' host %s failed check' % host)
                if self.mgr.warn_on_failed_host_check:
                    return 'host %s failed check: %s' % (host, err)
            else:
                self.log.debug(' host %s ok' % host)
        except Exception as e:
            self.log.debug(' host %s failed check' % host)
            return 'host %s failed check: %s' % (host, e)

    def _refresh_host_daemons(self, host) -> Optional[str]:
        try:
            out, err, code = self.mgr._run_cephadm(
                host, 'mon', 'ls', [], no_fsid=True)
            if code:
                return 'host %s cephadm ls returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s scrape failed: %s' % (host, e)
        ls = json.loads(''.join(out))
        dm = {}
        for d in ls:
            if not d['style'].startswith('cephadm'):
                continue
            if d['fsid'] != self.mgr._cluster_fsid:
                continue
            if '.' not in d['name']:
                continue
            sd = orchestrator.DaemonDescription()
            sd.last_refresh = datetime.datetime.utcnow()
            for k in ['created', 'started', 'last_configured', 'last_deployed']:
                v = d.get(k, None)
                if v:
                    setattr(sd, k, str_to_datetime(d[k]))
            sd.daemon_type = d['name'].split('.')[0]
            sd.daemon_id = '.'.join(d['name'].split('.')[1:])
            sd.hostname = host
            sd.container_id = d.get('container_id')
            if sd.container_id:
                # shorten the hash
                sd.container_id = sd.container_id[0:12]
            sd.container_image_name = d.get('container_image_name')
            sd.container_image_id = d.get('container_image_id')
            sd.version = d.get('version')
            if sd.daemon_type == 'osd':
                sd.osdspec_affinity = self.mgr.osd_service.get_osdspec_affinity(sd.daemon_id)
            if 'state' in d:
                sd.status_desc = d['state']
                sd.status = {
                    'running': 1,
                    'stopped': 0,
                    'error': -1,
                    'unknown': -1,
                }[d['state']]
            else:
                sd.status_desc = 'unknown'
                sd.status = None
            dm[sd.name()] = sd
        self.log.debug('Refreshed host %s daemons (%d)' % (host, len(dm)))
        self.mgr.cache.update_host_daemons(host, dm)
        self.mgr.cache.save_host(host)
        return None

    def _refresh_host_devices(self, host) -> Optional[str]:
        try:
            out, err, code = self.mgr._run_cephadm(
                host, 'osd',
                'ceph-volume',
                ['--', 'inventory', '--format=json', '--filter-for-batch'])
            if code:
                return 'host %s ceph-volume inventory returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s ceph-volume inventory failed: %s' % (host, e)
        devices = json.loads(''.join(out))
        try:
            out, err, code = self.mgr._run_cephadm(
                host, 'mon',
                'list-networks',
                [],
                no_fsid=True)
            if code:
                return 'host %s list-networks returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s list-networks failed: %s' % (host, e)
        networks = json.loads(''.join(out))
        self.log.debug('Refreshed host %s devices (%d) networks (%s)' % (
            host, len(devices), len(networks)))
        devices = inventory.Devices.from_json(devices)
        self.mgr.cache.update_host_devices_networks(host, devices.devices, networks)
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        return None

    def _refresh_host_osdspec_previews(self, host) -> bool:
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        self.log.debug(f'Refreshed OSDSpec previews for host <{host}>')
        return True

    def update_osdspec_previews(self, search_host: str = ''):
        # Set global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.add(search_host)
        previews = []
        # query OSDSpecs for host <search host> and generate/get the preview
        # There can be multiple previews for one host due to multiple OSDSpecs.
        previews.extend(self.mgr.osd_service.get_previews(search_host))
        self.log.debug(f"Loading OSDSpec previews to HostCache")
        self.mgr.cache.osdspec_previews[search_host] = previews
        # Unset global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.remove(search_host)

    def _deploy_etc_ceph_ceph_conf(self, host: str) -> Optional[str]:
        config = self.mgr.get_minimal_ceph_conf()

        try:
            with self.mgr._remote_connection(host) as tpl:
                conn, connr = tpl
                out, err, code = remoto.process.check(
                    conn,
                    ['mkdir', '-p', '/etc/ceph'])
                if code:
                    return f'failed to create /etc/ceph on {host}: {err}'
                out, err, code = remoto.process.check(
                    conn,
                    ['dd', 'of=/etc/ceph/ceph.conf'],
                    stdin=config.encode('utf-8')
                )
                if code:
                    return f'failed to create /etc/ceph/ceph.conf on {host}: {err}'
                self.mgr.cache.update_last_etc_ceph_ceph_conf(host)
                self.mgr.cache.save_host(host)
        except OrchestratorError as e:
            return f'failed to create /etc/ceph/ceph.conf on {host}: {str(e)}'
        return None

    def _check_for_strays(self) -> None:
        self.log.debug('_check_for_strays')
        for k in ['CEPHADM_STRAY_HOST',
                  'CEPHADM_STRAY_DAEMON']:
            if k in self.mgr.health_checks:
                del self.mgr.health_checks[k]
        if self.mgr.warn_on_stray_hosts or self.mgr.warn_on_stray_daemons:
            ls = self.mgr.list_servers()
            managed = self.mgr.cache.get_daemon_names()
            host_detail = []     # type: List[str]
            host_num_daemons = 0
            daemon_detail = []  # type: List[str]
            for item in ls:
                host = item.get('hostname')
                daemons = item.get('services')  # misnomer!
                missing_names = []
                for s in daemons:
                    name = '%s.%s' % (s.get('type'), s.get('id'))
                    if s.get('type') == 'rbd-mirror':
                        defaults = defaultdict(lambda: None, {'id': None})
                        metadata = self.mgr.get_metadata(
                            "rbd-mirror", s.get('id'), default=defaults)
                        if metadata['id']:
                            name = '%s.%s' % (s.get('type'), metadata['id'])
                        else:
                            self.log.debug(
                                "Failed to find daemon id for rbd-mirror service %s" % (s.get('id')))

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
                self.mgr.health_checks['CEPHADM_STRAY_HOST'] = {
                    'severity': 'warning',
                    'summary': '%d stray host(s) with %s daemon(s) '
                    'not managed by cephadm' % (
                        len(host_detail), host_num_daemons),
                    'count': len(host_detail),
                    'detail': host_detail,
                }
            if self.mgr.warn_on_stray_daemons and daemon_detail:
                self.mgr.health_checks['CEPHADM_STRAY_DAEMON'] = {
                    'severity': 'warning',
                    'summary': '%d stray daemons(s) not managed by cephadm' % (
                        len(daemon_detail)),
                    'count': len(daemon_detail),
                    'detail': daemon_detail,
                }
        self.mgr.set_health_checks(self.mgr.health_checks)

    def _apply_all_services(self) -> bool:
        r = False
        specs = []  # type: List[ServiceSpec]
        for sn, spec in self.mgr.spec_store.specs.items():
            specs.append(spec)
        for spec in specs:
            try:
                if self._apply_service(spec):
                    r = True
            except Exception as e:
                self.log.exception('Failed to apply %s spec %s: %s' % (
                    spec.service_name(), spec, e))
                self.mgr.events.for_service(spec, 'ERROR', 'Failed to apply: ' + str(e))

        return r

    def _config_fn(self, service_type) -> Optional[Callable[[ServiceSpec], None]]:
        fn = {
            'mds': self.mgr.mds_service.config,
            'rgw': self.mgr.rgw_service.config,
            'nfs': self.mgr.nfs_service.config,
            'iscsi': self.mgr.iscsi_service.config,
        }.get(service_type)
        return cast(Callable[[ServiceSpec], None], fn)

    def _apply_service(self, spec: ServiceSpec) -> bool:
        """
        Schedule a service.  Deploy new daemons or remove old ones, depending
        on the target label and count specified in the placement.
        """
        self.mgr.migration.verify_no_migration()

        daemon_type = spec.service_type
        service_name = spec.service_name()
        if spec.unmanaged:
            self.log.debug('Skipping unmanaged service %s' % service_name)
            return False
        if spec.preview_only:
            self.log.debug('Skipping preview_only service %s' % service_name)
            return False
        self.log.debug('Applying service %s spec' % service_name)

        config_func = self._config_fn(daemon_type)

        if daemon_type == 'osd':
            self.mgr.osd_service.create_from_spec(cast(DriveGroupSpec, spec))
            # TODO: return True would result in a busy loop
            # can't know if daemon count changed; create_from_spec doesn't
            # return a solid indication
            return False

        daemons = self.mgr.cache.get_daemons_by_service(service_name)

        public_network = None
        if daemon_type == 'mon':
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            if '/' in out:
                public_network = out.strip()
                self.log.debug('mon public_network is %s' % public_network)

        def matches_network(host):
            # type: (str) -> bool
            if not public_network:
                return False
            # make sure we have 1 or more IPs for that network on that
            # host
            return len(self.mgr.cache.networks[host].get(public_network, [])) > 0

        ha = HostAssignment(
            spec=spec,
            hosts=self.mgr._hosts_with_daemon_inventory(),
            get_daemons_func=self.mgr.cache.get_daemons_by_service,
            filter_new_host=matches_network if daemon_type == 'mon' else None,
        )

        hosts: List[HostPlacementSpec] = ha.place()
        self.log.debug('Usable hosts: %s' % hosts)

        r = None

        # sanity check
        if daemon_type in ['mon', 'mgr'] and len(hosts) < 1:
            self.log.debug('cannot scale mon|mgr below 1 (hosts=%s)' % hosts)
            return False

        # add any?
        did_config = False

        add_daemon_hosts: Set[HostPlacementSpec] = ha.add_daemon_hosts(hosts)
        self.log.debug('Hosts that will receive new daemons: %s' % add_daemon_hosts)

        remove_daemon_hosts: Set[orchestrator.DaemonDescription] = ha.remove_daemon_hosts(hosts)
        self.log.debug('Hosts that will loose daemons: %s' % remove_daemon_hosts)

        for host, network, name in add_daemon_hosts:
            daemon_id = self.mgr.get_unique_name(daemon_type, host, daemons,
                                                 prefix=spec.service_id,
                                                 forcename=name)

            if not did_config and config_func:
                if daemon_type == 'rgw':
                    rgw_config_func = cast(Callable[[RGWSpec, str], None], config_func)
                    rgw_config_func(cast(RGWSpec, spec), daemon_id)
                else:
                    config_func(spec)
                did_config = True

            daemon_spec = self.mgr.cephadm_services[daemon_type].make_daemon_spec(
                host, daemon_id, network, spec)
            self.log.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))

            try:
                daemon_spec = self.mgr.cephadm_services[daemon_type].prepare_create(daemon_spec)
                self.mgr._create_daemon(daemon_spec)
                r = True
            except (RuntimeError, OrchestratorError) as e:
                self.mgr.events.for_service(spec, 'ERROR',
                                            f"Failed while placing {daemon_type}.{daemon_id}"
                                            f"on {host}: {e}")
                # only return "no change" if no one else has already succeeded.
                # later successes will also change to True
                if r is None:
                    r = False
                continue

            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)

        # remove any?
        def _ok_to_stop(remove_daemon_hosts: Set[orchestrator.DaemonDescription]) -> bool:
            daemon_ids = [d.daemon_id for d in remove_daemon_hosts]
            r = self.mgr.cephadm_services[daemon_type].ok_to_stop(daemon_ids)
            return not r.retval

        while remove_daemon_hosts and not _ok_to_stop(remove_daemon_hosts):
            # let's find a subset that is ok-to-stop
            remove_daemon_hosts.pop()
        for d in remove_daemon_hosts:
            r = True
            # NOTE: we are passing the 'force' flag here, which means
            # we can delete a mon instances data.
            self.mgr._remove_daemon(d.name(), d.hostname)

        if r is None:
            r = False
        return r

    def _check_daemons(self) -> None:

        daemons = self.mgr.cache.get_daemons()
        daemons_post: Dict[str, List[orchestrator.DaemonDescription]] = defaultdict(list)
        for dd in daemons:
            # orphan?
            spec = self.mgr.spec_store.specs.get(dd.service_name(), None)
            if not spec and dd.daemon_type not in ['mon', 'mgr', 'osd']:
                # (mon and mgr specs should always exist; osds aren't matched
                # to a service spec)
                self.log.info('Removing orphan daemon %s...' % dd.name())
                self.mgr._remove_daemon(dd.name(), dd.hostname)

            # ignore unmanaged services
            if spec and spec.unmanaged:
                continue

            # These daemon types require additional configs after creation
            if dd.daemon_type in ['grafana', 'iscsi', 'prometheus', 'alertmanager', 'nfs']:
                daemons_post[dd.daemon_type].append(dd)

            if self.mgr.cephadm_services[dd.daemon_type].get_active_daemon(
               self.mgr.cache.get_daemons_by_service(dd.service_name())).daemon_id == dd.daemon_id:
                dd.is_active = True
            else:
                dd.is_active = False

            deps = self.mgr._calc_daemon_deps(dd.daemon_type, dd.daemon_id)
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
                    self.mgr._daemon_action(
                        daemon_type=dd.daemon_type,
                        daemon_id=dd.daemon_id,
                        host=dd.hostname,
                        action=action
                    )
                    self.mgr.cache.rm_scheduled_daemon_action(dd.hostname, dd.name())
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
            if daemon_type in self.mgr.requires_post_actions:
                self.mgr.requires_post_actions.remove(daemon_type)
                self.mgr._get_cephadm_service(daemon_type).daemon_check_post(daemon_descs)

    def convert_tags_to_repo_digest(self):
        if not self.mgr.use_repo_digest:
            return
        settings = self.mgr.upgrade.get_distinct_container_image_settings()
        digests: Dict[str, ContainerInspectInfo] = {}
        for container_image_ref in set(settings.values()):
            if not is_repo_digest(container_image_ref):
                image_info = self.mgr._get_container_image_info(container_image_ref)
                if image_info.repo_digest:
                    assert is_repo_digest(image_info.repo_digest), image_info
                digests[container_image_ref] = image_info

        for entity, container_image_ref in settings.items():
            if not is_repo_digest(container_image_ref):
                image_info = digests[container_image_ref]
                if image_info.repo_digest:
                    self.mgr.set_container_image(entity, image_info.repo_digest)
