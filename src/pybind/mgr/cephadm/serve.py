import datetime
import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, List, Dict, Optional, cast, Set, Callable

import remoto

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import ServiceSpec, HostPlacementSpec, RGWSpec
from cephadm.inventory import DATEFMT
from cephadm.schedule import HostAssignment
from cephadm.services.cephadmservice import CephadmDaemonSpec
from cephadm.utils import forall_hosts, cephadmNoImage
from orchestrator import OrchestratorError, DaemonDescription, set_exception_subject, \
    OrchestratorEvent

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CephadmServe:
    """
    This module contains functions that are executed in the
    serve() thread.

    Meaning they don't block the CLI
    """
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.cephadm_services = self.mgr.cephadm_services

    def serve(self) -> None:
        """
        The main loop of cephadm.

        A command handler will typically change the declarative state
        of cephadm. This loop will then attempt to apply this new state.
        """
        while self.mgr.run:

            try:

                # refresh daemons
                logger.debug('refreshing hosts and daemons')
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
                logger.debug('refreshing %s daemons' % host)
                r = self._refresh_host_daemons(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_registry_login(host) and self.mgr.registry_url:
                logger.debug(f"Logging `{host}` into custom registry")
                r = self._registry_login(host, self.mgr.registry_url, self.mgr.registry_username, self.mgr.registry_password)
                if r:
                    bad_hosts.append(r)

            if self.mgr.cache.host_needs_device_refresh(host):
                logger.debug('refreshing %s devices' % host)
                r = self._refresh_host_devices(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_osdspec_preview_refresh(host):
                logger.debug(f"refreshing OSDSpec previews for {host}")
                r = self._refresh_host_osdspec_previews(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_new_etc_ceph_ceph_conf(host):
                logger.debug(f"deploying new /etc/ceph/ceph.conf on `{host}`")
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
            
    def _check_for_strays(self):
        logger.debug('_check_for_strays')
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

    def _apply_all_services(self):
        r = False
        specs = [] # type: List[ServiceSpec]
        for sn, spec in self.mgr.spec_store.specs.items():
            specs.append(spec)
        for spec in specs:
            try:
                if self._apply_service(spec):
                    r = True
            except Exception as e:
                logger.exception('Failed to apply %s spec %s: %s' % (
                    spec.service_name(), spec, e))
                self.mgr.events.for_service(spec, 'ERROR', 'Failed to apply: ' + str(e))

        return r

    def _check_daemons(self):
        from cephadm.module import CephadmOrchestrator, CEPH_TYPES
        
        daemons = self.mgr.cache.get_daemons()
        daemons_post: Dict[str, List[DaemonDescription]] = defaultdict(list)
        for dd in daemons:
            # orphan?
            spec = self.mgr.spec_store.specs.get(dd.service_name(), None)
            if not spec and dd.daemon_type not in ['mon', 'mgr', 'osd']:
                # (mon and mgr specs should always exist; osds aren't matched
                # to a service spec)
                logger.info('Removing orphan daemon %s...' % dd.name())
                self.mgr._remove_daemon(dd.name(), dd.hostname)

            # ignore unmanaged services
            if spec and spec.unmanaged:
                continue

            # These daemon types require additional configs after creation
            if dd.daemon_type in ['grafana', 'iscsi', 'prometheus', 'alertmanager', 'nfs']:
                daemons_post[dd.daemon_type].append(dd)

            if self.cephadm_services[dd.daemon_type].get_active_daemon(
                    self.mgr.cache.get_daemons_by_service(dd.service_name())).daemon_id == dd.daemon_id:
                dd.is_active = True
            else:
                dd.is_active = False

            deps = self._calc_daemon_deps(dd.daemon_type, dd.daemon_id)
            last_deps, last_config = self.mgr.cache.get_daemon_last_config_deps(
                dd.hostname, dd.name())
            if last_deps is None:
                last_deps = []
            reconfig = False
            if not last_config:
                logger.info('Reconfiguring %s (unknown last config time)...' % (
                    dd.name()))
                reconfig = True
            elif last_deps != deps:
                logger.debug('%s deps %s -> %s' % (dd.name(), last_deps,
                                                     deps))
                logger.info('Reconfiguring %s (dependencies changed)...' % (
                    dd.name()))
                reconfig = True
            elif self.mgr.last_monmap and \
                    self.mgr.last_monmap > last_config and \
                    dd.daemon_type in CEPH_TYPES:
                logger.info('Reconfiguring %s (monmap changed)...' % dd.name())
                reconfig = True
            if reconfig:
                try:
                    self._create_daemon(
                        CephadmDaemonSpec(
                            host=dd.hostname,
                            daemon_id=dd.daemon_id,
                            daemon_type=dd.daemon_type),
                        reconfig=True)
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

    def _serve_sleep(self):
        sleep_interval = 600
        logger.debug('Sleeping for %d seconds', sleep_interval)
        ret = self.mgr.event.wait(sleep_interval)
        self.mgr.event.clear()

    def _check_host(self, host):
        if host not in self.mgr.inventory:
            return
        logger.debug(' checking %s' % host)
        try:
            out, err, code = self.mgr._run_cephadm(
                host, cephadmNoImage, 'check-host', [],
                error_ok=True, no_fsid=True)
            self.mgr.cache.update_last_host_check(host)
            self.mgr.cache.save_host(host)
            if code:
                logger.debug(' host %s failed check' % host)
                if self.mgr.warn_on_failed_host_check:
                    return 'host %s failed check: %s' % (host, err)
            else:
                logger.debug(' host %s ok' % host)
        except Exception as e:
            logger.debug(' host %s failed check' % host)
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
            sd = DaemonDescription()
            sd.last_refresh = datetime.datetime.utcnow()
            for k in ['created', 'started', 'last_configured', 'last_deployed']:
                v = d.get(k, None)
                if v:
                    setattr(sd, k, datetime.datetime.strptime(d[k], DATEFMT))
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
        logger.debug('Refreshed host %s daemons (%d)' % (host, len(dm)))
        self.mgr.cache.update_host_daemons(host, dm)
        self.mgr.cache.save_host(host)
        return None

    # function responsible for logging single host into custom registry
    def _registry_login(self, host, url, username, password):
        logger.debug(f"Attempting to log host {host} into custom registry @ {url}")
        # want to pass info over stdin rather than through normal list of args
        args_str = ("{\"url\": \"" + url + "\", \"username\": \"" + username + "\", "
                    " \"password\": \"" + password + "\"}")
        out, err, code = self.mgr._run_cephadm(
            host, 'mon', 'registry-login',
            ['--registry-json', '-'], stdin=args_str, error_ok=True)
        if code:
            return f"Host {host} failed to login to {url} as {username} with given password"
        return

    def _refresh_host_devices(self, host) -> Optional[str]:
        try:
            out, err, code = self.mgr._run_cephadm(
                host, 'osd',
                'ceph-volume',
                ['--', 'inventory', '--format=json'])
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
        logger.debug('Refreshed host %s devices (%d) networks (%s)' % (
            host, len(devices), len(networks)))
        devices = inventory.Devices.from_json(devices)
        self.mgr.cache.update_host_devices_networks(host, devices.devices, networks)
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        return None

    def _refresh_host_osdspec_previews(self, host) -> bool:
        self.update_osdspec_previews(host)
        self.mgr.cache.save_host(host)
        logger.debug(f'Refreshed OSDSpec previews for host <{host}>')
        return True

    def _deploy_etc_ceph_ceph_conf(self, host: str) -> Optional[str]:
        ret, config, err = self.mgr.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })

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

    def _apply_service(self, spec: ServiceSpec) -> bool:
        """
        Schedule a service.  Deploy new daemons or remove old ones, depending
        on the target label and count specified in the placement.
        """
        daemon_type = spec.service_type
        service_name = spec.service_name()
        if spec.unmanaged:
            logger.debug('Skipping unmanaged service %s' % service_name)
            return False
        if spec.preview_only:
            logger.debug('Skipping preview_only service %s' % service_name)
            return False
        logger.debug('Applying service %s spec' % service_name)

        config_func = self.mgr._config_fn(daemon_type)

        if daemon_type == 'osd':
            self.mgr.osd_service.create_from_spec(cast(DriveGroupSpec, spec))
            # TODO: return True would result in a busy loop
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
                logger.debug('mon public_network is %s' % public_network)

        def matches_network(host):
            # type: (str) -> bool
            if not public_network:
                return False
            # make sure we have 1 or more IPs for that network on that
            # host
            return len(self.mgr.cache.networks[host].get(public_network, [])) > 0

        ha = HostAssignment(
            spec=spec,
            get_hosts_func=self.mgr._get_hosts,
            get_daemons_func=self.mgr.cache.get_daemons_by_service,
            filter_new_host=matches_network if daemon_type == 'mon' else None,
        )

        hosts: List[HostPlacementSpec] = ha.place()
        logger.debug('Usable hosts: %s' % hosts)

        r = False

        # sanity check
        if daemon_type in ['mon', 'mgr'] and len(hosts) < 1:
            logger.debug('cannot scale mon|mgr below 1 (hosts=%s)' % hosts)
            return False

        # add any?
        did_config = False

        add_daemon_hosts: Set[HostPlacementSpec] = ha.add_daemon_hosts(hosts)
        logger.debug('Hosts that will receive new daemons: %s' % add_daemon_hosts)

        remove_daemon_hosts: Set[DaemonDescription] = ha.remove_daemon_hosts(hosts)
        logger.debug('Hosts that will loose daemons: %s' % remove_daemon_hosts)

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

            daemon_spec = self.cephadm_services[daemon_type].make_daemon_spec(host, daemon_id, network, spec)
            logger.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))

            self.cephadm_services[daemon_type].create(daemon_spec)

            # add to daemon list so next name(s) will also be unique
            sd = DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)
            r = True

        # remove any?
        def _ok_to_stop(remove_daemon_hosts: Set[DaemonDescription]) -> bool:
            daemon_ids = [d.daemon_id for d in remove_daemon_hosts]
            r = self.cephadm_services[daemon_type].ok_to_stop(daemon_ids)
            return not r.retval

        while remove_daemon_hosts and not _ok_to_stop(remove_daemon_hosts):
            # let's find a subset that is ok-to-stop
            remove_daemon_hosts.pop()
        for d in remove_daemon_hosts:
            # NOTE: we are passing the 'force' flag here, which means
            # we can delete a mon instances data.
            self._remove_daemon(d.name(), d.hostname)
            r = True

        return r

    def _calc_daemon_deps(self, daemon_type, daemon_id):
        need = {
            'prometheus': ['mgr', 'alertmanager', 'node-exporter'],
            'grafana': ['prometheus'],
            'alertmanager': ['mgr', 'alertmanager'],
        }
        deps = []
        for dep_type in need.get(daemon_type, []):
            for dd in self.mgr.cache.get_daemons_by_service(dep_type):
                deps.append(dd.name())
        return sorted(deps)

    def update_osdspec_previews(self, search_host: str = ''):
        # Set global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.add(search_host)
        previews = []
        # query OSDSpecs for host <search host> and generate/get the preview
        # There can be multiple previews for one host due to multiple OSDSpecs.
        previews.extend(self.mgr.osd_service.get_previews(search_host))
        logger.debug(f"Loading OSDSpec previews to HostCache")
        self.mgr.cache.osdspec_previews[search_host] = previews
        # Unset global 'pending' flag for host
        self.mgr.cache.loading_osdspec_preview.remove(search_host)

    def _create_daemon(self,
                       daemon_spec: CephadmDaemonSpec,
                       reconfig=False,
                       osd_uuid_map: Optional[Dict[str, Any]] = None,
                       redeploy=False,
                       ) -> str:

        with set_exception_subject('service', DaemonDescription(
                daemon_type=daemon_spec.daemon_type,
                daemon_id=daemon_spec.daemon_id,
                hostname=daemon_spec.host,
        ).service_id(), overwrite=True):

            start_time = datetime.datetime.utcnow()
            cephadm_config, deps = self.cephadm_services[daemon_spec.daemon_type].generate_config(daemon_spec)

            daemon_spec.extra_args.extend(['--config-json', '-'])

            # TCP port to open in the host firewall
            if daemon_spec.ports:
                daemon_spec.extra_args.extend(['--tcp-ports', ' '.join(map(str,daemon_spec.ports))])

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

            if self.mgr.cache.host_needs_registry_login(daemon_spec.host) and self.mgr.registry_url:
                self._registry_login(daemon_spec.host, self.mgr.registry_url, self.mgr.registry_username, self.mgr.registry_password)

            logger.info('%s daemon %s on %s' % (
                'Reconfiguring' if reconfig else 'Deploying',
                daemon_spec.name(), daemon_spec.host))

            out, err, code = self.mgr._run_cephadm(
                daemon_spec.host, daemon_spec.name(), 'deploy',
                [
                    '--name', daemon_spec.name(),
                ] + daemon_spec.extra_args,
                stdin=json.dumps(cephadm_config))
            if not code and daemon_spec.host in self.mgr.cache.daemons:
                # prime cached service state with what we (should have)
                # just created
                sd = DaemonDescription()
                sd.daemon_type = daemon_spec.daemon_type
                sd.daemon_id = daemon_spec.daemon_id
                sd.hostname = daemon_spec.host
                sd.status = 1
                sd.status_desc = 'starting'
                self.mgr.cache.add_daemon(daemon_spec.host, sd)
                if daemon_spec.daemon_type in ['grafana', 'iscsi', 'prometheus', 'alertmanager', 'nfs']:
                    self.mgr.requires_post_actions.add(daemon_spec.daemon_type)
            self.mgr.cache.invalidate_host_daemons(daemon_spec.host)
            self.mgr.cache.update_daemon_config_deps(daemon_spec.host, daemon_spec.name(), deps, start_time)
            self.mgr.cache.save_host(daemon_spec.host)
            msg = "{} {} on host '{}'".format(
                'Reconfigured' if reconfig else 'Deployed', daemon_spec.name(), daemon_spec.host)
            if not code:
                self.mgr.events.for_daemon(daemon_spec.name(), OrchestratorEvent.INFO, msg)
            else:
                what = 'reconfigure' if reconfig else 'deploy'
                self.mgr.events.for_daemon(daemon_spec.name(), OrchestratorEvent.ERROR, f'Failed to {what}: {err}')
            return msg

    @forall_hosts
    def _remove_daemons(self, name, host) -> str:
        return self._remove_daemon(name, host)

    def _remove_daemon(self, name, host) -> str:
        """
        Remove a daemon
        """
        (daemon_type, daemon_id) = name.split('.', 1)

        with set_exception_subject('service', DaemonDescription(
                daemon_type=daemon_type,
                daemon_id=daemon_id,
                hostname=host,
        ).service_id(), overwrite=True):


            self.cephadm_services[daemon_type].pre_remove(daemon_id)

            args = ['--name', name, '--force']
            logger.info('Removing daemon %s from %s' % (name, host))
            out, err, code = self.mgr._run_cephadm(
                host, name, 'rm-daemon', args)
            if not code:
                # remove item from cache
                self.mgr.cache.rm_daemon(host, name)
            self.mgr.cache.invalidate_host_daemons(host)
            return "Removed {} from host '{}'".format(name, host)