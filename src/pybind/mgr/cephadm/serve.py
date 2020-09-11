import logging
from typing import TYPE_CHECKING

from cephadm.utils import forall_hosts
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


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

                self.mgr.convert_tags_to_repo_digest()

                # refresh daemons
                self.log.debug('refreshing hosts and daemons')
                self._refresh_hosts_and_daemons()

                self.mgr._check_for_strays()

                self.mgr._update_paused_health()

                if not self.mgr.paused:
                    self.mgr.rm_util.process_removal_queue()

                    self.mgr.migration.migrate()
                    if self.mgr.migration.is_migration_ongoing():
                        continue

                    if self.mgr._apply_all_services():
                        continue  # did something, refresh

                    self.mgr._check_daemons()

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

    def _refresh_hosts_and_daemons(self) -> None:
        bad_hosts = []
        failures = []

        @forall_hosts
        def refresh(host):
            if self.mgr.cache.host_needs_check(host):
                r = self.mgr._check_host(host)
                if r is not None:
                    bad_hosts.append(r)
            if self.mgr.cache.host_needs_daemon_refresh(host):
                self.log.debug('refreshing %s daemons' % host)
                r = self.mgr._refresh_host_daemons(host)
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
                r = self.mgr._refresh_host_devices(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_osdspec_preview_refresh(host):
                self.log.debug(f"refreshing OSDSpec previews for {host}")
                r = self.mgr._refresh_host_osdspec_previews(host)
                if r:
                    failures.append(r)

            if self.mgr.cache.host_needs_new_etc_ceph_ceph_conf(host):
                self.log.debug(f"deploying new /etc/ceph/ceph.conf on `{host}`")
                r = self.mgr._deploy_etc_ceph_ceph_conf(host)
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
