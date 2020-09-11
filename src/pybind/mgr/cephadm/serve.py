import logging
from typing import TYPE_CHECKING

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
                self.mgr._refresh_hosts_and_daemons()

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

            self.mgr._serve_sleep()
        self.log.debug("serve exit")
