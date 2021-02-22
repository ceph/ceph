# flake8: noqa

from .module import OrchestratorCli

# usage: E.g. `from orchestrator import StatelessServiceSpec`
from ._interface import \
    Completion, TrivialReadCompletion, raise_if_exception, ProgressReference, pretty_print, _Promise, \
    CLICommand, _cli_write_command, _cli_read_command, CLICommandMeta, \
    Orchestrator, OrchestratorClientMixin, \
    OrchestratorValidationError, OrchestratorError, NoOrchestrator, \
    ServiceDescription, InventoryFilter, HostSpec, \
    DaemonDescription, DaemonDescriptionStatus, \
    OrchestratorEvent, set_exception_subject, \
    InventoryHost, DeviceLightLoc, \
    UpgradeStatusSpec, daemon_type_to_service, service_to_daemon_types


import os
if 'UNITTEST' in os.environ:
    import tests
