from ..orchestrator.cli import OrchestratorCLICommandBase


CephadmCLICommand = OrchestratorCLICommandBase.make_registry_subtype(
    "CephadmCLICommand")
