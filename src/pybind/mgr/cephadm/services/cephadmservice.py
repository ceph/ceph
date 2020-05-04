from typing import  TYPE_CHECKING

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CephadmService:
    """
    Base class for service types. Often providing a create() and config() fn.
    """
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr