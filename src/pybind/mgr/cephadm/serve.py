import logging
from typing import TYPE_CHECKING

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
