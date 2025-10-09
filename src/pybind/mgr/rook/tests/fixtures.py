from rook.module import RookOrchestrator
from orchestrator import raise_if_exception, OrchResult

try:
    from typing import Any
except ImportError:
    pass


def wait(m: RookOrchestrator, c: OrchResult) -> Any:
    return raise_if_exception(c)
