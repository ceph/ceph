import errno
from typing import Any, Tuple

from ceph.deployment.service_spec import (
    SpecValidationError
)
from functools import wraps
from mgr_module import CLICommandBase, HandlerFuncType, HandleCommandResult

from ._interface import OrchestratorError


class OrchestratorCLICommandBase(CLICommandBase):
    def __call__(self, func: HandlerFuncType) -> HandlerFuncType:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Tuple[int, str, str]:
            try:
                return func(*args, **kwargs)
            except (OrchestratorError, SpecValidationError) as e:
                # Do not print Traceback for expected errors.
                return HandleCommandResult(retval=e.errno, stderr=str(e))
            except ImportError as e:
                return HandleCommandResult(retval=-errno.ENOENT, stderr=str(e))
            except NotImplementedError:
                msg = 'This Orchestrator does not support `{}`'.format(self.prefix)
                return HandleCommandResult(retval=-errno.ENOENT, stderr=msg)
        return super().__call__(wrapper)


OrchestratorCLICommand = OrchestratorCLICommandBase.make_registry_subtype(
    "OrchestratorCLICommand")
