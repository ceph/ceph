# decorators.py - decorators used by cephadm and their helpers

import logging
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from .context import CephadmContext
from .constants import NO_DEPRECATED
from .exceptions import Error

FuncT = TypeVar('FuncT', bound=Callable)

logger = logging.getLogger()


def require_image(func: FuncT) -> FuncT:
    """
    Require the global --image flag to be set
    """

    @wraps(func)
    def _require_image(ctx: CephadmContext) -> Any:
        if not ctx.image:
            raise Error(
                'This command requires the global --image option to be set'
            )
        return func(ctx)

    return cast(FuncT, _require_image)


def deprecated_command(func: FuncT) -> FuncT:
    @wraps(func)
    def _deprecated_command(ctx: CephadmContext) -> Any:
        logger.warning(f'Deprecated command used: {func}')
        if NO_DEPRECATED:
            raise Error('running deprecated commands disabled')
        return func(ctx)

    return cast(FuncT, _deprecated_command)


def executes_early(func: FuncT) -> FuncT:
    """Decorator that indicates the command function is meant to have no
    dependencies and no environmental requirements and can therefore be
    executed as non-root and with no logging, etc. Commands that have this
    decorator applied must be simple and self-contained.
    """
    cast(Any, func)._execute_early = True
    return func
