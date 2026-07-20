# -*- coding: utf-8 -*-
import functools
import inspect
from typing import Any, Callable

from mgr_module import CLICommandBase, HandleCommandResult, HandlerFuncType

DBCLICommand = CLICommandBase.make_registry_subtype("DBCLICommand")


class DBCommand(dict):
    """
    Helper class to declare options for COMMANDS list.

    It also allows to specify prefix and args separately, as well as storing a
    handler callable.

    Usage:
    >>> def handler(): return 0, "", ""
    >>> DBCommand(prefix="example",
    ...           handler=handler,
    ...           perm='w')
    {'perm': 'w', 'poll': False}
    """

    def __init__(
            self,
            prefix: str,
            handler: HandlerFuncType,
            perm: str = "rw",
            poll: bool = False,
    ):
        super().__init__(perm=perm,
                         poll=poll)
        self.prefix = prefix
        self.handler = handler

    @staticmethod
    def returns_command_result(instance: Any,
                               f: HandlerFuncType) -> Callable[..., HandleCommandResult]:
        @functools.wraps(f)
        def wrapper(mgr: Any, *args: Any, **kwargs: Any) -> HandleCommandResult:
            retval, stdout, stderr = f(instance or mgr, *args, **kwargs)
            return HandleCommandResult(retval, stdout, stderr)
        wrapper.__signature__ = inspect.signature(f)  # type: ignore[attr-defined]
        return wrapper

    def register(self, instance: bool = False) -> HandlerFuncType:
        """
        Register a CLICommand handler. It allows an instance to register bound
        methods. In that case, the mgr instance is not passed, and it's expected
        to be available in the class instance.
        It also uses HandleCommandResult helper to return a wrapped a tuple of 3
        items.
        """
        cmd = DBCLICommand(prefix=self.prefix, perm=self['perm'])
        return cmd(self.returns_command_result(instance, self.handler))
        return None
