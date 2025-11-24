from typing import Any, Callable, Iterator, Tuple, no_type_check

import contextlib
import errno
import functools

import object_format
from mgr_module import CLICommandBase

from . import resourcelib
from .proto import Self


class _cmdlet:
    def __init__(self, func: Callable, cmd: Callable) -> None:
        self._func = func
        self.command = cmd

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)


class SMBCLICommandBase(CLICommandBase):
    """A combined decorator and descriptor. Sets up the common parts of the
    CLICommand and object formatter.
    As a descriptor, it returns objects that can be called and wrap the
    "normal" function but also have a `.command` attribute so the CLI wrapped
    version can also be used under the same namespace.

    Example:
    >>> class Example:
    ...     @SMBCLICommand('share foo', perm='r')
    ...     def foo(self):
    ...         return {'test': 1}
    ...
    >>> ex = Example()
    >>> assert ex.foo() == {'test': 1}
    >>> assert ex.foo.command(format='yaml') == (0, "test: 1\\n", "")
    """

    def __init__(self, name: str, perm: str) -> None:
        super().__init__(f"smb {name}", perm)

    @no_type_check
    def __call__(self, func: Callable) -> Self:
        self._func = func
        # the smb module assumes that it will always be used with python
        # versions sufficiently new enough to always use ordered dicts
        # (builtin).  We dont want the json/yaml sorted by keys losing our
        # ordered k-v pairs.
        _fmt = functools.partial(
            object_format.ObjectFormatAdapter,
            sort_json=False,
            sort_yaml=False,
        )

        rsp = object_format.Responder(_fmt)
        ewrap = error_wrapper()
        self._command = super().__call__(rsp(ewrap(func)))

        return self

    @no_type_check
    def __get__(self, obj: Any, objtype: Any = None) -> _cmdlet:
        return _cmdlet(
            self._func.__get__(obj, objtype),
            self._command.__get__(obj, objtype),
        )


SMBCLICommand = SMBCLICommandBase.make_registry_subtype("SMBCLICommand")


class InvalidInputValue(object_format.ErrorResponseBase):
    def format_response(self) -> Tuple[int, str, str]:
        return -errno.EINVAL, "", str(self)


@contextlib.contextmanager
def error_wrapper() -> Iterator[None]:
    """Context-decorator that converts between certain common exception types."""
    try:
        yield
    except resourcelib.ResourceTypeError as err:
        msg = f'failed to parse input: {err}'
        raise InvalidInputValue(msg) from err
