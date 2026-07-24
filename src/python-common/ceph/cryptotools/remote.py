"""Remote execution of cryptographic functions for the ceph mgr"""

# NB. This module exists to enapsulate the logic around running
# the cryptotools module that are forked off of the parent process
# to avoid the pyo3 subintepreters problem.
#
# The current implementation is simple using the builtin Python multiprocessing
# facilities to pass data around natively. Communication happens via regular IPC
# keeping any sensitive data off cmdlines. It is important that we avoid putting
# the sensitive data on the command line as that is visible in /proc.
#
# This simple implementation incurs the cost of spawning a full interpreter
# instance, however this occurs once per instantiated remote CryptoCaller.
# Since CryptoCaller is written as a class and the proxy allows registering
# additional classes we choose to we can have multiple implementations of the
# CryptoCaller as the interface is pure Python (provided types can be pickled).

from typing import Any, Callable, Optional, Type

import functools
import importlib
import logging
import multiprocessing
from multiprocessing.managers import BaseManager

from .caller import CryptoCaller

_ctmodule = 'ceph.cryptotools.cryptotools'

logger = logging.getLogger('ceph.cryptotools.remote')


class ProxyLoader:
    """ProxyLoader shim to defer loading the classes which may load one-per-process
    resources up until they are actually used in their own process.
    Without this the import would implicitly load all of the dependencies, including the
    PyO3 ones which are unable to run with the multiple subinterpreters.
    """

    def __init__(
        self, clsname: str, clspath: str, pkg: Optional[str] = None
    ) -> None:
        self.clsname = clsname
        self.clspath = clspath
        self.pkg = pkg
        self.target = None

    def initialize(self) -> None:
        logger.info("initializing remote process %s", self.clsname)
        target_cls = getattr(
            importlib.import_module(self.clspath, self.pkg), self.clsname
        )
        self.target = target_cls()

    def __call__(self) -> Any:
        if self.target is None:
            self.initialize()
        return self.target


# decorator to allow adding more classes in a clean way
def register_manager_class(
    name: str,
    cls: Type[Any],
    clsname: str,
    clspath: str,
    pkg: Optional[str] = __name__,
) -> Callable[[Type[BaseManager]], Type[BaseManager]]:

    def wrapper(mgr: Type[BaseManager]) -> Type[BaseManager]:
        functools.wraps(mgr)
        logger.info(
            "registering remato backend %s (via %s from %s (%s))",
            name,
            clsname,
            clspath,
            pkg,
        )
        mgr.register(name, ProxyLoader(clsname, clspath, pkg))
        return mgr

    return wrapper


@register_manager_class(
    "CryptoCaller", CryptoCaller, "InternalCryptoCaller", "..internal"
)
class CryptoProxyManager(BaseManager):
    """CryptoProxyManager proxies requests to cryptographic functions used by
    the ceph mgr into the InternalCryptoCaller class running in a different
    process.
    Running the crypto functions in a separate process avoids conflicts
    between the mgr's use of subintepreters and the cryptography module's
    use of PyO3 rust bindings.
    """

    def __init__(self) -> None:
        # "spawn" gives us the completely clean interpreter
        super().__init__(ctx=multiprocessing.get_context("spawn"))
