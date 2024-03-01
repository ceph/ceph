# deamon_form.py - base class for creating and managing daemons

import abc

from typing import Type, TypeVar, List

from .context import CephadmContext
from .daemon_identity import DaemonIdentity


class DaemonForm(abc.ABC):
    """Base class for all types used to build, customize, or otherwise give
    form to a deaemon managed by cephadm.
    """

    @classmethod
    @abc.abstractmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        """The for_daemon_type class method accepts a string identifying a
        daemon type and should return true if the class can form a daemon of
        the named type. Using a method allows supporting arbitrary daemon names
        and multiple names for a single class.
        """
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    @abc.abstractmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'DaemonForm':
        """The create class method acts as a common interface for creating
        any DaemonForm instance. This means that each class implementing a
        DaemonForm can have an __init__ tuned to it's specific needs but
        this common interface can be used to intantiate any DaemonForm.
        """
        raise NotImplementedError()  # pragma: no cover

    @property
    @abc.abstractmethod
    def identity(self) -> DaemonIdentity:
        """All DaemonForm instances must be able to identify themselves.
        The identity property returns a DaemonIdentity tied to the form
        being created or manged.
        """
        raise NotImplementedError()  # pragma: no cover


DF = TypeVar('DF', bound=DaemonForm)


# Optional daemon form subtypes follow:
# These optional subtypes use the abc modules __subclasshook__ feature.
# Classes that implement these "interfaces" do not need to inherit
# directly from these classes, but can simply implment the optional
# methods. If these methods are avilable then `isinstance` and
# `issubclass` will return true for that class and you can
# safely use the desired method(s).
# Example:
# >>> # daemon1 implements get_sysctl_settings
# >>> assert isinstance(daemon1, SysctlDaemonForm)
# >>> daemon1.get_sysctl_settings()
# >>> # daemon2 doesn't implement get_sysctl_settings
# >>> assert not isinstance(daemon2, SysctlDaemonForm)


class SysctlDaemonForm(DaemonForm, metaclass=abc.ABCMeta):
    """The SysctlDaemonForm is an optional subclass that some DaemonForm
    types may choose to implement. A SysctlDaemonForm must implement
    get_sysctl_settings.
    """

    @abc.abstractmethod
    def get_sysctl_settings(self) -> List[str]:
        """Return a list of sysctl settings for the deamon."""
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def __subclasshook__(cls, other: Type[DF]) -> bool:
        return callable(getattr(other, 'get_sysctl_settings', None))


class FirewalledServiceDaemonForm(DaemonForm, metaclass=abc.ABCMeta):
    """The FirewalledServiceDaemonForm is an optional subclass that some
    DaemonForm types may choose to implement. A FirewalledServiceDaemonForm
    must implement firewall_service_name.
    """

    @abc.abstractmethod
    def firewall_service_name(self) -> str:
        """Return the name of the service known to the firewalld system."""
        raise NotImplementedError()  # pragma: no cover

    @classmethod
    def __subclasshook__(cls, other: Type[DF]) -> bool:
        return callable(getattr(other, 'firewall_service_name', None))


_DAEMON_FORMERS = []


class UnexpectedDaemonTypeError(KeyError):
    pass


def register(cls: Type[DF]) -> Type[DF]:
    """Decorator to be placed on DaemonForm types if the type is to be added to
    the daemon form registry.
    """
    _DAEMON_FORMERS.append(cls)
    return cls


def choose(daemon_type: str) -> Type[DF]:
    """Return a daemon form *class* that is compatible with the given daemon
    type name.
    """
    for dftype in _DAEMON_FORMERS:
        if dftype.for_daemon_type(daemon_type):
            return dftype
    raise UnexpectedDaemonTypeError(daemon_type)


def create(ctx: CephadmContext, ident: DaemonIdentity) -> DaemonForm:
    cls: Type[DaemonForm] = choose(ident.daemon_type)
    return cls.create(ctx, ident)
