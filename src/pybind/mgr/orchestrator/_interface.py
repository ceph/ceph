
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""

import copy
import datetime
import enum
import errno
import logging
import pickle
import re

from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from functools import wraps, reduce, update_wrapper

from typing import TypeVar, Generic, List, Optional, Union, Tuple, Iterator, Callable, Any, \
    Sequence, Dict, cast, Mapping

try:
    from typing import Protocol  # Protocol was added in Python 3.8
except ImportError:
    class Protocol:  # type: ignore
        pass


import yaml

from ceph.deployment import inventory
from ceph.deployment.service_spec import (
    ArgumentList,
    ArgumentSpec,
    GeneralArgList,
    IngressSpec,
    IscsiServiceSpec,
    MDSSpec,
    NFSServiceSpec,
    RGWSpec,
    SNMPGatewaySpec,
    ServiceSpec,
    TunedProfileSpec,
    NvmeofServiceSpec
)
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.hostspec import HostSpec, SpecValidationError
from ceph.utils import datetime_to_str, str_to_datetime

from mgr_module import MgrModule, CLICommand, HandleCommandResult


logger = logging.getLogger(__name__)

T = TypeVar('T')
FuncT = TypeVar('FuncT', bound=Callable[..., Any])


class OrchestratorError(Exception):
    """
    General orchestrator specific error.

    Used for deployment, configuration or user errors.

    It's not intended for programming errors or orchestrator internal errors.
    """

    def __init__(self,
                 msg: str,
                 errno: int = -errno.EINVAL,
                 event_kind_subject: Optional[Tuple[str, str]] = None) -> None:
        super(Exception, self).__init__(msg)
        self.errno = errno
        # See OrchestratorEvent.subject
        self.event_subject = event_kind_subject


class NoOrchestrator(OrchestratorError):
    """
    No orchestrator in configured.
    """

    def __init__(self, msg: str = "No orchestrator configured (try `ceph orch set backend`)") -> None:
        super(NoOrchestrator, self).__init__(msg, errno=-errno.ENOENT)


class OrchestratorValidationError(OrchestratorError):
    """
    Raised when an orchestrator doesn't support a specific feature.
    """


@contextmanager
def set_exception_subject(kind: str, subject: str, overwrite: bool = False) -> Iterator[None]:
    try:
        yield
    except OrchestratorError as e:
        if overwrite or hasattr(e, 'event_subject'):
            e.event_subject = (kind, subject)
        raise


def handle_exception(prefix: str, perm: str, func: FuncT) -> FuncT:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (OrchestratorError, SpecValidationError) as e:
            # Do not print Traceback for expected errors.
            return HandleCommandResult(e.errno, stderr=str(e))
        except ImportError as e:
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
        except NotImplementedError:
            msg = 'This Orchestrator does not support `{}`'.format(prefix)
            return HandleCommandResult(-errno.ENOENT, stderr=msg)

    # misuse lambda to copy `wrapper`
    wrapper_copy = lambda *l_args, **l_kwargs: wrapper(*l_args, **l_kwargs)  # noqa: E731
    wrapper_copy._prefix = prefix  # type: ignore
    wrapper_copy._cli_command = CLICommand(prefix, perm)  # type: ignore
    wrapper_copy._cli_command.store_func_metadata(func)  # type: ignore
    wrapper_copy._cli_command.func = wrapper_copy  # type: ignore

    return cast(FuncT, wrapper_copy)


def handle_orch_error(f: Callable[..., T]) -> Callable[..., 'OrchResult[T]']:
    """
    Decorator to make Orchestrator methods return
    an OrchResult.
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> OrchResult[T]:
        try:
            return OrchResult(f(*args, **kwargs))
        except Exception as e:
            logger.exception(e)
            import os
            if 'UNITTEST' in os.environ:
                raise  # This makes debugging of Tracebacks from unittests a bit easier
            return OrchResult(None, exception=e)

    return cast(Callable[..., OrchResult[T]], wrapper)


class InnerCliCommandCallable(Protocol):
    def __call__(self, prefix: str) -> Callable[[FuncT], FuncT]:
        ...


def _cli_command(perm: str) -> InnerCliCommandCallable:
    def inner_cli_command(prefix: str) -> Callable[[FuncT], FuncT]:
        return lambda func: handle_exception(prefix, perm, func)
    return inner_cli_command


_cli_read_command = _cli_command('r')
_cli_write_command = _cli_command('rw')


class CLICommandMeta(type):
    """
    This is a workaround for the use of a global variable CLICommand.COMMANDS which
    prevents modules from importing any other module.

    We make use of CLICommand, except for the use of the global variable.
    """
    def __init__(cls, name: str, bases: Any, dct: Any) -> None:
        super(CLICommandMeta, cls).__init__(name, bases, dct)
        dispatch: Dict[str, CLICommand] = {}
        for v in dct.values():
            try:
                dispatch[v._prefix] = v._cli_command
            except AttributeError:
                pass

        def handle_command(self: Any, inbuf: Optional[str], cmd: dict) -> Any:
            if cmd['prefix'] not in dispatch:
                return self.handle_command(inbuf, cmd)

            return dispatch[cmd['prefix']].call(self, cmd, inbuf)

        cls.COMMANDS = [cmd.dump_cmd() for cmd in dispatch.values()]
        cls.handle_command = handle_command


class OrchResult(Generic[T]):
    """
    Stores a result and an exception. Mainly to circumvent the
    MgrModule.remote() method that hides all exceptions and for
    handling different sub-interpreters.
    """

    def __init__(self, result: Optional[T], exception: Optional[Exception] = None) -> None:
        self.result = result
        self.serialized_exception: Optional[bytes] = None
        self.exception_str: str = ''
        self.set_exception(exception)

    __slots__ = 'result', 'serialized_exception', 'exception_str'

    def set_exception(self, e: Optional[Exception]) -> None:
        if e is None:
            self.serialized_exception = None
            self.exception_str = ''
            return

        self.exception_str = f'{type(e)}: {str(e)}'
        try:
            self.serialized_exception = pickle.dumps(e)
        except pickle.PicklingError:
            logger.error(f"failed to pickle {e}")
            if isinstance(e, Exception):
                e = Exception(*e.args)
            else:
                e = Exception(str(e))
            # degenerate to a plain Exception
            self.serialized_exception = pickle.dumps(e)

    def result_str(self) -> str:
        """Force a string."""
        if self.result is None:
            return ''
        if isinstance(self.result, list):
            return '\n'.join(str(x) for x in self.result)
        return str(self.result)


def raise_if_exception(c: OrchResult[T]) -> T:
    """
    Due to different sub-interpreters, this MUST not be in the `OrchResult` class.
    """
    if c.serialized_exception is not None:
        try:
            e = pickle.loads(c.serialized_exception)
        except (KeyError, AttributeError):
            raise Exception(c.exception_str)
        raise e
    assert c.result is not None, 'OrchResult should either have an exception or a result'
    return c.result


def _hide_in_features(f: FuncT) -> FuncT:
    f._hide_in_features = True  # type: ignore
    return f


class Orchestrator(object):
    """
    Calls in this class may do long running remote operations, with time
    periods ranging from network latencies to package install latencies and large
    internet downloads.  For that reason, all are asynchronous, and return
    ``Completion`` objects.

    Methods should only return the completion and not directly execute
    anything, like network calls. Otherwise the purpose of
    those completions is defeated.

    Implementations are not required to start work on an operation until
    the caller waits on the relevant Completion objects.  Callers making
    multiple updates should not wait on Completions until they're done
    sending operations: this enables implementations to batch up a series
    of updates when wait() is called on a set of Completion objects.

    Implementations are encouraged to keep reasonably fresh caches of
    the status of the system: it is better to serve a stale-but-recent
    result read of e.g. device inventory than it is to keep the caller waiting
    while you scan hosts every time.
    """

    @_hide_in_features
    def is_orchestrator_module(self) -> bool:
        """
        Enable other modules to interrogate this module to discover
        whether it's usable as an orchestrator module.

        Subclasses do not need to override this.
        """
        return True

    @_hide_in_features
    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Report whether we can talk to the orchestrator.  This is the
        place to give the user a meaningful message if the orchestrator
        isn't running or can't be contacted.

        This method may be called frequently (e.g. every page load
        to conditionally display a warning banner), so make sure it's
        not too expensive.  It's okay to give a slightly stale status
        (e.g. based on a periodic background ping of the orchestrator)
        if that's necessary to make this method fast.

        .. note::
            `True` doesn't mean that the desired functionality
            is actually available in the orchestrator. I.e. this
            won't work as expected::

                >>> #doctest: +SKIP
                ... if OrchestratorClientMixin().available()[0]:  # wrong.
                ...     OrchestratorClientMixin().get_hosts()

        :return: boolean representing whether the module is available/usable
        :return: string describing any error
        :return: dict containing any module specific information
        """
        raise NotImplementedError()

    @_hide_in_features
    def get_feature_set(self) -> Dict[str, dict]:
        """Describes which methods this orchestrator implements

        .. note::
            `True` doesn't mean that the desired functionality
            is actually possible in the orchestrator. I.e. this
            won't work as expected::

                >>> #doctest: +SKIP
                ... api = OrchestratorClientMixin()
                ... if api.get_feature_set()['get_hosts']['available']:  # wrong.
                ...     api.get_hosts()

            It's better to ask for forgiveness instead::

                >>> #doctest: +SKIP
                ... try:
                ...     OrchestratorClientMixin().get_hosts()
                ... except (OrchestratorError, NotImplementedError):
                ...     ...

        :returns: Dict of API method names to ``{'available': True or False}``
        """
        module = self.__class__
        features = {a: {'available': getattr(Orchestrator, a, None) != getattr(module, a)}
                    for a in Orchestrator.__dict__
                    if not a.startswith('_') and not getattr(getattr(Orchestrator, a), '_hide_in_features', False)
                    }
        return features

    def cancel_completions(self) -> None:
        """
        Cancels ongoing completions. Unstuck the mgr.
        """
        raise NotImplementedError()

    def pause(self) -> None:
        raise NotImplementedError()

    def resume(self) -> None:
        raise NotImplementedError()

    def add_host(self, host_spec: HostSpec) -> OrchResult[str]:
        """
        Add a host to the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def hardware_light(self, light_type: str, action: str, hostname: str, device: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Light a chassis or device ident LED.

        :param light_type: led type (chassis or device).
        :param action: set or get status led.
        :param hostname: the name of the host.
        :param device: the device id (when light_type = 'device')
        """
        raise NotImplementedError()

    def hardware_powercycle(self, hostname: str, yes_i_really_mean_it: bool = False) -> OrchResult[str]:
        """
        Reboot a host.

        :param hostname: the name of the host being rebooted.
        """
        raise NotImplementedError()

    def hardware_shutdown(self, hostname: str, force: Optional[bool] = False, yes_i_really_mean_it: bool = False) -> OrchResult[str]:
        """
        Shutdown a host.

        :param hostname: the name of the host to shutdown.
        """
        raise NotImplementedError()

    def hardware_status(self, hostname: Optional[str] = None, category: Optional[str] = 'summary') -> OrchResult[str]:
        """
        Display hardware status.

        :param category: category
        :param hostname: hostname
        """
        raise NotImplementedError()

    def node_proxy_summary(self, hostname: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Return node-proxy summary

        :param hostname: hostname
        """
        raise NotImplementedError()

    def node_proxy_fullreport(self, hostname: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Return node-proxy full report

        :param hostname: hostname
        """
        raise NotImplementedError()

    def node_proxy_firmwares(self, hostname: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Return node-proxy firmwares report

        :param hostname: hostname
        """
        raise NotImplementedError()

    def node_proxy_criticals(self, hostname: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Return node-proxy criticals report

        :param hostname: hostname
        """
        raise NotImplementedError()

    def node_proxy_common(self, category: str, hostname: Optional[str] = None) -> OrchResult[Dict[str, Any]]:
        """
        Return node-proxy generic report

        :param hostname: hostname
        """
        raise NotImplementedError()

    def remove_host(self, host: str, force: bool, offline: bool, rm_crush_entry: bool) -> OrchResult[str]:
        """
        Remove a host from the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def drain_host(self, hostname: str, force: bool = False, keep_conf_keyring: bool = False, zap_osd_devices: bool = False) -> OrchResult[str]:
        """
        drain all daemons from a host

        :param hostname: hostname
        """
        raise NotImplementedError()

    def update_host_addr(self, host: str, addr: str) -> OrchResult[str]:
        """
        Update a host's address

        :param host: hostname
        :param addr: address (dns name or IP)
        """
        raise NotImplementedError()

    def get_hosts(self) -> OrchResult[List[HostSpec]]:
        """
        Report the hosts in the cluster.

        :return: list of HostSpec
        """
        raise NotImplementedError()

    def get_facts(self, hostname: Optional[str] = None) -> OrchResult[List[Dict[str, Any]]]:
        """
        Return hosts metadata(gather_facts).
        """
        raise NotImplementedError()

    def add_host_label(self, host: str, label: str) -> OrchResult[str]:
        """
        Add a host label
        """
        raise NotImplementedError()

    def remove_host_label(self, host: str, label: str, force: bool = False) -> OrchResult[str]:
        """
        Remove a host label
        """
        raise NotImplementedError()

    def host_ok_to_stop(self, hostname: str) -> OrchResult:
        """
        Check if the specified host can be safely stopped without reducing availability

        :param host: hostname
        """
        raise NotImplementedError()

    def enter_host_maintenance(self, hostname: str, force: bool = False, yes_i_really_mean_it: bool = False) -> OrchResult:
        """
        Place a host in maintenance, stopping daemons and disabling it's systemd target
        """
        raise NotImplementedError()

    def exit_host_maintenance(self, hostname: str) -> OrchResult:
        """
        Return a host from maintenance, restarting the clusters systemd target
        """
        raise NotImplementedError()

    def rescan_host(self, hostname: str) -> OrchResult:
        """Use cephadm to issue a disk rescan on each HBA

        Some HBAs and external enclosures don't automatically register
        device insertion with the kernel, so for these scenarios we need
        to manually rescan

        :param hostname: (str) host name
        """
        raise NotImplementedError()

    def get_inventory(self, host_filter: Optional['InventoryFilter'] = None, refresh: bool = False) -> OrchResult[List['InventoryHost']]:
        """
        Returns something that was created by `ceph-volume inventory`.

        :return: list of InventoryHost
        """
        raise NotImplementedError()

    def service_discovery_dump_cert(self) -> OrchResult:
        """
        Returns service discovery server root certificate

        :return: service discovery root certificate
        """
        raise NotImplementedError()

    def describe_service(self, service_type: Optional[str] = None, service_name: Optional[str] = None, refresh: bool = False) -> OrchResult[List['ServiceDescription']]:
        """
        Describe a service (of any kind) that is already configured in
        the orchestrator.  For example, when viewing an OSD in the dashboard
        we might like to also display information about the orchestrator's
        view of the service (like the kubernetes pod ID).

        When viewing a CephFS filesystem in the dashboard, we would use this
        to display the pods being currently run for MDS daemons.

        :return: list of ServiceDescription objects.
        """
        raise NotImplementedError()

    def list_daemons(self, service_name: Optional[str] = None, daemon_type: Optional[str] = None, daemon_id: Optional[str] = None, host: Optional[str] = None, refresh: bool = False) -> OrchResult[List['DaemonDescription']]:
        """
        Describe a daemon (of any kind) that is already configured in
        the orchestrator.

        :return: list of DaemonDescription objects.
        """
        raise NotImplementedError()

    @handle_orch_error
    def apply(self, specs: Sequence["GenericSpec"], no_overwrite: bool = False) -> List[str]:
        """
        Applies any spec
        """
        fns: Dict[str, Callable[..., OrchResult[str]]] = {
            'alertmanager': self.apply_alertmanager,
            'crash': self.apply_crash,
            'grafana': self.apply_grafana,
            'iscsi': self.apply_iscsi,
            'nvmeof': self.apply_nvmeof,
            'mds': self.apply_mds,
            'mgr': self.apply_mgr,
            'mon': self.apply_mon,
            'nfs': self.apply_nfs,
            'node-exporter': self.apply_node_exporter,
            'ceph-exporter': self.apply_ceph_exporter,
            'osd': lambda dg: self.apply_drivegroups([dg]),  # type: ignore
            'prometheus': self.apply_prometheus,
            'loki': self.apply_loki,
            'promtail': self.apply_promtail,
            'rbd-mirror': self.apply_rbd_mirror,
            'rgw': self.apply_rgw,
            'ingress': self.apply_ingress,
            'snmp-gateway': self.apply_snmp_gateway,
            'host': self.add_host,
        }

        def merge(l: OrchResult[List[str]], r: OrchResult[str]) -> OrchResult[List[str]]:  # noqa: E741
            l_res = raise_if_exception(l)
            r_res = raise_if_exception(r)
            l_res.append(r_res)
            return OrchResult(l_res)
        return raise_if_exception(reduce(merge, [fns[spec.service_type](spec) for spec in specs], OrchResult([])))

    def set_unmanaged(self, service_name: str, value: bool) -> OrchResult[str]:
        """
        Set unmanaged parameter to True/False for a given service

        :return: None
        """
        raise NotImplementedError()

    def plan(self, spec: Sequence["GenericSpec"]) -> OrchResult[List]:
        """
        Plan (Dry-run, Preview) a List of Specs.
        """
        raise NotImplementedError()

    def remove_daemons(self, names: List[str]) -> OrchResult[List[str]]:
        """
        Remove specific daemon(s).

        :return: None
        """
        raise NotImplementedError()

    def remove_service(self, service_name: str, force: bool = False) -> OrchResult[str]:
        """
        Remove a service (a collection of daemons).

        :return: None
        """
        raise NotImplementedError()

    def service_action(self, action: str, service_name: str) -> OrchResult[List[str]]:
        """
        Perform an action (start/stop/reload) on a service (i.e., all daemons
        providing the logical service).

        :param action: one of "start", "stop", "restart", "redeploy", "reconfig"
        :param service_name: service_type + '.' + service_id
                            (e.g. "mon", "mgr", "mds.mycephfs", "rgw.realm.zone", ...)
        :rtype: OrchResult
        """
        # assert action in ["start", "stop", "reload, "restart", "redeploy"]
        raise NotImplementedError()

    def daemon_action(self, action: str, daemon_name: str, image: Optional[str] = None) -> OrchResult[str]:
        """
        Perform an action (start/stop/reload) on a daemon.

        :param action: one of "start", "stop", "restart", "redeploy", "reconfig"
        :param daemon_name: name of daemon
        :param image: Container image when redeploying that daemon
        :rtype: OrchResult
        """
        # assert action in ["start", "stop", "reload, "restart", "redeploy"]
        raise NotImplementedError()

    def create_osds(self, drive_group: DriveGroupSpec) -> OrchResult[str]:
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).
        """
        raise NotImplementedError()

    def apply_drivegroups(self, specs: List[DriveGroupSpec]) -> OrchResult[List[str]]:
        """ Update OSD cluster """
        raise NotImplementedError()

    def set_unmanaged_flag(self,
                           unmanaged_flag: bool,
                           service_type: str = 'osd',
                           service_name: Optional[str] = None
                           ) -> HandleCommandResult:
        raise NotImplementedError()

    def preview_osdspecs(self,
                         osdspec_name: Optional[str] = 'osd',
                         osdspecs: Optional[List[DriveGroupSpec]] = None
                         ) -> OrchResult[str]:
        """ Get a preview for OSD deployments """
        raise NotImplementedError()

    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False,
                    zap: bool = False,
                    no_destroy: bool = False) -> OrchResult[str]:
        """
        :param osd_ids: list of OSD IDs
        :param replace: marks the OSD as being destroyed. See :ref:`orchestrator-osd-replace`
        :param force: Forces the OSD removal process without waiting for the data to be drained first.
        :param zap: Zap/Erase all devices associated with the OSDs (DESTROYS DATA)
        :param no_destroy: Do not destroy associated VGs/LVs with the OSD.


        .. note:: this can only remove OSDs that were successfully
            created (i.e. got an OSD ID).
        """
        raise NotImplementedError()

    def stop_remove_osds(self, osd_ids: List[str]) -> OrchResult:
        """
        TODO
        """
        raise NotImplementedError()

    def remove_osds_status(self) -> OrchResult:
        """
        Returns a status of the ongoing OSD removal operations.
        """
        raise NotImplementedError()

    def blink_device_light(self, ident_fault: str, on: bool, locations: List['DeviceLightLoc']) -> OrchResult[List[str]]:
        """
        Instructs the orchestrator to enable or disable either the ident or the fault LED.

        :param ident_fault: either ``"ident"`` or ``"fault"``
        :param on: ``True`` = on.
        :param locations: See :class:`orchestrator.DeviceLightLoc`
        """
        raise NotImplementedError()

    def zap_device(self, host: str, path: str) -> OrchResult[str]:
        """Zap/Erase a device (DESTROYS DATA)"""
        raise NotImplementedError()

    def add_daemon(self, spec: ServiceSpec) -> OrchResult[List[str]]:
        """Create daemons daemon(s) for unmanaged services"""
        raise NotImplementedError()

    def apply_mon(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update mon cluster"""
        raise NotImplementedError()

    def apply_mgr(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update mgr cluster"""
        raise NotImplementedError()

    def apply_mds(self, spec: MDSSpec) -> OrchResult[str]:
        """Update MDS cluster"""
        raise NotImplementedError()

    def apply_rgw(self, spec: RGWSpec) -> OrchResult[str]:
        """Update RGW cluster"""
        raise NotImplementedError()

    def apply_ingress(self, spec: IngressSpec) -> OrchResult[str]:
        """Update ingress daemons"""
        raise NotImplementedError()

    def apply_rbd_mirror(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update rbd-mirror cluster"""
        raise NotImplementedError()

    def apply_nfs(self, spec: NFSServiceSpec) -> OrchResult[str]:
        """Update NFS cluster"""
        raise NotImplementedError()

    def apply_iscsi(self, spec: IscsiServiceSpec) -> OrchResult[str]:
        """Update iscsi cluster"""
        raise NotImplementedError()

    def apply_nvmeof(self, spec: NvmeofServiceSpec) -> OrchResult[str]:
        """Update nvmeof cluster"""
        raise NotImplementedError()

    def apply_prometheus(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update prometheus cluster"""
        raise NotImplementedError()

    def get_prometheus_access_info(self) -> OrchResult[Dict[str, str]]:
        """get prometheus access information"""
        raise NotImplementedError()

    def set_alertmanager_access_info(self, user: str, password: str) -> OrchResult[str]:
        """set alertmanager access information"""
        raise NotImplementedError()

    def get_prometheus_cert(self, url: str) -> OrchResult[str]:
        """set prometheus target for multi-cluster"""
        raise NotImplementedError()

    def set_prometheus_access_info(self, user: str, password: str) -> OrchResult[str]:
        """set prometheus access information"""
        raise NotImplementedError()

    def set_prometheus_target(self, url: str) -> OrchResult[str]:
        """set prometheus target for multi-cluster"""
        raise NotImplementedError()

    def remove_prometheus_target(self, url: str) -> OrchResult[str]:
        """remove prometheus target for multi-cluster"""
        raise NotImplementedError()

    def get_alertmanager_access_info(self) -> OrchResult[Dict[str, str]]:
        """get alertmanager access information"""
        raise NotImplementedError()

    def apply_node_exporter(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a Node-Exporter daemon(s)"""
        raise NotImplementedError()

    def apply_ceph_exporter(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a ceph exporter daemon(s)"""
        raise NotImplementedError()

    def apply_loki(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a Loki daemon(s)"""
        raise NotImplementedError()

    def apply_promtail(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a Promtail daemon(s)"""
        raise NotImplementedError()

    def apply_crash(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a crash daemon(s)"""
        raise NotImplementedError()

    def apply_grafana(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update existing a grafana service"""
        raise NotImplementedError()

    def apply_alertmanager(self, spec: ServiceSpec) -> OrchResult[str]:
        """Update an existing AlertManager daemon(s)"""
        raise NotImplementedError()

    def apply_snmp_gateway(self, spec: SNMPGatewaySpec) -> OrchResult[str]:
        """Update an existing snmp gateway service"""
        raise NotImplementedError()

    def apply_tuned_profiles(self, specs: List[TunedProfileSpec], no_overwrite: bool) -> OrchResult[str]:
        """Add or update an existing tuned profile"""
        raise NotImplementedError()

    def rm_tuned_profile(self, profile_name: str) -> OrchResult[str]:
        """Remove a tuned profile"""
        raise NotImplementedError()

    def tuned_profile_ls(self) -> OrchResult[List[TunedProfileSpec]]:
        """See current tuned profiles"""
        raise NotImplementedError()

    def tuned_profile_add_setting(self, profile_name: str, setting: str, value: str) -> OrchResult[str]:
        """Change/Add a specific setting for a tuned profile"""
        raise NotImplementedError()

    def tuned_profile_rm_setting(self, profile_name: str, setting: str) -> OrchResult[str]:
        """Remove a specific setting for a tuned profile"""
        raise NotImplementedError()

    def upgrade_check(self, image: Optional[str], version: Optional[str]) -> OrchResult[str]:
        raise NotImplementedError()

    def upgrade_ls(self, image: Optional[str], tags: bool, show_all_versions: Optional[bool] = False) -> OrchResult[Dict[Any, Any]]:
        raise NotImplementedError()

    def upgrade_start(self, image: Optional[str], version: Optional[str], daemon_types: Optional[List[str]],
                      hosts: Optional[str], services: Optional[List[str]], limit: Optional[int]) -> OrchResult[str]:
        raise NotImplementedError()

    def upgrade_pause(self) -> OrchResult[str]:
        raise NotImplementedError()

    def upgrade_resume(self) -> OrchResult[str]:
        raise NotImplementedError()

    def upgrade_stop(self) -> OrchResult[str]:
        raise NotImplementedError()

    def upgrade_status(self) -> OrchResult['UpgradeStatusSpec']:
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_available(self) -> OrchResult:
        """
        Report on what versions are available to upgrade to

        :return: List of strings
        """
        raise NotImplementedError()


GenericSpec = Union[ServiceSpec, HostSpec]


def json_to_generic_spec(spec: dict) -> GenericSpec:
    if 'service_type' in spec and spec['service_type'] == 'host':
        return HostSpec.from_json(spec)
    else:
        return ServiceSpec.from_json(spec)


def daemon_type_to_service(dtype: str) -> str:
    mapping = {
        'mon': 'mon',
        'mgr': 'mgr',
        'mds': 'mds',
        'rgw': 'rgw',
        'osd': 'osd',
        'haproxy': 'ingress',
        'keepalived': 'ingress',
        'iscsi': 'iscsi',
        'nvmeof': 'nvmeof',
        'rbd-mirror': 'rbd-mirror',
        'cephfs-mirror': 'cephfs-mirror',
        'nfs': 'nfs',
        'grafana': 'grafana',
        'alertmanager': 'alertmanager',
        'prometheus': 'prometheus',
        'node-exporter': 'node-exporter',
        'ceph-exporter': 'ceph-exporter',
        'loki': 'loki',
        'promtail': 'promtail',
        'crash': 'crash',
        'crashcollector': 'crash',  # Specific Rook Daemon
        'container': 'container',
        'agent': 'agent',
        'node-proxy': 'node-proxy',
        'snmp-gateway': 'snmp-gateway',
        'elasticsearch': 'elasticsearch',
        'jaeger-agent': 'jaeger-agent',
        'jaeger-collector': 'jaeger-collector',
        'jaeger-query': 'jaeger-query'
    }
    return mapping[dtype]


def service_to_daemon_types(stype: str) -> List[str]:
    mapping = {
        'mon': ['mon'],
        'mgr': ['mgr'],
        'mds': ['mds'],
        'rgw': ['rgw'],
        'osd': ['osd'],
        'ingress': ['haproxy', 'keepalived'],
        'iscsi': ['iscsi'],
        'nvmeof': ['nvmeof'],
        'rbd-mirror': ['rbd-mirror'],
        'cephfs-mirror': ['cephfs-mirror'],
        'nfs': ['nfs'],
        'grafana': ['grafana'],
        'alertmanager': ['alertmanager'],
        'prometheus': ['prometheus'],
        'loki': ['loki'],
        'promtail': ['promtail'],
        'node-exporter': ['node-exporter'],
        'ceph-exporter': ['ceph-exporter'],
        'crash': ['crash'],
        'container': ['container'],
        'agent': ['agent'],
        'node-proxy': ['node-proxy'],
        'snmp-gateway': ['snmp-gateway'],
        'elasticsearch': ['elasticsearch'],
        'jaeger-agent': ['jaeger-agent'],
        'jaeger-collector': ['jaeger-collector'],
        'jaeger-query': ['jaeger-query'],
        'jaeger-tracing': ['elasticsearch', 'jaeger-query', 'jaeger-collector', 'jaeger-agent']
    }
    return mapping[stype]


KNOWN_DAEMON_TYPES: List[str] = list(
    sum((service_to_daemon_types(t) for t in ServiceSpec.KNOWN_SERVICE_TYPES), []))


class UpgradeStatusSpec(object):
    # Orchestrator's report on what's going on with any ongoing upgrade
    def __init__(self) -> None:
        self.in_progress = False  # Is an upgrade underway?
        self.target_image: Optional[str] = None
        self.services_complete: List[str] = []  # Which daemon types are fully updated?
        self.which: str = '<unknown>'  # for if user specified daemon types, services or hosts
        self.progress: Optional[str] = None  # How many of the daemons have we upgraded
        self.message = ""  # Freeform description
        self.is_paused: bool = False  # Is the upgrade paused?

    def to_json(self) -> dict:
        return {
            'in_progress': self.in_progress,
            'target_image': self.target_image,
            'which': self.which,
            'services_complete': self.services_complete,
            'progress': self.progress,
            'message': self.message,
            'is_paused': self.is_paused,
        }


def handle_type_error(method: FuncT) -> FuncT:
    @wraps(method)
    def inner(cls: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(cls, *args, **kwargs)
        except TypeError as e:
            error_msg = '{}: {}'.format(cls.__name__, e)
        raise OrchestratorValidationError(error_msg)
    return cast(FuncT, inner)


class DaemonDescriptionStatus(enum.IntEnum):
    unknown = -2
    error = -1
    stopped = 0
    running = 1
    starting = 2  #: Daemon is deployed, but not yet running

    @staticmethod
    def to_str(status: Optional['DaemonDescriptionStatus']) -> str:
        if status is None:
            status = DaemonDescriptionStatus.unknown
        return {
            DaemonDescriptionStatus.unknown: 'unknown',
            DaemonDescriptionStatus.error: 'error',
            DaemonDescriptionStatus.stopped: 'stopped',
            DaemonDescriptionStatus.running: 'running',
            DaemonDescriptionStatus.starting: 'starting',
        }.get(status, '<unknown>')


class DaemonDescription(object):
    """
    For responding to queries about the status of a particular daemon,
    stateful or stateless.

    This is not about health or performance monitoring of daemons: it's
    about letting the orchestrator tell Ceph whether and where a
    daemon is scheduled in the cluster.  When an orchestrator tells
    Ceph "it's running on host123", that's not a promise that the process
    is literally up this second, it's a description of where the orchestrator
    has decided the daemon should run.
    """

    def __init__(self,
                 daemon_type: Optional[str] = None,
                 daemon_id: Optional[str] = None,
                 hostname: Optional[str] = None,
                 container_id: Optional[str] = None,
                 container_image_id: Optional[str] = None,
                 container_image_name: Optional[str] = None,
                 container_image_digests: Optional[List[str]] = None,
                 version: Optional[str] = None,
                 status: Optional[DaemonDescriptionStatus] = None,
                 status_desc: Optional[str] = None,
                 last_refresh: Optional[datetime.datetime] = None,
                 created: Optional[datetime.datetime] = None,
                 started: Optional[datetime.datetime] = None,
                 last_configured: Optional[datetime.datetime] = None,
                 osdspec_affinity: Optional[str] = None,
                 last_deployed: Optional[datetime.datetime] = None,
                 events: Optional[List['OrchestratorEvent']] = None,
                 is_active: bool = False,
                 memory_usage: Optional[int] = None,
                 memory_request: Optional[int] = None,
                 memory_limit: Optional[int] = None,
                 cpu_percentage: Optional[str] = None,
                 service_name: Optional[str] = None,
                 ports: Optional[List[int]] = None,
                 ip: Optional[str] = None,
                 deployed_by: Optional[List[str]] = None,
                 rank: Optional[int] = None,
                 rank_generation: Optional[int] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 ) -> None:

        #: Host is at the same granularity as InventoryHost
        self.hostname: Optional[str] = hostname

        # Not everyone runs in containers, but enough people do to
        # justify having the container_id (runtime id) and container_image
        # (image name)
        self.container_id = container_id                  # runtime id
        self.container_image_id = container_image_id      # image id locally
        self.container_image_name = container_image_name  # image friendly name
        self.container_image_digests = container_image_digests  # reg hashes

        #: The type of service (osd, mon, mgr, etc.)
        self.daemon_type = daemon_type

        #: The orchestrator will have picked some names for daemons,
        #: typically either based on hostnames or on pod names.
        #: This is the <foo> in mds.<foo>, the ID that will appear
        #: in the FSMap/ServiceMap.
        self.daemon_id: Optional[str] = daemon_id
        self.daemon_name = self.name()

        #: Some daemon types have a numeric rank assigned
        self.rank: Optional[int] = rank
        self.rank_generation: Optional[int] = rank_generation

        self._service_name: Optional[str] = service_name

        #: Service version that was deployed
        self.version = version

        # Service status: -2 unknown, -1 error, 0 stopped, 1 running, 2 starting
        self._status = status

        #: Service status description when status == error.
        self.status_desc = status_desc

        #: datetime when this info was last refreshed
        self.last_refresh: Optional[datetime.datetime] = last_refresh

        self.created: Optional[datetime.datetime] = created
        self.started: Optional[datetime.datetime] = started
        self.last_configured: Optional[datetime.datetime] = last_configured
        self.last_deployed: Optional[datetime.datetime] = last_deployed

        #: Affinity to a certain OSDSpec
        self.osdspec_affinity: Optional[str] = osdspec_affinity

        self.events: List[OrchestratorEvent] = events or []

        self.memory_usage: Optional[int] = memory_usage
        self.memory_request: Optional[int] = memory_request
        self.memory_limit: Optional[int] = memory_limit

        self.cpu_percentage: Optional[str] = cpu_percentage

        self.ports: Optional[List[int]] = ports
        self.ip: Optional[str] = ip

        self.deployed_by = deployed_by

        self.is_active = is_active

        self.extra_container_args: Optional[ArgumentList] = None
        self.extra_entrypoint_args: Optional[ArgumentList] = None
        if extra_container_args:
            self.extra_container_args = ArgumentSpec.from_general_args(
                extra_container_args)
        if extra_entrypoint_args:
            self.extra_entrypoint_args = ArgumentSpec.from_general_args(
                extra_entrypoint_args)

    def __setattr__(self, name: str, value: Any) -> None:
        if value is not None and name in ('extra_container_args', 'extra_entrypoint_args'):
            for v in value:
                tname = str(type(v))
                if 'ArgumentSpec' not in tname:
                    raise TypeError(f"{name} is not all ArgumentSpec values: {v!r}(is {type(v)} in {value!r}")

        super().__setattr__(name, value)

    @property
    def status(self) -> Optional[DaemonDescriptionStatus]:
        return self._status

    @status.setter
    def status(self, new: DaemonDescriptionStatus) -> None:
        self._status = new
        self.status_desc = DaemonDescriptionStatus.to_str(new)

    def get_port_summary(self) -> str:
        if not self.ports:
            return ''
        return f"{self.ip or '*'}:{','.join(map(str, self.ports or []))}"

    def name(self) -> str:
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def matches_service(self, service_name: Optional[str]) -> bool:
        assert self.daemon_id is not None
        assert self.daemon_type is not None
        if service_name:
            return (daemon_type_to_service(self.daemon_type) + '.' + self.daemon_id).startswith(service_name + '.')
        return False

    def matches_digests(self, digests: Optional[List[str]]) -> bool:
        # the DaemonDescription class maintains a list of container digests
        # for the container image last reported as being used for the daemons.
        # This function checks if any of those digests match any of the digests
        # in the list of digests provided as an arg to this function
        if not digests or not self.container_image_digests:
            return False
        return any(d in digests for d in self.container_image_digests)

    def matches_image_name(self, image_name: Optional[str]) -> bool:
        # the DaemonDescription class has an attribute that tracks the image
        # name of the container image last reported as being used by the daemon.
        # This function compares if the image name provided as an arg matches
        # the image name in said attribute
        if not image_name or not self.container_image_name:
            return False
        return image_name == self.container_image_name

    def service_id(self) -> str:
        assert self.daemon_id is not None
        assert self.daemon_type is not None

        if self._service_name:
            if '.' in self._service_name:
                return self._service_name.split('.', 1)[1]
            else:
                return ''

        if self.daemon_type == 'osd':
            if self.osdspec_affinity and self.osdspec_affinity != 'None':
                return self.osdspec_affinity
            return ''

        def _match() -> str:
            assert self.daemon_id is not None
            err = OrchestratorError("DaemonDescription: Cannot calculate service_id: "
                                    f"daemon_id='{self.daemon_id}' hostname='{self.hostname}'")

            if not self.hostname:
                # TODO: can a DaemonDescription exist without a hostname?
                raise err

            # use the bare hostname, not the FQDN.
            host = self.hostname.split('.')[0]

            if host == self.daemon_id:
                # daemon_id == "host"
                return self.daemon_id

            elif host in self.daemon_id:
                # daemon_id == "service_id.host"
                # daemon_id == "service_id.host.random"
                pre, post = self.daemon_id.rsplit(host, 1)
                if not pre.endswith('.'):
                    # '.' sep missing at front of host
                    raise err
                elif post and not post.startswith('.'):
                    # '.' sep missing at end of host
                    raise err
                return pre[:-1]

            # daemon_id == "service_id.random"
            if self.daemon_type == 'rgw':
                v = self.daemon_id.split('.')
                if len(v) in [3, 4]:
                    return '.'.join(v[0:2])

            if self.daemon_type == 'iscsi':
                v = self.daemon_id.split('.')
                return '.'.join(v[0:-1])

            # daemon_id == "service_id"
            return self.daemon_id

        if daemon_type_to_service(self.daemon_type) in ServiceSpec.REQUIRES_SERVICE_ID:
            return _match()

        return self.daemon_id

    def service_name(self) -> str:
        if self._service_name:
            return self._service_name
        assert self.daemon_type is not None
        if daemon_type_to_service(self.daemon_type) in ServiceSpec.REQUIRES_SERVICE_ID:
            return f'{daemon_type_to_service(self.daemon_type)}.{self.service_id()}'
        return daemon_type_to_service(self.daemon_type)

    def __repr__(self) -> str:
        return "<DaemonDescription>({type}.{id})".format(type=self.daemon_type,
                                                         id=self.daemon_id)

    def __str__(self) -> str:
        return f"{self.name()} in status {self.status_desc} on {self.hostname}"

    def to_json(self) -> dict:
        out: Dict[str, Any] = OrderedDict()
        out['daemon_type'] = self.daemon_type
        out['daemon_id'] = self.daemon_id
        out['service_name'] = self._service_name
        out['daemon_name'] = self.name()
        out['hostname'] = self.hostname
        out['container_id'] = self.container_id
        out['container_image_id'] = self.container_image_id
        out['container_image_name'] = self.container_image_name
        out['container_image_digests'] = self.container_image_digests
        out['memory_usage'] = self.memory_usage
        out['memory_request'] = self.memory_request
        out['memory_limit'] = self.memory_limit
        out['cpu_percentage'] = self.cpu_percentage
        out['version'] = self.version
        out['status'] = self.status.value if self.status is not None else None
        out['status_desc'] = self.status_desc
        if self.daemon_type == 'osd':
            out['osdspec_affinity'] = self.osdspec_affinity
        out['is_active'] = self.is_active
        out['ports'] = self.ports
        out['ip'] = self.ip
        out['rank'] = self.rank
        out['rank_generation'] = self.rank_generation

        for k in ['last_refresh', 'created', 'started', 'last_deployed',
                  'last_configured']:
            if getattr(self, k):
                out[k] = datetime_to_str(getattr(self, k))

        if self.events:
            out['events'] = [e.to_json() for e in self.events]

        empty = [k for k, v in out.items() if v is None]
        for e in empty:
            del out[e]
        return out

    def to_dict(self) -> dict:
        out: Dict[str, Any] = OrderedDict()
        out['daemon_type'] = self.daemon_type
        out['daemon_id'] = self.daemon_id
        out['daemon_name'] = self.name()
        out['hostname'] = self.hostname
        out['container_id'] = self.container_id
        out['container_image_id'] = self.container_image_id
        out['container_image_name'] = self.container_image_name
        out['container_image_digests'] = self.container_image_digests
        out['memory_usage'] = self.memory_usage
        out['memory_request'] = self.memory_request
        out['memory_limit'] = self.memory_limit
        out['cpu_percentage'] = self.cpu_percentage
        out['version'] = self.version
        out['status'] = self.status.value if self.status is not None else None
        out['status_desc'] = self.status_desc
        if self.daemon_type == 'osd':
            out['osdspec_affinity'] = self.osdspec_affinity
        out['is_active'] = self.is_active
        out['ports'] = self.ports
        out['ip'] = self.ip

        for k in ['last_refresh', 'created', 'started', 'last_deployed',
                  'last_configured']:
            if getattr(self, k):
                out[k] = datetime_to_str(getattr(self, k))

        if self.events:
            out['events'] = [e.to_dict() for e in self.events]

        empty = [k for k, v in out.items() if v is None]
        for e in empty:
            del out[e]
        return out

    @classmethod
    @handle_type_error
    def from_json(cls, data: dict) -> 'DaemonDescription':
        c = data.copy()
        event_strs = c.pop('events', [])
        for k in ['last_refresh', 'created', 'started', 'last_deployed',
                  'last_configured']:
            if k in c:
                c[k] = str_to_datetime(c[k])
        events = [OrchestratorEvent.from_json(e) for e in event_strs]
        status_int = c.pop('status', None)
        if 'daemon_name' in c:
            del c['daemon_name']
        if 'service_name' in c and c['service_name'].startswith('osd.'):
            # if the service_name is a osd.NNN (numeric osd id) then
            # ignore it -- it is not a valid service_name and
            # (presumably) came from an older version of cephadm.
            try:
                int(c['service_name'][4:])
                del c['service_name']
            except ValueError:
                pass
        status = DaemonDescriptionStatus(status_int) if status_int is not None else None
        return cls(events=events, status=status, **c)

    def __copy__(self) -> 'DaemonDescription':
        # feel free to change this:
        return DaemonDescription.from_json(self.to_json())

    @staticmethod
    def yaml_representer(dumper: 'yaml.Dumper', data: 'DaemonDescription') -> yaml.Node:
        return dumper.represent_dict(cast(Mapping, data.to_json().items()))


yaml.add_representer(DaemonDescription, DaemonDescription.yaml_representer)


class ServiceDescription(object):
    """
    For responding to queries about the status of a particular service,
    stateful or stateless.

    This is not about health or performance monitoring of services: it's
    about letting the orchestrator tell Ceph whether and where a
    service is scheduled in the cluster.  When an orchestrator tells
    Ceph "it's running on host123", that's not a promise that the process
    is literally up this second, it's a description of where the orchestrator
    has decided the service should run.
    """

    def __init__(self,
                 spec: ServiceSpec,
                 container_image_id: Optional[str] = None,
                 container_image_name: Optional[str] = None,
                 service_url: Optional[str] = None,
                 last_refresh: Optional[datetime.datetime] = None,
                 created: Optional[datetime.datetime] = None,
                 deleted: Optional[datetime.datetime] = None,
                 size: int = 0,
                 running: int = 0,
                 events: Optional[List['OrchestratorEvent']] = None,
                 virtual_ip: Optional[str] = None,
                 ports: List[int] = []) -> None:
        # Not everyone runs in containers, but enough people do to
        # justify having the container_image_id (image hash) and container_image
        # (image name)
        self.container_image_id = container_image_id      # image hash
        self.container_image_name = container_image_name  # image friendly name

        # If the service exposes REST-like API, this attribute should hold
        # the URL.
        self.service_url = service_url

        # Number of daemons
        self.size = size

        # Number of daemons up
        self.running = running

        # datetime when this info was last refreshed
        self.last_refresh: Optional[datetime.datetime] = last_refresh
        self.created: Optional[datetime.datetime] = created
        self.deleted: Optional[datetime.datetime] = deleted

        self.spec: ServiceSpec = spec

        self.events: List[OrchestratorEvent] = events or []

        self.virtual_ip = virtual_ip
        self.ports = ports

    def service_type(self) -> str:
        return self.spec.service_type

    def __repr__(self) -> str:
        return f"<ServiceDescription of {self.spec.one_line_str()}>"

    def get_port_summary(self) -> str:
        if not self.ports:
            return ''
        ports = sorted([int(x) for x in self.ports])
        return f"{(self.virtual_ip or '?').split('/')[0]}:{','.join(map(str, ports or []))}"

    def to_json(self) -> OrderedDict:
        out = self.spec.to_json()
        status = {
            'container_image_id': self.container_image_id,
            'container_image_name': self.container_image_name,
            'service_url': self.service_url,
            'size': self.size,
            'running': self.running,
            'last_refresh': self.last_refresh,
            'created': self.created,
            'virtual_ip': self.virtual_ip,
            'ports': self.ports if self.ports else None,
        }
        for k in ['last_refresh', 'created']:
            if getattr(self, k):
                status[k] = datetime_to_str(getattr(self, k))
        status = {k: v for (k, v) in status.items() if v is not None}
        out['status'] = status
        if self.events:
            out['events'] = [e.to_json() for e in self.events]
        return out

    def to_dict(self) -> OrderedDict:
        out = self.spec.to_json()
        status = {
            'container_image_id': self.container_image_id,
            'container_image_name': self.container_image_name,
            'service_url': self.service_url,
            'size': self.size,
            'running': self.running,
            'last_refresh': self.last_refresh,
            'created': self.created,
            'virtual_ip': self.virtual_ip,
            'ports': self.ports if self.ports else None,
        }
        for k in ['last_refresh', 'created']:
            if getattr(self, k):
                status[k] = datetime_to_str(getattr(self, k))
        status = {k: v for (k, v) in status.items() if v is not None}
        out['status'] = status
        if self.events:
            out['events'] = [e.to_dict() for e in self.events]
        return out

    @classmethod
    @handle_type_error
    def from_json(cls, data: dict) -> 'ServiceDescription':
        c = data.copy()
        status = c.pop('status', {})
        event_strs = c.pop('events', [])
        spec = ServiceSpec.from_json(c)

        c_status = status.copy()
        for k in ['last_refresh', 'created']:
            if k in c_status:
                c_status[k] = str_to_datetime(c_status[k])
        events = [OrchestratorEvent.from_json(e) for e in event_strs]
        return cls(spec=spec, events=events, **c_status)

    @staticmethod
    def yaml_representer(dumper: 'yaml.Dumper', data: 'ServiceDescription') -> yaml.Node:
        return dumper.represent_dict(cast(Mapping, data.to_json().items()))


yaml.add_representer(ServiceDescription, ServiceDescription.yaml_representer)


class InventoryFilter(object):
    """
    When fetching inventory, use this filter to avoid unnecessarily
    scanning the whole estate.

    Typical use:

      filter by host when presenting UI workflow for configuring
      a particular server.
      filter by label when not all of estate is Ceph servers,
      and we want to only learn about the Ceph servers.
      filter by label when we are interested particularly
      in e.g. OSD servers.
    """

    def __init__(self, labels: Optional[List[str]] = None, hosts: Optional[List[str]] = None) -> None:

        #: Optional: get info about hosts matching labels
        self.labels = labels

        #: Optional: get info about certain named hosts only
        self.hosts = hosts


class InventoryHost(object):
    """
    When fetching inventory, all Devices are groups inside of an
    InventoryHost.
    """

    def __init__(self, name: str, devices: Optional[inventory.Devices] = None, labels: Optional[List[str]] = None, addr: Optional[str] = None) -> None:
        if devices is None:
            devices = inventory.Devices([])
        if labels is None:
            labels = []
        assert isinstance(devices, inventory.Devices)

        self.name = name  # unique within cluster.  For example a hostname.
        self.addr = addr or name
        self.devices = devices
        self.labels = labels

    def to_json(self) -> dict:
        return {
            'name': self.name,
            'addr': self.addr,
            'devices': self.devices.to_json(),
            'labels': self.labels,
        }

    @classmethod
    def from_json(cls, data: dict) -> 'InventoryHost':
        try:
            _data = copy.deepcopy(data)
            name = _data.pop('name')
            addr = _data.pop('addr', None) or name
            devices = inventory.Devices.from_json(_data.pop('devices'))
            labels = _data.pop('labels', list())
            if _data:
                error_msg = 'Unknown key(s) in Inventory: {}'.format(','.join(_data.keys()))
                raise OrchestratorValidationError(error_msg)
            return cls(name, devices, labels, addr)
        except KeyError as e:
            error_msg = '{} is required for {}'.format(e, cls.__name__)
            raise OrchestratorValidationError(error_msg)
        except TypeError as e:
            raise OrchestratorValidationError('Failed to read inventory: {}'.format(e))

    @classmethod
    def from_nested_items(cls, hosts: List[dict]) -> List['InventoryHost']:
        devs = inventory.Devices.from_json
        return [cls(item[0], devs(item[1].data)) for item in hosts]

    def __repr__(self) -> str:
        return "<InventoryHost>({name})".format(name=self.name)

    @staticmethod
    def get_host_names(hosts: List['InventoryHost']) -> List[str]:
        return [host.name for host in hosts]

    def __eq__(self, other: Any) -> bool:
        return self.name == other.name and self.devices == other.devices


class DeviceLightLoc(namedtuple('DeviceLightLoc', ['host', 'dev', 'path'])):
    """
    Describes a specific device on a specific host. Used for enabling or disabling LEDs
    on devices.

    hostname as in :func:`orchestrator.Orchestrator.get_hosts`

    device_id: e.g. ``ABC1234DEF567-1R1234_ABC8DE0Q``.
       See ``ceph osd metadata | jq '.[].device_ids'``
    """
    __slots__ = ()


class OrchestratorEvent:
    """
    Similar to K8s Events.

    Some form of "important" log message attached to something.
    """
    INFO = 'INFO'
    ERROR = 'ERROR'
    regex_v1 = re.compile(r'^([^ ]+) ([^:]+):([^ ]+) \[([^\]]+)\] "((?:.|\n)*)"$', re.MULTILINE)

    def __init__(self, created: Union[str, datetime.datetime], kind: str,
                 subject: str, level: str, message: str) -> None:
        if isinstance(created, str):
            created = str_to_datetime(created)
        self.created: datetime.datetime = created

        assert kind in "service daemon".split()
        self.kind: str = kind

        # service name, or daemon danem or something
        self.subject: str = subject

        # Events are not meant for debugging. debugs should end in the log.
        assert level in "INFO ERROR".split()
        self.level = level

        self.message: str = message

    __slots__ = ('created', 'kind', 'subject', 'level', 'message')

    def kind_subject(self) -> str:
        return f'{self.kind}:{self.subject}'

    def to_json(self) -> str:
        # Make a long list of events readable.
        created = datetime_to_str(self.created)
        return f'{created} {self.kind_subject()} [{self.level}] "{self.message}"'

    def to_dict(self) -> dict:
        # Convert events data to dict.
        return {
            'created': datetime_to_str(self.created),
            'subject': self.kind_subject(),
            'level': self.level,
            'message': self.message
        }

    @classmethod
    @handle_type_error
    def from_json(cls, data: str) -> "OrchestratorEvent":
        """
        >>> OrchestratorEvent.from_json('''2020-06-10T10:20:25.691255 daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on host 'ubuntu'"''').to_json()
        '2020-06-10T10:20:25.691255Z daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on host \\'ubuntu\\'"'

        :param data:
        :return:
        """
        match = cls.regex_v1.match(data)
        if match:
            return cls(*match.groups())
        raise ValueError(f'Unable to match: "{data}"')

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, OrchestratorEvent):
            return False

        return self.created == other.created and self.kind == other.kind \
            and self.subject == other.subject and self.message == other.message

    def __repr__(self) -> str:
        return f'OrchestratorEvent.from_json({self.to_json()!r})'


def _mk_orch_methods(cls: Any) -> Any:
    # Needs to be defined outside of for.
    # Otherwise meth is always bound to last key
    def shim(method_name: str) -> Callable:
        def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
            completion = self._oremote(method_name, args, kwargs)
            return completion
        return inner

    for name, method in Orchestrator.__dict__.items():
        if not name.startswith('_') and name not in ['is_orchestrator_module']:
            remote_call = update_wrapper(shim(name), method)
            setattr(cls, name, remote_call)
    return cls


@_mk_orch_methods
class OrchestratorClientMixin(Orchestrator):
    """
    A module that inherents from `OrchestratorClientMixin` can directly call
    all :class:`Orchestrator` methods without manually calling remote.

    Every interface method from ``Orchestrator`` is converted into a stub method that internally
    calls :func:`OrchestratorClientMixin._oremote`

    >>> class MyModule(OrchestratorClientMixin):
    ...    def func(self):
    ...        completion = self.add_host('somehost')  # calls `_oremote()`
    ...        self.log.debug(completion.result)

    .. note:: Orchestrator implementations should not inherit from `OrchestratorClientMixin`.
        Reason is, that OrchestratorClientMixin magically redirects all methods to the
        "real" implementation of the orchestrator.


    >>> import mgr_module
    >>> #doctest: +SKIP
    ... class MyImplementation(mgr_module.MgrModule, Orchestrator):
    ...     def __init__(self, ...):
    ...         self.orch_client = OrchestratorClientMixin()
    ...         self.orch_client.set_mgr(self.mgr))
    """

    def set_mgr(self, mgr: MgrModule) -> None:
        """
        Useable in the Dashboard that uses a global ``mgr``
        """

        self.__mgr = mgr  # Make sure we're not overwriting any other `mgr` properties

    def __get_mgr(self) -> Any:
        try:
            return self.__mgr
        except AttributeError:
            return self

    def _oremote(self, meth: Any, args: Any, kwargs: Any) -> Any:
        """
        Helper for invoking `remote` on whichever orchestrator is enabled

        :raises RuntimeError: If the remote method failed.
        :raises OrchestratorError: orchestrator failed to perform
        :raises ImportError: no `orchestrator` module or backend not found.
        """
        mgr = self.__get_mgr()

        try:
            o = mgr._select_orchestrator()
        except AttributeError:
            o = mgr.remote('orchestrator', '_select_orchestrator')

        if o is None:
            raise NoOrchestrator()

        mgr.log.debug("_oremote {} -> {}.{}(*{}, **{})".format(mgr.module_name, o, meth, args, kwargs))
        try:
            return mgr.remote(o, meth, *args, **kwargs)
        except Exception as e:
            if meth == 'get_feature_set':
                raise  # self.get_feature_set() calls self._oremote()
            f_set = self.get_feature_set()
            if meth not in f_set or not f_set[meth]['available']:
                raise NotImplementedError(f'{o} does not implement {meth}') from e
            raise
