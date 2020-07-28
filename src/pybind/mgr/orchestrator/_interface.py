
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""

import copy
import datetime
import errno
import logging
import pickle
import re
import time
import uuid

from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from functools import wraps

import yaml

from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec, \
    ServiceSpecValidationError, IscsiServiceSpec
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.hostspec import HostSpec

from mgr_module import MgrModule, CLICommand, HandleCommandResult

try:
    from typing import TypeVar, Generic, List, Optional, Union, Tuple, Iterator, Callable, Any, \
    Type, Sequence, Dict, cast
except ImportError:
    pass

logger = logging.getLogger(__name__)

DATEFMT = '%Y-%m-%dT%H:%M:%S.%f'

T = TypeVar('T')

class OrchestratorError(Exception):
    """
    General orchestrator specific error.

    Used for deployment, configuration or user errors.

    It's not intended for programming errors or orchestrator internal errors.
    """
    def __init__(self, msg, event_kind_subject: Optional[Tuple[str, str]]=None):
        super(Exception, self).__init__(msg)
        # See OrchestratorEvent.subject
        self.event_subject = event_kind_subject


class NoOrchestrator(OrchestratorError):
    """
    No orchestrator in configured.
    """
    def __init__(self, msg="No orchestrator configured (try `ceph orch set backend`)"):
        super(NoOrchestrator, self).__init__(msg)


class OrchestratorValidationError(OrchestratorError):
    """
    Raised when an orchestrator doesn't support a specific feature.
    """


@contextmanager
def set_exception_subject(kind, subject, overwrite=False):
    try:
        yield
    except OrchestratorError as e:
        if overwrite or hasattr(e, 'event_subject'):
            e.event_subject = (kind, subject)
        raise


def handle_exception(prefix, cmd_args, desc, perm, func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (OrchestratorError, ImportError, ServiceSpecValidationError) as e:
            # Do not print Traceback for expected errors.
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
        except NotImplementedError:
            msg = 'This Orchestrator does not support `{}`'.format(prefix)
            return HandleCommandResult(-errno.ENOENT, stderr=msg)

    # misuse partial to copy `wrapper`
    wrapper_copy = lambda *l_args, **l_kwargs: wrapper(*l_args, **l_kwargs)
    wrapper_copy._prefix = prefix  # type: ignore
    wrapper_copy._cli_command = CLICommand(prefix, cmd_args, desc, perm)  # type: ignore
    wrapper_copy._cli_command.func = wrapper_copy  # type: ignore

    return wrapper_copy


def _cli_command(perm):
    def inner_cli_command(prefix, cmd_args="", desc=""):
        return lambda func: handle_exception(prefix, cmd_args, desc, perm, func)
    return inner_cli_command


_cli_read_command = _cli_command('r')
_cli_write_command = _cli_command('rw')


class CLICommandMeta(type):
    """
    This is a workaround for the use of a global variable CLICommand.COMMANDS which
    prevents modules from importing any other module.

    We make use of CLICommand, except for the use of the global variable.
    """
    def __init__(cls, name, bases, dct):
        super(CLICommandMeta, cls).__init__(name, bases, dct)
        dispatch = {}  # type: Dict[str, CLICommand]
        for v in dct.values():
            try:
                dispatch[v._prefix] = v._cli_command
            except AttributeError:
                pass

        def handle_command(self, inbuf, cmd):
            if cmd['prefix'] not in dispatch:
                return self.handle_command(inbuf, cmd)

            return dispatch[cmd['prefix']].call(self, cmd, inbuf)

        cls.COMMANDS = [cmd.dump_cmd() for cmd in dispatch.values()]
        cls.handle_command = handle_command


def _no_result():
    return object()


class _Promise(object):
    """
    A completion may need multiple promises to be fulfilled. `_Promise` is one
    step.

    Typically ``Orchestrator`` implementations inherit from this class to
    build their own way of finishing a step to fulfil a future.

    They are not exposed in the orchestrator interface and can be seen as a
    helper to build orchestrator modules.
    """
    INITIALIZED = 1  # We have a parent completion and a next completion
    RUNNING = 2
    FINISHED = 3  # we have a final result

    NO_RESULT = _no_result()  # type: None
    ASYNC_RESULT = object()

    def __init__(self,
                 _first_promise=None,  # type: Optional["_Promise"]
                 value=NO_RESULT,  # type: Optional[Any]
                 on_complete=None,    # type: Optional[Callable]
                 name=None,  # type: Optional[str]
                 ):
        self._on_complete_ = on_complete
        self._name = name
        self._next_promise = None  # type: Optional[_Promise]

        self._state = self.INITIALIZED
        self._exception = None  # type: Optional[Exception]

        # Value of this _Promise. may be an intermediate result.
        self._value = value

        # _Promise is not a continuation monad, as `_result` is of type
        # T instead of (T -> r) -> r. Therefore we need to store the first promise here.
        self._first_promise = _first_promise or self  # type: '_Promise'

    @property
    def _exception(self):
        # type: () -> Optional[Exception]
        return getattr(self, '_exception_', None)

    @_exception.setter
    def _exception(self, e):
        self._exception_ = e
        try:
            self._serialized_exception_ = pickle.dumps(e) if e is not None else None
        except pickle.PicklingError:
            logger.error(f"failed to pickle {e}")
            if isinstance(e, Exception):
                e = Exception(*e.args)
            else:
                e = Exception(str(e))
            # degenerate to a plain Exception
            self._serialized_exception_ = pickle.dumps(e)

    @property
    def _serialized_exception(self):
        # type: () -> Optional[bytes]
        return getattr(self, '_serialized_exception_', None)



    @property
    def _on_complete(self):
        # type: () -> Optional[Callable]
        # https://github.com/python/mypy/issues/4125
        return self._on_complete_

    @_on_complete.setter
    def _on_complete(self, val):
        # type: (Optional[Callable]) -> None
        self._on_complete_ = val


    def __repr__(self):
        name = self._name or getattr(self._on_complete, '__name__', '??') if self._on_complete else 'None'
        val = repr(self._value) if self._value is not self.NO_RESULT else 'NA'
        return '{}(_s={}, val={}, _on_c={}, id={}, name={}, pr={}, _next={})'.format(
            self.__class__, self._state, val, self._on_complete, id(self), name, getattr(next, '_progress_reference', 'NA'), repr(self._next_promise)
        )

    def pretty_print_1(self):
        if self._name:
            name = self._name
        elif self._on_complete is None:
            name = 'lambda x: x'
        elif hasattr(self._on_complete, '__name__'):
            name = getattr(self._on_complete, '__name__')
        else:
            name = self._on_complete.__class__.__name__
        val = repr(self._value) if self._value not in (self.NO_RESULT, self.ASYNC_RESULT) else '...'
        prefix = {
            self.INITIALIZED: '      ',
            self.RUNNING:     '   >>>',
            self.FINISHED:    '(done)'
        }[self._state]
        return '{} {}({}),'.format(prefix, name, val)

    def then(self, on_complete):
        # type: (Any, Callable) -> Any
        """
        Call ``on_complete`` as soon as this promise is finalized.
        """
        assert self._state in (self.INITIALIZED, self.RUNNING)

        if self._next_promise is not None:
            return self._next_promise.then(on_complete)

        if self._on_complete is not None:
            self._set_next_promise(self.__class__(
                _first_promise=self._first_promise,
                on_complete=on_complete
            ))
            return self._next_promise

        else:
            self._on_complete = on_complete
            self._set_next_promise(self.__class__(_first_promise=self._first_promise))
            return self._next_promise

    def _set_next_promise(self, next):
        # type: (_Promise) -> None
        assert self is not next
        assert self._state in (self.INITIALIZED, self.RUNNING)

        self._next_promise = next
        assert self._next_promise is not None
        for p in iter(self._next_promise):
            p._first_promise = self._first_promise

    def _finalize(self, value=NO_RESULT):
        """
        Sets this promise to complete.

        Orchestrators may choose to use this helper function.

        :param value: new value.
        """
        if self._state not in (self.INITIALIZED, self.RUNNING):
            raise ValueError('finalize: {} already finished. {}'.format(repr(self), value))

        self._state = self.RUNNING

        if value is not self.NO_RESULT:
            self._value = value
        assert self._value is not self.NO_RESULT, repr(self)

        if self._on_complete:
            try:
                next_result = self._on_complete(self._value)
            except Exception as e:
                self.fail(e)
                return
        else:
            next_result = self._value

        if isinstance(next_result, _Promise):
            # hack: _Promise is not a continuation monad.
            next_result = next_result._first_promise  # type: ignore
            assert next_result not in self, repr(self._first_promise) + repr(next_result)
            assert self not in next_result
            next_result._append_promise(self._next_promise)
            self._set_next_promise(next_result)
            assert self._next_promise
            if self._next_promise._value is self.NO_RESULT:
                self._next_promise._value = self._value
            self.propagate_to_next()
        elif next_result is not self.ASYNC_RESULT:
            # simple map. simply forward
            if self._next_promise:
                self._next_promise._value = next_result
            else:
                # Hack: next_result is of type U, _value is of type T
                self._value = next_result  # type: ignore
            self.propagate_to_next()
        else:
            # asynchronous promise
            pass


    def propagate_to_next(self):
        self._state = self.FINISHED
        logger.debug('finalized {}'.format(repr(self)))
        if self._next_promise:
            self._next_promise._finalize()

    def fail(self, e):
        # type: (Exception) -> None
        """
        Sets the whole completion to be faild with this exception and end the
        evaluation.
        """
        if self._state == self.FINISHED:
            raise ValueError(
                'Invalid State: called fail, but Completion is already finished: {}'.format(str(e)))
        assert self._state in (self.INITIALIZED, self.RUNNING)
        logger.exception('_Promise failed')
        self._exception = e
        self._value = f'_exception: {e}'
        if self._next_promise:
            self._next_promise.fail(e)
        self._state = self.FINISHED

    def __contains__(self, item):
        return any(item is p for p in iter(self._first_promise))

    def __iter__(self):
        yield self
        elem = self._next_promise
        while elem is not None:
            yield elem
            elem = elem._next_promise

    def _append_promise(self, other):
        if other is not None:
            assert self not in other
            assert other not in self
            self._last_promise()._set_next_promise(other)

    def _last_promise(self):
        # type: () -> _Promise
        return list(iter(self))[-1]


class ProgressReference(object):
    def __init__(self,
                 message,  # type: str
                 mgr,
                 completion=None  # type: Optional[Callable[[], Completion]]
                ):
        """
        ProgressReference can be used within Completions::

            +---------------+      +---------------------------------+
            |               | then |                                 |
            | My Completion | +--> | on_complete=ProgressReference() |
            |               |      |                                 |
            +---------------+      +---------------------------------+

        See :func:`Completion.with_progress` for an easy way to create
        a progress reference

        """
        super(ProgressReference, self).__init__()
        self.progress_id = str(uuid.uuid4())
        self.message = message
        self.mgr = mgr

        #: The completion can already have a result, before the write
        #: operation is effective. progress == 1 means, the services are
        #: created / removed.
        self.completion = completion  # type: Optional[Callable[[], Completion]]

        #: if a orchestrator module can provide a more detailed
        #: progress information, it needs to also call ``progress.update()``.
        self.progress = 0.0

        self._completion_has_result = False
        self.mgr.all_progress_references.append(self)

    def __str__(self):
        """
        ``__str__()`` is used for determining the message for progress events.
        """
        return self.message or super(ProgressReference, self).__str__()

    def __call__(self, arg):
        self._completion_has_result = True
        self.progress = 1.0
        return arg

    @property
    def progress(self):
        return self._progress

    @progress.setter
    def progress(self, progress):
        assert progress <= 1.0
        self._progress = progress
        try:
            if self.effective:
                self.mgr.remote("progress", "complete", self.progress_id)
                self.mgr.all_progress_references = [p for p in self.mgr.all_progress_references if p is not self]
            else:
                self.mgr.remote("progress", "update", self.progress_id, self.message,
                                progress,
                                [("origin", "orchestrator")])
        except ImportError:
            # If the progress module is disabled that's fine,
            # they just won't see the output.
            pass

    @property
    def effective(self):
        return self.progress == 1 and self._completion_has_result

    def update(self):
        def progress_run(progress):
            self.progress = progress
        if self.completion:
            c = self.completion().then(progress_run)
            self.mgr.process([c._first_promise])
        else:
            self.progress = 1

    def fail(self):
        self._completion_has_result = True
        self.progress = 1


class Completion(_Promise, Generic[T]):
    """
    Combines multiple promises into one overall operation.

    Completions are composable by being able to
    call one completion from another completion. I.e. making them re-usable
    using Promises E.g.::

        >>> #doctest: +SKIP
        ... return Orchestrator().get_hosts().then(self._create_osd)

    where ``get_hosts`` returns a Completion of list of hosts and
    ``_create_osd`` takes a list of hosts.

    The concept behind this is to store the computation steps
    explicit and then explicitly evaluate the chain:

        >>> #doctest: +SKIP
        ... p = Completion(on_complete=lambda x: x*2).then(on_complete=lambda x: str(x))
        ... p.finalize(2)
        ... assert p.result = "4"

    or graphically::

        +---------------+      +-----------------+
        |               | then |                 |
        | lambda x: x*x | +--> | lambda x: str(x)|
        |               |      |                 |
        +---------------+      +-----------------+

    """
    def __init__(self,
                 _first_promise=None,  # type: Optional["Completion"]
                 value=_Promise.NO_RESULT,  # type: Any
                 on_complete=None,  # type: Optional[Callable]
                 name=None,  # type: Optional[str]
                 ):
        super(Completion, self).__init__(_first_promise, value, on_complete, name)

    @property
    def _progress_reference(self):
        # type: () -> Optional[ProgressReference]
        if hasattr(self._on_complete, 'progress_id'):
            return self._on_complete  # type: ignore
        return None

    @property
    def progress_reference(self):
        # type: () -> Optional[ProgressReference]
        """
        ProgressReference. Marks this completion
        as a write completeion.
        """

        references = [c._progress_reference for c in iter(self) if c._progress_reference is not None]
        if references:
            assert len(references) == 1
            return references[0]
        return None

    @classmethod
    def with_progress(cls,  # type: Any
                      message,  # type: str
                      mgr,
                      _first_promise=None,  # type: Optional["Completion"]
                      value=_Promise.NO_RESULT,  # type: Any
                      on_complete=None,  # type: Optional[Callable]
                      calc_percent=None  # type: Optional[Callable[[], Any]]
                      ):
        # type: (...) -> Any

        c = cls(
            _first_promise=_first_promise,
            value=value,
            on_complete=on_complete
        ).add_progress(message, mgr, calc_percent)

        return c._first_promise

    def add_progress(self,
                     message,  # type: str
                     mgr,
                     calc_percent=None  # type: Optional[Callable[[], Any]]
                     ):
        return self.then(
            on_complete=ProgressReference(
                message=message,
                mgr=mgr,
                completion=calc_percent
            )
        )

    def fail(self, e: Exception):
        super(Completion, self).fail(e)
        if self._progress_reference:
            self._progress_reference.fail()

    def finalize(self, result: Union[None, object, T]=_Promise.NO_RESULT):
        if self._first_promise._state == self.INITIALIZED:
            self._first_promise._finalize(result)

    @property
    def result(self) -> T:
        """
        The result of the operation that we were waited
        for.  Only valid after calling Orchestrator.process() on this
        completion.
        """
        last = self._last_promise()
        assert last._state == _Promise.FINISHED
        return cast(T, last._value)

    def result_str(self) -> str:
        """Force a string."""
        if self.result is None:
            return ''
        if isinstance(self.result, list):
            return '\n'.join(str(x) for x in self.result)
        return str(self.result)

    @property
    def exception(self):
        # type: () -> Optional[Exception]
        return self._last_promise()._exception

    @property
    def serialized_exception(self):
        # type: () -> Optional[bytes]
        return self._last_promise()._serialized_exception

    @property
    def has_result(self):
        # type: () -> bool
        """
        Has the operation already a result?

        For Write operations, it can already have a
        result, if the orchestrator's configuration is
        persistently written. Typically this would
        indicate that an update had been written to
        a manifest, but that the update had not
        necessarily been pushed out to the cluster.

        :return:
        """
        return self._last_promise()._state == _Promise.FINISHED

    @property
    def is_errored(self):
        # type: () -> bool
        """
        Has the completion failed. Default implementation looks for
        self.exception. Can be overwritten.
        """
        return self.exception is not None

    @property
    def needs_result(self):
        # type: () -> bool
        """
        Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a read operation only if it is not complete.
        """
        return not self.is_errored and not self.has_result

    @property
    def is_finished(self):
        # type: () -> bool
        """
        Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a read operation only if it is not complete.
        """
        return self.is_errored or (self.has_result)

    def pretty_print(self):

        reprs = '\n'.join(p.pretty_print_1() for p in iter(self._first_promise))
        return """<{}>[\n{}\n]""".format(self.__class__.__name__, reprs)


def pretty_print(completions):
    # type: (Sequence[Completion]) -> str
    return ', '.join(c.pretty_print() for c in completions)


def raise_if_exception(c):
    # type: (Completion) -> None
    """
    :raises OrchestratorError: Some user error or a config error.
    :raises Exception: Some internal error
    """
    if c.serialized_exception is not None:
        try:
            e = pickle.loads(c.serialized_exception)
        except (KeyError, AttributeError):
            raise Exception('{}: {}'.format(type(c.exception), c.exception))
        raise e


class TrivialReadCompletion(Completion[T]):
    """
    This is the trivial completion simply wrapping a result.
    """
    def __init__(self, result: T):
        super(TrivialReadCompletion, self).__init__()
        if result:
            self.finalize(result)


def _hide_in_features(f):
    f._hide_in_features = True
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
    def is_orchestrator_module(self):
        """
        Enable other modules to interrogate this module to discover
        whether it's usable as an orchestrator module.

        Subclasses do not need to override this.
        """
        return True

    @_hide_in_features
    def available(self):
        # type: () -> Tuple[bool, str]
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

        :return: two-tuple of boolean, string
        """
        raise NotImplementedError()

    @_hide_in_features
    def process(self, completions):
        # type: (List[Completion]) -> None
        """
        Given a list of Completion instances, process any which are
        incomplete.

        Callers should inspect the detail of each completion to identify
        partial completion/progress information, and present that information
        to the user.

        This method should not block, as this would make it slow to query
        a status, while other long running operations are in progress.
        """
        raise NotImplementedError()

    @_hide_in_features
    def get_feature_set(self):
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

    def cancel_completions(self):
        # type: () -> None
        """
        Cancels ongoing completions. Unstuck the mgr.
        """
        raise NotImplementedError()

    def pause(self):
        # type: () -> None
        raise NotImplementedError()

    def resume(self):
        # type: () -> None
        raise NotImplementedError()

    def add_host(self, host_spec):
        # type: (HostSpec) -> Completion[str]
        """
        Add a host to the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def remove_host(self, host):
        # type: (str) -> Completion[str]
        """
        Remove a host from the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def update_host_addr(self, host, addr):
        # type: (str, str) -> Completion[str]
        """
        Update a host's address

        :param host: hostname
        :param addr: address (dns name or IP)
        """
        raise NotImplementedError()

    def get_hosts(self):
        # type: () -> Completion[List[HostSpec]]
        """
        Report the hosts in the cluster.

        :return: list of HostSpec
        """
        raise NotImplementedError()

    def add_host_label(self, host, label):
        # type: (str, str) -> Completion[str]
        """
        Add a host label
        """
        raise NotImplementedError()

    def remove_host_label(self, host, label):
        # type: (str, str) -> Completion[str]
        """
        Remove a host label
        """
        raise NotImplementedError()

    def get_inventory(self, host_filter=None, refresh=False):
        # type: (Optional[InventoryFilter], bool) -> Completion[List[InventoryHost]]
        """
        Returns something that was created by `ceph-volume inventory`.

        :return: list of InventoryHost
        """
        raise NotImplementedError()

    def describe_service(self, service_type=None, service_name=None, refresh=False):
        # type: (Optional[str], Optional[str], bool) -> Completion[List[ServiceDescription]]
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

    def list_daemons(self, service_name=None, daemon_type=None, daemon_id=None, host=None, refresh=False):
        # type: (Optional[str], Optional[str], Optional[str], Optional[str], bool) -> Completion[List[DaemonDescription]]
        """
        Describe a daemon (of any kind) that is already configured in
        the orchestrator.

        :return: list of DaemonDescription objects.
        """
        raise NotImplementedError()

    def apply(self, specs: List["GenericSpec"]) -> Completion[List[str]]:
        """
        Applies any spec
        """
        fns: Dict[str, function] = {
            'alertmanager': self.apply_alertmanager,
            'crash': self.apply_crash,
            'grafana': self.apply_grafana,
            'iscsi': self.apply_iscsi,
            'mds': self.apply_mds,
            'mgr': self.apply_mgr,
            'mon': self.apply_mon,
            'nfs': self.apply_nfs,
            'node-exporter': self.apply_node_exporter,
            'osd': lambda dg: self.apply_drivegroups([dg]),
            'prometheus': self.apply_prometheus,
            'rbd-mirror': self.apply_rbd_mirror,
            'rgw': self.apply_rgw,
            'host': self.add_host,
        }

        def merge(ls, r):
            if isinstance(ls, list):
                return ls + [r]
            return [ls, r]

        spec, *specs = specs

        fn = cast(Callable[["GenericSpec"], Completion], fns[spec.service_type])
        completion = fn(spec)
        for s in specs:
            def next(ls):
                fn = cast(Callable[["GenericSpec"], Completion], fns[spec.service_type])
                return fn(s).then(lambda r: merge(ls, r))
            completion = completion.then(next)
        return completion

    def plan(self, spec: List["GenericSpec"]) -> Completion[List]:
        """
        Plan (Dry-run, Preview) a List of Specs.
        """
        raise NotImplementedError()

    def remove_daemons(self, names):
        # type: (List[str]) -> Completion[List[str]]
        """
        Remove specific daemon(s).

        :return: None
        """
        raise NotImplementedError()

    def remove_service(self, service_name):
        # type: (str) -> Completion[str]
        """
        Remove a service (a collection of daemons).

        :return: None
        """
        raise NotImplementedError()

    def service_action(self, action, service_name):
        # type: (str, str) -> Completion[List[str]]
        """
        Perform an action (start/stop/reload) on a service (i.e., all daemons
        providing the logical service).

        :param action: one of "start", "stop", "restart", "redeploy", "reconfig"
        :param service_type: e.g. "mds", "rgw", ...
        :param service_name: name of logical service ("cephfs", "us-east", ...)
        :rtype: Completion
        """
        #assert action in ["start", "stop", "reload, "restart", "redeploy"]
        raise NotImplementedError()

    def daemon_action(self, action, daemon_type, daemon_id):
        # type: (str, str, str) -> Completion[List[str]]
        """
        Perform an action (start/stop/reload) on a daemon.

        :param action: one of "start", "stop", "restart", "redeploy", "reconfig"
        :param name: name of daemon
        :rtype: Completion
        """
        #assert action in ["start", "stop", "reload, "restart", "redeploy"]
        raise NotImplementedError()

    def create_osds(self, drive_group):
        # type: (DriveGroupSpec) -> Completion[str]
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).
        """
        raise NotImplementedError()

    def apply_drivegroups(self, specs: List[DriveGroupSpec]) -> Completion[List[str]]:
        """ Update OSD cluster """
        raise NotImplementedError()

    def set_unmanaged_flag(self,
                           unmanaged_flag: bool,
                           service_type: str = 'osd',
                           service_name=None
                           ) -> HandleCommandResult:
        raise NotImplementedError()

    def preview_osdspecs(self,
                         osdspec_name: Optional[str] = 'osd',
                         osdspecs: Optional[List[DriveGroupSpec]] = None
                         ) -> Completion[str]:
        """ Get a preview for OSD deployments """
        raise NotImplementedError()

    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False) -> Completion[str]:
        """
        :param osd_ids: list of OSD IDs
        :param replace: marks the OSD as being destroyed. See :ref:`orchestrator-osd-replace`
        :param force: Forces the OSD removal process without waiting for the data to be drained first.
        Note that this can only remove OSDs that were successfully
        created (i.e. got an OSD ID).
        """
        raise NotImplementedError()

    def remove_osds_status(self):
        # type: () -> Completion
        """
        Returns a status of the ongoing OSD removal operations.
        """
        raise NotImplementedError()

    def blink_device_light(self, ident_fault, on, locations):
        # type: (str, bool, List[DeviceLightLoc]) -> Completion[List[str]]
        """
        Instructs the orchestrator to enable or disable either the ident or the fault LED.

        :param ident_fault: either ``"ident"`` or ``"fault"``
        :param on: ``True`` = on.
        :param locations: See :class:`orchestrator.DeviceLightLoc`
        """
        raise NotImplementedError()

    def zap_device(self, host, path):
        # type: (str, str) -> Completion[str]
        """Zap/Erase a device (DESTROYS DATA)"""
        raise NotImplementedError()

    def add_mon(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create mon daemon(s)"""
        raise NotImplementedError()

    def apply_mon(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update mon cluster"""
        raise NotImplementedError()

    def add_mgr(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create mgr daemon(s)"""
        raise NotImplementedError()

    def apply_mgr(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update mgr cluster"""
        raise NotImplementedError()

    def add_mds(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create MDS daemon(s)"""
        raise NotImplementedError()

    def apply_mds(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update MDS cluster"""
        raise NotImplementedError()

    def add_rgw(self, spec):
        # type: (RGWSpec) -> Completion[List[str]]
        """Create RGW daemon(s)"""
        raise NotImplementedError()

    def apply_rgw(self, spec):
        # type: (RGWSpec) -> Completion[str]
        """Update RGW cluster"""
        raise NotImplementedError()

    def add_rbd_mirror(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create rbd-mirror daemon(s)"""
        raise NotImplementedError()

    def apply_rbd_mirror(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update rbd-mirror cluster"""
        raise NotImplementedError()

    def add_nfs(self, spec):
        # type: (NFSServiceSpec) -> Completion[List[str]]
        """Create NFS daemon(s)"""
        raise NotImplementedError()

    def apply_nfs(self, spec):
        # type: (NFSServiceSpec) -> Completion[str]
        """Update NFS cluster"""
        raise NotImplementedError()

    def add_iscsi(self, spec):
        # type: (IscsiServiceSpec) -> Completion[List[str]]
        """Create iscsi daemon(s)"""
        raise NotImplementedError()

    def apply_iscsi(self, spec):
        # type: (IscsiServiceSpec) -> Completion[str]
        """Update iscsi cluster"""
        raise NotImplementedError()

    def add_prometheus(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create new prometheus daemon"""
        raise NotImplementedError()

    def apply_prometheus(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update prometheus cluster"""
        raise NotImplementedError()

    def add_node_exporter(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create a new Node-Exporter service"""
        raise NotImplementedError()

    def apply_node_exporter(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update existing a Node-Exporter daemon(s)"""
        raise NotImplementedError()

    def add_crash(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create a new crash service"""
        raise NotImplementedError()

    def apply_crash(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update existing a crash daemon(s)"""
        raise NotImplementedError()

    def add_grafana(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create a new Node-Exporter service"""
        raise NotImplementedError()

    def apply_grafana(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update existing a Node-Exporter daemon(s)"""
        raise NotImplementedError()

    def add_alertmanager(self, spec):
        # type: (ServiceSpec) -> Completion[List[str]]
        """Create a new AlertManager service"""
        raise NotImplementedError()

    def apply_alertmanager(self, spec):
        # type: (ServiceSpec) -> Completion[str]
        """Update an existing AlertManager daemon(s)"""
        raise NotImplementedError()

    def upgrade_check(self, image, version):
        # type: (Optional[str], Optional[str]) -> Completion[str]
        raise NotImplementedError()

    def upgrade_start(self, image, version):
        # type: (Optional[str], Optional[str]) -> Completion[str]
        raise NotImplementedError()

    def upgrade_pause(self):
        # type: () -> Completion[str]
        raise NotImplementedError()

    def upgrade_resume(self):
        # type: () -> Completion[str]
        raise NotImplementedError()

    def upgrade_stop(self):
        # type: () -> Completion[str]
        raise NotImplementedError()

    def upgrade_status(self):
        # type: () -> Completion[UpgradeStatusSpec]
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_available(self):
        # type: () -> Completion
        """
        Report on what versions are available to upgrade to

        :return: List of strings
        """
        raise NotImplementedError()


GenericSpec = Union[ServiceSpec, HostSpec]

def json_to_generic_spec(spec):
    # type: (dict) -> GenericSpec
    if 'service_type' in spec and spec['service_type'] == 'host':
        return HostSpec.from_json(spec)
    else:
        return ServiceSpec.from_json(spec)

class UpgradeStatusSpec(object):
    # Orchestrator's report on what's going on with any ongoing upgrade
    def __init__(self):
        self.in_progress = False  # Is an upgrade underway?
        self.target_image = None
        self.services_complete = []  # Which daemon types are fully updated?
        self.message = ""  # Freeform description


def handle_type_error(method):
    @wraps(method)
    def inner(cls, *args, **kwargs):
        try:
            return method(cls, *args, **kwargs)
        except TypeError as e:
            error_msg = '{}: {}'.format(cls.__name__, e)
        raise OrchestratorValidationError(error_msg)
    return inner


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
                 daemon_type=None,
                 daemon_id=None,
                 hostname=None,
                 container_id=None,
                 container_image_id=None,
                 container_image_name=None,
                 version=None,
                 status=None,
                 status_desc=None,
                 last_refresh=None,
                 created=None,
                 started=None,
                 last_configured=None,
                 osdspec_affinity=None,
                 last_deployed=None,
                 events: Optional[List['OrchestratorEvent']]=None):

        # Host is at the same granularity as InventoryHost
        self.hostname: str = hostname

        # Not everyone runs in containers, but enough people do to
        # justify having the container_id (runtime id) and container_image
        # (image name)
        self.container_id = container_id                  # runtime id
        self.container_image_id = container_image_id      # image hash
        self.container_image_name = container_image_name  # image friendly name

        # The type of service (osd, mon, mgr, etc.)
        self.daemon_type = daemon_type

        # The orchestrator will have picked some names for daemons,
        # typically either based on hostnames or on pod names.
        # This is the <foo> in mds.<foo>, the ID that will appear
        # in the FSMap/ServiceMap.
        self.daemon_id: str = daemon_id

        # Service version that was deployed
        self.version = version

        # Service status: -1 error, 0 stopped, 1 running
        self.status = status

        # Service status description when status == -1.
        self.status_desc = status_desc

        # datetime when this info was last refreshed
        self.last_refresh = last_refresh  # type: Optional[datetime.datetime]

        self.created = created    # type: Optional[datetime.datetime]
        self.started = started    # type: Optional[datetime.datetime]
        self.last_configured = last_configured # type: Optional[datetime.datetime]
        self.last_deployed = last_deployed    # type: Optional[datetime.datetime]

        # Affinity to a certain OSDSpec
        self.osdspec_affinity = osdspec_affinity  # type: Optional[str]

        self.events: List[OrchestratorEvent] = events or []

    def name(self):
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def matches_service(self, service_name):
        # type: (Optional[str]) -> bool
        if service_name:
            return self.name().startswith(service_name + '.')
        return False

    def service_id(self):
        def _match():
            err = OrchestratorError("DaemonDescription: Cannot calculate service_id: " \
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

            # daemon_id == "service_id"
            return self.daemon_id

        if self.daemon_type in ServiceSpec.REQUIRES_SERVICE_ID:
            return _match()

        return self.daemon_id

    def service_name(self):
        if self.daemon_type in ServiceSpec.REQUIRES_SERVICE_ID:
            return f'{self.daemon_type}.{self.service_id()}'
        return self.daemon_type

    def __repr__(self):
        return "<DaemonDescription>({type}.{id})".format(type=self.daemon_type,
                                                         id=self.daemon_id)

    def to_json(self):
        out = OrderedDict()
        out['daemon_type'] = self.daemon_type
        out['daemon_id'] = self.daemon_id
        out['hostname'] = self.hostname
        out['container_id'] = self.container_id
        out['container_image_id'] = self.container_image_id
        out['container_image_name'] = self.container_image_name
        out['version'] = self.version
        out['status'] = self.status
        out['status_desc'] = self.status_desc

        for k in ['last_refresh', 'created', 'started', 'last_deployed',
                  'last_configured']:
            if getattr(self, k):
                out[k] = getattr(self, k).strftime(DATEFMT)

        if self.events:
            out['events'] = [e.to_json() for e in self.events]

        empty = [k for k, v in out.items() if v is None]
        for e in empty:
            del out[e]
        return out

    @classmethod
    @handle_type_error
    def from_json(cls, data):
        c = data.copy()
        event_strs = c.pop('events', [])
        for k in ['last_refresh', 'created', 'started', 'last_deployed',
                  'last_configured']:
            if k in c:
                c[k] = datetime.datetime.strptime(c[k], DATEFMT)
        events = [OrchestratorEvent.from_json(e) for e in event_strs]
        return cls(events=events, **c)

    def __copy__(self):
        # feel free to change this:
        return DaemonDescription.from_json(self.to_json())

    @staticmethod
    def yaml_representer(dumper: 'yaml.SafeDumper', data: 'DaemonDescription'):
        return dumper.represent_dict(data.to_json().items())


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
                 container_image_id=None,
                 container_image_name=None,
                 rados_config_location=None,
                 service_url=None,
                 last_refresh=None,
                 created=None,
                 size=0,
                 running=0,
                 events: Optional[List['OrchestratorEvent']]=None):
        # Not everyone runs in containers, but enough people do to
        # justify having the container_image_id (image hash) and container_image
        # (image name)
        self.container_image_id = container_image_id      # image hash
        self.container_image_name = container_image_name  # image friendly name

        # Location of the service configuration when stored in rados
        # object. Format: "rados://<pool>/[<namespace/>]<object>"
        self.rados_config_location = rados_config_location

        # If the service exposes REST-like API, this attribute should hold
        # the URL.
        self.service_url = service_url

        # Number of daemons
        self.size = size

        # Number of daemons up
        self.running = running

        # datetime when this info was last refreshed
        self.last_refresh = last_refresh   # type: Optional[datetime.datetime]
        self.created = created   # type: Optional[datetime.datetime]

        self.spec: ServiceSpec = spec

        self.events: List[OrchestratorEvent] = events or []

    def service_type(self):
        return self.spec.service_type

    def __repr__(self):
        return f"<ServiceDescription of {self.spec.one_line_str()}>"

    def to_json(self) -> OrderedDict:
        out = self.spec.to_json()
        status = {
            'container_image_id': self.container_image_id,
            'container_image_name': self.container_image_name,
            'rados_config_location': self.rados_config_location,
            'service_url': self.service_url,
            'size': self.size,
            'running': self.running,
            'last_refresh': self.last_refresh,
            'created': self.created,
        }
        for k in ['last_refresh', 'created']:
            if getattr(self, k):
                status[k] = getattr(self, k).strftime(DATEFMT)
        status = {k: v for (k, v) in status.items() if v is not None}
        out['status'] = status
        if self.events:
            out['events'] = [e.to_json() for e in self.events]
        return out

    @classmethod
    @handle_type_error
    def from_json(cls, data: dict):
        c = data.copy()
        status = c.pop('status', {})
        event_strs = c.pop('events', [])
        spec = ServiceSpec.from_json(c)

        c_status = status.copy()
        for k in ['last_refresh', 'created']:
            if k in c_status:
                c_status[k] = datetime.datetime.strptime(c_status[k], DATEFMT)
        events = [OrchestratorEvent.from_json(e) for e in event_strs]
        return cls(spec=spec, events=events, **c_status)

    @staticmethod
    def yaml_representer(dumper: 'yaml.SafeDumper', data: 'DaemonDescription'):
        return dumper.represent_dict(data.to_json().items())


yaml.add_representer(ServiceDescription, ServiceDescription.yaml_representer)


class InventoryFilter(object):
    """
    When fetching inventory, use this filter to avoid unnecessarily
    scanning the whole estate.

    Typical use: filter by host when presenting UI workflow for configuring
                 a particular server.
                 filter by label when not all of estate is Ceph servers,
                 and we want to only learn about the Ceph servers.
                 filter by label when we are interested particularly
                 in e.g. OSD servers.

    """
    def __init__(self, labels=None, hosts=None):
        # type: (Optional[List[str]], Optional[List[str]]) -> None

        #: Optional: get info about hosts matching labels
        self.labels = labels

        #: Optional: get info about certain named hosts only
        self.hosts = hosts


class InventoryHost(object):
    """
    When fetching inventory, all Devices are groups inside of an
    InventoryHost.
    """
    def __init__(self, name, devices=None, labels=None, addr=None):
        # type: (str, Optional[inventory.Devices], Optional[List[str]], Optional[str]) -> None
        if devices is None:
            devices = inventory.Devices([])
        if labels is None:
            labels = []
        assert isinstance(devices, inventory.Devices)

        self.name = name  # unique within cluster.  For example a hostname.
        self.addr = addr or name
        self.devices = devices
        self.labels = labels

    def to_json(self):
        return {
            'name': self.name,
            'addr': self.addr,
            'devices': self.devices.to_json(),
            'labels': self.labels,
        }

    @classmethod
    def from_json(cls, data):
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
    def from_nested_items(cls, hosts):
        devs = inventory.Devices.from_json
        return [cls(item[0], devs(item[1].data)) for item in hosts]

    def __repr__(self):
        return "<InventoryHost>({name})".format(name=self.name)

    @staticmethod
    def get_host_names(hosts):
        # type: (List[InventoryHost]) -> List[str]
        return [host.name for host in hosts]

    def __eq__(self, other):
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
    regex_v1 = re.compile(r'^([^ ]+) ([^:]+):([^ ]+) \[([^\]]+)\] "(.*)"$')

    def __init__(self, created: Union[str, datetime.datetime], kind, subject, level, message):
        if isinstance(created, str):
            created = datetime.datetime.strptime(created, DATEFMT)
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
        created = self.created.strftime(DATEFMT)
        return f'{created} {self.kind_subject()} [{self.level}] "{self.message}"'


    @classmethod
    @handle_type_error
    def from_json(cls, data) -> "OrchestratorEvent":
        """
        >>> OrchestratorEvent.from_json('''2020-06-10T10:20:25.691255 daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on host 'ubuntu'"''').to_json()
        '2020-06-10T10:20:25.691255 daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on host \\'ubuntu\\'"'

        :param data:
        :return:
        """
        match = cls.regex_v1.match(data)
        if match:
            return cls(*match.groups())
        raise ValueError(f'Unable to match: "{data}"')


def _mk_orch_methods(cls):
    # Needs to be defined outside of for.
    # Otherwise meth is always bound to last key
    def shim(method_name):
        def inner(self, *args, **kwargs):
            completion = self._oremote(method_name, args, kwargs)
            return completion
        return inner

    for meth in Orchestrator.__dict__:
        if not meth.startswith('_') and meth not in ['is_orchestrator_module']:
            setattr(cls, meth, shim(meth))
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
    ...        self._orchestrator_wait([completion])
    ...        self.log.debug(completion.result)

    .. note:: Orchestrator implementations should not inherit from `OrchestratorClientMixin`.
        Reason is, that OrchestratorClientMixin magically redirects all methods to the
        "real" implementation of the orchestrator.


    >>> import mgr_module
    >>> #doctest: +SKIP
    ... class MyImplentation(mgr_module.MgrModule, Orchestrator):
    ...     def __init__(self, ...):
    ...         self.orch_client = OrchestratorClientMixin()
    ...         self.orch_client.set_mgr(self.mgr))
    """

    def set_mgr(self, mgr):
        # type: (MgrModule) -> None
        """
        Useable in the Dashbord that uses a global ``mgr``
        """

        self.__mgr = mgr  # Make sure we're not overwriting any other `mgr` properties

    def __get_mgr(self):
        try:
            return self.__mgr
        except AttributeError:
            return self

    def _oremote(self, meth, args, kwargs):
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

    def _orchestrator_wait(self, completions):
        # type: (List[Completion]) -> None
        """
        Wait for completions to complete (reads) or
        become persistent (writes).

        Waits for writes to be *persistent* but not *effective*.

        :param completions: List of Completions
        :raises NoOrchestrator:
        :raises RuntimeError: something went wrong while calling the process method.
        :raises ImportError: no `orchestrator` module or backend not found.
        """
        while any(not c.has_result for c in completions):
            self.process(completions)
            self.__get_mgr().log.info("Operations pending: %s",
                                      sum(1 for c in completions if not c.has_result))
            if any(c.needs_result for c in completions):
                time.sleep(1)
            else:
                break
