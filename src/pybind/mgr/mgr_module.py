import ceph_module  # noqa

from typing import cast, Tuple, Any, Dict, Generic, Optional, Callable, List, \
    Mapping, NamedTuple, Sequence, Union, Set, TYPE_CHECKING
if TYPE_CHECKING:
    import sys
    if sys.version_info >= (3, 8):
        from typing import Literal
    else:
        from typing_extensions import Literal

import inspect
import logging
import errno
import functools
import json
import subprocess
import threading
from collections import defaultdict
from enum import IntEnum, Enum
import rados
import re
import socket
import sqlite3
import sys
import time
from ceph_argparse import CephArgtype
from mgr_util import profile_method

if sys.version_info >= (3, 8):
    from typing import get_args, get_origin
else:
    def get_args(tp: Any) -> Any:
        if tp is Generic:
            return tp
        else:
            return getattr(tp, '__args__', ())

    def get_origin(tp: Any) -> Any:
        return getattr(tp, '__origin__', None)


ERROR_MSG_EMPTY_INPUT_FILE = 'Empty input file'
ERROR_MSG_NO_INPUT_FILE = 'Input file not specified'
# Full list of strings in "osd_types.cc:pg_state_string()"
PG_STATES = [
    "active",
    "clean",
    "down",
    "recovery_unfound",
    "backfill_unfound",
    "scrubbing",
    "degraded",
    "inconsistent",
    "peering",
    "repair",
    "recovering",
    "forced_recovery",
    "backfill_wait",
    "incomplete",
    "stale",
    "remapped",
    "deep",
    "backfilling",
    "forced_backfill",
    "backfill_toofull",
    "recovery_wait",
    "recovery_toofull",
    "undersized",
    "activating",
    "peered",
    "snaptrim",
    "snaptrim_wait",
    "snaptrim_error",
    "creating",
    "unknown",
    "premerge",
    "failed_repair",
    "laggy",
    "wait",
]

NFS_GANESHA_SUPPORTED_FSALS = ['CEPH', 'RGW']
NFS_POOL_NAME = '.nfs'

class CephReleases(IntEnum):
    argonaut = 1
    bobtail = 2
    cuttlefish = 3
    dumpling = 4
    emperor = 5
    firefly = 6
    giant = 7
    hammer = 8
    infernalis = 9
    jewel = 10
    kraken = 11
    luminous = 12
    mimic = 13
    nautilus = 14
    octopus = 15
    pacific = 16
    quincy = 17
    reef = 18
    squid = 19
    maximum = 20

class NotifyType(str, Enum):
    mon_map = 'mon_map'
    pg_summary = 'pg_summary'
    health = 'health'
    clog = 'clog'
    osd_map = 'osd_map'
    fs_map = 'fs_map'
    command = 'command'

    # these are disabled because there are no users.
    #  see Mgr.cc:
    # service_map = 'service_map'
    # mon_status = 'mon_status'
    #  see DaemonServer.cc:
    # perf_schema_update = 'perf_schema_update'


class CommandResult(object):
    """
    Use with MgrModule.send_command
    """

    def __init__(self, tag: Optional[str] = None):
        self.ev = threading.Event()
        self.outs = ""
        self.outb = ""
        self.r = 0

        # This is just a convenience for notifications from
        # C++ land, to avoid passing addresses around in messages.
        self.tag = tag if tag else ""

    def complete(self, r: int, outb: str, outs: str) -> None:
        self.r = r
        self.outb = outb
        self.outs = outs
        self.ev.set()

    def wait(self) -> Tuple[int, str, str]:
        self.ev.wait()
        return self.r, self.outb, self.outs


class HandleCommandResult(NamedTuple):
    """
    Tuple containing the result of `handle_command()`

    Only write to stderr if there is an error, or in extraordinary circumstances

    Avoid having `ceph foo bar` commands say "did foo bar" on success unless there
    is critical information to include there.

    Everything programmatically consumable should be put on stdout
    """
    retval: int = 0             # return code. E.g. 0 or -errno.EINVAL
    stdout: str = ""            # data of this result.
    stderr: str = ""            # Typically used for error messages.


class MonCommandFailed(RuntimeError): pass
class MgrDBNotReady(RuntimeError): pass


class OSDMap(ceph_module.BasePyOSDMap):
    def get_epoch(self) -> int:
        return self._get_epoch()

    def get_crush_version(self) -> int:
        return self._get_crush_version()

    def dump(self) -> Dict[str, Any]:
        return self._dump()

    def get_pools(self) -> Dict[int, Dict[str, Any]]:
        # FIXME: efficient implementation
        d = self._dump()
        return dict([(p['pool'], p) for p in d['pools']])

    def get_pools_by_name(self) -> Dict[str, Dict[str, Any]]:
        # FIXME: efficient implementation
        d = self._dump()
        return dict([(p['pool_name'], p) for p in d['pools']])

    def new_incremental(self) -> 'OSDMapIncremental':
        return self._new_incremental()

    def apply_incremental(self, inc: 'OSDMapIncremental') -> 'OSDMap':
        return self._apply_incremental(inc)

    def get_crush(self) -> 'CRUSHMap':
        return self._get_crush()

    def get_pools_by_take(self, take: int) -> List[int]:
        return self._get_pools_by_take(take).get('pools', [])

    def calc_pg_upmaps(self, inc: 'OSDMapIncremental',
                       max_deviation: int,
                       max_iterations: int = 10,
                       pools: Optional[List[str]] = None) -> int:
        if pools is None:
            pools = []
        return self._calc_pg_upmaps(
            inc,
            max_deviation, max_iterations, pools)

    def map_pool_pgs_up(self, poolid: int) -> List[int]:
        return self._map_pool_pgs_up(poolid)

    def pg_to_up_acting_osds(self, pool_id: int, ps: int) -> Dict[str, Any]:
        return self._pg_to_up_acting_osds(pool_id, ps)

    def pool_raw_used_rate(self, pool_id: int) -> float:
        return self._pool_raw_used_rate(pool_id)

    @classmethod
    def build_simple(cls, epoch: int = 1, uuid: Optional[str] = None, num_osd: int = -1) -> 'ceph_module.BasePyOSDMap':
        return cls._build_simple(epoch, uuid, num_osd)

    def get_ec_profile(self, name: str) -> Optional[List[Dict[str, str]]]:
        # FIXME: efficient implementation
        d = self._dump()
        return d['erasure_code_profiles'].get(name, None)

    def get_require_osd_release(self) -> str:
        d = self._dump()
        return d['require_osd_release']


class OSDMapIncremental(ceph_module.BasePyOSDMapIncremental):
    def get_epoch(self) -> int:
        return self._get_epoch()

    def dump(self) -> Dict[str, Any]:
        return self._dump()

    def set_osd_reweights(self, weightmap: Dict[int, float]) -> None:
        """
        weightmap is a dict, int to float.  e.g. { 0: .9, 1: 1.0, 3: .997 }
        """
        return self._set_osd_reweights(weightmap)

    def set_crush_compat_weight_set_weights(self, weightmap: Dict[str, float]) -> None:
        """
        weightmap is a dict, int to float.  devices only.  e.g.,
        { 0: 3.4, 1: 3.3, 2: 3.334 }
        """
        return self._set_crush_compat_weight_set_weights(weightmap)


class CRUSHMap(ceph_module.BasePyCRUSH):
    ITEM_NONE = 0x7fffffff
    DEFAULT_CHOOSE_ARGS = '-1'

    def dump(self) -> Dict[str, Any]:
        return self._dump()

    def get_item_weight(self, item: int) -> Optional[int]:
        return self._get_item_weight(item)

    def get_item_name(self, item: int) -> Optional[str]:
        return self._get_item_name(item)

    def find_takes(self) -> List[int]:
        return self._find_takes().get('takes', [])

    def find_roots(self) -> List[int]:
        return self._find_roots().get('roots', [])

    def get_take_weight_osd_map(self, root: int) -> Dict[int, float]:
        uglymap = self._get_take_weight_osd_map(root)
        return {int(k): v for k, v in uglymap.get('weights', {}).items()}

    @staticmethod
    def have_default_choose_args(dump: Dict[str, Any]) -> bool:
        return CRUSHMap.DEFAULT_CHOOSE_ARGS in dump.get('choose_args', {})

    @staticmethod
    def get_default_choose_args(dump: Dict[str, Any]) -> List[Dict[str, Any]]:
        choose_args = dump.get('choose_args')
        assert isinstance(choose_args, dict)
        return choose_args.get(CRUSHMap.DEFAULT_CHOOSE_ARGS, [])

    def get_rule(self, rule_name: str) -> Optional[Dict[str, Any]]:
        # TODO efficient implementation
        for rule in self.dump()['rules']:
            if rule_name == rule['rule_name']:
                return rule

        return None

    def get_rule_by_id(self, rule_id: int) -> Optional[Dict[str, Any]]:
        for rule in self.dump()['rules']:
            if rule['rule_id'] == rule_id:
                return rule

        return None

    def get_rule_root(self, rule_name: str) -> Optional[int]:
        rule = self.get_rule(rule_name)
        if rule is None:
            return None

        try:
            first_take = next(s for s in rule['steps'] if s.get('op') == 'take')
        except StopIteration:
            logging.warning("CRUSH rule '{0}' has no 'take' step".format(
                rule_name))
            return None
        else:
            return first_take['item']

    def get_osds_under(self, root_id: int) -> List[int]:
        # TODO don't abuse dump like this
        d = self.dump()
        buckets = dict([(b['id'], b) for b in d['buckets']])

        osd_list = []

        def accumulate(b: Dict[str, Any]) -> None:
            for item in b['items']:
                if item['id'] >= 0:
                    osd_list.append(item['id'])
                else:
                    try:
                        accumulate(buckets[item['id']])
                    except KeyError:
                        pass

        accumulate(buckets[root_id])

        return osd_list

    def device_class_counts(self) -> Dict[str, int]:
        result = defaultdict(int)  # type: Dict[str, int]
        # TODO don't abuse dump like this
        d = self.dump()
        for device in d['devices']:
            cls = device.get('class', None)
            result[cls] += 1

        return dict(result)


HandlerFuncType = Callable[..., Tuple[int, str, str]]

def _extract_target_func(
    f: HandlerFuncType
) -> Tuple[HandlerFuncType, Dict[str, Any]]:
    """In order to interoperate with other decorated functions,
    we need to find the original function which will provide
    the main set of arguments. While we descend through the
    stack of wrapped functions, gather additional arguments
    the decorators may want to provide.
    """
    # use getattr to keep mypy happy
    wrapped = getattr(f, "__wrapped__", None)
    if not wrapped:
        return f, {}
    extra_args: Dict[str, Any] = {}
    while wrapped is not None:
        extra_args.update(getattr(f, "extra_args", {}))
        f = wrapped
        wrapped = getattr(f, "__wrapped__", None)
    return f, extra_args


class CLICommand(object):
    COMMANDS = {}  # type: Dict[str, CLICommand]

    def __init__(self,
                 prefix: str,
                 perm: str = 'rw',
                 poll: bool = False):
        self.prefix = prefix
        self.perm = perm
        self.poll = poll
        self.func = None  # type: Optional[Callable]
        self.arg_spec = {}    # type: Dict[str, Any]
        self.first_default = -1

    KNOWN_ARGS = '_', 'self', 'mgr', 'inbuf', 'return'

    @classmethod
    def _load_func_metadata(cls: Any, f: HandlerFuncType) -> Tuple[str, Dict[str, Any], int, str]:
        f, extra_args = _extract_target_func(f)
        desc = (inspect.getdoc(f) or '').replace('\n', ' ')
        full_argspec = inspect.getfullargspec(f)
        arg_spec = full_argspec.annotations
        first_default = len(arg_spec)
        if full_argspec.defaults:
            first_default -= len(full_argspec.defaults)
        args = []
        positional = True
        for index, arg in enumerate(full_argspec.args):
            if arg in cls.KNOWN_ARGS:
                # record that this function takes an inbuf if it is present
                # in the full_argspec and not already in the arg_spec
                if arg == 'inbuf' and 'inbuf' not in arg_spec:
                    arg_spec['inbuf'] = 'str'
                continue
            if arg == '_end_positional_':
                positional = False
                continue
            if (
                arg == 'format'
                or arg_spec[arg] is Optional[bool]
                or arg_spec[arg] is bool
            ):
                # implicit switch to non-positional on any
                # Optional[bool] or the --format option
                positional = False
            assert arg in arg_spec, \
                f"'{arg}' is not annotated for {f}: {full_argspec}"
            has_default = index >= first_default
            args.append(CephArgtype.to_argdesc(arg_spec[arg],
                                               dict(name=arg),
                                               has_default,
                                               positional))
        for argname, argtype in extra_args.items():
            # avoid shadowing args from the function
            if argname in arg_spec:
                continue
            arg_spec[argname] = argtype
            args.append(CephArgtype.to_argdesc(
                argtype, dict(name=argname), has_default=True, positional=False
            ))
        return desc, arg_spec, first_default, ' '.join(args)

    def store_func_metadata(self, f: HandlerFuncType) -> None:
        self.desc, self.arg_spec, self.first_default, self.args = \
            self._load_func_metadata(f)

    def __call__(self, func: HandlerFuncType) -> HandlerFuncType:
        self.store_func_metadata(func)
        self.func = func
        self.COMMANDS[self.prefix] = self
        return self.func

    def _get_arg_value(self, kwargs_switch: bool, key: str, val: Any) -> Tuple[bool, str, Any]:
        def start_kwargs() -> bool:
            if isinstance(val, str) and '=' in val:
                k, v = val.split('=', 1)
                if k in self.arg_spec:
                    return True
            return False

        if not kwargs_switch:
            kwargs_switch = start_kwargs()

        if kwargs_switch:
            k, v = val.split('=', 1)
        else:
            k, v = key, val
        return kwargs_switch, k.replace('-', '_'), v

    def _collect_args_by_argspec(self, cmd_dict: Dict[str, Any]) -> Tuple[Dict[str, Any], Set[str]]:
        kwargs = {}
        special_args = set()
        kwargs_switch = False
        for index, (name, tp) in enumerate(self.arg_spec.items()):
            if name in CLICommand.KNOWN_ARGS:
                special_args.add(name)
                continue
            assert self.first_default >= 0
            raw_v = cmd_dict.get(name)
            if index >= self.first_default:
                if raw_v is None:
                    continue
            kwargs_switch, k, v = self._get_arg_value(kwargs_switch,
                                                      name, raw_v)
            kwargs[k] = CephArgtype.cast_to(tp, v)
        return kwargs, special_args

    def call(self,
             mgr: Any,
             cmd_dict: Dict[str, Any],
             inbuf: Optional[str] = None) -> HandleCommandResult:
        kwargs, specials = self._collect_args_by_argspec(cmd_dict)
        if inbuf:
            if 'inbuf' not in specials:
                return HandleCommandResult(
                    -errno.EINVAL,
                    '',
                    'Invalid command: Input file data (-i) not supported',
                )
            kwargs['inbuf'] = inbuf
        assert self.func
        return self.func(mgr, **kwargs)

    def dump_cmd(self) -> Dict[str, Union[str, bool]]:
        return {
            'cmd': '{} {}'.format(self.prefix, self.args),
            'desc': self.desc,
            'perm': self.perm,
            'poll': self.poll,
        }

    @classmethod
    def dump_cmd_list(cls) -> List[Dict[str, Union[str, bool]]]:
        return [cmd.dump_cmd() for cmd in cls.COMMANDS.values()]


def CLIReadCommand(prefix: str, poll: bool = False) -> CLICommand:
    return CLICommand(prefix, "r", poll)


def CLIWriteCommand(prefix: str, poll: bool = False) -> CLICommand:
    return CLICommand(prefix, "w", poll)


def CLICheckNonemptyFileInput(desc: str) -> Callable[[HandlerFuncType], HandlerFuncType]:
    def CheckFileInput(func: HandlerFuncType) -> HandlerFuncType:
        @functools.wraps(func)
        def check(*args: Any, **kwargs: Any) -> Tuple[int, str, str]:
            if 'inbuf' not in kwargs:
                return -errno.EINVAL, '', f'{ERROR_MSG_NO_INPUT_FILE}: Please specify the file '\
                                          f'containing {desc} with "-i" option'
            if isinstance(kwargs['inbuf'], str):
                # Delete new line separator at EOF (it may have been added by a text editor).
                kwargs['inbuf'] = kwargs['inbuf'].rstrip('\r\n').rstrip('\n')
            if not kwargs['inbuf'] or not kwargs['inbuf'].strip():
                return -errno.EINVAL, '', f'{ERROR_MSG_EMPTY_INPUT_FILE}: Please add {desc} to '\
                                           'the file'
            return func(*args, **kwargs)
        check.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
        return check
    return CheckFileInput

# If the mgr loses its lock on the database because e.g. the pgs were
# transiently down, then close it and allow it to be reopened.
MAX_DBCLEANUP_RETRIES = 3
def MgrModuleRecoverDB(func: Callable) -> Callable:
    @functools.wraps(func)
    def check(self: MgrModule, *args: Any, **kwargs: Any) -> Any:
        retries = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except sqlite3.DatabaseError as e:
                self.log.error(f"Caught fatal database error: {e}")
                retries = retries+1
                if retries > MAX_DBCLEANUP_RETRIES:
                    raise
                self.log.debug(f"attempting reopen of database")
                self.close_db()
                self.open_db();
                # allow retry of func(...)
    check.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return check

def CLIRequiresDB(func: HandlerFuncType) -> HandlerFuncType:
    @functools.wraps(func)
    def check(self: MgrModule, *args: Any, **kwargs: Any) -> Tuple[int, str, str]:
        if not self.db_ready():
            return -errno.EAGAIN, "", "mgr db not yet available"
        return func(self, *args, **kwargs)
    check.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return check

def _get_localized_key(prefix: str, key: str) -> str:
    return '{}/{}'.format(prefix, key)


"""
MODULE_OPTIONS types and Option Class
"""
if TYPE_CHECKING:
    OptionTypeLabel = Literal[
        'uint', 'int', 'str', 'float', 'bool', 'addr', 'addrvec', 'uuid', 'size', 'secs']


# common/options.h: value_t
OptionValue = Optional[Union[bool, int, float, str]]


class Option(Dict):
    """
    Helper class to declare options for MODULE_OPTIONS list.
    TODO: Replace with typing.TypedDict when in python_version >= 3.8
    """

    def __init__(
            self,
            name: str,
            default: OptionValue = None,
            type: 'OptionTypeLabel' = 'str',
            desc: Optional[str] = None,
            long_desc: Optional[str] = None,
            min: OptionValue = None,
            max: OptionValue = None,
            enum_allowed: Optional[List[str]] = None,
            tags: Optional[List[str]] = None,
            see_also: Optional[List[str]] = None,
            runtime: bool = False,
    ):
        super(Option, self).__init__(
            (k, v) for k, v in vars().items()
            if k != 'self' and v is not None)


class Command(dict):
    """
    Helper class to declare options for COMMANDS list.

    It also allows to specify prefix and args separately, as well as storing a
    handler callable.

    Usage:
    >>> def handler(): return 0, "", ""
    >>> Command(prefix="example",
    ...         handler=handler,
    ...         perm='w')
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
        cmd = CLICommand(prefix=self.prefix, perm=self['perm'])
        return cmd(self.returns_command_result(instance, self.handler))


class CPlusPlusHandler(logging.Handler):
    def __init__(self, module_inst: Any):
        super(CPlusPlusHandler, self).__init__()
        self._module = module_inst
        self.setFormatter(logging.Formatter("[{} %(levelname)-4s %(name)s] %(message)s"
                          .format(module_inst.module_name)))

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= self.level:
            self._module._ceph_log(self.format(record))


class ClusterLogHandler(logging.Handler):
    def __init__(self, module_inst: Any):
        super().__init__()
        self._module = module_inst
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        levelmap = {
            logging.DEBUG: MgrModule.ClusterLogPrio.DEBUG,
            logging.INFO: MgrModule.ClusterLogPrio.INFO,
            logging.WARNING: MgrModule.ClusterLogPrio.WARN,
            logging.ERROR: MgrModule.ClusterLogPrio.ERROR,
            logging.CRITICAL: MgrModule.ClusterLogPrio.ERROR,
        }
        level = levelmap[record.levelno]
        if record.levelno >= self.level:
            self._module.cluster_log(self._module.module_name,
                                     level,
                                     self.format(record))


class FileHandler(logging.FileHandler):
    def __init__(self, module_inst: Any):
        path = module_inst.get_ceph_option("log_file")
        idx = path.rfind(".log")
        if idx != -1:
            self.path = "{}.{}.log".format(path[:idx], module_inst.module_name)
        else:
            self.path = "{}.{}".format(path, module_inst.module_name)
        super(FileHandler, self).__init__(self.path, delay=True)
        self.setFormatter(logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)-4s] [%(name)s] %(message)s"))


class MgrModuleLoggingMixin(object):
    def _configure_logging(self,
                           mgr_level: str,
                           module_level: str,
                           cluster_level: str,
                           log_to_file: bool,
                           log_to_cluster: bool) -> None:
        self._mgr_level: Optional[str] = None
        self._module_level: Optional[str] = None
        self._root_logger = logging.getLogger()

        self._unconfigure_logging()

        # the ceph log handler is initialized only once
        self._mgr_log_handler = CPlusPlusHandler(self)
        self._cluster_log_handler = ClusterLogHandler(self)
        self._file_log_handler = FileHandler(self)

        self.log_to_file = log_to_file
        self.log_to_cluster = log_to_cluster

        self._root_logger.addHandler(self._mgr_log_handler)
        if log_to_file:
            self._root_logger.addHandler(self._file_log_handler)
        if log_to_cluster:
            self._root_logger.addHandler(self._cluster_log_handler)

        self._root_logger.setLevel(logging.NOTSET)
        self._set_log_level(mgr_level, module_level, cluster_level)

    def _unconfigure_logging(self) -> None:
        # remove existing handlers:
        rm_handlers = [
            h for h in self._root_logger.handlers
            if (isinstance(h, CPlusPlusHandler) or
                isinstance(h, FileHandler) or
                isinstance(h, ClusterLogHandler))]
        for h in rm_handlers:
            self._root_logger.removeHandler(h)
        self.log_to_file = False
        self.log_to_cluster = False

    def _set_log_level(self,
                       mgr_level: str,
                       module_level: str,
                       cluster_level: str) -> None:
        self._cluster_log_handler.setLevel(cluster_level.upper())

        module_level = module_level.upper() if module_level else ''
        if not self._module_level:
            # using debug_mgr level
            if not module_level and self._mgr_level == mgr_level:
                # no change in module level neither in debug_mgr
                return
        else:
            if self._module_level == module_level:
                # no change in module level
                return

        if not self._module_level and not module_level:
            level = self._ceph_log_level_to_python(mgr_level)
            self.getLogger().debug("setting log level based on debug_mgr: %s (%s)",
                                   level, mgr_level)
        elif self._module_level and not module_level:
            level = self._ceph_log_level_to_python(mgr_level)
            self.getLogger().warning("unsetting module log level, falling back to "
                                     "debug_mgr level: %s (%s)", level, mgr_level)
        elif module_level:
            level = module_level
            self.getLogger().debug("setting log level: %s", level)

        self._module_level = module_level
        self._mgr_level = mgr_level

        self._mgr_log_handler.setLevel(level)
        self._file_log_handler.setLevel(level)

    def _enable_file_log(self) -> None:
        # enable file log
        self.getLogger().warning("enabling logging to file")
        self.log_to_file = True
        self._root_logger.addHandler(self._file_log_handler)

    def _disable_file_log(self) -> None:
        # disable file log
        self.getLogger().warning("disabling logging to file")
        self.log_to_file = False
        self._root_logger.removeHandler(self._file_log_handler)

    def _enable_cluster_log(self) -> None:
        # enable cluster log
        self.getLogger().warning("enabling logging to cluster")
        self.log_to_cluster = True
        self._root_logger.addHandler(self._cluster_log_handler)

    def _disable_cluster_log(self) -> None:
        # disable cluster log
        self.getLogger().warning("disabling logging to cluster")
        self.log_to_cluster = False
        self._root_logger.removeHandler(self._cluster_log_handler)

    def _ceph_log_level_to_python(self, log_level: str) -> str:
        if log_level:
            try:
                ceph_log_level = int(log_level.split("/", 1)[0])
            except ValueError:
                ceph_log_level = 0
        else:
            ceph_log_level = 0

        log_level = "DEBUG"
        if ceph_log_level <= 0:
            log_level = "CRITICAL"
        elif ceph_log_level <= 1:
            log_level = "WARNING"
        elif ceph_log_level <= 4:
            log_level = "INFO"
        return log_level

    def getLogger(self, name: Optional[str] = None) -> logging.Logger:
        return logging.getLogger(name)


class MgrStandbyModule(ceph_module.BaseMgrStandbyModule, MgrModuleLoggingMixin):
    """
    Standby modules only implement a serve and shutdown method, they
    are not permitted to implement commands and they do not receive
    any notifications.

    They only have access to the mgrmap (for accessing service URI info
    from their active peer), and to configuration settings (read only).
    """

    MODULE_OPTIONS: List[Option] = []
    MODULE_OPTION_DEFAULTS = {}  # type: Dict[str, Any]

    def __init__(self, module_name: str, capsule: Any):
        super(MgrStandbyModule, self).__init__(capsule)
        self.module_name = module_name

        # see also MgrModule.__init__()
        for o in self.MODULE_OPTIONS:
            if 'default' in o:
                if 'type' in o:
                    self.MODULE_OPTION_DEFAULTS[o['name']] = o['default']
                else:
                    self.MODULE_OPTION_DEFAULTS[o['name']] = str(o['default'])

        # mock does not return a str
        mgr_level = cast(str, self.get_ceph_option("debug_mgr"))
        log_level = cast(str, self.get_module_option("log_level"))
        cluster_level = cast(str, self.get_module_option('log_to_cluster_level'))
        self._configure_logging(mgr_level, log_level, cluster_level,
                                False, False)

        # for backwards compatibility
        self._logger = self.getLogger()

    @classmethod
    def _register_options(cls, module_name: str) -> None:
        cls.MODULE_OPTIONS.append(
            Option(name='log_level', type='str', default="", runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_file', type='bool', default=False, runtime=True))
        if not [x for x in cls.MODULE_OPTIONS if x['name'] == 'log_to_cluster']:
            cls.MODULE_OPTIONS.append(
                Option(name='log_to_cluster', type='bool', default=False,
                       runtime=True))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_cluster_level', type='str', default='info',
                   runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))

    @property
    def log(self) -> logging.Logger:
        return self._logger

    def serve(self) -> None:
        """
        The serve method is mandatory for standby modules.
        :return:
        """
        raise NotImplementedError()

    def get_mgr_id(self) -> str:
        return self._ceph_get_mgr_id()

    def get_module_option(self, key: str, default: OptionValue = None) -> OptionValue:
        """
        Retrieve the value of a persistent configuration setting

        :param default: the default value of the config if it is not found
        """
        r = self._ceph_get_module_option(key)
        if r is None:
            return self.MODULE_OPTION_DEFAULTS.get(key, default)
        else:
            return r

    def get_ceph_option(self, key: str) -> OptionValue:
        return self._ceph_get_option(key)

    def get_store(self, key: str) -> Optional[str]:
        """
        Retrieve the value of a persistent KV store entry

        :param key: String
        :return: Byte string or None
        """
        return self._ceph_get_store(key)

    def get_localized_store(self, key: str, default: Optional[str] = None) -> Optional[str]:
        r = self._ceph_get_store(_get_localized_key(self.get_mgr_id(), key))
        if r is None:
            r = self._ceph_get_store(key)
            if r is None:
                r = default
        return r

    def get_active_uri(self) -> str:
        return self._ceph_get_active_uri()

    def get(self, data_name: str) -> Dict[str, Any]:
        return self._ceph_get(data_name)

    def get_mgr_ip(self) -> str:
        ips = self.get("mgr_ips").get('ips', [])
        if not ips:
            return socket.gethostname()
        return ips[0]

    def get_hostname(self) -> str:
        return socket.gethostname()

    def get_localized_module_option(self, key: str, default: OptionValue = None) -> OptionValue:
        r = self._ceph_get_module_option(key, self.get_mgr_id())
        if r is None:
            return self.MODULE_OPTION_DEFAULTS.get(key, default)
        else:
            return r


HealthChecksT = Mapping[str, Mapping[str, Union[int, str, Sequence[str]]]]
# {"type": service_type, "id": service_id}
ServiceInfoT = Dict[str, str]
# {"hostname": hostname,
#  "ceph_version": version,
#  "services": [service_info, ..]}
ServerInfoT = Dict[str, Union[str, List[ServiceInfoT]]]
PerfCounterT = Dict[str, Any]


class API:
    def DecoratorFactory(attr: str, default: Any):  # type: ignore
        class DecoratorClass:
            _ATTR_TOKEN = f'__ATTR_{attr.upper()}__'

            def __init__(self, value: Any=default) -> None:
                self.value = value

            def __call__(self, func: Callable) -> Any:
                setattr(func, self._ATTR_TOKEN, self.value)
                return func

            @classmethod
            def get(cls, func: Callable) -> Any:
                return getattr(func, cls._ATTR_TOKEN, default)

        return DecoratorClass

    perm = DecoratorFactory('perm', default='r')
    expose = DecoratorFactory('expose', default=False)(True)


class MgrModule(ceph_module.BaseMgrModule, MgrModuleLoggingMixin):
    MGR_POOL_NAME = ".mgr"

    COMMANDS = []  # type: List[Any]
    MODULE_OPTIONS: List[Option] = []
    MODULE_OPTION_DEFAULTS = {}  # type: Dict[str, Any]

    # Database Schema
    SCHEMA = None # type: Optional[str]
    SCHEMA_VERSIONED = None # type: Optional[List[str]]

    # Priority definitions for perf counters
    PRIO_CRITICAL = 10
    PRIO_INTERESTING = 8
    PRIO_USEFUL = 5
    PRIO_UNINTERESTING = 2
    PRIO_DEBUGONLY = 0

    # counter value types
    PERFCOUNTER_TIME = 1
    PERFCOUNTER_U64 = 2

    # counter types
    PERFCOUNTER_LONGRUNAVG = 4
    PERFCOUNTER_COUNTER = 8
    PERFCOUNTER_HISTOGRAM = 0x10
    PERFCOUNTER_TYPE_MASK = ~3

    # units supported
    BYTES = 0
    NONE = 1

    # Cluster log priorities
    class ClusterLogPrio(IntEnum):
        DEBUG = 0
        INFO = 1
        SEC = 2
        WARN = 3
        ERROR = 4

    def __init__(self, module_name: str, py_modules_ptr: object, this_ptr: object):
        self.module_name = module_name
        super(MgrModule, self).__init__(py_modules_ptr, this_ptr)

        for o in self.MODULE_OPTIONS:
            if 'default' in o:
                if 'type' in o:
                    # we'll assume the declared type matches the
                    # supplied default value's type.
                    self.MODULE_OPTION_DEFAULTS[o['name']] = o['default']
                else:
                    # module not declaring it's type, so normalize the
                    # default value to be a string for consistent behavior
                    # with default and user-supplied option values.
                    self.MODULE_OPTION_DEFAULTS[o['name']] = str(o['default'])

        mgr_level = cast(str, self.get_ceph_option("debug_mgr"))
        log_level = cast(str, self.get_module_option("log_level"))
        cluster_level = cast(str, self.get_module_option('log_to_cluster_level'))
        log_to_file = self.get_module_option("log_to_file")
        assert isinstance(log_to_file, bool)
        log_to_cluster = self.get_module_option("log_to_cluster")
        assert isinstance(log_to_cluster, bool)
        self._configure_logging(mgr_level, log_level, cluster_level,
                                log_to_file, log_to_cluster)

        # for backwards compatibility
        self._logger = self.getLogger()

        self._db = None # type: Optional[sqlite3.Connection]

        self._version = self._ceph_get_version()

        self._perf_schema_cache = None

        # Keep a librados instance for those that need it.
        self._rados: Optional[rados.Rados] = None

        # this does not change over the lifetime of an active mgr
        self._mgr_ips: Optional[str] = None

        self._db_lock = threading.Lock()

    @classmethod
    def _register_options(cls, module_name: str) -> None:
        cls.MODULE_OPTIONS.append(
            Option(name='log_level', type='str', default="", runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_file', type='bool', default=False, runtime=True))
        if not [x for x in cls.MODULE_OPTIONS if x['name'] == 'log_to_cluster']:
            cls.MODULE_OPTIONS.append(
                Option(name='log_to_cluster', type='bool', default=False,
                       runtime=True))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_cluster_level', type='str', default='info',
                   runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))

    @classmethod
    def _register_commands(cls, module_name: str) -> None:
        cls.COMMANDS.extend(CLICommand.dump_cmd_list())

    @property
    def log(self) -> logging.Logger:
        return self._logger

    def cluster_log(self, channel: str, priority: ClusterLogPrio, message: str) -> None:
        """
        :param channel: The log channel. This can be 'cluster', 'audit', ...
        :param priority: The log message priority.
        :param message: The message to log.
        """
        self._ceph_cluster_log(channel, priority.value, message)

    @property
    def version(self) -> str:
        return self._version

    @API.expose
    def pool_exists(self, name: str) -> bool:
        pools = [p['pool_name'] for p in self.get('osd_map')['pools']]
        return name in pools

    @API.expose
    def have_enough_osds(self) -> bool:
        # wait until we have enough OSDs to allow the pool to be healthy
        ready = 0
        for osd in self.get("osd_map")["osds"]:
            if osd["up"] and osd["in"]:
                ready += 1

        need = cast(int, self.get_ceph_option("osd_pool_default_size"))
        return ready >= need

    @API.perm('w')
    @API.expose
    def rename_pool(self, srcpool: str, destpool: str) -> None:
        c = {
            'prefix': 'osd pool rename',
            'format': 'json',
            'srcpool': srcpool,
            'destpool': destpool,
            'yes_i_really_mean_it': True
        }
        self.check_mon_command(c)

    @API.perm('w')
    @API.expose
    def create_pool(self, pool: str) -> None:
        c = {
            'prefix': 'osd pool create',
            'format': 'json',
            'pool': pool,
            'pg_num': 1,
            'pg_num_min': 1,
            'pg_num_max': 32,
            'yes_i_really_mean_it': True
        }
        self.check_mon_command(c)

    @API.perm('w')
    @API.expose
    def appify_pool(self, pool: str, app: str) -> None:
        c = {
            'prefix': 'osd pool application enable',
            'format': 'json',
            'pool': pool,
            'app': app,
            'yes_i_really_mean_it': True
        }
        self.check_mon_command(c)

    @API.perm('w')
    @API.expose
    def create_mgr_pool(self) -> None:
        self.log.info("creating mgr pool")

        ov = self.get_module_option_ex('devicehealth', 'pool_name', 'device_health_metrics')
        devhealth = cast(str, ov)
        if devhealth is not None and self.pool_exists(devhealth):
            self.log.debug("reusing devicehealth pool")
            self.rename_pool(devhealth, self.MGR_POOL_NAME)
            self.appify_pool(self.MGR_POOL_NAME, 'mgr')
        else:
            self.log.debug("creating new mgr pool")
            self.create_pool(self.MGR_POOL_NAME)
            self.appify_pool(self.MGR_POOL_NAME, 'mgr')

    def create_skeleton_schema(self, db: sqlite3.Connection) -> None:
        SQL = """
        CREATE TABLE IF NOT EXISTS MgrModuleKV (
          key TEXT PRIMARY KEY,
          value NOT NULL
        ) WITHOUT ROWID;
        INSERT OR IGNORE INTO MgrModuleKV (key, value) VALUES ('__version', 0);
        """

        db.executescript(SQL)

    def update_schema_version(self, db: sqlite3.Connection, version: int) -> None:
        SQL = "UPDATE OR ROLLBACK MgrModuleKV SET value = ? WHERE key = '__version';"

        db.execute(SQL, (version,))

    def set_kv(self, key: str, value: Any) -> None:
        SQL = "INSERT OR REPLACE INTO MgrModuleKV (key, value) VALUES (?, ?);"

        assert key[:2] != "__"

        self.log.debug(f"set_kv('{key}', '{value}')")

        with self._db_lock, self.db:
            self.db.execute(SQL, (key, value))

    @API.expose
    def get_kv(self, key: str) -> Any:
        SQL = "SELECT value FROM MgrModuleKV WHERE key = ?;"

        assert key[:2] != "__"

        self.log.debug(f"get_kv('{key}')")

        with self._db_lock, self.db:
            cur = self.db.execute(SQL, (key,))
            row = cur.fetchone()
            if row is None:
                return None
            else:
                v = row['value']
                self.log.debug(f" = {v}")
                return v

    def maybe_upgrade(self, db: sqlite3.Connection, version: int) -> None:
        if version <= 0:
            self.log.info(f"creating main.db for {self.module_name}")
            assert self.SCHEMA is not None
            db.executescript(self.SCHEMA)
            self.update_schema_version(db, 1)
        else:
            assert self.SCHEMA_VERSIONED is not None
            latest = len(self.SCHEMA_VERSIONED)
            if latest < version:
                raise RuntimeError(f"main.db version is newer ({version}) than module ({latest})")
            for i in range(version, latest):
                self.log.info(f"upgrading main.db for {self.module_name} from {i-1}:{i}")
                SQL = self.SCHEMA_VERSIONED[i]
                db.executescript(SQL)
            if version < latest:
                self.update_schema_version(db, latest)

    def load_schema(self, db: sqlite3.Connection) -> None:
        SQL = """
        SELECT value FROM MgrModuleKV WHERE key = '__version';
        """

        with db:
            self.create_skeleton_schema(db)
            cur = db.execute(SQL)
            row = cur.fetchone()
            self.maybe_upgrade(db, int(row['value']))
            assert cur.fetchone() is None
            cur.close()

    def configure_db(self, db: sqlite3.Connection) -> None:
        db.execute('PRAGMA FOREIGN_KEYS = 1')
        db.execute('PRAGMA JOURNAL_MODE = PERSIST')
        db.execute('PRAGMA PAGE_SIZE = 65536')
        db.execute('PRAGMA CACHE_SIZE = 64')
        db.execute('PRAGMA TEMP_STORE = memory')
        db.row_factory = sqlite3.Row
        self.load_schema(db)

    def close_db(self) -> None:
        with self._db_lock:
            if self._db is not None:
                self._db.close()
                self._db = None

    def open_db(self) -> Optional[sqlite3.Connection]:
        if not self.pool_exists(self.MGR_POOL_NAME):
            if not self.have_enough_osds():
                return None
            self.create_mgr_pool()
        uri = f"file:///{self.MGR_POOL_NAME}:{self.module_name}/main.db?vfs=ceph";
        self.log.debug(f"using uri {uri}")
        db = sqlite3.connect(uri, check_same_thread=False, uri=True)
        # if libcephsqlite reconnects, update the addrv for blocklist
        with db:
            cur = db.execute('SELECT json_extract(ceph_status(), "$.addr");')
            (addrv,) = cur.fetchone()
            assert addrv is not None
            self.log.debug(f"new libcephsqlite addrv = {addrv}")
            self._ceph_register_client("libcephsqlite", addrv, True)
        self.configure_db(db)
        return db

    @API.expose
    def db_ready(self) -> bool:
        with self._db_lock:
            try:
                return self.db is not None
            except MgrDBNotReady:
                return False

    @property
    def db(self) -> sqlite3.Connection:
        assert self._db_lock.locked()
        if self._db is not None:
            return self._db
        db_allowed = self.get_ceph_option("mgr_pool")
        if not db_allowed:
            raise MgrDBNotReady();
        self._db = self.open_db()
        if self._db is None:
            raise MgrDBNotReady();
        return self._db

    @property
    def release_name(self) -> str:
        """
        Get the release name of the Ceph version, e.g. 'nautilus' or 'octopus'.
        :return: Returns the release name of the Ceph version in lower case.
        :rtype: str
        """
        return self._ceph_get_release_name()

    @API.expose
    def lookup_release_name(self, major: int) -> str:
        return self._ceph_lookup_release_name(major)

    def get_context(self) -> object:
        """
        :return: a Python capsule containing a C++ CephContext pointer
        """
        return self._ceph_get_context()

    def notify(self, notify_type: NotifyType, notify_id: str) -> None:
        """
        Called by the ceph-mgr service to notify the Python plugin
        that new state is available.  This method is *only* called for
        notify_types that are listed in the NOTIFY_TYPES string list
        member of the module class.

        :param notify_type: string indicating what kind of notification,
                            such as osd_map, mon_map, fs_map, mon_status,
                            health, pg_summary, command, service_map
        :param notify_id:  string (may be empty) that optionally specifies
                            which entity is being notified about.  With
                            "command" notifications this is set to the tag
                            ``from send_command``.
        """
        pass

    def _config_notify(self) -> None:
        # check logging options for changes
        mgr_level = cast(str, self.get_ceph_option("debug_mgr"))
        module_level = cast(str, self.get_module_option("log_level"))
        cluster_level = cast(str, self.get_module_option("log_to_cluster_level"))
        assert isinstance(cluster_level, str)
        log_to_file = self.get_module_option("log_to_file", False)
        assert isinstance(log_to_file, bool)
        log_to_cluster = self.get_module_option("log_to_cluster", False)
        assert isinstance(log_to_cluster, bool)
        self._set_log_level(mgr_level, module_level, cluster_level)

        if log_to_file != self.log_to_file:
            if log_to_file:
                self._enable_file_log()
            else:
                self._disable_file_log()
        if log_to_cluster != self.log_to_cluster:
            if log_to_cluster:
                self._enable_cluster_log()
            else:
                self._disable_cluster_log()

        # call module subclass implementations
        self.config_notify()

    def config_notify(self) -> None:
        """
        Called by the ceph-mgr service to notify the Python plugin
        that the configuration may have changed.  Modules will want to
        refresh any configuration values stored in config variables.
        """
        pass

    def serve(self) -> None:
        """
        Called by the ceph-mgr service to start any server that
        is provided by this Python plugin.  The implementation
        of this function should block until ``shutdown`` is called.

        You *must* implement ``shutdown`` if you implement ``serve``
        """
        pass

    def shutdown(self) -> None:
        """
        Called by the ceph-mgr service to request that this
        module drop out of its serve() function.  You do not
        need to implement this if you do not implement serve()

        :return: None
        """
        if self._rados:
            addrs = self._rados.get_addrs()
            self._rados.shutdown()
            self._ceph_unregister_client(None, addrs)
            self._rados = None

    @API.expose
    def get(self, data_name: str) -> Any:
        """
        Called by the plugin to fetch named cluster-wide objects from ceph-mgr.

        :param str data_name: Valid things to fetch are osdmap_crush_map_text,
                osd_map, osd_map_tree, osd_map_crush, config, mon_map, fs_map,
                osd_metadata, pg_summary, io_rate, pg_dump, df, osd_stats,
                health, mon_status, devices, device <devid>, pg_stats,
                pool_stats, pg_ready, osd_ping_times, mgr_map, mgr_ips,
                modified_config_options, service_map, mds_metadata,
                have_local_config_map, osd_pool_stats, pg_status.

        Note:
            All these structures have their own JSON representations: experiment
            or look at the C++ ``dump()`` methods to learn about them.
        """
        obj =  self._ceph_get(data_name)
        if isinstance(obj, bytes):
            obj = json.loads(obj)

        return obj

    def _stattype_to_str(self, stattype: int) -> str:

        typeonly = stattype & self.PERFCOUNTER_TYPE_MASK
        if typeonly == 0:
            return 'gauge'
        if typeonly == self.PERFCOUNTER_LONGRUNAVG:
            # this lie matches the DaemonState decoding: only val, no counts
            return 'counter'
        if typeonly == self.PERFCOUNTER_COUNTER:
            return 'counter'
        if typeonly == self.PERFCOUNTER_HISTOGRAM:
            return 'histogram'

        return ''

    def _perfpath_to_path_labels(self, daemon: str,
                                 path: str) -> Tuple[str, Tuple[str, ...], Tuple[str, ...]]:
        if daemon.startswith('rgw.'):
            label_name = 'instance_id'
            daemon = daemon[len('rgw.'):]
        else:
            label_name = 'ceph_daemon'

        label_names = (label_name,)  # type: Tuple[str, ...]
        labels = (daemon,)  # type: Tuple[str, ...]

        if daemon.startswith('rbd-mirror.'):
            match = re.match(
                r'^rbd_mirror_image_([^/]+)/(?:(?:([^/]+)/)?)(.*)\.(replay(?:_bytes|_latency)?)$',
                path
            )
            if match:
                path = 'rbd_mirror_image_' + match.group(4)
                pool = match.group(1)
                namespace = match.group(2) or ''
                image = match.group(3)
                label_names += ('pool', 'namespace', 'image')
                labels += (pool, namespace, image)

        return path, label_names, labels,

    def _perfvalue_to_value(self, stattype: int, value: Union[int, float]) -> Union[float, int]:
        if stattype & self.PERFCOUNTER_TIME:
            # Convert from ns to seconds
            return value / 1000000000.0
        else:
            return value

    def _unit_to_str(self, unit: int) -> str:
        if unit == self.NONE:
            return "/s"
        elif unit == self.BYTES:
            return "B/s"
        else:
            raise ValueError(f'bad unit "{unit}"')

    @staticmethod
    def to_pretty_iec(n: int) -> str:
        for bits, suffix in [(60, 'Ei'), (50, 'Pi'), (40, 'Ti'), (30, 'Gi'),
                             (20, 'Mi'), (10, 'Ki')]:
            if n > 10 << bits:
                return str(n >> bits) + ' ' + suffix
        return str(n) + ' '

    @staticmethod
    def get_pretty_row(elems: Sequence[str], width: int) -> str:
        """
        Takes an array of elements and returns a string with those elements
        formatted as a table row. Useful for polling modules.

        :param elems: the elements to be printed
        :param width: the width of the terminal
        """
        n = len(elems)
        column_width = int(width / n)

        ret = '|'
        for elem in elems:
            ret += '{0:>{w}} |'.format(elem, w=column_width - 2)

        return ret

    def get_pretty_header(self, elems: Sequence[str], width: int) -> str:
        """
        Like ``get_pretty_row`` but adds dashes, to be used as a table title.

        :param elems: the elements to be printed
        :param width: the width of the terminal
        """
        n = len(elems)
        column_width = int(width / n)

        # dash line
        ret = '+'
        for i in range(0, n):
            ret += '-' * (column_width - 1) + '+'
        ret += '\n'

        # title
        ret += self.get_pretty_row(elems, width)
        ret += '\n'

        # dash line
        ret += '+'
        for i in range(0, n):
            ret += '-' * (column_width - 1) + '+'
        ret += '\n'

        return ret

    @API.expose
    def get_server(self, hostname: str) -> ServerInfoT:
        """
        Called by the plugin to fetch metadata about a particular hostname from
        ceph-mgr.

        This is information that ceph-mgr has gleaned from the daemon metadata
        reported by daemons running on a particular server.

        :param hostname: a hostname
        """
        return cast(ServerInfoT, self._ceph_get_server(hostname))

    @API.expose
    def get_perf_schema(self,
                        svc_type: str,
                        svc_name: str) -> Dict[str,
                                               Dict[str, Dict[str, Union[str, int]]]]:
        """
        Called by the plugin to fetch perf counter schema info.
        svc_name can be nullptr, as can svc_type, in which case
        they are wildcards

        :param str svc_type:
        :param str svc_name:
        :return: list of dicts describing the counters requested
        """
        return self._ceph_get_perf_schema(svc_type, svc_name)

    def get_rocksdb_version(self) -> str:
        """
        Called by the plugin to fetch the latest RocksDB version number.

        :return: str representing the major, minor, and patch RocksDB version numbers
        """
        return self._ceph_get_rocksdb_version()

    @API.expose
    def get_counter(self,
                    svc_type: str,
                    svc_name: str,
                    path: str) -> Dict[str, List[Tuple[float, int]]]:
        """
        Called by the plugin to fetch the latest performance counter data for a
        particular counter on a particular service.

        :param str svc_type:
        :param str svc_name:
        :param str path: a period-separated concatenation of the subsystem and the
            counter name, for example "mds.inodes".
        :return: A dict of counter names to their values. each value is a list of
            of two-tuples of (timestamp, value).  This may be empty if no data is
            available.
        """
        return self._ceph_get_counter(svc_type, svc_name, path)

    @API.expose
    def get_latest_counter(self,
                           svc_type: str,
                           svc_name: str,
                           path: str) -> Dict[str, Union[Tuple[float, int],
                                                         Tuple[float, int, int]]]:
        """
        Called by the plugin to fetch only the newest performance counter data
        point for a particular counter on a particular service.

        :param str svc_type:
        :param str svc_name:
        :param str path: a period-separated concatenation of the subsystem and the
            counter name, for example "mds.inodes".
        :return: A list of two-tuples of (timestamp, value) or three-tuple of
            (timestamp, value, count) is returned.  This may be empty if no
            data is available.
        """
        return self._ceph_get_latest_counter(svc_type, svc_name, path)

    @API.expose
    def list_servers(self) -> List[ServerInfoT]:
        """
        Like ``get_server``, but gives information about all servers (i.e. all
        unique hostnames that have been mentioned in daemon metadata)

        :return: a list of information about all servers
        :rtype: list
        """
        return cast(List[ServerInfoT], self._ceph_get_server(None))

    def get_metadata(self,
                     svc_type: str,
                     svc_id: str,
                     default: Optional[Dict[str, str]] = None) -> Optional[Dict[str, str]]:
        """
        Fetch the daemon metadata for a particular service.

        ceph-mgr fetches metadata asynchronously, so are windows of time during
        addition/removal of services where the metadata is not available to
        modules.  ``None`` is returned if no metadata is available.

        :param str svc_type: service type (e.g., 'mds', 'osd', 'mon')
        :param str svc_id: service id. convert OSD integer IDs to strings when
            calling this
        :rtype: dict, or None if no metadata found
        """
        metadata = self._ceph_get_metadata(svc_type, svc_id)
        if not metadata:
            return default
        return metadata

    @API.expose
    def get_daemon_status(self, svc_type: str, svc_id: str) -> Dict[str, str]:
        """
        Fetch the latest status for a particular service daemon.

        This method may return ``None`` if no status information is
        available, for example because the daemon hasn't fully started yet.

        :param svc_type: string (e.g., 'rgw')
        :param svc_id: string
        :return: dict, or None if the service is not found
        """
        return self._ceph_get_daemon_status(svc_type, svc_id)

    def check_mon_command(self, cmd_dict: dict, inbuf: Optional[str] = None) -> HandleCommandResult:
        """
        Wrapper around :func:`~mgr_module.MgrModule.mon_command`, but raises,
        if ``retval != 0``.
        """

        r = HandleCommandResult(*self.mon_command(cmd_dict, inbuf))
        if r.retval:
            raise MonCommandFailed(f'{cmd_dict["prefix"]} failed: {r.stderr} retval: {r.retval}')
        return r

    def mon_command(self, cmd_dict: dict, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Helper for modules that do simple, synchronous mon command
        execution.

        See send_command for general case.

        :return: status int, out std, err str
        """

        t1 = time.time()
        result = CommandResult()
        self.send_command(result, "mon", "", json.dumps(cmd_dict), "", inbuf)
        r = result.wait()
        t2 = time.time()

        self.log.debug("mon_command: '{0}' -> {1} in {2:.3f}s".format(
            cmd_dict['prefix'], r[0], t2 - t1
        ))

        return r

    def osd_command(self, cmd_dict: dict, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Helper for osd command execution.

        See send_command for general case. Also, see osd/OSD.cc for available commands.

        :param dict cmd_dict: expects a prefix and an osd id, i.e.:
            cmd_dict = {
                'prefix': 'perf histogram dump',
                'id': '0'
            }
        :return: status int, out std, err str
        """
        t1 = time.time()
        result = CommandResult()
        self.send_command(result, "osd", cmd_dict['id'], json.dumps(cmd_dict), "", inbuf)
        r = result.wait()
        t2 = time.time()

        self.log.debug("osd_command: '{0}' -> {1} in {2:.3f}s".format(
            cmd_dict['prefix'], r[0], t2 - t1
        ))

        return r

    def tell_command(self, daemon_type: str, daemon_id: str, cmd_dict: dict, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Helper for `ceph tell` command execution.

        See send_command for general case.

        :param dict cmd_dict: expects a prefix i.e.:
            cmd_dict = {
                'prefix': 'heap',
                'heapcmd': 'stats',
            }
        :return: status int, out std, err str
        """
        t1 = time.time()
        result = CommandResult()
        self.send_command(result, daemon_type, daemon_id, json.dumps(cmd_dict), "", inbuf)
        r = result.wait()
        t2 = time.time()

        self.log.debug("tell_command on {0}.{1}: '{2}' -> {3} in {4:.5f}s".format(
            daemon_type, daemon_id, cmd_dict['prefix'], r[0], t2 - t1
        ))

        return r

    def send_command(
            self,
            result: CommandResult,
            svc_type: str,
            svc_id: str,
            command: str,
            tag: str,
            inbuf: Optional[str] = None) -> None:
        """
        Called by the plugin to send a command to the mon
        cluster.

        :param CommandResult result: an instance of the ``CommandResult``
            class, defined in the same module as MgrModule.  This acts as a
            completion and stores the output of the command.  Use
            ``CommandResult.wait()`` if you want to block on completion.
        :param str svc_type:
        :param str svc_id:
        :param str command: a JSON-serialized command.  This uses the same
            format as the ceph command line, which is a dictionary of command
            arguments, with the extra ``prefix`` key containing the command
            name itself.  Consult MonCommands.h for available commands and
            their expected arguments.
        :param str tag: used for nonblocking operation: when a command
            completes, the ``notify()`` callback on the MgrModule instance is
            triggered, with notify_type set to "command", and notify_id set to
            the tag of the command.
        :param str inbuf: input buffer for sending additional data.
        """
        self._ceph_send_command(result, svc_type, svc_id, command, tag, inbuf)

    def tool_exec(
        self,
        args: List[str],
        timeout: int = 10,
        stdin: Optional[bytes] = None
    ) -> Tuple[int, str, str]:
        try:
            tool = args.pop(0)
            cmd = [
                tool,
                '-k', str(self.get_ceph_option('keyring')),
                '-n', f'mgr.{self.get_mgr_id()}',
            ] + args
            self.log.debug('exec: ' + ' '.join(cmd))
            p = subprocess.run(
                cmd,
                input=stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as ex:
            self.log.error(ex)
            return -errno.ETIMEDOUT, '', str(ex)
        if p.returncode:
            self.log.error(f'Non-zero return from {cmd}: {p.stderr.decode()}')
        return p.returncode, p.stdout.decode(), p.stderr.decode()

    def set_health_checks(self, checks: HealthChecksT) -> None:
        """
        Set the module's current map of health checks.  Argument is a
        dict of check names to info, in this form:

        ::

           {
             'CHECK_FOO': {
               'severity': 'warning',           # or 'error'
               'summary': 'summary string',
               'count': 4,                      # quantify badness
               'detail': [ 'list', 'of', 'detail', 'strings' ],
              },
             'CHECK_BAR': {
               'severity': 'error',
               'summary': 'bars are bad',
               'detail': [ 'too hard' ],
             },
           }

        :param list: dict of health check dicts
        """
        self._ceph_set_health_checks(checks)

    def _handle_command(self,
                        inbuf: str,
                        cmd: Dict[str, Any]) -> Union[HandleCommandResult,
                                                      Tuple[int, str, str]]:
        if cmd['prefix'] not in CLICommand.COMMANDS:
            return self.handle_command(inbuf, cmd)

        return CLICommand.COMMANDS[cmd['prefix']].call(self, cmd, inbuf)

    def handle_command(self,
                       inbuf: str,
                       cmd: Dict[str, Any]) -> Union[HandleCommandResult,
                                                     Tuple[int, str, str]]:
        """
        Called by ceph-mgr to request the plugin to handle one
        of the commands that it declared in self.COMMANDS

        Return a status code, an output buffer, and an
        output string.  The output buffer is for data results,
        the output string is for informative text.

        :param inbuf: content of any "-i <file>" supplied to ceph cli
        :type inbuf: str
        :param cmd: from Ceph's cmdmap_t
        :type cmd: dict

        :return: HandleCommandResult or a 3-tuple of (int, str, str)
        """

        # Should never get called if they didn't declare
        # any ``COMMANDS``
        raise NotImplementedError()

    def get_mgr_id(self) -> str:
        """
        Retrieve the name of the manager daemon where this plugin
        is currently being executed (i.e. the active manager).

        :return: str
        """
        return self._ceph_get_mgr_id()

    @API.expose
    def get_ceph_conf_path(self) -> str:
        return self._ceph_get_ceph_conf_path()

    @API.expose
    def get_mgr_ip(self) -> str:
        if not self._mgr_ips:
            ips = self.get("mgr_ips").get('ips', [])
            if not ips:
                return socket.gethostname()
            self._mgr_ips = ips[0]
        assert self._mgr_ips is not None
        return self._mgr_ips

    @API.expose
    def get_hostname(self) -> str:
        return socket.gethostname()

    @API.expose
    def get_ceph_option(self, key: str) -> OptionValue:
        return self._ceph_get_option(key)

    @API.expose
    def get_foreign_ceph_option(self, entity: str, key: str) -> OptionValue:
        return self._ceph_get_foreign_option(entity, key)

    def _validate_module_option(self, key: str) -> None:
        """
        Helper: don't allow get/set config callers to
        access config options that they didn't declare
        in their schema.
        """
        if key not in [o['name'] for o in self.MODULE_OPTIONS]:
            raise RuntimeError("Config option '{0}' is not in {1}.MODULE_OPTIONS".
                               format(key, self.__class__.__name__))

    def _get_module_option(self,
                           key: str,
                           default: OptionValue,
                           localized_prefix: str = "") -> OptionValue:
        r = self._ceph_get_module_option(self.module_name, key,
                                         localized_prefix)
        if r is None:
            return self.MODULE_OPTION_DEFAULTS.get(key, default)
        else:
            return r

    def get_module_option(self, key: str, default: OptionValue = None) -> OptionValue:
        """
        Retrieve the value of a persistent configuration setting
        """
        self._validate_module_option(key)
        return self._get_module_option(key, default)

    def get_module_option_ex(self, module: str,
                             key: str,
                             default: OptionValue = None) -> OptionValue:
        """
        Retrieve the value of a persistent configuration setting
        for the specified module.

        :param module: The name of the module, e.g. 'dashboard'
            or 'telemetry'.
        :param key: The configuration key, e.g. 'server_addr'.
        :param default: The default value to use when the
            returned value is ``None``. Defaults to ``None``.
        """
        if module == self.module_name:
            self._validate_module_option(key)
        r = self._ceph_get_module_option(module, key)
        return default if r is None else r

    @API.expose
    def get_store_prefix(self, key_prefix: str) -> Dict[str, str]:
        """
        Retrieve a dict of KV store keys to values, where the keys
        have the given prefix

        :param str key_prefix:
        :return: str
        """
        return self._ceph_get_store_prefix(key_prefix)

    def _set_localized(self,
                       key: str,
                       val: Optional[str],
                       setter: Callable[[str, Optional[str]], None]) -> None:
        return setter(_get_localized_key(self.get_mgr_id(), key), val)

    def get_localized_module_option(self, key: str, default: OptionValue = None) -> OptionValue:
        """
        Retrieve localized configuration for this ceph-mgr instance
        """
        self._validate_module_option(key)
        return self._get_module_option(key, default, self.get_mgr_id())

    def _set_module_option(self, key: str, val: Any) -> None:
        return self._ceph_set_module_option(self.module_name, key,
                                            None if val is None else str(val))

    def set_module_option(self, key: str, val: Any) -> None:
        """
        Set the value of a persistent configuration setting

        :param str key:
        :type val: str | None
        :raises ValueError: if `val` cannot be parsed or it is out of the specified range
        """
        self._validate_module_option(key)
        return self._set_module_option(key, val)

    def set_module_option_ex(self, module: str, key: str, val: OptionValue) -> None:
        """
        Set the value of a persistent configuration setting
        for the specified module.

        :param str module:
        :param str key:
        :param str val:
        """
        if module == self.module_name:
            self._validate_module_option(key)
        return self._ceph_set_module_option(module, key, str(val))

    @API.perm('w')
    @API.expose
    def set_localized_module_option(self, key: str, val: Optional[str]) -> None:
        """
        Set localized configuration for this ceph-mgr instance
        :param str key:
        :param str val:
        :return: str
        """
        self._validate_module_option(key)
        return self._set_localized(key, val, self._set_module_option)

    @API.perm('w')
    @API.expose
    def set_store(self, key: str, val: Optional[str]) -> None:
        """
        Set a value in this module's persistent key value store.
        If val is None, remove key from store
        """
        self._ceph_set_store(key, val)

    @API.expose
    def get_store(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a value from this module's persistent key value store
        """
        r = self._ceph_get_store(key)
        if r is None:
            return default
        else:
            return r

    @API.expose
    def get_localized_store(self, key: str, default: Optional[str] = None) -> Optional[str]:
        r = self._ceph_get_store(_get_localized_key(self.get_mgr_id(), key))
        if r is None:
            r = self._ceph_get_store(key)
            if r is None:
                r = default
        return r

    @API.perm('w')
    @API.expose
    def set_localized_store(self, key: str, val: Optional[str]) -> None:
        return self._set_localized(key, val, self.set_store)

    def self_test(self) -> Optional[str]:
        """
        Run a self-test on the module. Override this function and implement
        a best as possible self-test for (automated) testing of the module

        Indicate any failures by raising an exception.  This does not have
        to be pretty, it's mainly for picking up regressions during
        development, rather than use in the field.

        :return: None, or an advisory string for developer interest, such
                 as a json dump of some state.
        """
        pass

    def get_osdmap(self) -> OSDMap:
        """
        Get a handle to an OSDMap.  If epoch==0, get a handle for the latest
        OSDMap.
        :return: OSDMap
        """
        return cast(OSDMap, self._ceph_get_osdmap())

    @API.expose
    def get_latest(self, daemon_type: str, daemon_name: str, counter: str) -> int:
        data = self.get_latest_counter(
            daemon_type, daemon_name, counter)[counter]
        if data:
            return data[1]
        else:
            return 0

    @API.expose
    def get_latest_avg(self, daemon_type: str, daemon_name: str, counter: str) -> Tuple[int, int]:
        data = self.get_latest_counter(
            daemon_type, daemon_name, counter)[counter]
        if data:
            # https://github.com/python/mypy/issues/1178
            _, value, count = cast(Tuple[float, int, int], data)
            return value, count
        else:
            return 0, 0

    @API.expose
    @profile_method()
    def get_unlabeled_perf_counters(self, prio_limit: int = PRIO_USEFUL,
                              services: Sequence[str] = ("mds", "mon", "osd",
                                                         "rbd-mirror", "rgw",
                                                         "tcmu-runner")) -> Dict[str, dict]:
        """
        Return the perf counters currently known to this ceph-mgr
        instance, filtered by priority equal to or greater than `prio_limit`.

        The result is a map of string to dict, associating services
        (like "osd.123") with their counters.  The counter
        dict for each service maps counter paths to a counter
        info structure, which is the information from
        the schema, plus an additional "value" member with the latest
        value.
        """

        result = defaultdict(dict)  # type: Dict[str, dict]

        for server in self.list_servers():
            for service in cast(List[ServiceInfoT], server['services']):
                if service['type'] not in services:
                    continue

                schemas = self.get_perf_schema(service['type'], service['id'])
                if not schemas:
                    self.log.warning("No perf counter schema for {0}.{1}".format(
                        service['type'], service['id']
                    ))
                    continue

                # Value is returned in a potentially-multi-service format,
                # get just the service we're asking about
                svc_full_name = "{0}.{1}".format(
                    service['type'], service['id'])
                schema = schemas[svc_full_name]

                # Populate latest values
                for counter_path, counter_schema in schema.items():
                    # self.log.debug("{0}: {1}".format(
                    #     counter_path, json.dumps(counter_schema)
                    # ))
                    priority = counter_schema['priority']
                    assert isinstance(priority, int)
                    if priority < prio_limit:
                        continue

                    tp = counter_schema['type']
                    assert isinstance(tp, int)
                    counter_info = dict(counter_schema)
                    # Also populate count for the long running avgs
                    if tp & self.PERFCOUNTER_LONGRUNAVG:
                        v, c = self.get_latest_avg(
                            service['type'],
                            service['id'],
                            counter_path
                        )
                        counter_info['value'], counter_info['count'] = v, c
                        result[svc_full_name][counter_path] = counter_info
                    else:
                        counter_info['value'] = self.get_latest(
                            service['type'],
                            service['id'],
                            counter_path
                        )

                    result[svc_full_name][counter_path] = counter_info

        self.log.debug("returning {0} counter".format(len(result)))

        return result

    @API.expose
    def set_uri(self, uri: str) -> None:
        """
        If the module exposes a service, then call this to publish the
        address once it is available.

        :return: a string
        """
        return self._ceph_set_uri(uri)

    @API.perm('w')
    @API.expose
    def set_device_wear_level(self, devid: str, wear_level: float) -> None:
        return self._ceph_set_device_wear_level(devid, wear_level)

    @API.expose
    def have_mon_connection(self) -> bool:
        """
        Check whether this ceph-mgr daemon has an open connection
        to a monitor.  If it doesn't, then it's likely that the
        information we have about the cluster is out of date,
        and/or the monitor cluster is down.
        """

        return self._ceph_have_mon_connection()

    def update_progress_event(self,
                              evid: str,
                              desc: str,
                              progress: float,
                              add_to_ceph_s: bool) -> None:
        return self._ceph_update_progress_event(evid, desc, progress, add_to_ceph_s)

    @API.perm('w')
    @API.expose
    def complete_progress_event(self, evid: str) -> None:
        return self._ceph_complete_progress_event(evid)

    @API.perm('w')
    @API.expose
    def clear_all_progress_events(self) -> None:
        return self._ceph_clear_all_progress_events()

    @property
    def rados(self) -> rados.Rados:
        """
        A librados instance to be shared by any classes within
        this mgr module that want one.
        """
        if self._rados:
            return self._rados

        ctx_capsule = self.get_context()
        self._rados = rados.Rados(context=ctx_capsule)
        self._rados.connect()
        self._ceph_register_client(None, self._rados.get_addrs(), False)
        return self._rados

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        """
        Implement this function to report whether the module's dependencies
        are met.  For example, if the module needs to import a particular
        dependency to work, then use a try/except around the import at
        file scope, and then report here if the import failed.

        This will be called in a blocking way from the C++ code, so do not
        do any I/O that could block in this function.

        :return a 2-tuple consisting of a boolean and explanatory string
        """

        return True, ""

    @API.expose
    def remote(self, module_name: str, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Invoke a method on another module.  All arguments, and the return
        value from the other module must be serializable.

        Limitation: Do not import any modules within the called method.
        Otherwise you will get an error in Python 2::

            RuntimeError('cannot unmarshal code objects in restricted execution mode',)



        :param module_name: Name of other module.  If module isn't loaded,
                            an ImportError exception is raised.
        :param method_name: Method name.  If it does not exist, a NameError
                            exception is raised.
        :param args: Argument tuple
        :param kwargs: Keyword argument dict
        :raises RuntimeError: **Any** error raised within the method is converted to a RuntimeError
        :raises ImportError: No such module
        """
        return self._ceph_dispatch_remote(module_name, method_name,
                                          args, kwargs)

    def add_osd_perf_query(self, query: Dict[str, Any]) -> Optional[int]:
        """
        Register an OSD perf query.  Argument is a
        dict of the query parameters, in this form:

        ::

           {
             'key_descriptor': [
               {'type': subkey_type, 'regex': regex_pattern},
               ...
             ],
             'performance_counter_descriptors': [
               list, of, descriptor, types
             ],
             'limit': {'order_by': performance_counter_type, 'max_count': n},
           }

        Valid subkey types:
           'client_id', 'client_address', 'pool_id', 'namespace', 'osd_id',
           'pg_id', 'object_name', 'snap_id'
        Valid performance counter types:
           'ops', 'write_ops', 'read_ops', 'bytes', 'write_bytes', 'read_bytes',
           'latency', 'write_latency', 'read_latency'

        :param object query: query
        :rtype: int (query id)
        """
        return self._ceph_add_osd_perf_query(query)

    @API.perm('w')
    @API.expose
    def remove_osd_perf_query(self, query_id: int) -> None:
        """
        Unregister an OSD perf query.

        :param int query_id: query ID
        """
        return self._ceph_remove_osd_perf_query(query_id)

    @API.expose
    def get_osd_perf_counters(self, query_id: int) -> Optional[Dict[str, List[PerfCounterT]]]:
        """
        Get stats collected for an OSD perf query.

        :param int query_id: query ID
        """
        return self._ceph_get_osd_perf_counters(query_id)

    def add_mds_perf_query(self, query: Dict[str, Any]) -> Optional[int]:
        """
        Register an MDS perf query.  Argument is a
        dict of the query parameters, in this form:

        ::

           {
             'key_descriptor': [
               {'type': subkey_type, 'regex': regex_pattern},
               ...
             ],
             'performance_counter_descriptors': [
               list, of, descriptor, types
             ],
           }

        NOTE: 'limit' and 'order_by' are not supported (yet).

        Valid subkey types:
           'mds_rank', 'client_id'
        Valid performance counter types:
           'cap_hit_metric'

        :param object query: query
        :rtype: int (query id)
        """
        return self._ceph_add_mds_perf_query(query)

    @API.perm('w')
    @API.expose
    def remove_mds_perf_query(self, query_id: int) -> None:
        """
        Unregister an MDS perf query.

        :param int query_id: query ID
        """
        return self._ceph_remove_mds_perf_query(query_id)

    @API.expose

    def reregister_mds_perf_queries(self) -> None:
        """
        Re-register MDS perf queries.
        """
        return self._ceph_reregister_mds_perf_queries()

    def get_mds_perf_counters(self, query_id: int) -> Optional[Dict[str, List[PerfCounterT]]]:
        """
        Get stats collected for an MDS perf query.

        :param int query_id: query ID
        """
        return self._ceph_get_mds_perf_counters(query_id)

    def get_daemon_health_metrics(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get the list of health metrics per daemon. This includes SLOW_OPS health metrics
        in MON and OSD daemons, and PENDING_CREATING_PGS health metrics for OSDs.
        """
        return self._ceph_get_daemon_health_metrics()

    def is_authorized(self, arguments: Dict[str, str]) -> bool:
        """
        Verifies that the current session caps permit executing the py service
        or current module with the provided arguments. This provides a generic
        way to allow modules to restrict by more fine-grained controls (e.g.
        pools).

        :param arguments: dict of key/value arguments to test
        """
        return self._ceph_is_authorized(arguments)

    @API.expose
    def send_rgwadmin_command(self, args: List[str],
                              stdout_as_json: bool = True) -> Tuple[int, Union[str, dict], str]:
        try:
            cmd = [
                    'radosgw-admin',
                    '-c', str(self.get_ceph_conf_path()),
                    '-k', str(self.get_ceph_option('keyring')),
                    '-n', f'mgr.{self.get_mgr_id()}',
                ] + args
            self.log.debug('Executing %s', str(cmd))
            result = subprocess.run(  # pylint: disable=subprocess-run-check
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
            )
            stdout = result.stdout.decode('utf-8')
            stderr = result.stderr.decode('utf-8')
            if stdout and stdout_as_json:
                stdout = json.loads(stdout)
            if result.returncode:
                self.log.debug('Error %s executing %s: %s', result.returncode, str(cmd), stderr)
            return result.returncode, stdout, stderr
        except subprocess.CalledProcessError as ex:
            self.log.exception('Error executing radosgw-admin %s: %s', str(ex.cmd), str(ex.output))
            raise
        except subprocess.TimeoutExpired as ex:
            self.log.error('Timeout (10s) executing radosgw-admin %s', str(ex.cmd))
            raise
