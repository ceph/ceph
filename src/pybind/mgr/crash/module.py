import hashlib
from mgr_module import CLICommand, CLIReadCommand, CLIWriteCommand, MgrModule, Option
import datetime
import errno
import functools
import inspect
import json
from collections import defaultdict
from prettytable import PrettyTable
import re
from threading import Event, Lock
from typing import cast, Any, Callable, DefaultDict, Dict, Iterable, List, Optional, Tuple, TypeVar, \
    Union, TYPE_CHECKING


DATEFMT = '%Y-%m-%dT%H:%M:%S.%f'
OLD_DATEFMT = '%Y-%m-%d %H:%M:%S.%f'

MAX_WAIT = 600
MIN_WAIT = 60


FuncT = TypeVar('FuncT', bound=Callable)


def with_crashes(func: FuncT) -> FuncT:
    @functools.wraps(func)
    def wrapper(self: 'Module', *args: Any, **kwargs: Any) -> Tuple[int, str, str]:
        with self.crashes_lock:
            if not self.crashes:
                self._load_crashes()
            return func(self, *args, **kwargs)
    wrapper.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return cast(FuncT, wrapper)


CrashT = Dict[str, Union[str, List[str]]]


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(
            name='warn_recent_interval',
            type='secs',
            default=60 * 60 * 24 * 14,
            desc='time interval in which to warn about recent crashes',
            runtime=True),
        Option(
            name='retain_interval',
            type='secs',
            default=60 * 60 * 24 * 365,
            desc='how long to retain crashes before pruning them',
            runtime=True),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.crashes: Optional[Dict[str, CrashT]] = None
        self.crashes_lock = Lock()
        self.run = True
        self.event = Event()
        if TYPE_CHECKING:
            self.warn_recent_interval = 0.0
            self.retain_interval = 0.0

    def shutdown(self) -> None:
        self.run = False
        self.event.set()

    def serve(self) -> None:
        self.config_notify()
        while self.run:
            with self.crashes_lock:
                self._refresh_health_checks()
                self._prune(self.retain_interval)
            wait = min(MAX_WAIT, max(self.warn_recent_interval / 100, MIN_WAIT))
            self.event.wait(wait)
            self.event.clear()

    def config_notify(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))

    def _load_crashes(self) -> None:
        raw = self.get_store_prefix('crash/')
        self.crashes = {k[6:]: json.loads(m) for (k, m) in raw.items()}

    def _refresh_health_checks(self) -> None:
        if not self.crashes:
            self._load_crashes()
        assert self.crashes is not None
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self.warn_recent_interval)
        recent = {
            crashid: crash for crashid, crash in self.crashes.items()
            if (self.time_from_string(cast(str, crash['timestamp'])) > cutoff
                and 'archived' not in crash)
        }

        def prune_detail(ls: List[str]) -> int:
            num = len(ls)
            if num > 30:
                ls = ls[0:30]
                ls.append('and %d more' % (num - 30))
            return num

        daemon_crashes = []
        module_crashes = []
        for c in recent.values():
            if 'mgr_module' in c:
                module_crashes.append(c)
            else:
                daemon_crashes.append(c)
        daemon_detail = [
            '%s crashed on host %s at %s' % (
                crash.get('entity_name', 'unidentified daemon'),
                crash.get('utsname_hostname', '(unknown)'),
                crash.get('timestamp', 'unknown time'))
            for crash in daemon_crashes]
        module_detail = [
            'mgr module %s crashed in daemon %s on host %s at %s' % (
                crash.get('mgr_module', 'unidentified module'),
                crash.get('entity_name', 'unidentified daemon'),
                crash.get('utsname_hostname', '(unknown)'),
                crash.get('timestamp', 'unknown time'))
            for crash in module_crashes]
        daemon_num = prune_detail(daemon_detail)
        module_num = prune_detail(module_detail)

        health_checks: Dict[str, Dict[str, Union[int, str, List[str]]]] = {}
        if daemon_detail:
            self.log.debug('daemon detail %s' % daemon_detail)
            health_checks['RECENT_CRASH'] = {
                'severity': 'warning',
                'summary': '%d daemons have recently crashed' % (daemon_num),
                'count': daemon_num,
                'detail': daemon_detail,
            }
        if module_detail:
            self.log.debug('module detail %s' % module_detail)
            health_checks['RECENT_MGR_MODULE_CRASH'] = {
                'severity': 'warning',
                'summary': '%d mgr modules have recently crashed' % (module_num),
                'count': module_num,
                'detail': module_detail,
            }

        self.set_health_checks(health_checks)

    def time_from_string(self, timestr: str) -> datetime.datetime:
        # drop the 'Z' timezone indication, it's always UTC
        timestr = timestr.rstrip('Z')
        try:
            return datetime.datetime.strptime(timestr, DATEFMT)
        except ValueError:
            return datetime.datetime.strptime(timestr, OLD_DATEFMT)

    def validate_crash_metadata(self, inbuf: str) -> Dict[str, Union[str, List[str]]]:
        # raise any exceptions to caller
        metadata = json.loads(inbuf)
        for f in ['crash_id', 'timestamp']:
            if f not in metadata:
                raise AttributeError("missing '%s' field" % f)
        _ = self.time_from_string(metadata['timestamp'])
        return metadata

    def timestamp_filter(self, f: Callable[[datetime.datetime], bool]) -> Iterable[Tuple[str, CrashT]]:
        """
        Filter crash reports by timestamp.

        :param f: f(time) return true to keep crash report
        :returns: crash reports for which f(time) returns true
        """
        def inner(pair: Tuple[str, CrashT]) -> bool:
            _, crash = pair
            time = self.time_from_string(cast(str, crash["timestamp"]))
            return f(time)
        assert self.crashes is not None
        return filter(inner, self.crashes.items())

    # stack signature helpers

    def sanitize_backtrace(self, bt: List[str]) -> List[str]:
        ret = list()
        for func_record in bt:
            # split into two fields on last space, take the first one,
            # strip off leading ( and trailing )
            func_plus_offset = func_record.rsplit(' ', 1)[0][1:-1]
            ret.append(func_plus_offset.split('+')[0])

        return ret

    ASSERT_MATCHEXPR = re.compile(r'(?s)(.*) thread .* time .*(: .*)\n')

    def sanitize_assert_msg(self, msg: str) -> str:
        # (?s) allows matching newline.  get everything up to "thread" and
        # then after-and-including the last colon-space.  This skips the
        # thread id, timestamp, and file:lineno, because file is already in
        # the beginning, and lineno may vary.
        matched = self.ASSERT_MATCHEXPR.match(msg)
        assert matched
        return ''.join(matched.groups())

    def calc_sig(self, bt: List[str], assert_msg: Optional[str]) -> str:
        sig = hashlib.sha256()
        for func in self.sanitize_backtrace(bt):
            sig.update(func.encode())
        if assert_msg:
            sig.update(self.sanitize_assert_msg(assert_msg).encode())
        return ''.join('%02x' % c for c in sig.digest())

    # command handlers

    @CLIReadCommand('crash info')
    @with_crashes
    def do_info(self, id: str) -> Tuple[int, str, str]:
        """
        show crash dump metadata
        """
        crashid = id
        assert self.crashes is not None
        crash = self.crashes.get(crashid)
        if not crash:
            return errno.EINVAL, '', 'crash info: %s not found' % crashid
        val = json.dumps(crash, indent=4, sort_keys=True)
        return 0, val, ''

    @CLICommand('crash post')
    def do_post(self, inbuf: str) -> Tuple[int, str, str]:
        """
        Add a crash dump (use -i <jsonfile>)
        """
        try:
            metadata = self.validate_crash_metadata(inbuf)
        except Exception as e:
            return errno.EINVAL, '', 'malformed crash metadata: %s' % e
        if 'backtrace' in metadata:
            backtrace = cast(List[str], metadata.get('backtrace'))
            assert_msg = cast(Optional[str], metadata.get('assert_msg'))
            metadata['stack_sig'] = self.calc_sig(backtrace, assert_msg)
        crashid = cast(str, metadata['crash_id'])
        assert self.crashes is not None
        if crashid not in self.crashes:
            self.crashes[crashid] = metadata
            key = 'crash/%s' % crashid
            self.set_store(key, json.dumps(metadata))
            self._refresh_health_checks()
        return 0, '', ''

    def ls(self) -> Tuple[int, str, str]:
        if not self.crashes:
            self._load_crashes()
        return self.do_ls_all('')

    def _do_ls(self, t: Iterable[CrashT], format: Optional[str]) -> Tuple[int, str, str]:
        r = sorted(t, key=lambda i: i['crash_id'])
        if format in ('json', 'json-pretty'):
            return 0, json.dumps(r, indent=4, sort_keys=True), ''
        else:
            table = PrettyTable(['ID', 'ENTITY', 'NEW'],
                                border=False)
            table.left_padding_width = 0
            table.right_padding_width = 2
            table.align['ID'] = 'l'
            table.align['ENTITY'] = 'l'
            for c in r:
                table.add_row([c.get('crash_id'),
                               c.get('entity_name', 'unknown'),
                               '' if 'archived' in c else '*'])
            return 0, table.get_string(), ''

    @CLIReadCommand('crash ls')
    @with_crashes
    def do_ls_all(self, format: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Show new and archived crash dumps
        """
        assert self.crashes is not None
        return self._do_ls(self.crashes.values(), format)

    @CLIReadCommand('crash ls-new')
    @with_crashes
    def do_ls_new(self, format: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Show new crash dumps
        """
        assert self.crashes is not None
        t = [crash for crashid, crash in self.crashes.items()
             if 'archived' not in crash]
        return self._do_ls(t, format)

    @CLICommand('crash rm')
    @with_crashes
    def do_rm(self, id: str) -> Tuple[int, str, str]:
        """
        Remove a saved crash <id>
        """
        crashid = id
        assert self.crashes is not None
        if crashid in self.crashes:
            del self.crashes[crashid]
            key = 'crash/%s' % crashid
            self.set_store(key, None)       # removes key
            self._refresh_health_checks()
        return 0, '', ''

    @CLICommand('crash prune')
    @with_crashes
    def do_prune(self, keep: int) -> Tuple[int, str, str]:
        """
        Remove crashes older than <keep> days
        """
        self._prune(keep * datetime.timedelta(days=1).total_seconds())
        return 0, '', ''

    def _prune(self, seconds: float) -> None:
        now = datetime.datetime.utcnow()
        cutoff = now - datetime.timedelta(seconds=seconds)
        removed_any = False
        # make a copy of the list, since we'll modify self.crashes below
        to_prune = list(self.timestamp_filter(lambda ts: ts <= cutoff))
        assert self.crashes is not None
        for crashid, crash in to_prune:
            del self.crashes[crashid]
            key = 'crash/%s' % crashid
            self.set_store(key, None)
            removed_any = True
        if removed_any:
            self._refresh_health_checks()

    @CLIWriteCommand('crash archive')
    @with_crashes
    def do_archive(self, id: str) -> Tuple[int, str, str]:
        """
        Acknowledge a crash and silence health warning(s)
        """
        crashid = id
        assert self.crashes is not None
        crash = self.crashes.get(crashid)
        if not crash:
            return errno.EINVAL, '', 'crash info: %s not found' % crashid
        if not crash.get('archived'):
            crash['archived'] = str(datetime.datetime.utcnow())
            self.crashes[crashid] = crash
            key = 'crash/%s' % crashid
            self.set_store(key, json.dumps(crash))
            self._refresh_health_checks()
        return 0, '', ''

    @CLIWriteCommand('crash archive-all')
    @with_crashes
    def do_archive_all(self) -> Tuple[int, str, str]:
        """
        Acknowledge all new crashes and silence health warning(s)
        """
        assert self.crashes is not None
        for crashid, crash in self.crashes.items():
            if not crash.get('archived'):
                crash['archived'] = str(datetime.datetime.utcnow())
                self.crashes[crashid] = crash
                key = 'crash/%s' % crashid
                self.set_store(key, json.dumps(crash))
        self._refresh_health_checks()
        return 0, '', ''

    @CLIReadCommand('crash stat')
    @with_crashes
    def do_stat(self) -> Tuple[int, str, str]:
        """
        Summarize recorded crashes
        """
        # age in days for reporting, ordered smallest first
        AGE_IN_DAYS = [1, 3, 7]
        retlines = list()

        BinnedStatsT = Dict[str, Union[int, datetime.datetime, List[str]]]

        def binstr(bindict: BinnedStatsT) -> str:
            binlines = list()
            id_list = cast(List[str], bindict['idlist'])
            count = len(id_list)
            if count:
                binlines.append(
                    '%d older than %s days old:' % (count, bindict['age'])
                )
                for crashid in id_list:
                    binlines.append(crashid)
            return '\n'.join(binlines)

        total = 0
        now = datetime.datetime.utcnow()
        bins: List[BinnedStatsT] = []
        for age in AGE_IN_DAYS:
            agelimit = now - datetime.timedelta(days=age)
            bins.append({
                'age': age,
                'agelimit': agelimit,
                'idlist': list()
            })

        assert self.crashes is not None
        for crashid, crash in self.crashes.items():
            total += 1
            stamp = self.time_from_string(cast(str, crash['timestamp']))
            for bindict in bins:
                if stamp <= cast(datetime.datetime, bindict['agelimit']):
                    cast(List[str], bindict['idlist']).append(crashid)
                    # don't count this one again
                    continue

        retlines.append('%d crashes recorded' % total)

        for bindict in bins:
            retlines.append(binstr(bindict))
        return 0, '\n'.join(retlines), ''

    @CLIReadCommand('crash json_report')
    @with_crashes
    def do_json_report(self, hours: int) -> Tuple[int, str, str]:
        """
        Crashes in the last <hours> hours
        """
        # Return a machine readable summary of recent crashes.
        report: DefaultDict[str, int] = defaultdict(lambda: 0)
        assert self.crashes is not None
        for crashid, crash in self.crashes.items():
            pname = cast(str, crash.get("process_name", "unknown"))
            if not pname:
                pname = "unknown"
            report[pname] += 1

        return 0, '', json.dumps(report, sort_keys=True)

    def self_test(self) -> None:
        # test time conversion
        timestr = '2018-06-22T20:35:38.058818Z'
        old_timestr = '2018-06-22 20:35:38.058818Z'
        dt = self.time_from_string(timestr)
        if dt != datetime.datetime(2018, 6, 22, 20, 35, 38, 58818):
            raise RuntimeError('time_from_string() failed')
        dt = self.time_from_string(old_timestr)
        if dt != datetime.datetime(2018, 6, 22, 20, 35, 38, 58818):
            raise RuntimeError('time_from_string() (old) failed')
