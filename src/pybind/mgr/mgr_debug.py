"""
On-demand Python introspection for ceph-mgr, called from the admin socket.

Nothing here runs unless an operator asks for it: there are no hooks or
idle background threads, and tracemalloc is only active between
"py mem start" and "py mem stop".

Admin socket clients give up after 10 seconds, so slow requests (sampling,
memory reports on a large heap) run in a worker thread.  If the result is
not ready in time, the caller is told to repeat the same command to
collect it.
"""

import functools
import os
import sys
import threading
import time
import tracemalloc
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

MAX_DURATION = 60.0
MAX_RATE = 1000
REPLY_WAIT = 8.0

_mgr_path = os.path.dirname(os.path.abspath(__file__)) + os.sep
_baseline: Optional[tracemalloc.Snapshot] = None
_SKIP_FILES = (tracemalloc.__file__, __file__)
# threads running this code, hidden from stack dumps and samples
_own_threads: Set[int] = set()


class _Job:
    def __init__(self, key: str, fn: Callable[[], str]) -> None:
        self.key = key
        self.result: Optional[str] = None
        self.error: Optional[BaseException] = None
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, args=(fn,),
                                       name='mgr_debug', daemon=True)
        self.thread.start()

    def _run(self, fn: Callable[[], str]) -> None:
        _own_threads.add(threading.get_ident())
        try:
            self.result = fn()
        except BaseException as e:
            self.error = e
        finally:
            _own_threads.discard(threading.get_ident())


_job: Optional[_Job] = None


def _run_async(key: str, fn: Callable[[], str]) -> str:
    """Run fn in a worker and wait up to REPLY_WAIT for it.  Only one job
    at a time; repeating the command for a running job collects it."""
    global _job
    if _job is None:
        _job = _Job(key, fn)
    elif _job.key != key:
        if _job.thread.is_alive():
            return 'busy with "{}" for {:.0f}s, try again later\n'.format(
                _job.key, time.monotonic() - _job.started)
        # an earlier job finished but was never collected
        _job = _Job(key, fn)
    job = _job
    job.thread.join(REPLY_WAIT)
    if job.thread.is_alive():
        return ('"{}" running for {:.0f}s, repeat the command to get the '
                'result\n'.format(key, time.monotonic() - job.started))
    _job = None
    if job.error is not None:
        raise job.error
    assert job.result is not None
    return job.result


def _short(filename: str) -> str:
    if filename.startswith(_mgr_path):
        return filename[len(_mgr_path):]
    return os.path.basename(filename)


@functools.lru_cache(maxsize=4096)
def _module_of(filename: str) -> str:
    """Map a source file to the mgr module that owns it."""
    if filename.startswith(_mgr_path):
        rel = filename[len(_mgr_path):]
        if os.sep in rel:
            return rel.split(os.sep, 1)[0]
        return 'mgr'  # mgr_module.py, mgr_util.py, ...
    return '<other>'


def _innermost_module(filenames: List[str]) -> str:
    """filenames run from the innermost frame outwards.  Skip shared code
    so time or memory spent in mgr_module.py or a library is charged to
    the module that called it."""
    for filename in filenames:
        mod = _module_of(filename)
        if mod not in ('<other>', 'mgr'):
            return mod
    return _module_of(filenames[0]) if filenames else '<other>'


def _frame_files(frame: Any) -> List[str]:
    files = []
    while frame is not None:
        files.append(frame.f_code.co_filename)
        frame = frame.f_back
    return files


def _other_frames() -> Dict[int, Any]:
    frames = sys._current_frames()
    for ident in _own_threads | {threading.get_ident()}:
        frames.pop(ident, None)
    return frames


def stacks() -> str:
    names = {t.ident: t.name for t in threading.enumerate()}
    out: List[str] = []
    for ident, frame in sorted(_other_frames().items()):
        out.append('Thread {} "{}" module={}\n'.format(
            ident, names.get(ident, '?'),
            _innermost_module(_frame_files(frame))))
        # no source lines: traceback's linecache would keep every file
        # on the stack in memory for the life of the mgr
        f = frame
        while f is not None:
            out.append('    {} ({}:{})\n'.format(
                f.f_code.co_name, _short(f.f_code.co_filename), f.f_lineno))
            f = f.f_back
        out.append('\n')
    return ''.join(out)


def _collapse(frame: Any) -> str:
    stack = []
    f = frame
    while f is not None:
        stack.append('{} ({}:{})'.format(
            f.f_code.co_name, _short(f.f_code.co_filename), f.f_lineno))
        f = f.f_back
    stack.append(_innermost_module(_frame_files(frame)))
    return ';'.join(reversed(stack))


def sample(duration: float, rate: int) -> str:
    """Collapsed stacks ("frame;frame;... count"), flamegraph.pl input."""
    duration = min(max(duration, 0.1), MAX_DURATION)
    interval = 1.0 / min(max(rate, 1), MAX_RATE)
    counts: Counter = Counter()
    nsamples = 0
    end = time.monotonic() + duration
    while time.monotonic() < end:
        counts.update(_collapse(f) for f in _other_frames().values())
        nsamples += 1
        time.sleep(interval)
    lines = ['{} {}'.format(k, v) for k, v in counts.most_common()]
    lines.append('# samples={} duration={:.1f}s'.format(nsamples, duration))
    return '\n'.join(lines) + '\n'


def _fmt_size(n: int, signed: bool = True) -> str:
    fmt = '{:+d} {}' if signed else '{:d} {}'
    for unit in ('B', 'KiB', 'MiB'):
        if abs(n) < 1024:
            return fmt.format(int(n), unit)
        n = int(n / 1024)
    return fmt.format(int(n), 'GiB')


Rows = List[Tuple[tracemalloc.Traceback, int, int]]


def _report(title: str, rows: Rows, limit: int, signed: bool) -> str:
    """rows are (traceback, size, count).  A traceback runs from the oldest
    frame to the one that made the allocation."""
    per_module: Dict[str, List[int]] = {}
    per_line: Dict[Tuple[str, int], List[int]] = {}
    for tb, size, count in rows:
        files = [f.filename for f in reversed(tb)]
        if any(f in _SKIP_FILES for f in files):
            continue
        fr = tb[-1]
        mod = _innermost_module(files)
        for acc in (per_module.setdefault(mod, [0, 0]),
                    per_line.setdefault((fr.filename, fr.lineno), [0, 0])):
            acc[0] += size
            acc[1] += count

    cfmt = '{:+d}' if signed else '{:d}'
    out = ['{}, by module:\n'.format(title)]
    for mod, (size, count) in sorted(per_module.items(),
                                     key=lambda kv: -kv[1][0]):
        out.append('  {:<24} {:>12} {} blocks\n'.format(
            mod, _fmt_size(size, signed), cfmt.format(count)))
    out.append('\ntop {} lines:\n'.format(limit))
    lines = sorted(per_line.items(), key=lambda kv: -kv[1][0])[:limit]
    for (filename, lineno), (size, count) in lines:
        out.append('  {:>12} {:>7} blocks  {}:{}\n'.format(
            _fmt_size(size, signed), cfmt.format(count), _short(filename),
            lineno))
    return ''.join(out)


def _top(limit: int) -> str:
    rows = [(st.traceback, st.size, st.count)
            for st in tracemalloc.take_snapshot().statistics('traceback')]
    return _report('live memory allocated since tracing started', rows,
                   limit, signed=False)


def _diff(base: tracemalloc.Snapshot, limit: int) -> str:
    cur = tracemalloc.take_snapshot()
    rows = [(st.traceback, st.size_diff, st.count_diff)
            for st in cur.compare_to(base, 'traceback')]
    return _report('growth since baseline', rows, limit, signed=True)


def mem(action: str, limit: int, nframes: int) -> str:
    global _baseline
    if action == 'start':
        if tracemalloc.is_tracing():
            return 'already tracing\n'
        tracemalloc.start(max(nframes, 1))
        _baseline = tracemalloc.take_snapshot()
        return 'tracing started (nframes={})\n'.format(max(nframes, 1))
    if action == 'stop':
        tracemalloc.stop()
        _baseline = None
        return 'tracing stopped\n'
    if not tracemalloc.is_tracing():
        return 'not tracing, run "py mem start" first\n'
    if action == 'status':
        cur, peak = tracemalloc.get_traced_memory()
        return 'traced={} peak={} overhead={} nframes={}\n'.format(
            cur, peak, tracemalloc.get_tracemalloc_memory(),
            tracemalloc.get_traceback_limit())
    if action == 'snapshot':
        _baseline = tracemalloc.take_snapshot()
        return 'baseline reset\n'
    if action == 'top':
        return _run_async('py mem top --limit {}'.format(limit),
                          lambda: _top(limit))
    if action == 'diff':
        base = _baseline
        if base is None:
            return 'no baseline, run "py mem snapshot" first\n'
        return _run_async('py mem diff --limit {}'.format(limit),
                          lambda: _diff(base, limit))
    raise ValueError('unknown action {}'.format(action))


def handle(cmd: Dict[str, Any]) -> str:
    """Entry point from PyModuleRegistry::handle_py_debug()."""
    _own_threads.add(threading.get_ident())
    try:
        prefix = cmd['prefix']
        if prefix == 'py stacks':
            duration = float(cmd.get('duration', 0))
            if duration <= 0:
                return stacks()
            rate = int(cmd.get('rate', 50))
            return _run_async(
                'py stacks --duration {} --rate {}'.format(duration, rate),
                lambda: sample(duration, rate))
        if prefix == 'py mem':
            return mem(cmd['action'], int(cmd.get('limit', 20)),
                       int(cmd.get('nframes', 1)))
        raise ValueError('unknown command {}'.format(prefix))
    finally:
        _own_threads.discard(threading.get_ident())
