#!/usr/bin/env python3
"""
Check that perf counters declare their units.

ceph-exporter and the mgr prometheus module name a counter's unit in its
HELP text: seconds for time counters, and bytes for counters declared with
UNIT_BYTES. A counter that holds bytes or a latency without declaring it
gets no unit. This scans the PerfCountersBuilder declarations under src/
and fails when:

- a u64 counter whose name contains "bytes" doesn't pass UNIT_BYTES.
  Rates (_bps), percentages and histograms are exempt.
- a counter whose name says it is a latency is a u64 counter rather than
  a time counter, unless the name carries its unit (_ms, _msec, _us,
  _usec, _ns, _nsec).

Usage: check-perf-counter-units.py [SRC_DIR]
"""

import pathlib
import re
import sys

DECL = re.compile(
    r'\.add_(u64_counter_histogram|u64_counter|u64_avg|u64|time_avg|time)'
    r'\s*\((.*?)\)\s*;', re.S)
STRING = re.compile(r'"([^"]*)"')
BYTES_NAME = re.compile(r'bytes')
BYTES_EXEMPT = re.compile(r'bps|percent')
LATENCY_NAME = re.compile(r'(^|_)(lat|latency)(_|$)')
NAMED_UNIT = re.compile(r'_(ms|msec|us|usec|ns|nsec)$')


def submodule_paths(root):
    """Submodule directories, from .gitmodules when there is one."""
    gitmodules = root.parent / '.gitmodules'
    if not gitmodules.exists():
        return set()
    paths = re.findall(r'^\s*path\s*=\s*(\S+)', gitmodules.read_text(), re.M)
    return {(root.parent / p).resolve() for p in paths}


def source_files(root):
    skip = submodule_paths(root) | {(root / 'test').resolve()}
    stack = [root]
    while stack:
        d = stack.pop()
        for p in d.iterdir():
            if p.is_dir():
                if p.resolve() not in skip:
                    stack.append(p)
            elif p.suffix in ('.cc', '.h'):
                yield p


def unit_variables(text):
    """Names that a file binds to UNIT_BYTES, such as ubytes."""
    return set(re.findall(r'(\w+)\s*=\s*(?:unit_t::)?UNIT_BYTES\b', text))


def check(root):
    problems = []
    for path in sorted(source_files(root)):
        text = path.read_text(errors='ignore')
        if '.add_' not in text:
            continue
        unit_vars = unit_variables(text)
        for m in DECL.finditer(text):
            kind, args = m.group(1), m.group(2)
            strings = STRING.findall(args)
            if not strings:
                continue
            name = strings[0]
            where = f'{path.relative_to(root.parent)}:{text.count(chr(10), 0, m.start()) + 1}'
            idents = set(re.findall(r'\b\w+\b', STRING.sub('', args)))
            declares_bytes = 'UNIT_BYTES' in idents or bool(idents & unit_vars)
            is_u64 = kind.startswith('u64') and 'histogram' not in kind
            if (is_u64 and BYTES_NAME.search(name) and
                    not BYTES_EXEMPT.search(name) and not declares_bytes):
                problems.append(f'{where}: "{name}" counts bytes but does '
                                'not declare UNIT_BYTES')
            if (is_u64 and LATENCY_NAME.search(name) and
                    not NAMED_UNIT.search(name)):
                problems.append(f'{where}: "{name}" is a latency in a u64 '
                                'counter; use add_time or add_time_avg, or '
                                'put the unit in the name')
    return problems


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                        else pathlib.Path(__file__).resolve().parent.parent)
    problems = check(root.resolve())
    for p in problems:
        print(p)
    if problems:
        print(f'{len(problems)} perf counter(s) without a unit; see '
              'src/test/check-perf-counter-units.py')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
