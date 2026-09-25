#!/usr/bin/env python3
"""Expand compact OSD PG log prefixes back to the full PG state.

When debug_osd is below 20 (and its memory level is not above its log
level) the OSD prints the full "pg[...]" state prefix only when it has
changed since the last full prefix printed for that PG; other lines carry
a compact prefix such as

    osd.3 pg_epoch: 844 pg[6.cs0( v 844'13576) p3(0) r=0 active+clean]

The compact prefix is only used when the full state is byte-identical to
the last full prefix for the same PG, so this script can restore the full
prefix on every line.  Usage:

    expand_pg_log_prefix.py [ceph-osd.N.log ...] > expanded.log

Reads stdin when no file is given.  Lines that are not PG lines, and
compact lines with no earlier full prefix, are printed unchanged.

A crash calls Log::dump_recent(), which replays up to log_max_recent
buffered log entries, in their original order, after a "--- begin dump
of recent events ---" marker. Those entries were logged before whatever
full prefix this script has seen most recently, so this script forgets
all full prefixes at that marker; only full prefixes inside the dump are
used to expand compact lines inside it.
"""

import re
import sys

PG_RE = re.compile(r'(osd\.\d+) pg_epoch: \d+ pg\[')
DUMP_MARKER = '--- begin dump of recent events ---'


def pg_segment(line, start):
    """Return the end index (exclusive) of the bracket-balanced 'pg[...]'
    segment starting at start, or -1 if it is not balanced on this line."""
    depth = 0
    for i in range(start + 2, len(line)):
        ch = line[i]
        if ch in '[(':
            depth += 1
        elif ch in '])':
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def expand(stream, out):
    last_full = {}
    for line in stream:
        if DUMP_MARKER in line:
            # Entries in a recent-events dump were logged before whatever
            # full prefix we have seen so far is known to still apply, so
            # forget it; only full prefixes inside the dump can expand
            # compact lines inside the dump.
            last_full.clear()
            out.write(line)
            continue
        m = PG_RE.search(line)
        if m:
            start = m.end() - 3          # index of "pg["
            end = pg_segment(line, start)
            if end > 0:
                seg = line[start:end]
                pgid = seg[3:].split('(', 1)[0]
                key = (m.group(1), pgid)
                if ' lpr=' in seg:
                    last_full[key] = seg
                elif not seg.endswith('(unlocked)]') and key in last_full:
                    line = line[:start] + last_full[key] + line[end:]
        out.write(line)


def main():
    if len(sys.argv) == 1:
        expand(sys.stdin, sys.stdout)
    for path in sys.argv[1:]:
        with open(path, errors='replace') as f:
            expand(f, sys.stdout)


if __name__ == '__main__':
    main()
