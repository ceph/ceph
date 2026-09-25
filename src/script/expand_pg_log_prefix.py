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
compact lines with no earlier full prefix, are printed unchanged.  If
several files are given (e.g. a rotated log split across
ceph-osd.N.log.1 and ceph-osd.N.log), list them oldest first: full
prefixes carry over from one file to the next in the order given, so an
older file's state is used to expand compact lines at the start of the
next one.

A crash calls Log::dump_recent(), which replays up to log_max_recent
buffered log entries, in their original order, after a "--- begin dump
of recent events ---" marker. Those entries were logged before whatever
full prefix this script has seen most recently, so this script forgets
all full prefixes at that marker; only full prefixes inside the dump are
used to expand compact lines inside it.
"""

import gzip
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


def expand(stream, out, last_full):
    """Expand compact PG prefixes read from stream, writing to out.

    last_full is a dict of (osd, pgid) -> full "pg[...]" segment, owned by
    the caller and mutated in place, so state can be threaded across
    multiple calls (see main()) instead of being rebuilt from scratch for
    each file.
    """
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


def open_log(path):
    """Open a plain or .gz log for text reading.

    Debug logs can contain raw object names or omap keys that are not
    valid UTF-8; decode as latin-1 (which maps every byte 0-255 to a
    codepoint, so it never raises) rather than silently corrupting bytes
    with errors='replace', or crashing on strict decoding.
    """
    if path.endswith('.gz'):
        return gzip.open(path, 'rt', encoding='latin-1')
    return open(path, encoding='latin-1')


def main():
    last_full = {}
    # Reconfigure stdio to latin-1 too, so bytes read from stdin (or from a
    # file, above) pass through to stdout unchanged instead of being
    # decoded/encoded with the locale's (usually strict UTF-8) codec.
    sys.stdin.reconfigure(encoding='latin-1')
    sys.stdout.reconfigure(encoding='latin-1')
    if len(sys.argv) == 1:
        expand(sys.stdin, sys.stdout, last_full)
        return
    for path in sys.argv[1:]:
        with open_log(path) as f:
            expand(f, sys.stdout, last_full)


if __name__ == '__main__':
    main()
