#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil; coding:utf-8 -*-
# vim: ts=4 sw=4 smarttab expandtab fileencoding=utf-8
#
# Ceph - scalable distributed file system
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.

"""
ceph_log_budget.py - measure the volume of a Ceph daemon debug log.

Every log line records the level of the dout() that produced it:

  2026-07-10T07:29:46.450+0000 7f8adb893640 10 osd.3 844 dequeue_op ...
  <timestamp>                  <thread>    <level> <message>

so a log captured at debug level 20 tells us exactly what the same run would
have cost at any lower level N: the lines whose level is <= N ("kept"
lines).  This tool reports, for one or more log files (plain or .gz):

  * lines/bytes by level, and the bytes that would be kept at --level N,
  * lines/bytes by component (osd, bluestore, bluefs, ms, rocksdb, ...),
  * the top message templates overall and among the kept lines, optionally
    attributed to source files (--source-root),
  * the number of client ops (genuine "dequeue_op osd_op(client...) ...
    prio ..." start lines, falling back to finish lines when there are
    none) and the kept bytes per client op, the kept fraction and the kept
    byte rate.  A requeued op (waiting_for_readable, blocked on an object,
    a map-wait requeue) is dequeued, and so counted, once per attempt: the
    count is "client ops dequeued", not "distinct client ops",
  * budget checks (--max-*): exit status 1 if any budget is exceeded.

Continuation lines (entries that contain '\\n') are attributed to the entry
that started them and counted as multi-line entries.

The script only uses the Python 3.6 standard library: qa/tasks/log_budget.py
copies it to test nodes and runs it there, and also imports it to evaluate
budgets and attribute templates to source files.  See
doc/dev/osd_internals/debug_log_levels.rst.

Examples:
  ceph_log_budget.py ceph-osd.3.log.gz
  ceph_log_budget.py --level 10 --json ceph-osd.*.log > report.json
  ceph_log_budget.py --level 10 --max-kept-bytes-per-op 20480 \\
      --source-root ~/ceph ceph-osd.3.log
"""

import argparse
import bisect
import datetime
import gzip
import json
import math
import os
import re
import sys

REPORT_VERSION = 1

# "<timestamp> <thread-id> <level> <message>"; the level is printed with
# "%2d" so single-digit levels are preceded by two spaces.
HEADER_RE = re.compile(rb'^(\d{4}-\d\d-\d\dT[0-9:.]+\S*) (\S+) +(-?\d+) ')
# Log::dump_recent() (a crash backtrace) writes these two markers raw, with
# no timestamp/thread/level header, and everything in between (replayed
# recent-event entries, each prefixed with "%6ld> " before its own embedded
# timestamp so it still doesn't match HEADER_RE, plus the "--- logging
# levels ---" and thread-name dump) only repeats entries already counted
# elsewhere in the log.  Skip the whole region rather than miscounting it
# as a continuation of whatever entry preceded it.
CRASH_DUMP_BEGIN = b'--- begin dump of recent events ---'
CRASH_DUMP_END = b'--- end dump of recent events ---'
PG_RE = re.compile(r'^osd\.\d+ pg_epoch: \d+ ')
OSD_RE = re.compile(r'^osd\.\d+ (?:\d+ )?')
BLUESTORE_RE = re.compile(
    r'^(bluestore(?:\.\w+)?(?:\([^)]*\))?(?:\.\w+\([^)]*\))?) ')
HEX_RE = re.compile(r'0x[0-9a-fA-F]+')
HASH_RE = re.compile(r'[0-9a-f]{8,}')
NUM_RE = re.compile(r'\d+')

# First words of messages that do not carry a subsystem prefix but belong
# to bluestore (allocators, freelist, blob code).
BLUESTORE_TOKENS = frozenset([
    '_add', '_rm', 'maybe_unpin', '_trim_to', '_do_put_new_blobs', 'blobs',
    '_defer_or_allocate', 'BtreeAllocator', 'AvlAllocator', 'fbmap_alloc',
    'freelist', 'do_write', 'HybridAllocator', 'BitmapAllocator',
    'StupidAllocator', 'Btree2Allocator'])
# (message prefix, component); first match wins; default is "osd" because
# most unprefixed lines in an OSD log come from PGLog and friends.
COMPONENT_PREFIXES = (
    ('--', 'ms'),
    ('bluefs', 'bluefs'),
    ('_wait_for_aio', 'bluefs'),
    ('bluestore', 'bluestore'),
    ('set::SBMAP', 'bluestore'),
    ('rocksdb', 'rocksdb'),
    ('bdev', 'bdev'),
    ('monclient', 'monc'),
    ('mgrc', 'mgrc'),
    ('auth', 'auth'),
    ('cephx', 'auth'),
    ('heartbeat_map', 'heartbeatmap'),
    ('prioritycache', 'prioritycache'),
    ('log_channel', 'clog'),
)

CLIENT_OP_MARK = b'dequeue_op osd_op(client.'
CLIENT_OP_FINISH_MARK = b' finish latency'
# Only the genuine dequeue_op start line carries ' prio ' (the message
# priority, logged right after the op).  A requeue (waiting_for_readable,
# blocked on an object, a map-wait requeue) is dequeued, and therefore
# logged, once per attempt without becoming a second client op; a line like
# "dequeue_op osd_op(client....) pg 1.2s0 is deleting, dropping" also
# contains CLIENT_OP_MARK but has neither ' prio ' nor the finish mark and
# so is not counted as either a start or a finish.
CLIENT_OP_PRIO_MARK = b' prio '

# Source directories searched by attribute_sources().
SOURCE_DIRS = ('src/osd', 'src/os', 'src/msg', 'src/common', 'src/mon',
               'src/mgr', 'src/osdc', 'src/kv', 'src/blk', 'src/log',
               'src/global', 'src/auth', 'src/include', 'src/messages',
               'src/erasure-code', 'src/crush', 'src/cls', 'src/objclass')
SOURCE_EXTS = ('.cc', '.h', '.cpp', '.hpp')


def open_log(path):
    if path.endswith('.gz'):
        return gzip.open(path, 'rb')
    return open(path, 'rb')


def estimated_size(path):
    try:
        size = os.path.getsize(path)
    except OSError:
        return 0
    # debug logs typically compress 10:1 with gzip -5
    return size * 10 if path.endswith('.gz') else size


def classify(msg):
    """Return (component, body) for a decoded message (text after the
    level).  body is the message without the daemon/pg prefix."""
    if msg.startswith('--'):
        return 'ms', msg
    m = PG_RE.match(msg)
    if m:
        p = msg.find('pg[', m.end())
        if p >= 0:
            depth = 0
            i = p + 2
            n = len(msg)
            while i < n:
                c = msg[i]
                if c == '[' or c == '(':
                    depth += 1
                elif c == ']' or c == ')':
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            return 'osd', msg[i + 2:]
        return 'osd', msg[m.end():]
    m = OSD_RE.match(msg)
    if m:
        return 'osd', msg[m.end():]
    m = BLUESTORE_RE.match(msg)
    if m:
        return 'bluestore', msg[m.end():]
    if msg.startswith('bluefs '):
        return 'bluefs', msg[7:]
    for prefix, comp in COMPONENT_PREFIXES:
        if msg.startswith(prefix):
            return comp, msg
    if msg.split(' ', 1)[0] in BLUESTORE_TOKENS:
        return 'bluestore', msg
    return 'osd', msg


def token_of(body):
    t = body.split(' ', 1)[0]
    t = t.split('(', 1)[0].rstrip(':')
    if '::' in t:
        t = t.split('::')[-1]
    return t


def template_of(body):
    s = HEX_RE.sub('X', body)
    s = HASH_RE.sub('H', s)
    s = NUM_RE.sub('N', s)
    return ' '.join(s.split(' ')[:6])[:120]


def parse_ts(ts):
    try:
        return datetime.datetime.strptime(ts[:23], '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return None


class Analysis(object):
    """Accumulates statistics over one or more log files."""

    def __init__(self, level=10, sample=1, since=None, until=None,
                 op_regex=None, max_templates=300000):
        self.level = level
        self.sample = max(1, int(sample))
        self.since = since.encode() if since else None
        self.until = until.encode() if until else None
        self.op_re = re.compile(op_regex.encode()) if op_regex else None
        self.max_templates = max_templates
        self.files = []
        self.lines = 0
        self.bytes = 0
        self.entries = 0
        self.kept_lines = 0
        self.kept_bytes = 0
        self.multiline = 0
        self.kept_multiline = 0
        self.first_kept_multiline_example = None
        self.levels = {}          # level -> [lines, bytes]
        self.components = {}      # comp -> [lines, bytes, kept_lines, kept_bytes]
        self.templates = {}       # (comp, level, tmpl) -> [lines, bytes, token, example]
        self.op_starts = 0
        self.op_finishes = 0
        self.op_matches = 0
        self.first_ts = None
        self.last_ts = None
        self.max_level_seen = None

    def add_file(self, path):
        self.files.append(path)
        with open_log(path) as fh:
            # If this is a plain (non-.gz) file, it may still be growing
            # under us (e.g. a live OSD log read while the daemon keeps
            # running): cap the scan at the size it had when we opened it,
            # so a log that is appended faster than we can read it does not
            # make the scan run unbounded.
            limit = None
            if not path.endswith('.gz'):
                try:
                    limit = os.fstat(fh.fileno()).st_size
                except (OSError, AttributeError):
                    limit = None
            self._scan(fh, limit)

    def _scan(self, fh, limit=None):
        level = self.level
        sample = self.sample
        since = self.since
        until = self.until
        op_re = self.op_re
        levels = self.levels
        components = self.components
        templates = self.templates
        cur = None            # [level, comp, tmpl_key or None, multiline?]
        skipping = False
        in_crash_dump = False
        consumed = 0
        n = 0
        for raw in fh:
            if limit is not None and consumed >= limit:
                # Don't chase a file that is still being appended to faster
                # than we can read it: stop at the size it had when we
                # opened it.
                break
            ln = len(raw)
            consumed += ln
            m = HEADER_RE.match(raw)
            if m is None:
                if raw.startswith(CRASH_DUMP_BEGIN):
                    in_crash_dump = True
                    cur = None
                    continue
                if in_crash_dump:
                    if raw.startswith(CRASH_DUMP_END):
                        in_crash_dump = False
                    continue
                # continuation of a multi-line entry
                if skipping or cur is None:
                    continue
                self.lines += 1
                self.bytes += ln
                lv, comp, key, multi = cur
                if not multi:
                    cur[3] = True
                    self.multiline += 1
                    if lv <= level:
                        self.kept_multiline += 1
                        if (self.first_kept_multiline_example is None and
                                key is not None):
                            self.first_kept_multiline_example = \
                                templates[key][3]
                st = levels[lv]
                st[0] += 1
                st[1] += ln
                c = components[comp]
                c[0] += 1
                c[1] += ln
                if lv <= level:
                    self.kept_lines += 1
                    self.kept_bytes += ln
                    c[2] += 1
                    c[3] += ln
                if key is not None:
                    templates[key][1] += ln * sample
                continue
            ts = m.group(1)
            if (since is not None and ts < since) or \
                    (until is not None and ts > until):
                skipping = True
                continue
            skipping = False
            if self.first_ts is None or ts < self.first_ts:
                self.first_ts = ts
            if self.last_ts is None or ts > self.last_ts:
                self.last_ts = ts
            lv = int(m.group(3))
            if self.max_level_seen is None or lv > self.max_level_seen:
                self.max_level_seen = lv
            rest = raw[m.end():]
            self.lines += 1
            self.entries += 1
            self.bytes += ln
            st = levels.get(lv)
            if st is None:
                st = levels[lv] = [0, 0]
            st[0] += 1
            st[1] += ln
            if op_re is not None:
                if op_re.search(rest):
                    self.op_matches += 1
            elif CLIENT_OP_MARK in rest:
                if CLIENT_OP_FINISH_MARK in rest:
                    self.op_finishes += 1
                elif CLIENT_OP_PRIO_MARK in rest:
                    self.op_starts += 1
            n += 1
            if n % sample == 0:
                msg = rest.decode('latin-1').rstrip('\n')
                comp, body = classify(msg)
                key = (comp, lv, template_of(body))
                t = templates.get(key)
                if t is None:
                    if len(templates) >= self.max_templates:
                        self._prune()
                    t = templates[key] = [0, 0, token_of(body), msg[:300]]
                t[0] += sample
                t[1] += ln * sample
            else:
                # cheap classification: component totals only
                key = None
                if rest.startswith(b'osd.'):
                    comp = 'osd'
                elif rest.startswith(b'bluestore'):
                    comp = 'bluestore'
                elif rest.startswith(b'bluefs '):
                    comp = 'bluefs'
                elif rest.startswith(b'--'):
                    comp = 'ms'
                else:
                    comp = classify(rest.decode('latin-1'))[0]
            c = components.get(comp)
            if c is None:
                c = components[comp] = [0, 0, 0, 0]
            c[0] += 1
            c[1] += ln
            if lv <= level:
                self.kept_lines += 1
                self.kept_bytes += ln
                c[2] += 1
                c[3] += ln
            cur = [lv, comp, key, False]

    def _prune(self):
        # drop the rarest templates so memory stays bounded on huge logs
        for k in [k for k, v in self.templates.items() if v[0] <= self.sample]:
            del self.templates[k]

    def client_ops(self):
        if self.op_re is not None:
            return self.op_matches
        return self.op_starts if self.op_starts else self.op_finishes

    def duration(self):
        if self.first_ts is None:
            return 0.0
        a = parse_ts(self.first_ts.decode())
        b = parse_ts(self.last_ts.decode())
        if a is None or b is None:
            return 0.0
        return max(0.0, (b - a).total_seconds())

    def top(self, n, kept_only):
        items = []
        for (comp, lv, tmpl), v in self.templates.items():
            if kept_only and lv > self.level:
                continue
            items.append({'component': comp, 'level': lv, 'template': tmpl,
                          'token': v[2], 'lines': v[0], 'bytes': v[1],
                          'kept': lv <= self.level, 'example': v[3]})
        items.sort(key=lambda x: -x['bytes'])
        return items[:n]

    def report(self, top=25):
        ops = self.client_ops()
        dur = self.duration()
        r = {
            'version': REPORT_VERSION,
            'files': self.files,
            'level': self.level,
            'sample': self.sample,
            'first_ts': self.first_ts.decode() if self.first_ts else None,
            'last_ts': self.last_ts.decode() if self.last_ts else None,
            'duration_s': dur,
            'max_level_seen': self.max_level_seen,
            'lines': self.lines,
            'entries': self.entries,
            'bytes': self.bytes,
            'multiline_entries': self.multiline,
            'kept': {
                'lines': self.kept_lines,
                'bytes': self.kept_bytes,
                'multiline_entries': self.kept_multiline,
                'first_multiline_example': self.first_kept_multiline_example,
                'fraction': (float(self.kept_bytes) / self.bytes
                             if self.bytes else 0.0),
                'bytes_per_s': (self.kept_bytes / dur) if dur > 0 else None,
            },
            'bytes_per_s': (self.bytes / dur) if dur > 0 else None,
            'client_ops': ops,
            'kept_bytes_per_op': (float(self.kept_bytes) / ops
                                  if ops else None),
            'bytes_per_op': float(self.bytes) / ops if ops else None,
            'levels': dict((str(k), {'lines': v[0], 'bytes': v[1]})
                           for k, v in sorted(self.levels.items())),
            'components': dict(
                (k, {'lines': v[0], 'bytes': v[1], 'kept_lines': v[2],
                     'kept_bytes': v[3]})
                for k, v in sorted(self.components.items())),
            'top_templates': self.top(top, False),
            'top_kept_templates': self.top(top, True),
        }
        return r


def merge_reports(reports, top=25):
    """Merge per-daemon reports (e.g. all OSDs of a job) into one summary
    with the same keys as report().  Templates are merged by
    (component, level, template)."""
    out = {'version': REPORT_VERSION, 'files': [], 'lines': 0, 'entries': 0,
           'bytes': 0, 'multiline_entries': 0, 'client_ops': 0,
           'kept': {'lines': 0, 'bytes': 0, 'multiline_entries': 0,
                    'first_multiline_example': None},
           'levels': {}, 'components': {}, 'daemons': 0,
           'max_level_seen': None, 'duration_s': 0.0}
    tmpl = {}
    for r in reports:
        if not r:
            continue
        out['daemons'] += 1
        out['level'] = r.get('level')
        out['files'].extend(r.get('files', []))
        for k in ('lines', 'entries', 'bytes', 'multiline_entries',
                  'client_ops'):
            out[k] += r.get(k) or 0
        for k in ('lines', 'bytes', 'multiline_entries'):
            out['kept'][k] += r['kept'].get(k) or 0
        if out['kept']['first_multiline_example'] is None:
            out['kept']['first_multiline_example'] = \
                r['kept'].get('first_multiline_example')
        out['duration_s'] = max(out['duration_s'], r.get('duration_s') or 0.0)
        mls = r.get('max_level_seen')
        if mls is not None and (out['max_level_seen'] is None or
                                mls > out['max_level_seen']):
            out['max_level_seen'] = mls
        for lv, v in r.get('levels', {}).items():
            d = out['levels'].setdefault(lv, {'lines': 0, 'bytes': 0})
            d['lines'] += v['lines']
            d['bytes'] += v['bytes']
        for c, v in r.get('components', {}).items():
            d = out['components'].setdefault(
                c, {'lines': 0, 'bytes': 0, 'kept_lines': 0, 'kept_bytes': 0})
            for k in d:
                d[k] += v.get(k, 0)
        # a template can be in both lists of one report: count it once
        seen = {}
        for t in r.get('top_templates', []) + r.get('top_kept_templates', []):
            key = (t['component'], t['level'], t['template'])
            if key not in seen:
                seen[key] = t
        for key, t in seen.items():
            d = tmpl.get(key)
            if d is None:
                tmpl[key] = dict(t)
            else:
                d['lines'] += t['lines']
                d['bytes'] += t['bytes']
    b = out['bytes']
    kb = out['kept']['bytes']
    out['kept']['fraction'] = float(kb) / b if b else 0.0
    ops = out['client_ops']
    out['kept_bytes_per_op'] = float(kb) / ops if ops else None
    out['bytes_per_op'] = float(b) / ops if ops else None
    # rates are per daemon: average over daemons
    dur = out['duration_s']
    n = max(1, out['daemons'])
    out['kept']['bytes_per_s'] = (float(kb) / n / dur) if dur > 0 else None
    out['bytes_per_s'] = (float(b) / n / dur) if dur > 0 else None
    ts = list(tmpl.values())
    ts.sort(key=lambda x: -x['bytes'])
    out['top_templates'] = ts[:top]
    out['top_kept_templates'] = [t for t in ts if t.get('kept')][:top]
    return out


def check_budgets(report, max_kept_bytes_per_op=None, max_kept_fraction=None,
                  max_kept_mb_per_s=None, max_kept_multiline_entries=None,
                  min_client_ops=1000, offenders=5):
    """Return a list of violations (dicts).  A budget that cannot be
    evaluated (too few client ops, log not captured at level >= 20, no
    duration) is skipped and noted in report['budget_notes'].
    max_kept_multiline_entries is off (None) by default: policy rule 3
    (every kept entry is a single line, for grep/lnav) is measured
    (report['kept']['multiline_entries']) but not enforced until a caller
    opts in."""
    notes = report.setdefault('budget_notes', [])
    viol = []
    worst = [{'component': t['component'], 'level': t['level'],
              'template': t['template'], 'bytes': t['bytes'],
              'source': t.get('source', [])}
             for t in report.get('top_kept_templates', [])[:offenders]]

    def add(name, limit, actual, **extra):
        v = {'budget': name, 'limit': limit, 'actual': actual,
             'offenders': worst}
        v.update(extra)
        viol.append(v)

    if max_kept_bytes_per_op is not None:
        ops = report.get('client_ops') or 0
        if ops < min_client_ops:
            notes.append('max_kept_bytes_per_op not evaluated: only %d '
                         'client ops (< %d)' % (ops, min_client_ops))
        elif report['kept_bytes_per_op'] > max_kept_bytes_per_op:
            add('max_kept_bytes_per_op', max_kept_bytes_per_op,
                report['kept_bytes_per_op'])
    if max_kept_fraction is not None:
        mls = report.get('max_level_seen')
        if mls is None or mls < 20:
            notes.append('max_kept_fraction not evaluated: log was not '
                         'captured at debug level 20 (max level seen %s)'
                         % mls)
        elif report['kept']['fraction'] > max_kept_fraction:
            add('max_kept_fraction', max_kept_fraction,
                report['kept']['fraction'])
    if max_kept_mb_per_s is not None:
        rate = report['kept'].get('bytes_per_s')
        if rate is None:
            notes.append('max_kept_mb_per_s not evaluated: no duration')
        elif rate / 1e6 > max_kept_mb_per_s:
            add('max_kept_mb_per_s', max_kept_mb_per_s, rate / 1e6)
    if max_kept_multiline_entries is not None:
        n = report['kept'].get('multiline_entries') or 0
        if n > max_kept_multiline_entries:
            example = report['kept'].get('first_multiline_example')
            add('max_kept_multiline_entries', max_kept_multiline_entries, n,
                **({'example': example} if example else {}))
    return viol


class SourceIndex(object):
    """All source text under SOURCE_DIRS concatenated, for literal search."""

    def __init__(self, root):
        parts = []
        self.starts = []
        self.paths = []
        pos = 0
        for d in SOURCE_DIRS:
            top = os.path.join(root, d)
            for dirpath, dirnames, filenames in os.walk(top):
                dirnames.sort()
                for f in sorted(filenames):
                    if not f.endswith(SOURCE_EXTS):
                        continue
                    p = os.path.join(dirpath, f)
                    try:
                        with open(p, encoding='utf-8', errors='replace') as fh:
                            text = fh.read()
                    except OSError:
                        continue
                    self.starts.append(pos)
                    self.paths.append(os.path.relpath(p, root))
                    parts.append(text)
                    parts.append('\0')
                    pos += len(text) + 1
        self.text = ''.join(parts)

    def find(self, needle, limit, log_only=True, require=None):
        """Return up to limit+1 'path:line' hits for needle.  With
        log_only, only hits inside a dout()/derr statement count; with
        require, only hits in files that also contain that string."""
        hits = []
        text = self.text
        i = text.find(needle)
        while i >= 0 and len(hits) <= limit:
            f = bisect.bisect_right(self.starts, i) - 1
            ok = not log_only or self._in_log_statement(i)
            if ok and require:
                end = (self.starts[f + 1] if f + 1 < len(self.starts)
                       else len(text))
                ok = text.find(require, self.starts[f], end) >= 0
            if ok:
                line = text.count('\n', self.starts[f], i) + 1
                hit = '%s:%d' % (self.paths[f], line)
                if hit not in hits:
                    hits.append(hit)
            i = text.find(needle, i + 1)
        return hits

    def _in_log_statement(self, i):
        text = self.text
        lo = max(0, i - 400)
        st = max(text.rfind(';', lo, i), text.rfind('{', lo, i),
                 text.rfind('}', lo, i))
        seg = text[st + 1 if st >= 0 else lo:i]
        return 'dout' in seg or 'derr' in seg


FRAGMENT_RE = re.compile(r"[A-Za-z_ .,:;=/'<>\[\]\-!?]{8,}")


def source_candidates(example):
    """Return (function token, literal fragments) for a log message: the
    fragments are the word sequences between variable parts of the message
    that probably appear verbatim in the dout() statement, longest first."""
    comp, body = classify(example)
    first = body.split(' ', 1)[0]
    rest = body[len(first):]          # the first word is usually __func__
    frags = []
    seen = set()
    for f in FRAGMENT_RE.findall(rest):
        words = f.split()
        for i in range(len(words)):
            for j in range(len(words), i, -1):
                s = ' '.join(words[i:j])
                if len(s) >= 8 and s not in seen:
                    seen.add(s)
                    frags.append(s)
    frags.sort(key=lambda s: -len(s))
    tok = token_of(body)
    words = body.split(' ', 2)
    # "ECCommonL try_finish_rmw: ..." - a class-name dout prefix followed
    # by __func__: the function is the second word
    if len(words) > 2 and not words[0].endswith(':') and \
            '(' not in words[0] and words[1].endswith(':'):
        tok = token_of(words[1])
    return tok, frags[:60]


def attribute_sources(templates, source_root, max_templates=40,
                      max_hits=4, index=None):
    """Add a 'source' list ('path:line' strings) to each of the first
    max_templates template dicts by searching literal fragments of the
    example line in dout() statements of the source tree; fall back to the
    definition of the function named by the first word.  Returns the
    SourceIndex so callers can reuse it."""
    if index is None:
        if not source_root or \
                not os.path.isdir(os.path.join(source_root, 'src')):
            return None
        index = SourceIndex(source_root)
    for t in templates[:max_templates]:
        tok, frags = source_candidates(t.get('example', ''))
        found = []
        if len(tok) >= 4:
            for frag in frags:
                hits = index.find(frag, max_hits, require=tok)
                if 0 < len(hits) <= max_hits:
                    found = hits
                    break
        if not found and len(tok) >= 6:
            hits = index.find('::' + tok + '(', max_hits, log_only=False)
            if 0 < len(hits) <= max_hits:
                found = [h + ' (' + tok + ')' for h in hits]
        t['source'] = found
    return index


def human(n):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(n) < 1000 or unit == 'TB':
            return '%.1f%s' % (n, unit)
        n /= 1000.0


def print_text(r, out=None):
    out = out or sys.stdout
    b = r['bytes'] or 1
    k = r['kept']
    p = lambda s='': out.write(s + '\n')  # noqa: E731
    p('files: %s' % ' '.join(r['files']))
    p('interval: %s .. %s (%.1fs)  max level seen: %s  sample: 1/%s' % (
        r.get('first_ts'), r.get('last_ts'), r.get('duration_s') or 0,
        r.get('max_level_seen'), r.get('sample', 1)))
    p('total: %d lines %s  (%s/s)  multi-line entries: %d' % (
        r['lines'], human(r['bytes']),
        human(r['bytes_per_s']) if r.get('bytes_per_s') else '-',
        r['multiline_entries']))
    p('kept at level <= %d: %d lines %s = %.1f%% of total  (%s/s)  '
      'multi-line entries: %d' % (
          r['level'], k['lines'], human(k['bytes']), 100.0 * k['fraction'],
          human(k['bytes_per_s']) if k.get('bytes_per_s') else '-',
          k['multiline_entries']))
    if r.get('client_ops'):
        p('client ops: %d  bytes/op: total %s kept %s' % (
            r['client_ops'], human(r['bytes_per_op']),
            human(r['kept_bytes_per_op'])))
    else:
        p('client ops: 0 (no "dequeue_op osd_op(client." lines)')
    p('')
    p('by level:')
    for lv, v in sorted(r['levels'].items(), key=lambda x: int(x[0])):
        p('  %3s %10d lines %10s %5.1f%%' % (
            lv, v['lines'], human(v['bytes']), 100.0 * v['bytes'] / b))
    p('by component:')
    for c, v in sorted(r['components'].items(), key=lambda x: -x[1]['bytes']):
        p('  %-14s %10s %5.1f%%  kept %10s %5.1f%%' % (
            c, human(v['bytes']), 100.0 * v['bytes'] / b,
            human(v['kept_bytes']), 100.0 * v['kept_bytes'] / b))
    for title, key in (('top templates (all levels)', 'top_templates'),
                       ('top kept templates (level <= %d)' % r['level'],
                        'top_kept_templates')):
        p('')
        p(title + ':')
        for t in r[key]:
            p('  %5.2f%% %10s %9d  %-9s %2d | %s%s' % (
                100.0 * t['bytes'] / b, human(t['bytes']), t['lines'],
                t['component'], t['level'], t['template'],
                ('  [' + ', '.join(t['source']) + ']')
                if t.get('source') else ''))
    for n in r.get('budget_notes', []):
        p('note: ' + n)
    for v in r.get('violations', []):
        p('BUDGET EXCEEDED: %s: actual %.4g > limit %.4g' % (
            v['budget'], v['actual'], v['limit']))
        if v.get('example'):
            p('    example: ' + v['example'])
        for o in v['offenders']:
            p('    %s %2d %s %s %s' % (
                o['component'], o['level'], human(o['bytes']),
                o['template'], ' '.join(o.get('source', []))))


def main(argv=None):
    ap = argparse.ArgumentParser(
        description='Measure Ceph daemon debug-log volume by level, '
                    'component and message template.',
        epilog='See doc/dev/osd_internals/debug_log_levels.rst.')
    ap.add_argument('logs', nargs='+', help='log files (plain or .gz)')
    ap.add_argument('--level', type=int, default=10,
                    help='would-be debug level: lines at or below it are '
                         '"kept" (default 10)')
    ap.add_argument('--since', help='ignore lines before this timestamp '
                    '(e.g. 2026-07-10T07:29:46)')
    ap.add_argument('--until', help='ignore lines after this timestamp')
    ap.add_argument('--top', type=int, default=25,
                    help='number of top templates to report')
    ap.add_argument('--sample', default='1',
                    help='compute templates on every Nth entry (totals are '
                         'always exact); "auto" picks N so that about '
                         '--sample-target-bytes are analysed')
    ap.add_argument('--sample-target-bytes', type=int, default=2 << 30)
    ap.add_argument('--op-regex',
                    help='regex identifying one client op per matching line '
                         '(default: dequeue_op osd_op(client. start lines, '
                         'or finish lines if there are no start lines)')
    ap.add_argument('--source-root',
                    help='ceph source checkout used to attribute top '
                         'templates to source files ("auto": the checkout '
                         'containing this script)')
    ap.add_argument('--json', action='store_true', help='print JSON')
    ap.add_argument('--max-kept-bytes-per-op', type=float,
                    help='budget: kept bytes per client op')
    ap.add_argument('--max-kept-fraction', type=float,
                    help='budget: kept bytes / total bytes (log must have '
                         'been captured at level >= 20)')
    ap.add_argument('--max-kept-mb-per-s', type=float,
                    help='budget: kept MB per second')
    ap.add_argument('--max-kept-multiline-entries', type=int,
                    help='budget: kept (level<=N) entries spanning more '
                         'than one line; every kept entry should be a '
                         'single line for grep/lnav (off by default)')
    ap.add_argument('--min-client-ops', type=int, default=1000,
                    help='evaluate the per-op budget only with at least '
                         'this many client ops (default 1000)')
    a = ap.parse_args(argv)

    if a.sample == 'auto':
        total = sum(estimated_size(p) for p in a.logs)
        sample = max(1, int(math.ceil(float(total) / a.sample_target_bytes)))
    else:
        try:
            sample = max(1, int(a.sample))
        except ValueError:
            ap.error('--sample must be an integer or "auto"')
    an = Analysis(level=a.level, sample=sample, since=a.since,
                  until=a.until, op_regex=a.op_regex)
    for p in a.logs:
        try:
            an.add_file(p)
        except (OSError, EOFError) as e:
            print('ceph_log_budget: %s: %s' % (p, e), file=sys.stderr)
            return 2
    r = an.report(top=a.top)
    root = a.source_root
    if root == 'auto':
        root = os.path.normpath(os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', '..'))
    if root:
        idx = attribute_sources(r['top_templates'], root)
        attribute_sources(r['top_kept_templates'], root, index=idx)
    r['violations'] = check_budgets(
        r, a.max_kept_bytes_per_op, a.max_kept_fraction, a.max_kept_mb_per_s,
        a.max_kept_multiline_entries, a.min_client_ops)
    if a.json:
        json.dump(r, sys.stdout, indent=1, sort_keys=True)
        sys.stdout.write('\n')
    else:
        print_text(r)
    return 1 if r['violations'] else 0


if __name__ == '__main__':
    sys.exit(main())
