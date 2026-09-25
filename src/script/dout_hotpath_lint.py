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
dout_hotpath_lint.py - ratchet on low-level debug output in hot paths.

Debug level 10 is the "lean" level (see
doc/dev/osd_internals/debug_log_levels.rst): at most one or two lines per
IO per layer on the good path.  This lint counts, for a manifest of hot-path
functions, the debug-log statements (dout, ldout, ldpp_dout, psdout, ...)
whose level is <= 10 and compares the counts with a checked-in baseline.
It fails if any function gained such a statement, so that a new good-path
level-10 line in e.g. OSD::dequeue_op or BlueStore::_txc_state_proc is
noticed at "make check" time.

Statements are counted per *statement*, not per execution: a line inside a
loop counts once.  Error-path lines that deliberately stay at <= 10 can be
exempted by putting the marker comment

    // dout-lint: error-path

on the same line as the statement or on the line before it.

The manifest and the baseline counts live in one JSON file:

    {
      "version": 1,
      "max_level": 10,
      "functions": [
        {"file": "src/osd/OSD.cc", "function": "OSD::dequeue_op",
         "le10": 3, "dynamic": 0},
        ...
      ]
    }

"function" is the qualified name exactly as written in the out-of-line
definition (all overloads with that name in the file are summed).
"le10" / "dynamic" are the baseline counts; null means "not yet recorded"
(run with --update to fill them in).

Usage:
    dout_hotpath_lint.py --source-root <ceph checkout> \\
        --baseline src/script/dout_hotpath_baseline.json [--update] [--strict]

A need_dynamic() level, such as `need_dynamic(cond ? 20 : 10)`, is
classified by the minimum of its branches, since that is the level that
would need a baseline bump; a level that cannot be resolved that way (a
bare variable or a function call) is reported but does not fail the
ratchet, since telling it apart from a genuine new low-level line would
need dataflow analysis across statements.

A manifest function that is no longer found (renamed or moved) is reported
as a note, not a failure, so that an unrelated refactor does not fail
make check: pass --strict to make it fatal.

Exit status: 0 ok, 1 ratchet violated (or, with --strict, a stale count or
a missing function), 2 usage error.
"""

import argparse
import json
import os
import re
import sys

# Debug-log macros whose last argument is the level.
LEVEL_MACROS = (
    'dout', 'ldout', 'ldpp_dout', 'psdout', 'lsubdout', 'lgeneric_dout',
    'lgeneric_subdout', 'subdout', 'ldlog_p1', 'generic_dout',
)
# Error macros (level -1) that take arguments.
ERR_PAREN_MACROS = ('lderr', 'ldpp_derr', 'lgeneric_derr')
# Error macros used without arguments ("derr << ...").
ERR_BARE_MACROS = ('derr', 'generic_derr')

PAREN_RE = re.compile(
    r'(?<![\w.:>])(' + '|'.join(LEVEL_MACROS + ERR_PAREN_MACROS) + r')\s*\(')
BARE_RE = re.compile(
    r'(?<![\w.:>])(' + '|'.join(ERR_BARE_MACROS) + r')\b(?!\s*\()')
INT_RE = re.compile(r'^-?\d+$')
QUALIFIERS_RE = re.compile(
    r'^(?:\s|const\b|noexcept\b|override\b|final\b|&&|&|->[\s\w:<>,*&]+)*$')
EXEMPT_MARKER = 'dout-lint: error-path'


def _mask_comments_and_literals(text):
    """Blank comments and string/char literals (keeping newlines)."""
    out = list(text)
    n = len(text)
    i = 0

    def blank(a, b):
        for j in range(a, b):
            if out[j] != '\n':
                out[j] = ' '

    while i < n:
        c = text[i]
        if text.startswith('//', i):
            e = text.find('\n', i)
            e = n if e < 0 else e
            blank(i, e)
            i = e
        elif text.startswith('/*', i):
            e = text.find('*/', i + 2)
            e = n if e < 0 else e + 2
            blank(i, e)
            i = e
        elif c == '"':
            if i > 0 and text[i - 1] == 'R':
                # raw string literal: R"delim( ... )delim"
                p = text.find('(', i)
                delim = text[i + 1:p] if p > 0 else ''
                e = text.find(')' + delim + '"', p) if p > 0 else -1
                e = n if e < 0 else e + len(delim) + 2
                blank(i + 1, e - 1)
                i = e
                continue
            j = i + 1
            while j < n and text[j] != '"' and text[j] != '\n':
                if text[j] == '\\':
                    j += 1
                j += 1
            blank(i + 1, j)
            i = j + 1
        elif c == "'":
            if (i > 0 and text[i - 1].isalnum() and i + 1 < n and
                    text[i + 1].isalnum() and
                    not (text[i - 1] in 'uUL8' and
                         (i < 2 or not text[i - 2].isalnum()))):
                i += 1        # digit separator, e.g. 1'000'000
                continue
            j = i + 1
            while j < n and text[j] != "'" and text[j] != '\n':
                if text[j] == '\\':
                    j += 1
                j += 1
            blank(i + 1, j)
            i = j + 1
        else:
            i += 1
    return ''.join(out)


def mask_source(text):
    """Return text with comments, string/char literals, preprocessor
    directives and the #elif/#else branches of conditionals replaced by
    spaces.  Only the first branch of each #if chain is kept so that code
    like "#ifdef X\n if (a) {\n#else\n if (b) {\n#endif" still has
    balanced braces.  Newlines and offsets are preserved, so positions in
    the masked text map 1:1 onto the original text."""
    masked = _mask_comments_and_literals(text)
    lines = masked.split('\n')
    stack = []            # per open #if: True while in a skipped branch
    continued = False     # previous directive line ended with '\\'
    for idx, line in enumerate(lines):
        stripped = line.lstrip()
        if continued or stripped.startswith('#'):
            if not continued:
                directive = stripped[1:].lstrip().split(' ', 1)[0]
                if directive.startswith('if'):
                    stack.append(False)
                elif directive in ('elif', 'else') and stack:
                    stack[-1] = True
                elif directive == 'endif' and stack:
                    stack.pop()
            continued = line.rstrip().endswith('\\')
            lines[idx] = ' ' * len(line)
        elif any(stack):
            lines[idx] = ' ' * len(line)
    return '\n'.join(lines)


def match_close(masked, pos, open_ch, close_ch):
    """pos is the index of open_ch; return index of the matching close_ch
    or -1."""
    depth = 0
    n = len(masked)
    i = pos
    while i < n:
        c = masked[i]
        if c == open_ch:
            depth += 1
        elif c == close_ch:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def find_function_bodies(masked, qualified_name):
    """Return a list of (body_start, body_end) for every out-of-line
    definition of qualified_name in the masked text (body_start is the
    index of '{', body_end the index of the matching '}')."""
    bodies = []
    pat = re.compile(r'(?<![\w:])' + re.escape(qualified_name) + r'\s*\(')
    for m in pat.finditer(masked):
        lparen = m.end() - 1
        rparen = match_close(masked, lparen, '(', ')')
        if rparen < 0:
            continue
        brace = masked.find('{', rparen)
        semi = masked.find(';', rparen)
        if brace < 0 or (0 <= semi < brace):
            continue          # declaration or call
        if not QUALIFIERS_RE.match(masked[rparen + 1:brace]):
            continue          # e.g. a call inside "if (...) {"
        end = match_close(masked, brace, '{', '}')
        if end < 0:
            continue
        bodies.append((brace, end))
    return bodies


def split_args(s):
    """Split a macro argument string on top-level commas."""
    args, depth, cur = [], 0, []
    for c in s:
        if c in '([{':
            depth += 1
        elif c in ')]}':
            depth -= 1
        if c == ',' and depth == 0:
            args.append(''.join(cur))
            cur = []
        else:
            cur.append(c)
    args.append(''.join(cur))
    return [a.strip() for a in args]


CALL_RE = re.compile(r'^([\w:]+)\s*\((.*)\)$', re.DOTALL)


def _strip_outer_parens(a):
    """Strip a redundant outer '(...)' pair, but only when the leading '('
    is actually matched by the trailing ')' (not e.g. in "(a)+(b)")."""
    while a.startswith('(') and a.endswith(')'):
        depth = 0
        matched = True
        for i, c in enumerate(a):
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0 and i != len(a) - 1:
                    matched = False
                    break
        if not matched:
            break
        a = a[1:-1].strip()
    return a


def _split_ternary(s):
    """Return (pos of top-level '?', pos of its matching ':') in s, or
    (None, None).  Skips '::' (scope resolution) and nested ternaries."""
    depth = 0
    qpos = None
    nested = 0
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c in '([{':
            depth += 1
        elif c in ')]}':
            depth -= 1
        elif depth == 0 and c == ':' and i + 1 < n and s[i + 1] == ':':
            i += 2
            continue
        elif depth == 0 and c == '?':
            if qpos is None:
                qpos = i
            else:
                nested += 1
        elif depth == 0 and c == ':' and qpos is not None:
            if nested > 0:
                nested -= 1
            else:
                return qpos, i
        i += 1
    return None, None


def classify_level(arg):
    """Return the debug level of a dout()-family macro's level argument as
    an int, or None if it cannot be resolved to one.  Handles a literal
    int, a redundant '(...)' wrapper, ceph::dout::need_dynamic(...), and a
    '?:' conditional whose branches both resolve to a level (the level is
    then the minimum of the two, since that is the level that would need a
    baseline bump).  A bare identifier or function call (e.g. a level
    computed by a helper on an earlier line) is not resolvable: it is
    reported but not gated (see the "dynamic" note in main())."""
    a = _strip_outer_parens(arg.strip())
    if INT_RE.match(a):
        return int(a)
    m = CALL_RE.match(a)
    if m and m.group(1).rsplit('::', 1)[-1] == 'need_dynamic':
        return classify_level(m.group(2))
    q, c = _split_ternary(a)
    if q is not None:
        t = classify_level(a[q + 1:c])
        f = classify_level(a[c + 1:])
        if t is not None and f is not None:
            return min(t, f)
    return None


def count_statements(text, masked, start, end, max_level):
    """Count debug statements in masked[start:end]."""
    counts = {'le10': 0, 'dynamic': 0, 'exempt': 0, 'err': 0,
              'le15': 0, 'le20': 0, 'gt20': 0}
    body = masked[start:end]
    hits = []
    for m in PAREN_RE.finditer(body):
        name = m.group(1)
        lparen = start + m.end() - 1
        rparen = match_close(masked, lparen, '(', ')')
        if rparen < 0:
            continue
        if name in ERR_PAREN_MACROS:
            level = -1
        else:
            level = classify_level(split_args(masked[lparen + 1:rparen])[-1])
            if level is None:
                level = 'dynamic'
        hits.append((start + m.start(), level))
    for m in BARE_RE.finditer(body):
        hits.append((start + m.start(), -1))
    for pos, level in hits:
        if level == 'dynamic':
            key = 'dynamic'
        elif level < 0:
            counts['err'] += 1
            continue
        elif level <= max_level:
            key = 'le10'
        elif level <= 15:
            counts['le15'] += 1
            continue
        elif level <= 20:
            counts['le20'] += 1
            continue
        else:
            counts['gt20'] += 1
            continue
        # exemption marker on this line or the previous one (raw text)
        ls = text.rfind('\n', 0, pos)
        prev = text.rfind('\n', 0, ls) if ls > 0 else -1
        le = text.find('\n', pos)
        le = len(text) if le < 0 else le
        if EXEMPT_MARKER in text[prev + 1:le]:
            counts['exempt'] += 1
        else:
            counts[key] += 1
    return counts


def analyse(source_root, manifest, max_level):
    cache = {}
    results = []
    for ent in manifest:
        path = os.path.join(source_root, ent['file'])
        res = {'file': ent['file'], 'function': ent['function'],
               'found': False}
        if path not in cache:
            try:
                with open(path, encoding='utf-8', errors='replace') as f:
                    text = f.read()
                cache[path] = (text, mask_source(text))
            except OSError:
                cache[path] = None
        if cache[path] is not None:
            text, masked = cache[path]
            bodies = find_function_bodies(masked, ent['function'])
            if bodies:
                res['found'] = True
                res['definitions'] = len(bodies)
                total = None
                for s, e in bodies:
                    c = count_statements(text, masked, s, e, max_level)
                    if total is None:
                        total = c
                    else:
                        for k in c:
                            total[k] += c[k]
                res.update(total)
        results.append(res)
    return results


def write_baseline(path, base):
    """Write the manifest/baseline with one function per line so that
    diffs of the checked-in file stay readable."""
    ents = []
    for e in base.get('functions', []):
        ents.append('  ' + json.dumps({'file': e['file'],
                                       'function': e['function'],
                                       'le10': e.get('le10'),
                                       'dynamic': e.get('dynamic')}))
    with open(path, 'w') as f:
        f.write('{\n "version": %d,\n "max_level": %d,\n "functions": [\n'
                % (int(base.get('version', 1)), int(base.get('max_level', 10))))
        f.write(',\n'.join(ents))
        f.write('\n ]\n}\n')


def main(argv=None):
    ap = argparse.ArgumentParser(
        description='Fail if hot-path functions gained debug output at '
                    'level <= 10 (see doc/dev/osd_internals/'
                    'debug_log_levels.rst).')
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument('--source-root',
                    default=os.path.normpath(os.path.join(here, '..', '..')),
                    help='ceph source checkout (default: derived from the '
                         'location of this script)')
    ap.add_argument('--baseline',
                    default=os.path.join(here, 'dout_hotpath_baseline.json'),
                    help='manifest + baseline JSON file')
    ap.add_argument('--update', action='store_true',
                    help='rewrite the baseline counts from the current source')
    ap.add_argument('--strict', action='store_true',
                    help='also fail when a count went down (baseline stale) '
                         'or a manifest function is not found (renamed or '
                         'moved)')
    ap.add_argument('--verbose', '-v', action='store_true',
                    help='print every function, not just changes')
    a = ap.parse_args(argv)

    try:
        with open(a.baseline) as f:
            base = json.load(f)
    except (OSError, ValueError) as e:
        print('dout_hotpath_lint: cannot read %s: %s' % (a.baseline, e),
              file=sys.stderr)
        return 2
    max_level = int(base.get('max_level', 10))
    manifest = base.get('functions', [])
    results = analyse(a.source_root, manifest, max_level)

    if a.update:
        missing = [r for r in results if not r['found']]
        for ent, r in zip(manifest, results):
            if r['found']:
                ent['le10'] = r['le10']
                ent['dynamic'] = r['dynamic']
        write_baseline(a.baseline, base)
        for r in missing:
            print('dout_hotpath_lint: NOT FOUND %s in %s' %
                  (r['function'], r['file']), file=sys.stderr)
        print('dout_hotpath_lint: updated %s (%d functions)' %
              (a.baseline, len(manifest)))
        return 1 if missing else 0

    failures = 0
    stale = 0
    print('dout_hotpath_lint: level<=%d debug statements in %d hot-path '
          'functions (baseline %s)' % (max_level, len(manifest), a.baseline))
    renamed = 0
    for ent, r in zip(manifest, results):
        name = '%s:%s' % (r['file'], r['function'])
        if not r['found']:
            print('  %s %s: definition not found; if it was renamed or '
                  'moved, update the manifest in %s' %
                  ('FAIL' if a.strict else 'note', name, a.baseline))
            if a.strict:
                failures += 1
            else:
                renamed += 1
            continue
        msgs = []
        failed = False
        key = 'le10'
        want = ent.get(key)
        have = r[key]
        if want is None:
            msgs.append('%s=%d (no baseline)' % (key, have))
            failed = True
        elif have > want:
            msgs.append('%s %d > baseline %d' % (key, have, want))
            failed = True
        elif have < want:
            msgs.append('%s %d < baseline %d (lower the baseline)' %
                        (key, have, want))
            stale += 1
        if r['dynamic']:
            # need_dynamic() with a level that count_statements() could not
            # resolve to a literal (e.g. a variable computed by a helper on
            # an earlier line).  Not gated -- that would need dataflow
            # analysis across statements -- but shown so a reviewer can
            # check the levels used by eye; see classify_level().
            base_dynamic = ent.get('dynamic')
            note = 'dynamic=%d' % r['dynamic']
            if base_dynamic is not None and r['dynamic'] != base_dynamic:
                note += ' (baseline %d, not gated)' % base_dynamic
            msgs.append(note)
        if failed:
            failures += 1
        if msgs:
            print('  %s %s: %s' % ('FAIL' if failed else 'note', name,
                                   '; '.join(msgs)))
        elif a.verbose:
            print('  ok   %s: le10=%d dynamic=%d exempt=%d le15=%d le20=%d '
                  'err=%d' % (name, r['le10'], r['dynamic'], r['exempt'],
                              r['le15'], r['le20'], r['err']))
    if failures:
        print('dout_hotpath_lint: FAILED. A hot-path function gained a debug '
              'statement at level <= %d.  Level %d is the lean level: move '
              'good-path per-step output to 15 or 20, enrich an existing '
              'line instead of adding one, or mark a genuine error path with '
              '"// %s".  If the increase is intended, run\n'
              '  %s --update\nand commit the new baseline with a '
              'justification.  See doc/dev/osd_internals/'
              'debug_log_levels.rst.' % (max_level, max_level, EXEMPT_MARKER,
                                         os.path.relpath(__file__)))
        return 1
    if renamed:
        print('dout_hotpath_lint: %d function(s) not found (see "note" '
              'lines above); pass --strict to make this fatal, e.g. in a '
              'dedicated, non-gating job.  Most likely they were renamed or '
              'moved: update the manifest in %s.' % (renamed, a.baseline))
    if stale:
        print('dout_hotpath_lint: %d count(s) went down; run with --update '
              'to ratchet the baseline.' % stale)
        if a.strict:
            return 1
    print('dout_hotpath_lint: OK')
    return 0


if __name__ == '__main__':
    sys.exit(main())
