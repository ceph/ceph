#!/usr/bin/env python3
"""check-commit-msg.py - Validate commit messages against Ceph conventions.

Checks that commit messages comply with the rules described in
SubmittingPatches.rst.  Rules enforced:

  * Subject line is at most 72 characters long.
  * Subject line is prefixed with a subsystem/component followed by a colon,
    e.g. ``osd: fix memory leak`` or ``mgr/dashboard: add user page``.
  * If the commit has a body, the second line must be blank.
  * A ``Signed-off-by:`` trailer must be present.

Optional but checked rules (warnings):

  * Body lines are at most 72 characters long (URLs and quoted blocks are
    exempt).

Usage::

    # Check a single commit
    src/script/check-commit-msg.py <sha>

    # Check a range of commits
    src/script/check-commit-msg.py <since>..<until>
    src/script/check-commit-msg.py HEAD~5..HEAD

    # Read a raw commit message from stdin (useful for commit-msg git hooks)
    src/script/check-commit-msg.py --stdin

    # Read raw message from a file (also useful for git hooks)
    src/script/check-commit-msg.py --file <path>

Exit code is 0 when all commits pass; non-zero when at least one violation is
found.

See SubmittingPatches.rst for the full Ceph commit-message conventions.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Constants / compiled regexps
# ---------------------------------------------------------------------------

# Subject line must be at most this many characters.
MAX_SUBJECT_LEN: int = 72

# Body lines should be at most this many characters (advisory / warning only).
MAX_BODY_LINE_LEN: int = 72

# Valid subsystem prefix: one or more path-like segments (letters, digits,
# hyphens, underscores, dots) separated by slashes.  Examples:
#   osd  mds  mgr/dashboard  doc/rados/api  qa/suites  bluestore
_SUBSYSTEM_RE = re.compile(
    r"^(?P<subsystem>[\w][\w./-]*):\s+(?P<description>.+)$"
)

# Signed-off-by trailer (required).
_SIGNED_OFF_BY_RE = re.compile(
    r"^Signed-off-by:\s+.+\s+<[^>]+>$", re.MULTILINE
)

# Lines that are exempt from body line-length checks (URLs, quoted, etc.)
_LONG_LINE_EXEMPT_RE = re.compile(
    r"(https?://|ftp://|^\s*>|Signed-off-by:|Co-authored-by:|Reviewed-by:|"
    r"Tested-by:|Fixes:|Reported-by:|cherry picked from commit)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Violation:
    kind: str   # "error" or "warning"
    message: str


@dataclass
class CommitResult:
    sha: str
    subject: str
    violations: List[Violation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not any(v.kind == "error" for v in self.violations)

    @property
    def has_warnings(self) -> bool:
        return any(v.kind == "warning" for v in self.violations)


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------

def validate_message(raw_message: str) -> List[Violation]:
    """Validate *raw_message* and return a (possibly empty) list of Violations.

    *raw_message* is the full commit message text, including trailers, as
    returned by ``git log --format=%B``.
    """
    violations: List[Violation] = []
    lines = raw_message.splitlines()

    # Strip trailing blank lines so we don't penalise well-formed messages.
    while lines and not lines[-1].strip():
        lines.pop()

    if not lines:
        violations.append(Violation("error", "Commit message is empty."))
        return violations

    subject = lines[0]
    body_lines = lines[1:]

    # ------------------------------------------------------------------
    # 1. Subject line length
    # ------------------------------------------------------------------
    if len(subject) > MAX_SUBJECT_LEN:
        violations.append(
            Violation(
                "error",
                f"Subject line is {len(subject)} characters long; "
                f"maximum is {MAX_SUBJECT_LEN}.\n"
                f"  Subject: {subject!r}",
            )
        )

    # ------------------------------------------------------------------
    # 2. Subject must follow "subsystem: description" format
    # ------------------------------------------------------------------
    m = _SUBSYSTEM_RE.match(subject)
    if not m:
        violations.append(
            Violation(
                "error",
                "Subject line does not follow 'subsystem: description' "
                "format.\n"
                f"  Subject: {subject!r}\n"
                "  Expected format example: 'osd: fix memory leak in "
                "BlueStore'",
            )
        )
    else:
        description = m.group("description")
        if not description.strip():
            violations.append(
                Violation(
                    "error",
                    "Subject line has an empty description after the "
                    "subsystem prefix.\n"
                    f"  Subject: {subject!r}",
                )
            )

    # ------------------------------------------------------------------
    # 3. Second line must be blank (if body exists)
    # ------------------------------------------------------------------
    if body_lines and body_lines[0].strip():
        violations.append(
            Violation(
                "error",
                "The second line of the commit message must be blank "
                "(separating subject from body).\n"
                f"  Second line: {body_lines[0]!r}",
            )
        )

    # ------------------------------------------------------------------
    # 4. Signed-off-by trailer is required
    # ------------------------------------------------------------------
    if not _SIGNED_OFF_BY_RE.search(raw_message):
        violations.append(
            Violation(
                "error",
                "Missing 'Signed-off-by:' trailer.\n"
                "  Add a line like: "
                "Signed-off-by: Your Name <your@email.example.com>\n"
                "  Tip: use 'git commit -s' to add it automatically.",
            )
        )

    # ------------------------------------------------------------------
    # 5. Body line length (warning only)
    # ------------------------------------------------------------------
    # Skip the blank separator line (index 0 in body_lines).
    for i, line in enumerate(body_lines[1:], start=3):
        if len(line) > MAX_BODY_LINE_LEN and not _LONG_LINE_EXEMPT_RE.search(
            line
        ):
            violations.append(
                Violation(
                    "warning",
                    f"Body line {i} is {len(line)} characters long; "
                    f"consider wrapping at {MAX_BODY_LINE_LEN}.\n"
                    f"  Line: {line!r}",
                )
            )

    return violations


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _git(*args: str) -> str:
    """Run a git command and return stdout, raising on failure."""
    result = subprocess.run(
        ["git"] + list(args),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed:\n{result.stderr.strip()}"
        )
    return result.stdout


def get_commits_in_range(revision_range: str) -> List[Tuple[str, str]]:
    """Return [(sha, message), ...] for commits in *revision_range*.

    *revision_range* may be a single SHA or a range like ``A..B``.
    """
    # Use NUL-delimited output so multi-line messages are handled safely.
    raw = _git(
        "log",
        "--format=%H%x00%B%x00",
        revision_range,
    )

    commits: List[Tuple[str, str]] = []
    # Split on double-NUL (sha NUL body NUL between records).
    parts = raw.split("\x00")
    # parts alternates: sha, body, sha, body, ... possibly ending with ''
    i = 0
    while i + 1 < len(parts):
        sha = parts[i].strip()
        message = parts[i + 1]
        if sha:
            commits.append((sha, message))
        i += 2

    return commits


# ---------------------------------------------------------------------------
# Formatting / output
# ---------------------------------------------------------------------------

_RESET = "\033[0m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_GREEN = "\033[32m"
_BOLD = "\033[1m"


def _color(text: str, code: str, use_color: bool) -> str:
    if use_color:
        return f"{code}{text}{_RESET}"
    return text


def print_result(
    result: CommitResult, use_color: bool = True, verbose: bool = False
) -> None:
    short_sha = result.sha[:12] if len(result.sha) >= 12 else result.sha
    short_subject = (
        result.subject[:60] + "…"
        if len(result.subject) > 60
        else result.subject
    )
    prefix = f"  commit {short_sha}: {short_subject!r}"

    if result.passed and not result.has_warnings:
        if verbose:
            print(_color("  PASS", _GREEN, use_color) + f"  {prefix[2:]}")
        return

    if not result.passed:
        status = _color("  FAIL", _RED, use_color)
    else:
        status = _color("  WARN", _YELLOW, use_color)

    print(f"{status}  {short_sha} {short_subject!r}")
    for v in result.violations:
        if v.kind == "error":
            label = _color("    ERROR:", _RED, use_color)
        else:
            label = _color("    WARNING:", _YELLOW, use_color)
        # Indent continuation lines.
        detail = v.message.replace("\n", "\n    " + " " * 8)
        print(f"{label} {detail}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--stdin",
        action="store_true",
        help="Read a raw commit message from stdin instead of querying git.",
    )
    group.add_argument(
        "--file",
        metavar="PATH",
        help="Read a raw commit message from PATH instead of querying git.",
    )
    parser.add_argument(
        "revision",
        nargs="?",
        metavar="REVISION",
        help=(
            "A single commit SHA or revision range (e.g. HEAD~3..HEAD). "
            "Omit when using --stdin or --file."
        ),
    )
    parser.add_argument(
        "--no-color",
        dest="color",
        action="store_false",
        default=sys.stdout.isatty(),
        help="Disable color output.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Also print PASS lines for commits that pass all checks.",
    )
    parser.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Treat warnings as errors (non-zero exit code).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    commits: List[Tuple[str, str]] = []

    if args.stdin:
        raw = sys.stdin.read()
        commits = [("<stdin>", raw)]
    elif args.file:
        raw = Path(args.file).read_text()
        commits = [("<file>", raw)]
    elif args.revision:
        try:
            commits = get_commits_in_range(args.revision)
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        if not commits:
            print(
                f"warning: no commits found for revision '{args.revision}'.",
                file=sys.stderr,
            )
            return 0
    else:
        print(
            "error: provide a REVISION, --stdin, or --file.",
            file=sys.stderr,
        )
        return 1

    results: List[CommitResult] = []
    for sha, message in commits:
        violations = validate_message(message)
        subject = message.splitlines()[0] if message.strip() else ""
        results.append(CommitResult(sha=sha, subject=subject, violations=violations))

    total = len(results)
    failed = sum(1 for r in results if not r.passed)
    warned = sum(
        1 for r in results if r.passed and r.has_warnings
    )

    for result in results:
        print_result(result, use_color=args.color, verbose=args.verbose)

    # Summary line
    if args.verbose or failed or warned:
        parts = [f"{total} commit(s) checked"]
        if failed:
            parts.append(_color(f"{failed} failed", _RED, args.color))
        if warned:
            parts.append(_color(f"{warned} with warnings", _YELLOW, args.color))
        if not failed and not warned:
            parts.append(_color("all passed", _GREEN, args.color))
        print("\n" + ", ".join(parts) + ".")

    if failed:
        return 1
    if args.warnings_as_errors and warned:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
