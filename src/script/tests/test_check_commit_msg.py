"""Tests for src/script/check-commit-msg.py.

Run with::

    pytest src/script/tests/test_check_commit_msg.py

or::

    python3 -m pytest src/script/tests/test_check_commit_msg.py
"""

import importlib.util
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Import the module under test (it has hyphens in its name, so we load it
# directly via importlib rather than using the normal import statement).
# ---------------------------------------------------------------------------

_SCRIPT = Path(__file__).parent.parent / "check-commit-msg.py"
_spec = importlib.util.spec_from_file_location("check_commit_msg", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["check_commit_msg"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

validate_message = _mod.validate_message
Violation = _mod.Violation
main = _mod.main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def errors(msg: str):
    """Return only error-level violations for *msg*."""
    return [v for v in validate_message(msg) if v.kind == "error"]


def warnings(msg: str):
    """Return only warning-level violations for *msg*."""
    return [v for v in validate_message(msg) if v.kind == "warning"]


def violation_messages(violations):
    return [v.message for v in violations]


# ---------------------------------------------------------------------------
# Well-formed messages (should produce no errors)
# ---------------------------------------------------------------------------

class TestValidMessages:
    def test_minimal_valid(self):
        msg = (
            "osd: fix memory leak in BlueStore\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        assert errors(msg) == []

    def test_with_body(self):
        msg = (
            "mgr/dashboard: add user management page\n"
            "\n"
            "This commit adds a new user management page to the Ceph\n"
            "Dashboard.  It allows administrators to create, edit, and\n"
            "delete cluster users through the web UI.\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        assert errors(msg) == []

    def test_with_fixes_trailer(self):
        msg = (
            "mon: prevent election storm on leader change\n"
            "\n"
            "Detailed explanation here.\n"
            "\n"
            "Fixes: http://tracker.ceph.com/issues/12345\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        assert errors(msg) == []

    def test_path_subsystem_prefix(self):
        """Subsystem can be a path-like string such as doc/rados/api."""
        msg = (
            "doc/rados/api: document new CRUSH rule option\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        assert errors(msg) == []

    def test_deep_path_subsystem(self):
        msg = (
            "qa/suites/rados/basic: add test for scrub\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        assert errors(msg) == []

    def test_no_body(self):
        """Commit with only a subject line and trailer is valid."""
        msg = "rbd: fix clone operation\n\nSigned-off-by: Jane Dev <jane@example.com>\n"
        assert errors(msg) == []

    def test_subject_exactly_72_chars(self):
        # 72 chars exactly: no error
        prefix = "osd: "
        desc = "x" * (72 - len(prefix))
        msg = f"{prefix}{desc}\n\nSigned-off-by: Jane Dev <jane@example.com>\n"
        assert len(msg.splitlines()[0]) == 72
        assert errors(msg) == []

    def test_multiple_signed_off_by(self):
        msg = (
            "common: improve logging output\n"
            "\n"
            "Signed-off-by: Alice <alice@example.com>\n"
            "Signed-off-by: Bob <bob@example.com>\n"
        )
        assert errors(msg) == []


# ---------------------------------------------------------------------------
# Subject line length
# ---------------------------------------------------------------------------

class TestSubjectLength:
    def test_subject_too_long(self):
        prefix = "osd: "
        desc = "x" * (73 - len(prefix))  # one char over the limit
        msg = f"{prefix}{desc}\n\nSigned-off-by: Jane Dev <jane@example.com>\n"
        errs = errors(msg)
        assert any("72" in e.message for e in errs), errs

    def test_subject_much_too_long(self):
        msg = (
            "osd: " + "a" * 100 + "\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("characters long" in e.message for e in errs)


# ---------------------------------------------------------------------------
# Subsystem prefix
# ---------------------------------------------------------------------------

class TestSubsystemPrefix:
    def test_missing_prefix(self):
        msg = (
            "fix memory leak\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)

    def test_no_colon(self):
        msg = (
            "osd fix memory leak\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)

    def test_colon_only(self):
        msg = (
            ": empty subsystem\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)

    def test_empty_description_after_colon(self):
        msg = (
            "osd:   \n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        # Either the regex doesn't match at all OR the empty-description check fires.
        assert len(errs) >= 1

    def test_negative_example_update_driver(self):
        """'update driver X' is listed as a negative example in the docs."""
        msg = (
            "update driver X\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)

    def test_negative_example_bug_fix(self):
        msg = (
            "bug fix for driver X\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)

    def test_negative_example_fix_issue(self):
        msg = (
            "fix issue 99999\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("subsystem: description" in e.message for e in errs)


# ---------------------------------------------------------------------------
# Blank second line
# ---------------------------------------------------------------------------

class TestBlankSecondLine:
    def test_missing_blank_line(self):
        msg = (
            "osd: fix memory leak\n"
            "This line should be blank.\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("second line" in e.message.lower() for e in errs)

    def test_blank_line_with_spaces(self):
        """A line containing only spaces is treated as a blank separator line."""
        msg = (
            "osd: fix memory leak\n"
            "   \n"
            "Body text.\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        # "   ".strip() is empty, so the check treats it as a blank line.
        errs = errors(msg)
        assert not any("second line" in e.message.lower() for e in errs)


# ---------------------------------------------------------------------------
# Signed-off-by trailer
# ---------------------------------------------------------------------------

class TestSignedOffBy:
    def test_missing_signed_off_by(self):
        msg = "osd: fix memory leak\n\nBody text.\n"
        errs = errors(msg)
        assert any("Signed-off-by" in e.message for e in errs)

    def test_malformed_signed_off_by_no_email(self):
        msg = (
            "osd: fix memory leak\n"
            "\n"
            "Signed-off-by: Jane Dev\n"
        )
        errs = errors(msg)
        assert any("Signed-off-by" in e.message for e in errs)

    def test_malformed_signed_off_by_wrong_tag(self):
        msg = (
            "osd: fix memory leak\n"
            "\n"
            "Signed off by: Jane Dev <jane@example.com>\n"
        )
        errs = errors(msg)
        assert any("Signed-off-by" in e.message for e in errs)


# ---------------------------------------------------------------------------
# Body line length (warnings)
# ---------------------------------------------------------------------------

class TestBodyLineLength:
    def test_long_body_line_warning(self):
        long_line = "w" * 80
        msg = (
            "osd: some change\n"
            "\n"
            f"{long_line}\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        warns = warnings(msg)
        assert any("characters long" in w.message for w in warns)

    def test_long_url_exempt(self):
        """Long lines containing URLs should not trigger a warning."""
        long_url = "https://tracker.ceph.com/issues/" + "9" * 60
        msg = (
            "osd: some change\n"
            "\n"
            f"See {long_url} for details.\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        warns = warnings(msg)
        assert warns == []

    def test_long_signed_off_by_exempt(self):
        long_sop = "Signed-off-by: " + "X" * 80 + " <x@example.com>"
        msg = (
            "osd: some change\n"
            "\n"
            "Short body.\n"
            "\n"
            f"{long_sop}\n"
        )
        warns = warnings(msg)
        assert warns == []


# ---------------------------------------------------------------------------
# Empty / degenerate input
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_message(self):
        errs = errors("")
        assert any("empty" in e.message.lower() for e in errs)

    def test_only_whitespace(self):
        errs = errors("   \n\n  ")
        assert any("empty" in e.message.lower() for e in errs)

    def test_subject_only_no_trailer(self):
        msg = "osd: single line commit\n"
        errs = errors(msg)
        assert any("Signed-off-by" in e.message for e in errs)


# ---------------------------------------------------------------------------
# CLI integration tests (using main())
# ---------------------------------------------------------------------------

class TestCLI:
    def test_stdin_valid(self, monkeypatch, capsys):
        import io
        good_msg = (
            "osd: fix memory leak in BlueStore\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        monkeypatch.setattr("sys.stdin", io.StringIO(good_msg))
        rc = main(["--stdin", "--no-color"])
        assert rc == 0

    def test_stdin_invalid(self, monkeypatch, capsys):
        import io
        bad_msg = "no prefix and no signed-off-by\n"
        monkeypatch.setattr("sys.stdin", io.StringIO(bad_msg))
        rc = main(["--stdin", "--no-color"])
        assert rc == 1

    def test_file_valid(self, tmp_path):
        good_msg = (
            "osd: fix memory leak in BlueStore\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        p = tmp_path / "commit_msg.txt"
        p.write_text(good_msg)
        rc = main(["--file", str(p), "--no-color"])
        assert rc == 0

    def test_file_invalid(self, tmp_path):
        bad_msg = "missing prefix\n\nno trailer\n"
        p = tmp_path / "commit_msg.txt"
        p.write_text(bad_msg)
        rc = main(["--file", str(p), "--no-color"])
        assert rc == 1

    def test_no_args_returns_error(self, capsys):
        rc = main(["--no-color"])
        assert rc == 1

    def test_warnings_as_errors_flag(self, tmp_path):
        """A message with only warnings + --warnings-as-errors exits non-zero."""
        long_line = "w" * 80
        msg = (
            "osd: some change\n"
            f"\n{long_line}\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        p = tmp_path / "commit_msg.txt"
        p.write_text(msg)
        rc = main(["--file", str(p), "--no-color", "--warnings-as-errors"])
        assert rc == 1

    def test_verbose_shows_pass(self, tmp_path, capsys):
        good_msg = (
            "osd: fix memory leak\n"
            "\n"
            "Signed-off-by: Jane Dev <jane@example.com>\n"
        )
        p = tmp_path / "commit_msg.txt"
        p.write_text(good_msg)
        main(["--file", str(p), "--no-color", "--verbose"])
        out = capsys.readouterr().out
        assert "PASS" in out
