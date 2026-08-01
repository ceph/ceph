"""
Unit tests for ptl-tool.py's credential-failure handling.

ptl-tool.py has no existing automated test suite; this file targets only the
two code paths touched by the "fail fast on invalid credentials" fix so a
regression can be caught without needing live GitHub/Redmine access or a
real git checkout.

Run with:
    python3 -m venv /tmp/ptl-test-venv
    /tmp/ptl-test-venv/bin/pip install GitPython python-redmine requests pytest
    /tmp/ptl-test-venv/bin/pytest src/script/test_ptl_tool.py -v
"""
import importlib.util
import logging
import sys
from pathlib import Path
from unittest import mock

import pytest

SCRIPT_PATH = Path(__file__).parent / "ptl-tool.py"


@pytest.fixture(scope="module")
def ptl_tool():
    """
    ptl-tool.py's filename isn't a valid module name (hyphen), so it's loaded
    directly from its file path. Its module-level code only performs local,
    side-effect-free work when imported from within a git checkout (which
    this test file always is, since it lives next to ptl-tool.py in the repo).
    """
    spec = importlib.util.spec_from_file_location("ptl_tool", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    # Dataclass field resolution (used by AuditContext/AuditLabels below) looks
    # the module up via sys.modules[cls.__module__], so it must be registered
    # there before exec_module() runs the class bodies.
    sys.modules["ptl_tool"] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


# ---------------------------------------------------------------------------
# verify_redmine_auth(): invalid/expired Redmine API key must fail fast, with
# a clear message, before any PR merging or branch pushing can happen.
# ---------------------------------------------------------------------------

def test_verify_redmine_auth_raises_systemexit_on_invalid_key(ptl_tool):
    R = mock.Mock()
    R.user.get.side_effect = ptl_tool.redminelib.exceptions.AuthError()
    with pytest.raises(SystemExit) as exc_info:
        ptl_tool.verify_redmine_auth(R)
    message = str(exc_info.value)
    assert "Redmine authentication failed" in message
    assert "before any PRs are merged or branches pushed" in message


def test_verify_redmine_auth_raises_systemexit_on_forbidden_key(ptl_tool):
    """A key that authenticates but lacks permission should fail the same way."""
    R = mock.Mock()
    R.user.get.side_effect = ptl_tool.redminelib.exceptions.ForbiddenError()
    with pytest.raises(SystemExit):
        ptl_tool.verify_redmine_auth(R)


def test_verify_redmine_auth_passes_on_valid_key(ptl_tool):
    R = mock.Mock()
    R.user.get.return_value = {"id": 1, "login": "yuriw"}
    ptl_tool.verify_redmine_auth(R)  # must not raise
    R.user.get.assert_called_once_with('current')


def test_verify_redmine_auth_does_not_swallow_other_errors(ptl_tool):
    """Only auth/permission failures are handled here; anything else should
    propagate normally rather than being misreported as a credentials issue."""
    R = mock.Mock()
    R.user.get.side_effect = ptl_tool.redminelib.exceptions.ServerError()
    with pytest.raises(ptl_tool.redminelib.exceptions.ServerError):
        ptl_tool.verify_redmine_auth(R)


# ---------------------------------------------------------------------------
# AuditReport.post_consolidated_review(): a failed GitHub write (bad/expired
# token, insufficient scope) must be logged, not silently dropped.
# ---------------------------------------------------------------------------

def _report_with_one_issue(ptl_tool):
    report = ptl_tool.AuditReport()
    report.add("Conflict/Deviation", "some finding that needs a reviewer's attention")
    return report


def test_post_consolidated_review_logs_success_on_2xx(ptl_tool, caplog):
    report = _report_with_one_issue(ptl_tool)
    session = mock.Mock()
    session.post.return_value = FakeResponse(201)
    with caplog.at_level(logging.INFO, logger=ptl_tool.log.name):
        report.post_consolidated_review(session, pr=12345, dry_run=False)
    session.post.assert_called_once()
    assert any(
        "Successfully posted consolidated review to PR #12345" in r.message
        for r in caplog.records
    )


def test_post_consolidated_review_logs_error_on_failure(ptl_tool, caplog):
    report = _report_with_one_issue(ptl_tool)
    session = mock.Mock()
    session.post.return_value = FakeResponse(401, "Bad credentials")
    with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
        report.post_consolidated_review(session, pr=12345, dry_run=False)
    assert any(
        "Failed to post consolidated review to PR #12345" in r.message
        and "401" in r.message
        for r in caplog.records
    )


def test_post_consolidated_review_dry_run_never_calls_session(ptl_tool):
    report = _report_with_one_issue(ptl_tool)
    session = mock.Mock()
    report.post_consolidated_review(session, pr=12345, dry_run=True)
    session.post.assert_not_called()


def test_post_consolidated_review_noop_when_no_issues(ptl_tool):
    """An empty report has nothing to post, so the GitHub call should be skipped
    entirely -- this stays true whether or not credentials are valid."""
    report = ptl_tool.AuditReport()
    session = mock.Mock()
    report.post_consolidated_review(session, pr=12345, dry_run=False)
    session.post.assert_not_called()
