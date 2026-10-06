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
import builtins
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


# ---------------------------------------------------------------------------
# merge_pr_or_abort(): merge conflicts must be handled gracefully with
# automatic abort and clear error messaging
# ---------------------------------------------------------------------------

def test_merge_pr_or_abort_success(ptl_tool):
    """Successful merge should complete without calling abort."""
    G = mock.Mock()
    tip = mock.Mock()
    tip.hexsha = "abc123"
    message = "Merge PR #123"
    
    ptl_tool.merge_pr_or_abort(G, tip, message, 123)
    
    G.git.merge.assert_called_once_with("abc123", '--no-ff', m=message)


def test_merge_pr_or_abort_conflict_aborts_and_exits(ptl_tool, caplog):
    """Merge conflict should trigger abort and raise SystemExit with clear message."""
    G = mock.Mock()
    tip = mock.Mock()
    tip.hexsha = "abc123"
    message = "Merge PR #456"
    
    # Simulate merge conflict
    G.git.merge.side_effect = [
        ptl_tool.git.exc.GitCommandError('merge', 'CONFLICT'),
        None  # abort succeeds
    ]
    
    with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
        with pytest.raises(SystemExit) as exc_info:
            ptl_tool.merge_pr_or_abort(G, tip, message, 456)
    
    # Verify merge was attempted
    assert G.git.merge.call_count == 2
    G.git.merge.assert_any_call("abc123", '--no-ff', m=message)
    G.git.merge.assert_any_call('--abort')
    
    # Verify error message mentions the PR number
    message = str(exc_info.value)
    assert "456" in message
    assert "merge conflict" in message.lower()
    
    # Verify error was logged
    assert any(
        "Failed to merge PR #456" in r.message
        for r in caplog.records
    )


def test_merge_pr_or_abort_conflict_abort_fails(ptl_tool, caplog):
    """If abort also fails, original error should still be reported."""
    G = mock.Mock()
    tip = mock.Mock()
    tip.hexsha = "abc123"
    message = "Merge PR #789"
    
    # Simulate merge conflict AND abort failure
    merge_error = ptl_tool.git.exc.GitCommandError('merge', 'CONFLICT')
    abort_error = ptl_tool.git.exc.GitCommandError('merge --abort', 'fatal: no merge to abort')
    G.git.merge.side_effect = [merge_error, abort_error]
    
    with caplog.at_level(logging.WARNING, logger=ptl_tool.log.name):
        with pytest.raises(SystemExit) as exc_info:
            ptl_tool.merge_pr_or_abort(G, tip, message, 789)
    
    # Verify both merge and abort were attempted
    assert G.git.merge.call_count == 2
    
    # Verify the SystemExit message still references the original PR
    message = str(exc_info.value)
    assert "789" in message
    
    # Verify warning about abort failure was logged
    assert any(
        "Failed to abort merge" in r.message
        for r in caplog.records if r.levelname == "WARNING"
    )

# ---------------------------------------------------------------------------
# ensure_clean_checkout(): leftover in-progress merges from previous runs
# must be detected and automatically cleaned up before any operations begin
# ---------------------------------------------------------------------------

def _fake_exists_for_multiple(path_results):
    """os.path.exists side_effect that validates exact paths checked and returns
    appropriate results for each. path_results is a dict mapping expected paths
    to their return values."""
    def fake_exists(path):
        if path not in path_results:
            raise AssertionError(f"unexpected exists() check on {path!r}, expected one of {list(path_results.keys())}")
        return path_results[path]
    return fake_exists


def test_ensure_clean_checkout_clean_repo(ptl_tool):
    """When no MERGE_HEAD, no CHERRY_PICK_HEAD, and worktree is clean, function should be a no-op."""
    G = mock.Mock()
    G.git_dir = "/fake/repo/.git"
    G.is_dirty.return_value = False

    path_results = {
        "/fake/repo/.git/MERGE_HEAD": False,
        "/fake/repo/.git/CHERRY_PICK_HEAD": False,
    }
    
    with mock.patch("os.path.exists", side_effect=_fake_exists_for_multiple(path_results)):
        ptl_tool.ensure_clean_checkout(G)

    # Should check if worktree is dirty
    G.is_dirty.assert_called_once()
    
    # Should not attempt to abort anything
    G.git.merge.assert_not_called()


def test_ensure_clean_checkout_merge_in_progress(ptl_tool):
    """When MERGE_HEAD exists, function should raise SystemExit without attempting abort."""
    G = mock.Mock()
    G.git_dir = "/fake/repo/.git"

    path_results = {
        "/fake/repo/.git/MERGE_HEAD": True,
    }
    
    with mock.patch("os.path.exists", side_effect=_fake_exists_for_multiple(path_results)):
        with pytest.raises(SystemExit) as exc_info:
            ptl_tool.ensure_clean_checkout(G)

    # Should NOT call merge --abort (key behavioral change)
    G.git.merge.assert_not_called()
    
    # Should raise SystemExit with helpful message
    message = str(exc_info.value)
    assert "in-progress merge" in message.lower()
    assert "git merge --abort" in message


def test_ensure_clean_checkout_cherry_pick_in_progress(ptl_tool):
    """When CHERRY_PICK_HEAD exists (but not MERGE_HEAD), function should raise SystemExit."""
    G = mock.Mock()
    G.git_dir = "/fake/repo/.git"

    path_results = {
        "/fake/repo/.git/MERGE_HEAD": False,
        "/fake/repo/.git/CHERRY_PICK_HEAD": True,
    }
    
    with mock.patch("os.path.exists", side_effect=_fake_exists_for_multiple(path_results)):
        with pytest.raises(SystemExit) as exc_info:
            ptl_tool.ensure_clean_checkout(G)

    # Should NOT call any git commands
    G.git.merge.assert_not_called()
    
    # Should raise SystemExit with cherry-pick-specific message
    message = str(exc_info.value)
    assert "cherry-pick" in message.lower()
    assert "git cherry-pick --abort" in message


def test_ensure_clean_checkout_dirty_worktree(ptl_tool):
    """When worktree is dirty (no HEAD files), function should raise SystemExit."""
    G = mock.Mock()
    G.git_dir = "/fake/repo/.git"
    G.is_dirty.return_value = True

    path_results = {
        "/fake/repo/.git/MERGE_HEAD": False,
        "/fake/repo/.git/CHERRY_PICK_HEAD": False,
    }
    
    with mock.patch("os.path.exists", side_effect=_fake_exists_for_multiple(path_results)):
        with pytest.raises(SystemExit) as exc_info:
            ptl_tool.ensure_clean_checkout(G)

    # Should have checked is_dirty
    G.is_dirty.assert_called_once()
    
    # Should NOT call any git commands
    G.git.merge.assert_not_called()
    
    # Should raise SystemExit with uncommitted changes message
    message = str(exc_info.value)
    assert "uncommitted changes" in message.lower()
    assert "commit" in message.lower() or "stash" in message.lower()


def test_log_flag_adds_filehandler(ptl_tool, tmp_path, monkeypatch):
    """Without a label, the log file should be the generic ptl-tool.log in cwd."""
    logger = ptl_tool.log
    handlers_before = list(logger.handlers)
    monkeypatch.chdir(tmp_path)
    try:
        ret = ptl_tool.add_file_log_handler(logger)

        file_handlers = [
            h for h in logger.handlers
            if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1
        expected = str(tmp_path / 'ptl-tool.log')
        assert file_handlers[0].baseFilename == expected
        assert ret == expected
    finally:
        logger.handlers = handlers_before


def test_log_flag_uses_label_for_filename(ptl_tool, tmp_path, monkeypatch):
    """When a label is provided, the log file should be <label>.log in cwd."""
    logger = ptl_tool.log
    handlers_before = list(logger.handlers)
    monkeypatch.chdir(tmp_path)
    try:
        ptl_tool.add_file_log_handler(logger, label='wip-bharath8-testing')

        file_handlers = [
            h for h in logger.handlers
            if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1
        assert file_handlers[0].baseFilename == str(
            tmp_path / 'wip-bharath8-testing.log')
    finally:
        logger.handlers = handlers_before


def test_prompt_logged_exactly_once(ptl_tool, tmp_path, monkeypatch):
    """logged_input() must record the prompt exactly once in the log file."""
    logger = ptl_tool.log
    handlers_before = list(logger.handlers)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(builtins, "input", lambda prompt='': 'the-answer')
    try:
        log_path = ptl_tool.add_file_log_handler(logger, label="prompt-once")
        result = ptl_tool.logged_input("ask> ")
        assert result == "the-answer"
    finally:
        logger.handlers = handlers_before

    contents = Path(log_path).read_text()
    assert "ask> the-answer" in contents
    assert contents.count("ask> ") == 1


def test_logged_input_records_prompt_and_response_with_filehandler(ptl_tool, tmp_path, monkeypatch):
    """logged_input() should capture both prompt text and typed response."""
    logger = ptl_tool.log
    handlers_before = list(logger.handlers)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(builtins, "input", lambda prompt='': 'user-answer')
    try:
        log_path = ptl_tool.add_file_log_handler(logger, label="capture")
        result = ptl_tool.logged_input("prompt> ")
        assert result == "user-answer"
    finally:
        logger.handlers = handlers_before

    contents = Path(log_path).read_text()
    assert "prompt> user-answer" in contents


def test_logged_input_without_filehandler_preserves_behavior(ptl_tool, monkeypatch):
    """Without a FileHandler, logged_input() should behave like plain input()."""
    monkeypatch.setattr(builtins, "input", lambda prompt='': 'plain-answer')
    assert ptl_tool.logged_input("plain> ") == "plain-answer"


# ---------------------------------------------------------------------------
# build_subject_with_branch_history(): on --update-qa the new branch becomes the
# subject anchor and prior branches accumulate newest-first in a parenthesized
# trailing list.
# ---------------------------------------------------------------------------

B1 = "wip-yuri-testing-20261002.125630-umbrella"
B2 = "wip-yuri-testing-20261003.180813-umbrella"
B3 = "wip-yuri-testing-20261005.175703-umbrella"
B4 = "wip-yuri-testing-20261006.163134-umbrella"


def test_subject_first_update_moves_branch_into_history(ptl_tool):
    assert ptl_tool.build_subject_with_branch_history(B1, B2) == f"{B2} ({B1})"


def test_subject_accumulates_newest_first(ptl_tool):
    """Replays the real tracker 81324 update sequence."""
    s = ptl_tool.build_subject_with_branch_history(B1, B2)
    s = ptl_tool.build_subject_with_branch_history(s, B3)
    s = ptl_tool.build_subject_with_branch_history(s, B4)
    assert s == f"{B4} ({B3}, {B2}, {B1})"


def test_subject_idempotent_reparse(ptl_tool):
    """A subject already in '<anchor> (<history>)' form re-parses, never nests."""
    existing = f"{B3} ({B2}, {B1})"
    assert ptl_tool.build_subject_with_branch_history(existing, B4) == \
        f"{B4} ({B3}, {B2}, {B1})"


def test_subject_noop_on_same_branch(ptl_tool):
    existing = f"{B2} ({B1})"
    assert ptl_tool.build_subject_with_branch_history(existing, B2) == existing


def test_subject_leaves_non_branch_parens_intact(ptl_tool):
    """A human subject ending in '(...)' that isn't branch history is preserved."""
    base = "umbrella integration (round 2)"
    assert ptl_tool.build_subject_with_branch_history(base, B2) == f"{B2} ({base})"


def test_subject_respects_length_cap(ptl_tool):
    existing = f"{B4} ({B3}, {B2}, {B1})"
    new = "wip-yuri-testing-20261007.101010-umbrella"
    result = ptl_tool.build_subject_with_branch_history(existing, new, max_len=120)
    assert len(result) <= 120
    assert result.startswith(f"{new} (")
    assert result.endswith("...)")  # oldest entries dropped


def test_subject_truncation_marker_reparses_without_nesting(ptl_tool):
    """A previously length-capped subject (trailing '...') must re-parse cleanly
    on the next update: no nested parens, '...' not treated as a branch."""
    capped = f"{B3} ({B2}, ...)"
    result = ptl_tool.build_subject_with_branch_history(capped, B4)
    assert result == f"{B4} ({B3}, {B2})"
    assert result.count("(") == 1 and result.count(")") == 1


def test_subject_promotes_existing_entry_without_dup(ptl_tool):
    """Re-running a branch already in the history promotes it to anchor once,
    leaving no duplicate."""
    existing = f"{B4} ({B2}, {B1})"
    result = ptl_tool.build_subject_with_branch_history(existing, B2)
    assert result == f"{B2} ({B4}, {B1})"
    assert result.count(B2) == 1


def test_subject_length_cap_stays_parseable_with_huge_names(ptl_tool):
    """Even when a single entry can't fit, the result stays parseable
    (never a mid-name/mid-paren hard cut)."""
    anchor = "wip-" + "a" * 120 + "-20261006.163134-x"
    new = "wip-" + "b" * 120 + "-20261006.163134-y"
    result = ptl_tool.build_subject_with_branch_history(anchor, new, max_len=150)
    assert len(result) <= 150
    # history dropped entirely -> bare new branch, no dangling open paren
    assert result == new[:150]
    assert "(" not in result
