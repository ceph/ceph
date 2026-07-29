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
# check_pr_approvals(): pre-flight approval gate for --pr-label merges,
# native equivalent of ptl-check-approvals.sh. Uses one batched GraphQL call
# (reviewDecision is only exposed there, same field `gh pr list
# --json reviewDecision` reads) instead of N REST calls.
# ---------------------------------------------------------------------------

class FakeJSONResponse(FakeResponse):
    def __init__(self, status_code, json_data=None, text=""):
        super().__init__(status_code, text)
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


def _graphql_response(decisions):
    """decisions: dict of pr_number -> (reviewDecision, title, author_login)."""
    repo = {}
    for i, pr in enumerate(decisions):
        review_decision, title, author = decisions[pr]
        repo[f"pr{i}"] = {
            "number": pr,
            "title": title,
            "reviewDecision": review_decision,
            "author": {"login": author},
        }
    return FakeJSONResponse(200, {"data": {"repository": repo}})


def test_check_pr_approvals_noop_when_no_prs(ptl_tool):
    session = mock.Mock()
    result = ptl_tool.check_pr_approvals(session, [], "wip-yuri-testing")
    assert result == []
    session.post.assert_not_called()


def test_check_pr_approvals_all_approved_returns_unchanged_no_prompt(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("APPROVED", "fix a", "alice"),
        200: ("APPROVED", "fix b", "bob"),
    })
    with mock.patch("builtins.input") as fake_input:
        result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing")
    fake_input.assert_not_called()
    assert result == [100, 200]
    session.post.assert_called_once()


def test_check_pr_approvals_graphql_query_includes_all_pr_numbers(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("APPROVED", "a", "alice"),
        200: ("APPROVED", "b", "bob"),
        300: ("APPROVED", "c", "carol"),
    })
    ptl_tool.check_pr_approvals(session, [100, 200, 300], "wip-yuri-testing")
    query = session.post.call_args.kwargs["json"]["query"]
    assert "pr0: pullRequest(number: 100)" in query
    assert "pr1: pullRequest(number: 200)" in query
    assert "pr2: pullRequest(number: 300)" in query


def test_check_pr_approvals_graphql_error_response_exits(ptl_tool):
    session = mock.Mock()
    session.post.return_value = FakeResponse(401, "Bad credentials")
    with pytest.raises(SystemExit):
        ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")


def test_check_pr_approvals_ci_mode_skips_prompt_and_proceeds(ptl_tool, caplog):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
    })
    with mock.patch("builtins.input") as fake_input:
        with caplog.at_level(logging.WARNING, logger=ptl_tool.log.name):
            result = ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing", ci_mode=True)
    fake_input.assert_not_called()
    assert result == [100]
    assert any("NOT approved" in r.message and "--ci-mode" in r.message for r in caplog.records)


def test_check_pr_approvals_accept_posts_reminder_comment_and_keeps_all_prs(ptl_tool):
    session = mock.Mock()
    session.post.side_effect = [
        _graphql_response({
            100: ("REVIEW_REQUIRED", "needs review", "alice"),
            200: ("APPROVED", "already good", "bob"),
        }),
        FakeResponse(201),
    ]
    with mock.patch("builtins.input", return_value="a"):
        result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing")
    assert result == [100, 200]
    assert session.post.call_count == 2
    comment_call = session.post.call_args_list[1]
    assert comment_call.args[0] == "https://api.github.com/repos/ceph/ceph/issues/100/comments"
    assert "@alice" in comment_call.kwargs["json"]["body"]
    assert "wip-yuri-testing" in comment_call.kwargs["json"]["body"]
    session.delete.assert_not_called()


def test_check_pr_approvals_accept_with_two_unapproved_prs_comments_on_both(ptl_tool):
    """The single-unapproved-PR accept test above only proves one comment POST
    fires; this confirms the `for u in unapproved` loop actually iterates
    rather than stopping after the first PR."""
    session = mock.Mock()
    session.post.side_effect = [
        _graphql_response({
            100: ("REVIEW_REQUIRED", "needs review", "alice"),
            300: ("REVIEW_REQUIRED", "also needs review", "carol"),
        }),
        FakeResponse(201),
        FakeResponse(201),
    ]
    with mock.patch("builtins.input", return_value="a"):
        result = ptl_tool.check_pr_approvals(session, [100, 300], "wip-yuri-testing")
    assert result == [100, 300]
    assert session.post.call_count == 3  # 1 GraphQL fetch + 2 comments
    commented_urls = {c.args[0] for c in session.post.call_args_list[1:]}
    assert commented_urls == {
        "https://api.github.com/repos/ceph/ceph/issues/100/comments",
        "https://api.github.com/repos/ceph/ceph/issues/300/comments",
    }


def test_check_pr_approvals_accept_unknown_author_has_no_broken_mention(ptl_tool):
    """A PR whose author resolved to None must not produce a comment addressed
    to the literal string '@unknown' -- that pings no one and looks broken."""
    session = mock.Mock()
    session.post.side_effect = [
        _graphql_response({100: ("REVIEW_REQUIRED", "needs review", None)}),
        FakeResponse(201),
    ]
    with mock.patch("builtins.input", return_value="a"):
        ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")
    comment_call = session.post.call_args_list[1]
    assert "@unknown" not in comment_call.kwargs["json"]["body"]


def test_check_pr_approvals_missing_pr_node_raises_systemexit(ptl_tool):
    """If GraphQL returns no node for one of the aliased PRs (e.g. it doesn't
    exist in this repo), the PR's approval status is unverifiable -- this must
    fail closed (exit) rather than silently treating it as approved."""
    session = mock.Mock()
    session.post.return_value = FakeJSONResponse(200, {"data": {"repository": {}}})
    with pytest.raises(SystemExit):
        ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")


def test_check_pr_approvals_remove_deletes_label_and_excludes_pr(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
        200: ("APPROVED", "already good", "bob"),
    })
    session.delete.return_value = FakeResponse(200)
    with mock.patch("builtins.input", return_value="r"):
        result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing")
    assert result == [200]
    session.delete.assert_called_once_with(
        "https://api.github.com/repos/ceph/ceph/issues/100/labels/wip-yuri-testing",
        auth=mock.ANY,
    )


def test_check_pr_approvals_remove_delete_404_treated_as_already_removed(ptl_tool, caplog):
    """A second, approved PR keeps `remaining` non-empty so this test can assert
    the 404 log message on its own, independent of the all-unapproved exit path
    covered separately below."""
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
        200: ("APPROVED", "already good", "bob"),
    })
    session.delete.return_value = FakeResponse(404)
    with mock.patch("builtins.input", return_value="r"):
        with caplog.at_level(logging.INFO, logger=ptl_tool.log.name):
            result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing")
    assert result == [200]
    assert any("already absent" in r.message for r in caplog.records)


def test_check_pr_approvals_remove_all_unapproved_exits_nonzero(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
    })
    session.delete.return_value = FakeResponse(200)
    with mock.patch("builtins.input", return_value="r"):
        with pytest.raises(SystemExit):
            ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")


def test_check_pr_approvals_ignore_returns_unchanged_no_mutation(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
        200: ("APPROVED", "already good", "bob"),
    })
    with mock.patch("builtins.input", return_value="i"):
        result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing")
    assert result == [100, 200]
    assert session.post.call_count == 1  # only the GraphQL fetch, no comment
    session.delete.assert_not_called()


def test_check_pr_approvals_invalid_choice_reprompts(ptl_tool, capsys):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
    })
    with mock.patch("builtins.input", side_effect=["bogus", "i"]):
        result = ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")
    assert result == [100]
    assert "Invalid choice" in capsys.readouterr().out


def test_check_pr_approvals_dry_run_accept_never_posts_comment(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
    })
    with mock.patch("builtins.input", return_value="a"):
        result = ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing", dry_run=True)
    assert result == [100]
    assert session.post.call_count == 1  # GraphQL only, no comment POST


def test_check_pr_approvals_dry_run_remove_never_calls_delete_but_still_excludes(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "needs review", "alice"),
        200: ("APPROVED", "already good", "bob"),
    })
    with mock.patch("builtins.input", return_value="r"):
        result = ptl_tool.check_pr_approvals(session, [100, 200], "wip-yuri-testing", dry_run=True)
    assert result == [200]
    session.delete.assert_not_called()


def test_check_pr_approvals_normalizes_non_ascii_title_without_crashing(ptl_tool, capsys):
    session = mock.Mock()
    session.post.return_value = _graphql_response({
        100: ("REVIEW_REQUIRED", "fix café crash", "alice"),
    })
    with mock.patch("builtins.input", return_value="i"):
        result = ptl_tool.check_pr_approvals(session, [100], "wip-yuri-testing")
    assert result == [100]
    out = capsys.readouterr().out
    assert "caf?" in out
    assert "é" not in out


# ---------------------------------------------------------------------------
# ApprovalCheck: gates --final-merge/--audit specifically (via
# verify_pr_readiness()'s BaseAuditCheck framework), unlike check_pr_approvals()
# above which used to run -- unconditionally, and only interactively -- on
# every --pr-label/--integration/--qe-label batch merge. Per review feedback
# on PR #70549 (https://github.com/ceph/ceph/pull/70549#pullrequestreview-4813309786),
# that was too disruptive for ordinary QA batch runs; only a final upstream
# merge of an unapproved PR should actually be gated.
# ---------------------------------------------------------------------------

def _single_pr_graphql_response(review_decision):
    return FakeJSONResponse(200, {"data": {"repository": {"pullRequest": {"reviewDecision": review_decision}}}})


def _audit_ctx(ptl_tool, session, pr=100, ci_mode=False):
    report = ptl_tool.AuditReport()
    args = mock.Mock(ci_mode=ci_mode)
    ctx = ptl_tool.AuditContext(
        G=mock.Mock(), session=session, R=mock.Mock(), pr=pr, pr_commits=[],
        tip=mock.Mock(), base="squid", args=args, report=report,
    )
    return ctx, report


def test_approval_check_approved_pr_is_noop(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("APPROVED")
    ctx, report = _audit_ctx(ptl_tool, session)
    with mock.patch("builtins.input") as fake_input:
        ptl_tool.ApprovalCheck().run(ctx)
    fake_input.assert_not_called()
    assert not report.has_errors()


def test_approval_check_ci_mode_adds_error_without_prompting(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    ctx, report = _audit_ctx(ptl_tool, session, ci_mode=True)
    with mock.patch("builtins.input") as fake_input:
        ptl_tool.ApprovalCheck().run(ctx)
    fake_input.assert_not_called()
    assert report.has_errors()


def test_approval_check_interactive_record_failure_adds_to_report(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    ctx, report = _audit_ctx(ptl_tool, session)
    with mock.patch("builtins.input", return_value="r"):
        ptl_tool.ApprovalCheck().run(ctx)
    assert report.has_errors()


def test_approval_check_interactive_proceed_does_not_fail_report(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    ctx, report = _audit_ctx(ptl_tool, session)
    with mock.patch("builtins.input", return_value="p"):
        ptl_tool.ApprovalCheck().run(ctx)
    assert not report.has_errors()


def test_approval_check_interactive_skip_to_merge_raises(ptl_tool):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    ctx, report = _audit_ctx(ptl_tool, session)
    with mock.patch("builtins.input", return_value="m"):
        with pytest.raises(ptl_tool.SkipToMerge):
            ptl_tool.ApprovalCheck().run(ctx)


def test_approval_check_invalid_choice_reprompts(ptl_tool, capsys):
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    ctx, report = _audit_ctx(ptl_tool, session)
    with mock.patch("builtins.input", side_effect=["bogus", "p"]):
        ptl_tool.ApprovalCheck().run(ctx)
    assert "Invalid choice" in capsys.readouterr().out


def test_approval_check_graphql_error_response_exits(ptl_tool):
    session = mock.Mock()
    session.post.return_value = FakeResponse(401, "Bad credentials")
    ctx, report = _audit_ctx(ptl_tool, session)
    with pytest.raises(SystemExit):
        ptl_tool.ApprovalCheck().run(ctx)


def test_approval_check_missing_pr_node_raises_systemexit(ptl_tool):
    session = mock.Mock()
    session.post.return_value = FakeJSONResponse(200, {"data": {"repository": {"pullRequest": None}}})
    ctx, report = _audit_ctx(ptl_tool, session)
    with pytest.raises(SystemExit):
        ptl_tool.ApprovalCheck().run(ctx)


def test_verify_pr_readiness_includes_approval_check_regardless_of_base(ptl_tool):
    """ApprovalCheck must run for a final merge into main too, not just backports
    (unlike CommitParityCheck/ConflictSimulationCheck/RedmineLinkageCheck, which
    are backport-specific and skipped when base == 'main')."""
    session = mock.Mock()
    session.post.return_value = _single_pr_graphql_response("REVIEW_REQUIRED")
    # ci_mode's post-audit-failure path also calls _hide_previous_bot_reviews(),
    # which paginates PR reviews via GET before posting the consolidated
    # review -- give it an empty, well-formed page so it's a no-op.
    session.get.return_value = FakeJSONResponse(200, [], "")
    session.get.return_value.headers = {}
    G = mock.Mock()
    args = mock.Mock(ci_mode=True, audit=True, dry_run=True)
    with mock.patch.object(ptl_tool, "MergeConflictCheck") as FakeMergeCheck:
        FakeMergeCheck.return_value.run.return_value = None
        passed = ptl_tool.verify_pr_readiness(G, session, mock.Mock(), 100, [], mock.Mock(), "main", args)
    assert passed is False  # ApprovalCheck's ci-mode failure should have been picked up
