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


# ---------------------------------------------------------------------------
# audit_tracker_and_relabel(): the --qe-label prompt's 'a' option. Verifies a
# matched tracker is QA Approved before offering to swap its PR label for a
# caller-chosen set of "done" labels (TESTED + ready-to-merge by default) on
# every open PR still carrying it. Mutates real GitHub labels when actually
# applied, so every path that could reach session.delete/session.post is
# tested for exactly when those calls should (and should not) happen.
# ---------------------------------------------------------------------------

def _fake_issue(ptl_tool, status_name="QA Approved", issue_id=78399, owned_prs=(100, 111, 222, 333)):
    """owned_prs seeds the tracker's own '"PR #N":' description markers (the
    same format manage_qa_tracker() builds and later re-parses) -- defaults
    to every PR number used across the existing tests below so none of them
    need to know about ownership-filtering to keep passing."""
    issue = mock.Mock()
    issue.status.name = status_name
    issue.id = issue_id
    issue.description = "\n".join(
        f'|"PR #{n}":https://github.com/ceph/ceph/pull/{n}|author|labels|title|'
        for n in owned_prs
    )
    return issue


def _patch_get(ptl_tool, monkeypatch, prs):
    """Patch the module-level get() generator to yield one page of PR-shaped
    GitHub issue-search results, sidestepping response/pagination plumbing
    that's a separate concern from this function's own logic."""
    items = [
        {"number": num, "title": title, "pull_request": {}}
        for num, title in prs
    ]
    monkeypatch.setattr(ptl_tool, "get", lambda *a, **k: iter([items]))


def test_audit_not_approved_returns_without_prompting(ptl_tool, monkeypatch):
    """A tracker that isn't QA Approved yet must bail out before the first
    input() call -- nothing to relabel, nothing to ask about."""
    issue = _fake_issue(ptl_tool, status_name="QA Testing")
    session = mock.Mock()
    inputs = mock.Mock(side_effect=AssertionError("input() should not be called"))
    with mock.patch("builtins.input", inputs):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "not_approved"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_cancel_at_label_prompt(ptl_tool, monkeypatch):
    """Typing 'q' at the very first prompt cancels before any GitHub call."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    with mock.patch("builtins.input", side_effect=["q"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.get.assert_not_called()
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_no_open_prs_is_a_noop(ptl_tool, monkeypatch):
    """An approved tracker whose label has zero open PRs left (already
    relabeled, or merged) has nothing to do -- must not prompt for target
    labels or touch GitHub write endpoints."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [])
    with mock.patch("builtins.input", side_effect=[""]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_cancel_at_target_labels_prompt(ptl_tool, monkeypatch):
    """Backing out at the second prompt (after PRs were already fetched and
    shown) must still change nothing."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    with mock.patch("builtins.input", side_effect=["", "q"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_rejects_empty_target_labels(ptl_tool, monkeypatch):
    """Input that's non-blank but parses to zero labels (e.g. ',,,') must
    be rejected before the plan/confirm step, not silently treated as
    'strip the old label and add nothing'."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    with mock.patch("builtins.input", side_effect=["", ",,,"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_dry_run_never_calls_session_write_methods(ptl_tool, monkeypatch):
    """--dry-run must show the full plan but never touch delete/post, same
    guarantee as post_consolidated_review's own dry-run test above."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "PR one"), (222, "PR two")])
    with mock.patch("builtins.input", side_effect=["", ""]):
        result = ptl_tool.audit_tracker_and_relabel(
            session, issue, "wip-yuri-testing", dry_run=True
        )
    assert result == "completed"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_declining_final_confirm_changes_nothing(ptl_tool, monkeypatch):
    """Reaching the plan and then answering anything but 'y' at the final
    [y/N] confirm must not mutate any labels."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    with mock.patch("builtins.input", side_effect=["", "", "n"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_default_label_accepted_on_blank_input(ptl_tool, monkeypatch):
    """Pressing Enter at the label prompt must use the tracker's own
    --qe-label value, not something re-derived from the branch name."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "t")])
    session.delete.return_value = FakeResponse(200)
    session.post.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    del_url = session.delete.call_args.args[0]
    assert "labels/wip-yuri-testing" in del_url


def test_audit_custom_label_overrides_default(ptl_tool, monkeypatch):
    """Typing a different label at the first prompt must be used instead of
    the default -- this is the 'allow a different label' requirement."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    monkeypatch.setattr(ptl_tool, "get", lambda session, url, params=None, paging=True: iter(
        [[{"number": 111, "title": "t", "pull_request": {}}]]
        if params.get("labels") == "wip-someone-else-testing" else []
    ))
    session.delete.return_value = FakeResponse(200)
    session.post.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["wip-someone-else-testing", "", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    del_url = session.delete.call_args.args[0]
    assert "labels/wip-someone-else-testing" in del_url


def test_audit_custom_target_labels_override_default(ptl_tool, monkeypatch):
    """Typing target labels at the second prompt must be used verbatim
    instead of TESTED/ready-to-merge."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    session.delete.return_value = FakeResponse(200)
    session.post.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "backport-approved, needs-cherry-pick", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    posted = session.post.call_args.kwargs["json"]
    assert posted == {"labels": ["backport-approved", "needs-cherry-pick"]}


def test_audit_applies_to_every_pr_when_confirmed(ptl_tool, monkeypatch):
    """The real apply path must delete the old label and add the default
    target labels on every PR found, not just the first."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "PR one"), (222, "PR two"), (333, "PR three")])
    session.delete.return_value = FakeResponse(200)
    session.post.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    assert session.delete.call_count == 3
    assert session.post.call_count == 3
    posted = session.post.call_args.kwargs["json"]
    assert posted == {"labels": ["TESTED", "ready-to-merge"]}


def test_audit_post_failure_for_one_pr_skips_its_delete_but_continues_others(ptl_tool, monkeypatch, caplog):
    """New labels go on FIRST: if adding them to one PR fails outright, that
    PR's old label must be left alone (nothing to clean up on a failed add),
    but the run should keep going for the remaining PRs, not abort entirely."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "bad PR"), (222, "good PR")])
    session.post.side_effect = [FakeResponse(500, "server error"), FakeResponse(200)]
    session.delete.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
            result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    assert session.post.call_count == 2
    # only the second (successful-add) PR should have gotten a DELETE
    assert session.delete.call_count == 1
    assert any("Failed to add labels to #111" in r.message for r in caplog.records)


def test_audit_delete_failure_after_post_success_does_not_undo_new_labels(ptl_tool, monkeypatch, caplog):
    """The core no-orphaning guarantee: if adding the new labels succeeds
    but removing the old one then fails, the new labels must NOT be rolled
    back -- the PR just keeps both labels until a retry, it's never left
    with neither."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    session.post.return_value = FakeResponse(200)
    session.delete.return_value = FakeResponse(500, "server error")
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
            result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    session.post.assert_called_once()
    posted = session.post.call_args.kwargs["json"]
    assert posted == {"labels": ["TESTED", "ready-to-merge"]}
    assert any("failed to remove" in r.message and "#111" in r.message for r in caplog.records)


def test_audit_exits_nonzero_when_no_pr_actually_updated(ptl_tool, monkeypatch, caplog):
    """If every PR in the batch fails to get its new labels added, the
    function must not report 'completed' -- the caller treats that as
    unconditional success and would exit 0 having changed nothing."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "PR one"), (222, "PR two")])
    session.post.return_value = FakeResponse(403, "Resource not accessible")
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
            with pytest.raises(SystemExit) as exc_info:
                ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert exc_info.value.code == 1
    session.delete.assert_not_called()


def test_audit_delete_404_is_treated_as_already_gone(ptl_tool, monkeypatch):
    """A 404 on DELETE (label already removed by a previous partial run)
    is not a real failure -- the PR should still get the new labels added."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    session.delete.return_value = FakeResponse(404)
    session.post.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    session.post.assert_called_once()


def test_audit_post_failure_logs_status_and_body(ptl_tool, monkeypatch, caplog):
    """A failed add-labels call's status code and response body must both
    land in the log, matching the existing post_consolidated_review
    error-handling convention. (This particular batch also has a second,
    successful PR, so it exercises the log message without triggering the
    all-failed SystemExit covered separately above.)"""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "bad PR"), (222, "good PR")])
    session.post.side_effect = [FakeResponse(403, "Resource not accessible"), FakeResponse(200)]
    session.delete.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["", "", "y"]):
        with caplog.at_level(logging.ERROR, logger=ptl_tool.log.name):
            result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    assert any(
        "Failed to add labels to #111" in r.message and "403" in r.message
        for r in caplog.records
    )


def test_audit_requests_only_open_prs(ptl_tool, monkeypatch):
    """The GitHub issue-search call must ask for state='open' explicitly --
    this is the only thing keeping closed/merged PRs that still carry the
    label out of the relabel plan, since the response is never otherwise
    filtered by state client-side."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    seen_params = {}

    def fake_get(session, url, params=None, paging=True):
        seen_params.update(params)
        return iter([[{"number": 111, "title": "t", "pull_request": {}}]])

    monkeypatch.setattr(ptl_tool, "get", fake_get)
    with mock.patch("builtins.input", side_effect=["", ""]):
        result = ptl_tool.audit_tracker_and_relabel(
            session, issue, "wip-yuri-testing", dry_run=True
        )
    assert result == "completed"
    assert seen_params.get("state") == "open"


def test_audit_skips_non_pr_items_in_mixed_response(ptl_tool, monkeypatch, capsys):
    """A plain issue (no 'pull_request' key) sharing the label with real PRs
    must be excluded from the plan -- only items GitHub itself tags as PRs
    should ever be touched."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    items = [
        {"number": 100, "title": "just a tracking issue, not a PR"},
        {"number": 111, "title": "PR one", "pull_request": {}},
        {"number": 222, "title": "PR two", "pull_request": {}},
    ]
    monkeypatch.setattr(ptl_tool, "get", lambda *a, **k: iter([items]))
    with mock.patch("builtins.input", side_effect=["", ""]):
        result = ptl_tool.audit_tracker_and_relabel(
            session, issue, "wip-yuri-testing", dry_run=True
        )
    assert result == "completed"
    out = capsys.readouterr().out
    assert "#111" in out
    assert "#222" in out
    assert "#100" not in out
    assert "2 PR(s)" in out


def test_audit_delete_url_quotes_label_with_special_characters(ptl_tool, monkeypatch):
    """A label containing characters that aren't URL-safe (space, slash)
    must be percent-encoded in the DELETE path -- not interpolated raw,
    which would build a broken or wrong URL."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    session.post.return_value = FakeResponse(200)
    session.delete.return_value = FakeResponse(200)
    with mock.patch("builtins.input", side_effect=["wip yuri/testing", "", "y"]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "completed"
    del_url = session.delete.call_args.args[0]
    assert "wip%20yuri%2Ftesting" in del_url
    assert "wip yuri/testing" not in del_url


def test_audit_handles_non_ascii_pr_titles(ptl_tool, monkeypatch, capsys):
    """A PR title containing non-ASCII characters must not crash the plan
    listing -- this is the same UnicodeEncodeError class already hit once
    for this function's own UI strings under a non-UTF-8 stdout, except
    titles are external, contributor-authored data we can't just rewrite
    as ASCII ourselves, so they need to be normalized defensively."""
    issue = _fake_issue(ptl_tool)
    session = mock.Mock()
    items = [{"number": 111, "title": "fix café 🎉 crash", "pull_request": {}}]
    monkeypatch.setattr(ptl_tool, "get", lambda *a, **k: iter([items]))
    with mock.patch("builtins.input", side_effect=["", ""]):
        result = ptl_tool.audit_tracker_and_relabel(
            session, issue, "wip-yuri-testing", dry_run=True
        )
    assert result == "completed"
    out = capsys.readouterr().out
    assert "#111" in out
    assert "café" not in out
    assert "🎉" not in out


def test_audit_excludes_labeled_prs_not_in_tracker_description(ptl_tool, monkeypatch, capsys):
    """A label can be reused for a later, unrelated round of testing once a
    ticket is already approved. #222 carries the label but was never part
    of THIS tracker's own PR list -- it must be skipped and reported, while
    #111 (which the tracker's description actually lists) still proceeds."""
    issue = _fake_issue(ptl_tool, owned_prs=(111,))
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "owned PR"), (222, "unrelated later PR")])
    with mock.patch("builtins.input", side_effect=["", ""]):
        result = ptl_tool.audit_tracker_and_relabel(
            session, issue, "wip-yuri-testing", dry_run=True
        )
    assert result == "completed"
    out = capsys.readouterr().out
    assert "#111" in out
    assert "SKIP #222" in out
    # #222 must not appear in the "PRs to update" plan itself, only in the
    # SKIP listing above it.
    assert "  #222" not in out


def test_audit_cancels_when_tracker_description_has_no_pr_list(ptl_tool, monkeypatch):
    """A tracker whose description has no '\"PR #N\":' entries at all gives
    us nothing to verify ownership against -- refuse to relabel by label
    text alone rather than silently falling back to the unfiltered set."""
    issue = _fake_issue(ptl_tool, owned_prs=())
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "some PR")])
    with mock.patch("builtins.input", side_effect=[""]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()


def test_audit_noop_when_no_labeled_pr_is_owned_by_tracker(ptl_tool, monkeypatch):
    """The tracker has a real PR list, but none of the currently labeled
    PRs are on it (the label has been fully reused elsewhere) -- must be a
    clean no-op, not an error, and must not prompt for target labels."""
    issue = _fake_issue(ptl_tool, owned_prs=(999,))
    session = mock.Mock()
    _patch_get(ptl_tool, monkeypatch, [(111, "unrelated PR")])
    with mock.patch("builtins.input", side_effect=[""]):
        result = ptl_tool.audit_tracker_and_relabel(session, issue, "wip-yuri-testing")
    assert result == "cancelled"
    session.delete.assert_not_called()
    session.post.assert_not_called()
