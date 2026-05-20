#!/usr/bin/python3

# README:
#
# This tool's purpose is to make it easier to merge PRs into test branches and
# into main.
#
# == Getting Started ==
#
# You will probably want to setup a virtualenv for running this script:
#
#    (
#      virtualenv ~/ptl-venv
#      source ~/ptl-venv/bin/activate
#      pip3 install GitPython python-redmine requests
#    )
#
# Then run the tool with:
#
#    (source ~/ptl-venv/bin/activate && python3 src/script/ptl-tool.py --help)
#
# Important files in your $HOME:
#
#  ~/.redmine_key -- Your redmine API access key from right side of: https://tracker.ceph.com/my/account
#
#  ~/.github_token -- Your github Personal access token: https://github.com/settings/tokens
#
# Some important environment variables:
#
#  - PTL_TOOL_GITHUB_TOKEN (your github Personal access token, or what is stored in ~/.github_token)
#  - PTL_TOOL_REDMINE_API_KEY (your redmine api key, or what is stored in ~/redmine_key)
#  - PTL_TOOL_USER (your desired username embedded in test branch names)

import argparse
from dataclasses import dataclass, field
import datetime
from typing import List, Tuple
import difflib
import hashlib
from getpass import getuser
import itertools
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import webbrowser

MISSING_DEPS = []

try:
    import git # https://github.com/gitpython-developers/gitpython
except ImportError:
    MISSING_DEPS.append("GitPython")

try:
    import redminelib
    from redminelib import Redmine  # https://pypi.org/project/python-redmine/
except ImportError:
    redminelib = None
    Redmine = None
    MISSING_DEPS.append("python-redmine")

try:
    import requests
except ImportError:
    MISSING_DEPS.append("requests")

if MISSING_DEPS:
    print("ERROR: Missing required dependencies: " + ", ".join(MISSING_DEPS), file=sys.stderr)
    print("\nPlease set up a virtual environment and install the dependencies by following these steps:", file=sys.stderr)
    print("  1. Create a virtual environment: python3 -m venv ~/ptl-venv", file=sys.stderr)
    print("  2. Activate it:                  source ~/ptl-venv/bin/activate", file=sys.stderr)
    print("  3. Install dependencies:         pip install GitPython python-redmine requests\n", file=sys.stderr)
    sys.exit(1)

from os.path import expanduser

BASE_PROJECT = os.getenv("PTL_TOOL_BASE_PROJECT", "ceph")
BASE_REPO = os.getenv("PTL_TOOL_BASE_REPO", "ceph")
BASE_REMOTE_URL = os.getenv("PTL_TOOL_BASE_REMOTE_URL", f"https://github.com/{BASE_PROJECT}/{BASE_REPO}.git")
CI_REPO = os.getenv("PTL_TOOL_CI_REPO", "ceph-ci")
CI_REMOTE_URL = os.getenv("PTL_TOOL_CI_REMOTE_URL", f"git@github.com:{BASE_PROJECT}/{CI_REPO}.git")
GITDIR = os.getenv("PTL_TOOL_GITDIR", ".")
GITHUB_TOKEN = None
try:
    with open(expanduser("~/.github_token")) as f:
        GITHUB_TOKEN = f.read().strip()
except FileNotFoundError:
    pass
GITHUB_TOKEN = os.getenv("PTL_TOOL_GITHUB_TOKEN", GITHUB_TOKEN)
INDICATIONS = [
    re.compile(r"(Reviewed-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile(r"(Acked-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile(r"(Tested-by: .+ <[\w@.-]+>)", re.IGNORECASE),
]
REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID = 21
REDMINE_CUSTOM_FIELD_ID_RELEASE = 16
REDMINE_CUSTOM_FIELD_ID_SHAMAN_BUILD = 26
REDMINE_CUSTOM_FIELD_ID_QA_RUNS = 27
REDMINE_CUSTOM_FIELD_ID_QA_RELEASE = 28
REDMINE_CUSTOM_FIELD_ID_QA_TAGS = 3
REDMINE_CUSTOM_FIELD_ID_GIT_BRANCH = 29
REDMINE_ENDPOINT = "https://tracker.ceph.com"
REDMINE_TRACKER_ID_BACKPORT = 9
REDMINE_STATUS_ID_REJECTED = 6
REDMINE_STATUS_ID_RESOLVED = 3
REDMINE_STATUS_ID_CLOSED = 5
REDMINE_PROJECT_QA = "ceph-qa"
REDMINE_TRACKER_QA = "QA Run"
REDMINE_USER = os.getenv("PTL_TOOL_REDMINE_USER", getuser())
REDMINE_API_KEY = None
try:
    with open(expanduser("~/.redmine_key")) as f:
        REDMINE_API_KEY = f.read().strip()
except FileNotFoundError:
    pass
REDMINE_API_KEY = os.getenv("PTL_TOOL_REDMINE_API_KEY", REDMINE_API_KEY)
SPECIAL_BRANCHES = ('main', 'luminous', 'jewel', 'HEAD')
TEST_BRANCH = os.getenv("PTL_TOOL_TEST_BRANCH", "wip-{user}-testing-%Y%m%d.%H%M%S")
USER = os.getenv("PTL_TOOL_USER", getuser())

SANDBOX_CFG = [
    'rerere.enabled=false',
    'commit.gpgSign=false',
    'core.hooksPath=/dev/null',
    'user.name=PTL Tool',
    'user.email=ptl-tool@ceph.com'
]

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

# find containing git dir
try:
    git_dir = git.Repo(GITDIR, search_parent_directories=True).working_tree_dir
except (git.NoSuchPathError, git.InvalidGitRepositoryError) as e:
    raise SystemExit(f"error: not a git repository: {GITDIR}") from e

CONTRIBUTORS = {}
NEW_CONTRIBUTORS = {}

BZ_MATCH = re.compile("(.*https?://bugzilla.redhat.com/.*)")
TRACKER_MATCH = re.compile("(.*https?://tracker.ceph.com/.*)")

@dataclass
class AuditLabels:
    queue: str = None
    passed: str = None
    failed: str = None


class SkipToMerge(Exception):
    """Raised to bypass remaining verification checks and proceed directly to merge."""
    pass

@dataclass
class AuditContext:
    G: git.Repo
    session: requests.Session
    R: 'Redmine'
    pr: int
    pr_commits: list
    tip: git.Commit
    base: str
    args: argparse.Namespace
    report: 'AuditReport'
    found_prs: set = field(default_factory=set)
    visualizer_md_table: str = ""

class BaseAuditCheck:
    @property
    def name(self) -> str:
        raise NotImplementedError

    def run(self, ctx: AuditContext) -> None:
        """
        Executes the check. Appends to ctx.report on failure.
        May raise SkipToMerge to short-circuit remaining checks.
        """
        raise NotImplementedError

class AuditReport:
    def __init__(self):
        self.issues: List[Tuple[str, str]] = []
        self.interactive_failures: int = 0
        self.visualizer_md_table: str = ""
        self.redmine_linkage_correct: bool = False

    def set_visualizer(self, table: str):
        self.visualizer_md_table = table

    def add(self, category: str, md_text: str):
        self.issues.append((category, md_text))

    def record_failure(self):
        self.record_interactive_failure()

    def record_interactive_failure(self):
        self.interactive_failures += 1

    def _get_active_issues(self):
        if self.redmine_linkage_correct:
            return [i for i in self.issues if i != "Multiple Source PRs"]
        return self.issues

    def has_errors(self) -> bool:
        return len(self._get_active_issues()) > 0 or self.interactive_failures > 0

    def get_consolidated_text(self) -> str:
        blocks = []
        for cat, text in self._get_active_issues():
            if text:
                blocks.append(text)
        
        if self.visualizer_md_table and blocks:
            blocks.append(f"### Commit Parity Visualizer\n\n{self.visualizer_md_table}")

        blocks.append(textwrap.dedent("""\n\n
            🛟 **Need Help?**

            If you need technical help resolving these issues, please consult with the Component Lead. If you need administrative overrides, please see the `#ceph-upstream-releases` channel on [Slack](https://docs.ceph.com/en/latest/start/get-involved/) **and** request a review from the @ceph/ceph-release-manager.

            📋 **Component Lead / Release Manager**

            To override the audit failure, apply `releng-audit-override` label or comment `/audit override`.
            """))
            
        return "\n\n---\n\n".join(blocks)

    def post_consolidated_review(self, session: requests.Session, pr: int, dry_run: bool = False, ci_mode: bool = False):
        """
        Combines all collected md_text in self.issues into a single 
        GitHub review payload and posts it via the API.
        """
        consolidated_text = self.get_consolidated_text()
        if consolidated_text:
            if ci_mode:
                footer = "\n\n---\n\n⚠️ **Note**: Automated audit checks will be suspended on future pushes to prevent comment spam while you work.\n\nWhen you are ready for a new audit, please **remove the `releng-audit-fail` label** or comment `/audit retest`."
                
                if os.getenv("GITHUB_ACTIONS") == "true":
                    gh_server = os.getenv("GITHUB_SERVER_URL", "https://github.com")
                    gh_repo = os.getenv("GITHUB_REPOSITORY", f"{BASE_PROJECT}/{BASE_REPO}")
                    gh_run_id = os.getenv("GITHUB_RUN_ID", "nil")
                    footer += f"\n\n**CI Run Log**: [View Workflow Details]({gh_server}/{gh_repo}/actions/runs/{gh_run_id})"

                consolidated_text += footer


            if dry_run:
                log.info(f"[DRY RUN] Would post consolidated review to PR #{pr}:\n{consolidated_text}")
            else:
                payload = {'body': consolidated_text, 'event': 'REQUEST_CHANGES'}
                if ci_mode:
                    # The CI check failure is sufficient to block merge.
                    payload['event'] = 'COMMENT'

                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}/reviews"
                session.post(endpoint, auth=GithubBearerAuth(), json=payload)


def parse_audit_labels(value):
    if not value:
        return None
    parts = [p.strip() for p in value.split(',')]
    if len(parts) == 1:
        return AuditLabels(queue=parts[0])
    if len(parts) == 2:
        return AuditLabels(passed=parts[0], failed=parts[1])
    if len(parts) == 3:
        return AuditLabels(queue=parts[0], passed=parts[1], failed=parts[2])
    raise argparse.ArgumentTypeError("Audit labels must be 'queue', 'passed,failed', or 'queue,passed,failed'")

class GithubBearerAuth(requests.auth.AuthBase):
    def __call__(self, r):
        if GITHUB_TOKEN:
            r.headers['Authorization'] = f'Bearer {GITHUB_TOKEN}'
        r.headers['Accept'] = 'application/vnd.github.v3+json'
        return r

_PR_CACHE = {}
def get_pr_info(session, pr):
    if pr not in _PR_CACHE:
        log.info("Fetching information for PR #%d", pr)
        endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}"
        _PR_CACHE[pr] = next(get(session, endpoint, paging=False))
    return _PR_CACHE[pr]

def get_pr_tracker_string(session, pr, response=None):
    if not response:
        response = get_pr_info(session, pr)
    return f'* "PR #{pr}":{response["html_url"]} -- {response["title"].strip()}'

def get(session, url, params=None, paging=True):
    if params is None:
        params = {}
    params['per_page'] = 100

    log.debug(f"Fetching {url}")
    response = session.get(url, auth=GithubBearerAuth(), params=params)
    log.debug(f"Response = {response}; links = {response.headers.get('link', '')}")

    if response.status_code != 200:
        log.error(f"Failed to fetch {url}: {response}")
        sys.exit(1)
    j = response.json()
    yield j
    if paging:
        link = response.headers.get('link', None)
        page = 2
        while link is not None and 'next' in link:
            log.debug(f"Fetching {url}")
            new_params = dict(params)
            new_params.update({'page': page})
            response = session.get(url, auth=GithubBearerAuth(), params=new_params)
            log.debug(f"Response = {response}; links = {response.headers.get('link', '')}")
            if response.status_code != 200:
                log.error(f"Failed to fetch {url}: {response}")
                sys.exit(1)
            yield response.json()
            link = response.headers.get('link', None)
            page += 1

def resolve_ref(G, ref, remote_url, always_fetch=False):
    """
    Attempts to resolve a git reference locally. If it fails, or if
    always_fetch is True, it fetches the ref from the remote.
    Returns the git.Commit object.
    """
    try:
        c = G.commit(ref)
        is_commit = (len(ref) >= 7 and c.hexsha.startswith(ref))
        if not always_fetch or is_commit:
            log.debug(f"Resolved {ref} locally to {c.hexsha[:8]}.")
            return c
    except (git.exc.BadName, ValueError):
        pass
        
    log.info(f"Fetching {ref} from {remote_url}")
    try:
        G.git.fetch(remote_url, ref)
        return G.commit("FETCH_HEAD")
    except git.exc.GitCommandError as e:
        log.error(f"Could not fetch {ref} from {remote_url}: {e}")
        sys.exit(1)

def get_credits(session, pr, pr_req):
    comments = [pr_req]

    log.debug(f"Getting comments for #{pr}")
    endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/comments"
    for c in get(session, endpoint):
        comments.extend(c)

    log.debug(f"Getting reviews for #{pr}")
    endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}/reviews"
    reviews = []
    for c in get(session, endpoint):
        comments.extend(c)
        reviews.extend(c)

    log.debug(f"Getting review comments for #{pr}")
    endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}/comments"
    for c in get(session, endpoint):
        comments.extend(c)

    credits = set()
    for comment in comments:
        body = comment["body"]
        if body:
            url = comment["html_url"]
            for m in BZ_MATCH.finditer(body):
                log.info("[ {url} ] BZ cited: {cite}".format(url=url, cite=m.group(1)))
            for m in TRACKER_MATCH.finditer(body):
                log.info("[ {url} ] Ceph tracker cited: {cite}".format(url=url, cite=m.group(1)))
            for indication in INDICATIONS:
                for cap in indication.findall(comment["body"]):
                    credits.add(cap)

    new_new_contributors = {}
    for review in reviews:
        if review["state"] == "APPROVED":
            user = review["user"]["login"]
            try:
                credits.add("Reviewed-by: "+CONTRIBUTORS[user])
            except KeyError as e:
                try:
                    credits.add("Reviewed-by: "+NEW_CONTRIBUTORS[user])
                except KeyError as e:
                    try:
                        name = input("Need name for contributor \"%s\" (use ^D to skip); Reviewed-by: " % user)
                        name = name.strip()
                        if len(name) == 0:
                            continue
                        NEW_CONTRIBUTORS[user] = name
                        new_new_contributors[user] = name
                        credits.add("Reviewed-by: "+name)
                    except EOFError as e:
                        continue

    return "\n".join(credits), new_new_contributors

def format_parity_row(left_sha, left_msg, right_sha, right_msg, is_missing=False, is_extra=False, right_prefix=""):
    """Helper to format visualizer rows."""
    if is_missing:
        left_col = "\033[91m<<<< MISSING IN BACKPORT >>>>\033[0m"
        visible_left_len = 29
    else:
        l_msg = (left_msg[:25] + '...') if len(left_msg) > 28 else left_msg
        left_col = f"[ {left_sha[:8]} ] {l_msg}"
        visible_left_len = len(left_col)

    if is_extra:
        right_col = "\033[93m<<<< EXTRA IN BACKPORT >>>>\033[0m"
    else:
        r_msg = (right_msg[:20] + '...') if len(right_msg) > 23 else right_msg
        right_col = f"{right_prefix}[ {right_sha[:8]} ] {r_msg}"

    padding = max(1, 47 - visible_left_len)
    return f"{left_col}{' ' * padding}{right_col}"

def format_parity_row_md(left_sha, left_msg, right_sha, right_msg, is_missing=False, is_extra=False, right_prefix=""):
    """Helper to format visualizer rows for Markdown tables without truncation."""
    if is_missing:
        left_col = "**<<<< MISSING IN BACKPORT >>>>**"
    else:
        left_col = f"{left_sha} {left_msg}"

    if is_extra:
        right_col = "**<<<< EXTRA IN BACKPORT >>>>**"
    else:
        right_col = f"{right_sha} {right_msg}"

    source_pr = f"**{right_prefix.strip()}**" if right_prefix.strip() else ""

    return f"| {left_col} | {source_pr} | {right_col} |"

def make_pipe(content, fds_to_close, threads):
    """
    Helper to create a pipe, write content to it in a background thread,
    and return the path to the file descriptor.
    """
    r, w = os.pipe()
    fds_to_close.append(r)

    def writer():
        try:
            with os.fdopen(w, 'wb') as pipe_w:
                pipe_w.write(content.encode('utf-8'))
        except Exception:
            pass

    t = threading.Thread(target=writer)
    t.start()
    threads.append(t)
    return f"/dev/fd/{r}", r

def open_in_browser(urls):
    if not urls:
        return
    for url in urls:
        log.debug("Opening %s", url)

    if len(urls) == 1:
        return webbrowser.open_new(urls[0])

    # We are using an html file to hold the URLs because browsers are dumb;
    # they won't open all the urls in a single **new window**.
    js_array = ", ".join(f"'{url}'" for url in urls)
    list_items = "".join(f'        <li><a href="{url}" target="_blank" style="color: #8cb4ff;">{url}</a></li>\n' for url in urls)

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>PTL Tool - Tab Launcher</title>
    <style>
        body {{ font-family: sans-serif; padding: 2rem; background: #222; color: #eee; max-width: 800px; margin: 0 auto; line-height: 1.5; }}
        .notice {{ background: #333; padding: 1.5rem; border-left: 4px solid #e2b714; margin-bottom: 2rem; border-radius: 0 4px 4px 0; }}
        button {{ padding: 12px 24px; font-size: 16px; cursor: pointer; background: #0060df; color: white; border: none; border-radius: 4px; }}
        button:hover {{ background: #003eaa; }}
        a {{ color: #8cb4ff; }}
    </style>
</head>
<body>
    <h2>🚀 Ready to launch tabs</h2>
    
    <div class="notice">
        <strong>Why are you seeing this?</strong><br>
        Firefox has a known race condition when attempting to open a new window and multiple tabs simultaneously via the command line. This launcher page is a workaround to guarantee all your requested PRs and commits open reliably in this dedicated window.<br><br>
        <strong>First-time setup (Popup Blocker):</strong><br>
        When you click the button below, Firefox's popup blocker will likely intercept it. 
        <ol>
            <li>Look for the yellow warning bar below the address bar.</li>
            <li>Click <strong>Options</strong> (or <strong>Preferences</strong>).</li>
            <li>Select <strong>Allow pop-ups for file://.../ptl-tool-tab-launcher.html</strong>.</li>
        </ol>
        <em>Note: Because this tool now uses a static, predictable file name, you are only whitelisting this specific script's output, not all local files on your computer!</em>
    </div>

    <button id="openBtn">Open All Tabs</button>
    <ul style="margin-top: 20px; line-height: 1.8;">
{list_items}    </ul>
    <script>
    const urls = [{js_array}];
    document.getElementById('openBtn').addEventListener('click', () => {{
        urls.reverse().forEach(url => window.open(url, '_blank'));
    }});
    </script>
</body>
</html>
"""

    # Use a static filename so the user only has to whitelist this exact file, not all file://
    launcher_path = os.path.join(tempfile.gettempdir(), "ptl-tool-tab-launcher.html")
    with open(launcher_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    target_url = f"file://{launcher_path}"
    webbrowser.open_new(target_url)

def post_draft_review(session, pr, initial_text, base=None):
    """
    Opens an editor with the draft text, previews it, and prompts the user to
    post it. Returns the action taken ('r', 'c', or False).
    """
    rfa_msg = textwrap.dedent("""\n\n
    🛟 **Need Help?**
    If you need technical help resolving these issues, please consult with the
    Component Lead. If you need administrative overrides, please see the
    `#ceph-upstream-releases` channel on [Slack](https://docs.ceph.com/en/latest/start/get-involved/)
    **and** request a review from the @Ceph Release Manager.
    """)
    editor = os.environ.get('EDITOR', 'vim')
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.md', delete=False) as tf:
        tf.write(initial_text)
        tf_path = tf.name
        
    try:
        while True:
            try:
                subprocess.run([editor, tf_path])
            except FileNotFoundError:
                log.error(f"Editor '{editor}' not found. Please set the EDITOR environment variable.")
                return False
            with open(tf_path, 'r', encoding='utf-8') as f_read:
                final_text = f_read.read().strip()
                
            print("\n" + "="*80)
            print("DRAFT REVIEW PREVIEW:")
            print("-" * 80)
            print(final_text)
            print("="*80 + "\n")
            
            confirm = input(f"Post this feedback to PR #{pr}? [r/c/e/m/N] (r=request changes, c=comment, e=edit again, m=skip to merge, n=cancel): ").strip().lower()
            if confirm == 'm':
                raise SkipToMerge()
            elif confirm in ('r', 'c'):
                event = 'REQUEST_CHANGES' if confirm == 'r' else 'COMMENT'
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}/reviews"
                r = session.post(endpoint, auth=GithubBearerAuth(), json={'body': final_text, 'event': event})
                if r.status_code in (200, 201):
                    log.info(f"Successfully posted {event} to PR #{pr}")
                    return confirm
                else:
                    log.error(f"Failed to post review: {r.status_code} {r.text}")
                    return False
            elif confirm == 'e':
                continue
            else:
                print("Review cancelled.")
                return False
    finally:
        os.unlink(tf_path)

def get_custom_field(issue, field_id):
    try:
        for cf in issue.custom_fields:
            if cf.id == field_id:
                return cf.value
    except (AttributeError, redminelib.exceptions.ResourceAttrError):
        pass
    return None

class CommitParityCheck(BaseAuditCheck):
    @property
    def name(self) -> str:
        return "Commit Parity"

    def _validate_formats(self, pr_commits, base):
        bp_cherry_picks = []
        invalid_format_commits = []
        cp_regex = re.compile(r"\(cherry picked from commit ([0-9a-f]{7,40})\)")
        for commit in pr_commits:
            m = cp_regex.search(commit.message)
            is_cherry_pick = bool(m)
            if not is_cherry_pick and not commit.summary.startswith(f"{base}:"):
                invalid_format_commits.append(commit)

            if is_cherry_pick:
                bp_cherry_picks.append((commit, m.group(1)))
        return bp_cherry_picks, invalid_format_commits, cp_regex

    def _map_upstream_commits(self, G, bp_cherry_picks):
        missing_commits = []
        found_prs = set()
        analyzed_merges = set()
        pr_mapping = {}
        bp_commits_mapped = set()
        
        valid_ref = None
        for ref in ['main', 'origin/main', 'upstream/main']:
            try:
                G.git.rev_parse('--verify', ref)
                valid_ref = ref
                break
            except git.exc.GitCommandError:
                pass

        if valid_ref:
            for commit, orig_sha in bp_cherry_picks:
                try:
                    # Find merge commit using intersection of ancestry-path and first-parent
                    ancestry = G.git.rev_list(f"{orig_sha}..{valid_ref}", '--ancestry-path').splitlines()
                    first_parent = G.git.rev_list(f"{orig_sha}..{valid_ref}", '--first-parent').splitlines()
                    
                    first_parent_set = set(first_parent)
                    merge_sha = None
                    for c in reversed(ancestry):
                        if c in first_parent_set:
                            merge_sha = c
                            break

                    if merge_sha and merge_sha not in analyzed_merges:
                        analyzed_merges.add(merge_sha)
                        # Extract the original PR commits using merge parents (merge^1..merge^2)
                        orig_pr_commits = G.git.rev_list(f"{merge_sha}^1..{merge_sha}^2").splitlines()
                        orig_pr_commits.reverse() # chronological
                        
                        merge_msg = G.commit(merge_sha).summary
                        m_pr = re.search(r'(?:Merge PR|Merge pull request) #(\d+)', merge_msg, re.IGNORECASE)
                        pr_name = f"PR #{m_pr.group(1)}" if m_pr else f"Merge {merge_sha[:8]}"
                        found_prs.add(pr_name)
                        pr_mapping[pr_name] = []

                        for o_commit_sha in orig_pr_commits:
                            o_summary = G.commit(o_commit_sha).summary
                            bp_match = next((c for c, o_sha in bp_cherry_picks if o_commit_sha.startswith(o_sha)), None)
                            
                            pr_mapping[pr_name].append({
                                'o_sha': o_commit_sha,
                                'o_summary': o_summary,
                                'bp_commit': bp_match,
                                'm_sha': merge_sha
                            })
                            
                            if bp_match:
                                bp_commits_mapped.add(bp_match.hexsha)
                            else:
                                missing_commits.append((pr_name, o_commit_sha, o_summary, merge_sha))
                except git.exc.GitCommandError:
                    log.debug(f"Local DAG traversal skipped/failed for {orig_sha[:8]}")
        else:
            log.warning("Could not find local main/upstream ref to perform DAG parity check.")

        if found_prs:
            log.info(f"Original PRs identified in this backport: {', '.join(sorted(list(found_prs)))}")
            if len(found_prs) > 1:
                print("\033[91m" + "="*80)
                print("WARNING: Multiple original PRs detected in this backport!")
                print("Normally we expect exactly one main PR per backport.")
                print("Detected: " + ", ".join(sorted(list(found_prs))))
                print("="*80 + "\033[0m")

        return {
            'found_prs': found_prs,
            'pr_mapping': pr_mapping,
            'bp_commits_mapped': bp_commits_mapped,
            'missing_commits': missing_commits,
            'analyzed_merges': analyzed_merges,
            'valid_ref': valid_ref
        }

    def _generate_visualizer(self, pr_commits, pr, pr_mapping, missing_commits):
        visualizer_text = ""
        visualizer_lines = []
        visualizer_md_table = ""
        visualizer_md_lines = []
        
        if pr_mapping or pr_commits:
            visualizer_lines.append("=" * 80)
            visualizer_lines.append("COMMIT PARITY VISUALIZER")
            visualizer_lines.append("=" * 80)
            
            visualizer_md_lines.append(f"| BACKPORT PR #{pr} | SOURCE PR | SOURCE STATUS |")
            visualizer_md_lines.append("|---|---|---|")

            bp_to_source = {}
            for pr_name, commit_list in pr_mapping.items():
                for item in commit_list:
                    if item['bp_commit']:
                        bp_to_source[item['bp_commit'].hexsha] = (pr_name, item)
                        
            unprinted_missing = {}
            for pr_name, o_sha, o_summary, m_sha in missing_commits:
                if pr_name not in unprinted_missing:
                    unprinted_missing[pr_name] = []
                unprinted_missing[pr_name].append((o_sha, o_summary))
            
            visualizer_lines.append(f"BACKPORT PR #{pr}".ljust(47) + "SOURCE PR / STATUS")
            visualizer_lines.append("-" * 80)
            
            current_pr = None
            for bp_c in pr_commits:
                if bp_c.hexsha in bp_to_source:
                    pr_name, item = bp_to_source[bp_c.hexsha]
                    if current_pr is not None and pr_name != current_pr:
                        if current_pr in unprinted_missing:
                            for m_sha, m_summary in unprinted_missing[current_pr]:
                                prefix = " " * (len(current_pr) + 1)
                                visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                                visualizer_md_lines.append(format_parity_row_md(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                            del unprinted_missing[current_pr]
                        visualizer_lines.append("")
                        
                    prefix = f"{pr_name} " if pr_name != current_pr else " " * (len(pr_name) + 1)
                    current_pr = pr_name
                    visualizer_lines.append(format_parity_row(bp_c.hexsha, bp_c.summary, item['o_sha'], item['o_summary'], right_prefix=prefix))
                    visualizer_md_lines.append(format_parity_row_md(bp_c.hexsha, bp_c.summary, item['o_sha'], item['o_summary'], right_prefix=prefix))
                else:
                    if current_pr is not None:
                        if current_pr in unprinted_missing:
                            for m_sha, m_summary in unprinted_missing[current_pr]:
                                prefix = " " * (len(current_pr) + 1)
                                visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                                visualizer_md_lines.append(format_parity_row_md(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                            del unprinted_missing[current_pr]
                        visualizer_lines.append("")
                    current_pr = None
                    visualizer_lines.append(format_parity_row(bp_c.hexsha, bp_c.summary, None, None, is_extra=True))
                    visualizer_md_lines.append(format_parity_row_md(bp_c.hexsha, bp_c.summary, None, None, is_extra=True))
            
            if current_pr is not None and current_pr in unprinted_missing:
                for m_sha, m_summary in unprinted_missing[current_pr]:
                    prefix = " " * (len(current_pr) + 1)
                    visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                    visualizer_md_lines.append(format_parity_row_md(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                del unprinted_missing[current_pr]
                
            for pr_name, missing_list in unprinted_missing.items():
                visualizer_lines.append("")
                first = True
                for m_sha, m_summary in missing_list:
                    prefix = f"{pr_name} " if first else " " * (len(pr_name) + 1)
                    visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                    visualizer_md_lines.append(format_parity_row_md(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                    first = False
    
            visualizer_lines.append("=" * 80)
            
            visualizer_text = "\n".join(visualizer_lines)
            visualizer_md_table = "\n".join(visualizer_md_lines)
            
        return visualizer_text, visualizer_md_table

    def _handle_invalid_formats(self, ctx, invalid_format_commits):
        if ctx.args.ci_mode:
            md_text = f"### Automated Backport Parity Review - Invalid Commit Format\n\n"
            md_text += f"The following commits have an invalid format. Backport commits must either include a standard `(cherry picked from commit ...)` line or start with the target branch name (e.g., `{ctx.base}:`).\n\n"
            for commit in invalid_format_commits:
                md_text += f"* `{commit.hexsha[:8]}` {commit.summary}\n"
            md_text += "\n[Be familiar with the rules and guidelines for writing backports.](https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst)\n\n"
            ctx.report.add("Invalid Commit Format", md_text)
        else:
            log.error(f"Found {len(invalid_format_commits)} commit(s) with an invalid format. Must be cherry-pick or start with '{ctx.base}:'")
            md_text = f"### Automated Backport Parity Review - Invalid Commit Format\n\n"
            md_text += f"The following commits have an invalid format. Backport commits must either include a standard `(cherry picked from commit ...)` line or start with the target branch name (e.g., `{ctx.base}:`).\n\n"
            for commit in invalid_format_commits:
                log.error(f"  {commit.hexsha[:8]} {commit.summary}")
                md_text += f"* `{commit.hexsha[:8]}` {commit.summary}\n"
            md_text += "\n[Be familiar with the rules and guidelines for writing backports.](https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst)\n\n"
            
            while True:
                ans = input("Do you want to allow these commits anyway? [p/m/r/o/q] (p=proceed, m=skip to merge, r=add to review, o=open PR in browser, q=quit) ").strip().lower()
                if ans == 'o':
                    url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{ctx.pr}"
                    open_in_browser([url])
                    print(f"Opened {url} in browser.")
                elif ans == 'm':
                    raise SkipToMerge()
                elif ans == 'r':
                    ctx.report.add("Invalid Commit Format", md_text)
                    ctx.report.record_failure()
                    break
                elif ans == 'q':
                    sys.exit(1)
                elif ans == 'p':
                    break
                else:
                    print("Invalid choice. Please enter p, m, r, o, or q.")

    def _handle_unmerged_commits(self, ctx, unmerged_cps, cp_regex):
        md_text = "### Automated Backport Parity Review - Unmerged Commits Detected\n\n"
        md_text += "This backport contains cherry-picks of commits that do not appear to be merged into the `main` branch (could not find an associated merge commit or PR). Backports should only contain commits that have already been merged upstream.\n\n"
        md_text += "#### Unmerged Cherry-Picks\n"
        for c in unmerged_cps:
            orig_sha = cp_regex.search(c.message).group(1)
            md_text += f"* Backport commit {c.hexsha[:8]} cherry-picks {orig_sha[:8]}\n"

        md_text += "\n[Be familiar with the rules and guidelines for writing backports.](https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst)\n\n"
            
        if ctx.args.ci_mode:
            ctx.report.add("Unmerged Cherry-Picks", md_text)
        else:
            while True:
                ans = input("Unmerged cherry-picks detected! [p]roceed, [m] skip to merge, [o]pen browser to investigate, [r]eview PR (add to review), [q]uit: ").strip().lower()
                if ans == 'p':
                    break
                elif ans == 'm':
                    raise SkipToMerge()
                elif ans == 'q':
                    log.error("Rejecting PR due to unmerged cherry-picks.")
                    sys.exit(1)
                elif ans == 'r':
                    ctx.report.add("Unmerged Cherry-Picks", md_text)
                    ctx.report.record_failure()
                    break
                elif ans == 'o':
                    bp_pr_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{ctx.pr}"
                    urls_to_open = [bp_pr_url]
                    for c in unmerged_cps:
                        orig_sha = cp_regex.search(c.message).group(1)
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{orig_sha}")
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{c.hexsha}")
                    open_in_browser(urls_to_open)
                    print("Opened URLs in browser.")
                else:
                    print("Invalid choice. Please enter p, m, o, r, or q.")

    def _handle_parity_mismatch(self, ctx, missing_commits, extra_commits, found_prs):
        md_text = f"### Automated Backport Parity Review\n\n"
        if len(found_prs) > 1:
            md_text += f":warning: **Multiple Original PRs Detected:** {', '.join(sorted(list(found_prs)))}\n\n"
        md_text += "Discrepancies detected between the backport PR and the original source PR(s)."
        md_text += " Please see the Commit Parity Visualizer below.\n\n"

        if ctx.args.ci_mode:
            ctx.report.add("Parity Mismatch", md_text)
        else:
            while True:
                ans = input("Parity mismatch! [p]roceed, [m] skip to merge, [o]pen browser to investigate, [r]eview PR (add to review), [q]uit: ").strip().lower()
                if ans == 'p':
                    break
                elif ans == 'm':
                    raise SkipToMerge()
                elif ans == 'q':
                    log.error("Rejecting PR due to incomplete backport.")
                    sys.exit(1)
                elif ans == 'r':
                    ctx.report.add("Parity Mismatch", md_text)
                    ctx.report.record_failure()
                    break
                elif ans == 'o':
                    bp_pr_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{ctx.pr}"
                    urls_to_open = [bp_pr_url]
                    for pr_name, o_sha, o_summary, m_sha in missing_commits:
                        m_pr = re.search(r'#(\d+)', pr_name)
                        if m_pr:
                            urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{m_pr.group(1)}")
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{m_sha}")
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{o_sha}")
                    for c in extra_commits:
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{c.hexsha}")
                    open_in_browser(urls_to_open)
                    print("Opened URLs in browser.")
                else:
                    print("Invalid choice. Please enter p, m, o, r, or q.")

    def _handle_multiple_prs(self, ctx, found_prs):
        md_text = f"### Automated Backport Parity Review - Multiple PRs Detected\n\n"
        md_text += f"This backport appears to pull commits from multiple `main` PRs including: {', '.join(sorted(list(found_prs)))}.\n\n"
        md_text += "This must be made explicit in the backport PR description. Furthermore, each backport tracker ticket associated with these `main` PRs must be linked to this PR.\n\n"
        
        if ctx.args.ci_mode:
            ctx.report.add("Multiple Source PRs", md_text)
        else:
            while True:
                ans = input("Multiple original PRs detected! Do you want to add a review requesting justification? [p/r/m/o] (p=proceed/ignore, r=add to review, m=skip to merge, o=open PRs in browser): ").strip().lower()
                if ans == 'o':
                    url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{ctx.pr}"
                    urls_to_open = [url]
                    for pr_str in found_prs:
                        m_pr = re.search(r'#(\d+)', pr_str)
                        if m_pr:
                            urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{m_pr.group(1)}")
                    open_in_browser(urls_to_open)
                    print("Opened URLs in browser.")
                elif ans == 'r':
                    ctx.report.add("Multiple Source PRs", md_text)
                    ctx.report.record_failure()
                    break
                elif ans == 'm':
                    raise SkipToMerge()
                elif ans == 'p':
                    break
                else:
                    print("Invalid choice. Please enter p, r, m, or o.")

    def run(self, ctx: AuditContext) -> None:
        log.info("Verifying commit parity with original PR(s) locally...")
        
        bp_cherry_picks, invalid_format_commits, cp_regex = self._validate_formats(ctx.pr_commits, ctx.base)
        
        mapping = self._map_upstream_commits(ctx.G, bp_cherry_picks)
        
        vis_text, vis_md = self._generate_visualizer(ctx.pr_commits, ctx.pr, mapping['pr_mapping'], mapping['missing_commits'])
        
        if vis_text:
            print(vis_text)
        if vis_md:
            ctx.report.set_visualizer(vis_md)

        if invalid_format_commits:
            self._handle_invalid_formats(ctx, invalid_format_commits)

        extra_commits = [c for c in ctx.pr_commits if c.hexsha not in mapping['bp_commits_mapped']]
        unmerged_cps = [c for c in extra_commits if cp_regex.search(c.message)]
        
        if mapping['valid_ref'] and unmerged_cps:
            self._handle_unmerged_commits(ctx, unmerged_cps, cp_regex)

        if mapping['missing_commits'] or extra_commits:
            self._handle_parity_mismatch(ctx, mapping['missing_commits'], extra_commits, mapping['found_prs'])
        elif mapping['analyzed_merges']:
            print("\033[92mCommit parity check passed! All upstream commits from identified PRs are present.\033[0m")
            if len(mapping['found_prs']) > 1:
                self._handle_multiple_prs(ctx, mapping['found_prs'])

        ctx.visualizer_md_table = vis_md
        ctx.found_prs = mapping['found_prs']

class ConflictSimulationCheck(BaseAuditCheck):
    @property
    def name(self) -> str:
        return "Conflict Simulation"

    def run(self, ctx: AuditContext) -> None:
        """
        Creates a temporary worktree to dry-run the cherry-pick sequence, verifying
        conflict resolutions dynamically.
        """
        G, session, pr, pr_commits, base, report, args = ctx.G, ctx.session, ctx.pr, ctx.pr_commits, ctx.base, ctx.report, ctx.args
        always_fetch = args.always_fetch
        visualizer_md_table = ctx.visualizer_md_table
        wt_dir = tempfile.mkdtemp(prefix="ptl-worktree-")

        try:
            # Unconditionally fetch the target base branch to ensure the simulation worktree
            # accurately reflects the latest remote state, avoiding false-positive conflicts.
            base_commit = resolve_ref(G, base, BASE_REMOTE_URL, True)
            G.git.worktree('add', '--detach', wt_dir, base_commit.hexsha)
            wt_repo = git.Repo(wt_dir)
            
            cp_regex = re.compile(r"\(cherry picked from commit ([0-9a-f]{7,40})\)")
            auto_approve_conflicts = False
            first_conflict = True
            
            recorded_deviations = []
    
            for commit in pr_commits:
                if len(commit.parents) > 1:
                    log.error(f"Commit {commit.hexsha[:8]} is a merge commit. Not allowed.")
                    sys.exit(1)
                
                m = cp_regex.search(commit.message)
                is_cherry_pick = bool(m)
    
                if is_cherry_pick:
                    orig_sha = m.group(1)
                    log.info(f"Simulating cherry-pick of {orig_sha[:8]} for {commit.hexsha[:8]} ...")
                    c = resolve_ref(wt_repo, orig_sha, BASE_REMOTE_URL, always_fetch)
    
                    has_conflict = False
                    clean_tree = None
                    try:
                        wt_repo.git(c=SANDBOX_CFG).cherry_pick(c.hexsha)
                        clean_tree = wt_repo.head.commit.tree.hexsha
                        wt_repo.git.reset('--hard', 'HEAD~1')
                    except git.exc.GitCommandError:
                        has_conflict = True
                        unmerged = wt_repo.git.diff('--name-only', '--diff-filter=U').splitlines()
                        wt_repo.git.cherry_pick('--abort')
    
                    # Always apply the backport commit so the worktree matches the PR sequence
                    try:
                        wt_repo.git(c=SANDBOX_CFG).cherry_pick("--allow-empty", commit.hexsha)
                    except git.exc.GitCommandError:
                        log.error(f"Failed to apply backport commit {commit.hexsha[:8]}! The PR likely has conflicts with the base branch and needs a rebase.")
                        wt_repo.git.cherry_pick('--abort')
                        
                        if args.ci_mode:
                            md_text = f"### Automated PR Review - Rebase Required\n\nBackport commit `{commit.hexsha[:8]}` failed to apply cleanly to the base branch during simulation. The PR likely has conflicts and needs a rebase."
                            report.add("Simulation Failure", md_text)
                            raise SkipToMerge()
                            
                        while True:
                            ans = input("How do you want to handle this? [p/m/r/o/q] (p=proceed simulation anyway, m=skip to merge, r=add to review and skip simulation, o=open PR in browser, q=quit) ").strip().lower()
                            if ans == 'o':
                                url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                                open_in_browser([url])
                                print(f"Opened {url} in browser.")
                            elif ans == 'm':
                                raise SkipToMerge()
                            elif ans == 'r':
                                md_text = f"### Automated PR Review - Rebase Required\n\nBackport commit `{commit.hexsha[:8]}` failed to apply cleanly to the base branch during simulation. The PR likely has conflicts and needs a rebase."
                                report.add("Simulation Failure", md_text)
                                report.record_failure()
                                raise SkipToMerge()
                            elif ans == 'q':
                                sys.exit(1)
                            elif ans == 'p':
                                break
                            else:
                                print("Invalid choice. Please enter p, m, r, o, or q.")
                        
                    backport_tree = wt_repo.head.commit.tree.hexsha
    
                    is_sneaky = (not has_conflict) and (clean_tree != backport_tree)
    
                    if has_conflict or is_sneaky:
                        if auto_approve_conflicts:
                            log.info(f"Skipping approval and taking backport commit for {orig_sha[:8]}.")
                            continue
    
                        if first_conflict:
                            if args.ci_mode:
                                ans = 'i'
                            else:
                                while True:
                                    ans = input(f"Conflict or unapproved deviation detected in {c.hexsha[:8]}. Do you want to interactively review this PR? [i/a/m/o]\n(i = interactively review, a = auto-approve remaining, m = skip to merge, o = open in browser) ").strip().lower()
                                    if ans == 'o':
                                        bp_pr_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                                        commit_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{c.hexsha}"
                                        open_in_browser([bp_pr_url, commit_url])
                                        print("Opened PR and commit URLs in browser.")
                                    elif ans in ['i', 'a', 'm']:
                                        break
                                    else:
                                        print("Invalid choice. Please enter i, a, m, or o.")
                            first_conflict = False
                            if ans == 'm':
                                log.info("Skipping ahead to merge.")
                                raise SkipToMerge()
                            elif ans == 'a':
                                log.info("Auto-approving and skipping interactive checks for this PR.")
                                auto_approve_conflicts = True
                                continue
    
                        editor = os.environ.get('EDITOR', 'vim')
                        if has_conflict:
                            if not args.ci_mode: log.warning(f"Opening {editor} to inspect conflict in {c.hexsha[:8]} ...")
                        else:
                            if not args.ci_mode: log.warning(f"Opening {editor} to inspect unexplained deviation in clean cherry-pick {c.hexsha[:8]} ...")
                            unmerged = wt_repo.git.diff('--name-only', clean_tree, backport_tree).splitlines()
                        for f in unmerged:
                            log.debug("Unmerged: %s", f)
                        
                        if unmerged:
                            # Generate a diff of the commit messages
                            orig_msg = c.message.splitlines(keepends=True)
                            bp_msg = commit.message.splitlines(keepends=True)
                            msg_diff = "".join(difflib.unified_diff(orig_msg, bp_msg, 
                                                                    fromfile=f'Original ({c.hexsha[:8]})', 
                                                                    tofile=f'Backport ({commit.hexsha[:8]})'))
    
                            # Generate git range-diff to highlight changes between the original patch and backport
                            try:
                                diff = wt_repo.git.range_diff(f"{c.hexsha}^!", f"{commit.hexsha}^!")
                            except git.exc.GitCommandError as e:
                                log.warning(f"Could not generate range-diff: {e}")
                                diff = f"Could not generate range-diff:\n{e}"
                            
                            # Concatenate with a marker
                            diff = f"{msg_diff}\n" + "="*80 + "\nRANGE DIFF\n" + "="*80 + "\n\n" + diff
    
                        prompt_text = "Does the PR properly explain and resolve this conflict/deviation? [p/r/s/m/o/e]\n(p = proceed to next check, r = request review, s = skip remaining checks, m = skip to merge, o = open in browser, e = re-examine in editor) "
                        
                        open_editor = not args.ci_mode
                        while True:
                            if unmerged and open_editor:
                                # Present file-specific 3-pane view (range-diff, orig patch, backport patch)
                                for f in unmerged:
                                    try:
                                        file_diff = f"Conflict in {f}:\n\n" + diff
                                        orig_patch = wt_repo.git.show(c.hexsha, "--", f)
                                        bp_patch = wt_repo.git.show(commit.hexsha, "--", f)
    
                                        if orig_patch == "":
                                            orig_patch = "(file not found or empty)"
                                        if bp_patch == "":
                                            bp_patch = "(file not found or empty)"
    
                                        fds_to_close = []
                                        threads = []
    
                                        cmd = [editor]
                                        if any(ed in editor for ed in ['vim', 'nvim', 'vi']):
                                            cmd.append('-O')
                                        
                                        pass_fds = []
                                        if file_diff:
                                            path, r_fd = make_pipe(file_diff, fds_to_close, threads)
                                            cmd.append(path)
                                            pass_fds.append(r_fd)
                                        
                                        path1, r_fd1 = make_pipe(orig_patch, fds_to_close, threads)
                                        cmd.append(path1)
                                        pass_fds.append(r_fd1)
                                        
                                        path2, r_fd2 = make_pipe(bp_patch, fds_to_close, threads)
                                        cmd.append(path2)
                                        pass_fds.append(r_fd2)
                                        
                                        try:
                                            subprocess.run(cmd, pass_fds=pass_fds)
                                        except FileNotFoundError:
                                            log.error(f"Editor '{editor}' not found. Please set the EDITOR environment variable.")
                                        
                                        for t in threads:
                                            t.join()
                                        for fd in fds_to_close:
                                            os.close(fd)
                                    except git.exc.GitCommandError as e:
                                        log.warning(f"Could not generate patch comparison for {f}: {e}")
                            
                            open_editor = False
                            
                            if args.ci_mode:
                                ans = 'r'
                                break
                                
                            ans = input(prompt_text).strip().lower()
                            if ans == 'o':
                                bp_pr_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                                orig_commit_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{c.hexsha}"
                                bp_commit_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{commit.hexsha}"
                                open_in_browser([bp_pr_url, orig_commit_url, bp_commit_url])
                                print("Opened relevant URLs in browser.")
                            elif ans == 'e':
                                log.info(f"Re-opening {editor} to examine conflicts...")
                                open_editor = True
                            elif ans in ['p', 'r', 's', 'm']:
                                break
                            else:
                                print("Invalid choice. Please enter p, r, s, m, o, or e.")
                        if ans == 'm':
                            log.info("Skipping ahead to merge.")
                            raise SkipToMerge()
                        elif ans == 's':
                            log.info("Skipping remaining checks for this PR.")
                            auto_approve_conflicts = True
                        elif ans == 'p':
                            pass # Already applied backport commit
                        elif ans == 'r':
                            log.error(f"Rejecting PR due to unjustified/unapproved change in {commit.hexsha[:8]}. Adding to draft review...")
                            diff_text = diff if 'diff' in locals() else "No range diff available."
                            
                            recorded_deviations.append({
                                'orig_sha': c.hexsha,
                                'bp_sha': commit.hexsha,
                                'unmerged': unmerged,
                                'diff_text': diff_text
                            })
                            report.record_failure()
                            continue
                else:
                    log.info(f"Applying branch-specific commit {commit.hexsha[:8]} ...")
                    wt_repo.git(c=SANDBOX_CFG).cherry_pick("--allow-empty", commit.hexsha)
            
            if recorded_deviations:
                md_text = """
                ### Automated Backport Parity Review - Backport Deviation Alert
                
                A conflict or unapproved deviation was detected during the simulation of this backport. The code in this PR does not match a clean cherry-pick of the upstream commits.
                
                """
                md_text = textwrap.dedent(md_text)
                
                for dev in recorded_deviations:
                    md_text += f"#### Deviation in Backport {dev['bp_sha'][:8]} (cherry-pick of {dev['orig_sha'][:8]})\n\n"
                    
                    if dev['unmerged']:
                        md_text += "**Affected File(s)**\n"
                        for f_name in dev['unmerged']:
                            f_hash = hashlib.sha256(f_name.encode('utf-8')).hexdigest()
                            md_text += f"* `{f_name}`\n"
                            md_text += f"  * Original: https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{dev['orig_sha']}#diff-{f_hash}\n"
                            md_text += f"  * Backport: https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{dev['bp_sha']}#diff-{f_hash}\n"
                        md_text += "\n"
                        
                    md_text += f"**Range Diff**\n<details><summary>Click to expand</summary>\n\n```diff\n{dev['diff_text']}\n```\n</details>\n\n"

                footer = """
                **How to proceed:**
                * **Authors (Genuine Conflicts):** If this is a genuine conflict requiring manual resolution, ensure your resolution is correct. You **must** explain the conflict resolution in the commit message (e.g., leave the standard Git `Conflicts:` block intact) and include an explanation for changes.
                * **Authors (Need Help?):** Reach out to the Component Lead for technical guidance on complex code conflicts.
                * **Component Leads (Review):** Please review the Range Diff(s) above to verify the author's manual conflict resolution is correct for this release branch. If the deviation is intentional, documented, and approved then the component lead or @ceph/ceph-release-manager can bypass this check by commenting `/audit override`.

                [Be familiar with the rules and guidelines for writing backports.](https://github.com/ceph/ceph/blob/main/SubmittingPatches-backports.rst)
                """
                md_text += textwrap.dedent(footer)
                
                report.add("Conflict/Deviation", md_text)

            log.info("Verification of backport cherry-pick for PR #%d is complete.", pr)
        finally:
            log.info("Removing temporary worktree `%s'", wt_dir)
            G.git.worktree('remove', '--force', wt_dir)

class RedmineLinkageCheck(BaseAuditCheck):
    @property
    def name(self) -> str:
        return "Redmine Linkage"

    def run(self, ctx: AuditContext) -> None:
        session, R, bp_pr, base, report, args = ctx.session, ctx.R, ctx.pr, ctx.base, ctx.report, ctx.args
        found_prs = ctx.found_prs

        if not R:
            log.debug("Redmine connection not available, skipping linkage audit.")
            return

        log.info("Verifying Redmine tracker linkage...")
        irregularities = []
        notes = []
    
        if not found_prs:
            log.warning(f"No original PRs were identified for backport PR #{bp_pr}.")
            irregularities.append(f"**Missing Original PR(s):** Could not identify any `main` PRs associated with this backport. Ensure the backport commits properly reference the original PRs.")
            
            log.info(f"Searching Redmine directly for any tracker linked to backport PR #{bp_pr}...")
            filters = {
                f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}": bp_pr,
                "status_id": "*"
            }
            direct_trackers = list(R.issue.filter(**filters))
            if not direct_trackers:
                log.info(f"Returned no results. No tracker tickets found linked to PR #{bp_pr}.")
                irregularities.append(f"**Orphaned Backport PR:** Could not find any Redmine tracker linked directly to this backport PR #{bp_pr}.")
            else:
                tracker_urls = [f"{REDMINE_ENDPOINT}/issues/{t.id}" for t in direct_trackers]
                log.info(f"Found backport tracker(s): {', '.join(tracker_urls)}")
                for bp_tracker in direct_trackers:
                    if bp_tracker.status.id == REDMINE_STATUS_ID_REJECTED:
                        irregularities.append(f"**Rejected Backport Ticket:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) is marked as Rejected. Perhaps this backport PR should be closed?")
                    elif bp_tracker.status.id in (REDMINE_STATUS_ID_RESOLVED, REDMINE_STATUS_ID_CLOSED):
                        irregularities.append(f"**Closed/Resolved Backport Ticket:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) is already {bp_tracker.status.name}. Please reconcile.")
    
        for pr_name in found_prs:
            m_pr = re.search(r'#(\d+)', pr_name)
            if not m_pr:
                continue
            orig_pr = int(m_pr.group(1))
    
            pr_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{orig_pr}"
            log.info(f"Checking Redmine for main PR #{orig_pr} ({pr_url})...")
            filters = {
                f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}": orig_pr,
                "status_id": "*"
            }
            main_trackers = list(R.issue.filter(**filters))
            
            if main_trackers:
                tracker_urls = [f"{REDMINE_ENDPOINT}/issues/{t.id}" for t in main_trackers]
                log.info(f"Found main tracker(s): {', '.join(tracker_urls)}")
            else:
                log.info(f"No tracker found with Pull Request ID '{orig_pr}', searching descriptions...")
                search_results = R.search(str(orig_pr), resources=['issues'])
                
                # R.search returns a dictionary grouped by resource type.
                # Extract the list of issues, defaulting to an empty list if none exist.
                issues = search_results.get('issue', [])
                
                for res in issues:
                    try:
                        issue = R.issue.get(res.id)
                        if issue.tracker.id != REDMINE_TRACKER_ID_BACKPORT and hasattr(issue, 'description') and issue.description:
                            match = re.search(r'https://github\.com/[^/]+/[^/]+/pull/(\d+)', issue.description.strip(), re.MULTILINE)
                            if match and int(match.group(1)) == orig_pr:
                                main_trackers.append(issue)
                                log.info(f"Found main tracker #{issue.id} ({REDMINE_ENDPOINT}/issues/{issue.id}) via description search.")
                                print(f"Found PR #{orig_pr} in description of issue #{issue.id}.")
                                if args.ci_mode:
                                    irregularities.append(f"**Malformed Main Tracker:** Main tracker [#{issue.id}]({REDMINE_ENDPOINT}/issues/{issue.id}) has the PR link in the description rather than the 'Pull Request ID' field. Please fix this.")
                                else:
                                    ans = input(f"Fix tracker #{issue.id} by moving PR link from description to 'Pull Request ID' field? [y/N/m]: ").strip().lower()
                                    if ans == 'm':
                                        raise SkipToMerge()
                                    elif ans == 'y':
                                        new_desc = issue.description.replace(match.group(0), "").strip()
                                        if args.dry_run:
                                            log.info(f"[DRY RUN] Would update main tracker #{issue.id} description and set Pull Request ID to {orig_pr}")
                                        else:
                                            R.issue.update(issue.id, description=new_desc, custom_fields=[{'id': REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID, 'value': str(orig_pr)}])
                                            log.info(f"Updated issue #{issue.id}")
                                        notes.append(f"Note: Main tracker [#{issue.id}]({REDMINE_ENDPOINT}/issues/{issue.id}) was automatically updated to set the Pull Request ID field properly (it was previously only in the description).")
                    except redminelib.exceptions.ResourceNotFoundError:
                        pass
            
            if not main_trackers:
                log.warning(f"Failed to find any main trackers for PR #{orig_pr} ({pr_url})")
                irregularities.append(f"**Orphaned Main PR:** Could not find a Redmine tracker for `main` PR #{orig_pr}. Please create a ticket, set its 'Pull Request ID', populate the 'Backports' field, and ensure it is in the 'Pending Backport' state.")
                continue
            
            for main_tracker in main_trackers:
                log.debug(f"Investigating relations for main tracker #{main_tracker.id} ({REDMINE_ENDPOINT}/issues/{main_tracker.id})")
                bp_trackers = []
                try:
                    for rel in main_tracker.relations:
                        if rel.relation_type == 'copied_to':
                            try:
                                rel_issue = R.issue.get(rel.issue_to_id)
                                if rel_issue.tracker.id == REDMINE_TRACKER_ID_BACKPORT:
                                    cf_release = get_custom_field(rel_issue, REDMINE_CUSTOM_FIELD_ID_RELEASE)
                                    if cf_release == base:
                                        bp_trackers.append(rel_issue)
                            except redminelib.exceptions.ResourceNotFoundError:
                                pass
                except redminelib.exceptions.ResourceAttrError:
                    pass
    
                if not bp_trackers:
                    log.warning(f"No backport trackers found for main tracker #{main_tracker.id} ({REDMINE_ENDPOINT}/issues/{main_tracker.id}) targeting base '{base}'")
                    irregularities.append(f"**Missing Backport Tracker:** Main tracker [#{main_tracker.id}]({REDMINE_ENDPOINT}/issues/{main_tracker.id}) does not have a backport tracker for `{base}`. Please adjust the 'Backports' field on the main tracker appropriately and remove 'backport_processed' from 'Tags (freeform)'.")
                    continue
                
                for bp_tracker in bp_trackers:
                    log.info(f"Found backport tracker #{bp_tracker.id} ({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) linked to main tracker #{main_tracker.id}")
                    cf_pr = get_custom_field(bp_tracker, REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID)
                    
                    # Check description if PR ID is missing
                    if not cf_pr and hasattr(bp_tracker, 'description') and bp_tracker.description:
                        match = re.search(r'https://github\.com/[^/]+/[^/]+/pull/(\d+)', bp_tracker.description.strip(), re.MULTILINE)
                        if match:
                            found_pr = match.group(1)
                            log.debug(f"Found PR #{found_pr} in description of backport tracker #{bp_tracker.id}")
                            print(f"Found PR #{found_pr} in description of backport tracker #{bp_tracker.id}.")
                            if args.ci_mode:
                                irregularities.append(f"**Malformed Backport Tracker:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) has the PR link in the description rather than the 'Pull Request ID' field. Please fix this.")
                            else:
                                ans = input(f"Fix tracker #{bp_tracker.id} by moving PR link from description to 'Pull Request ID' field? [y/N/m]: ").strip().lower()
                                if ans == 'm':
                                    raise SkipToMerge()
                                elif ans == 'y':
                                    new_desc = bp_tracker.description.replace(match.group(0), "").strip()
                                    if args.dry_run:
                                        log.info(f"[DRY RUN] Would update backport tracker #{bp_tracker.id} description and set Pull Request ID to {found_pr}")
                                    else:
                                        R.issue.update(bp_tracker.id, description=new_desc, custom_fields=[{'id': REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID, 'value': str(found_pr)}])
                                        log.info(f"Updated backport tracker #{bp_tracker.id}")
                                    notes.append(f"Note: Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) was automatically updated to set the Pull Request ID field properly (it was previously only found in the description).")
                                    cf_pr = found_pr
    
                    if bp_tracker.status.id == REDMINE_STATUS_ID_REJECTED:
                        irregularities.append(f"**Rejected Backport Ticket:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) is marked as Rejected. Perhaps this backport PR should be closed?")
                    elif bp_tracker.status.id in (REDMINE_STATUS_ID_RESOLVED, REDMINE_STATUS_ID_CLOSED):
                        irregularities.append(f"**Closed/Resolved Backport Ticket:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) is already {bp_tracker.status.name}. Please reconcile.")
                    
                    if not cf_pr:
                        log.warning(f"Backport tracker #{bp_tracker.id} is missing a Pull Request ID.")
                        irregularities.append(f"**Missing PR Link:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) has no 'Pull Request ID' field set. Please link it to PR #{bp_pr}.")
                    elif str(cf_pr) != str(bp_pr):
                        log.warning(f"Backport tracker #{bp_tracker.id} points to PR #{cf_pr}, but we are auditing PR #{bp_pr}.")
                        irregularities.append(f"**Mismatched PR Link:** Backport tracker [#{bp_tracker.id}]({REDMINE_ENDPOINT}/issues/{bp_tracker.id}) points to PR #{cf_pr}, not this backport PR #{bp_pr}.")
        
        if not irregularities:
            report.redmine_linkage_correct = True

        if irregularities or notes:
            md_text = "### Automated Redmine Linkage Audit\n\n"
            if irregularities:
                md_text += "The following tracking irregularities were found:\n"
                for irr in irregularities:
                    md_text += f"* {irr}\n"
            if notes:
                md_text += "\n" + "\n".join([f"* {note}" for note in notes]) + "\n"
            
            print("\n\033[93m" + "="*80)
            print("REDMINE LINKAGE IRREGULARITIES DETECTED")
            print("="*80 + "\033[0m")
            
            if args.ci_mode:
                report.add("Redmine Linkage", md_text)
            else:
                while True:
                    ans = input("Redmine irregularities detected! [p/r/m] (p=proceed, r=add to review, m=skip to merge): ").strip().lower()
                    if ans == 'p':
                        break
                    elif ans == 'm':
                        raise SkipToMerge()
                    elif ans == 'r':
                        report.add("Redmine Linkage", md_text)
                        report.record_failure()
                        break

class MergeConflictCheck(BaseAuditCheck):
    @property
    def name(self) -> str:
        return "Merge Conflict"

    def run(self, ctx: AuditContext) -> None:
        G, session, pr, tip, base, report, args = ctx.G, ctx.session, ctx.pr, ctx.tip, ctx.base, ctx.report, ctx.args
        log.info("Performing trivial merge check for PR #%d...", pr)
        wt_dir = tempfile.mkdtemp(prefix="ptl-merge-check-")
        has_base_conflicts = False
        try:
            G.git.worktree('add', '--detach', wt_dir, G.head.commit)
            wt_repo = git.Repo(wt_dir)
            try:
                wt_repo.git(c=SANDBOX_CFG).merge(tip.hexsha, '--no-commit', '--no-ff')
            except git.exc.GitCommandError as e:
                log.debug(f"Merge failed: {e}")
                has_base_conflicts = True
            finally:
                try:
                    wt_repo.git.merge('--abort')
                except git.exc.GitCommandError:
                    pass
        finally:
            G.git.worktree('remove', '--force', wt_dir)

        if has_base_conflicts:
            log.error(f"PR #{pr} has conflicts with the target base branch.")
            md_text = "### Automated PR Review - Rebase Required\n\n"
            md_text += f"This PR currently has merge conflicts with the target base branch. Please rebase and resolve the conflicts."
            if args.ci_mode:
                report.add("Base Conflicts", md_text)
            else:
                while True:
                    ans = input(f"PR #{pr} needs a rebase! Do you want to add a review requesting a rebase? [p/r/m/q/o] (p=proceed/skip check, r=add to review, m=skip to merge, q=abort, o=open PR in browser): ").strip().lower()
                    if ans == 'o':
                        url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                        open_in_browser([url])
                        print(f"Opened {url} in browser.")
                    elif ans == 'm':
                        raise SkipToMerge()
                    elif ans == 'p':
                        log.warning(f"Skipping rebase check for PR #{pr}.")
                        break
                    elif ans == 'r':
                        report.add("Base Conflicts", md_text)
                        report.record_failure()
                        break
                    elif ans == 'q' or ans == '':
                        log.error(f"Aborting script due to unmergeable PR #{pr}.")
                        sys.exit(1)
                    else:
                        print("Invalid choice. Please enter p, r, m, q, or o.")

def verify_pr_readiness(G, session, R, pr, pr_commits, tip, base, args):
    report = AuditReport()
    ctx = AuditContext(G, session, R, pr, pr_commits, tip, base, args, report)
    
    checks: List[BaseAuditCheck] = [
        MergeConflictCheck(),
    ]
    
    if base != 'main':
        checks.append(CommitParityCheck())
        if not args.skip_conflict_check:
            checks.append(ConflictSimulationCheck())
        checks.append(RedmineLinkageCheck())

    try:
        for check in checks:
            try:
                check.run(ctx)
            except SkipToMerge:
                log.info(f"Skipping remaining checks for PR #{pr}.")
                break
    except SystemExit:
        if not args.audit:
            raise
        return False

    if report.has_errors():
        log.error(f"Audit failed for PR #{pr}.")
        if args.ci_mode:
            report.post_consolidated_review(session, pr, dry_run=args.dry_run, ci_mode=args.ci_mode)
        else:
            consolidated_text = report.get_consolidated_text()
            if consolidated_text:
                post_draft_review(session, pr, consolidated_text, base=base)
        return False
        
    return True

def manage_qa_tracker(args, R, session, branch, prs, tag, qa_tracker_description, base, created_branch):
    if not (args.create_qa or args.update_qa):
        return

    if not created_branch:
        log.error("branch already exists!")
        sys.exit(1)
    project = R.project.get(REDMINE_PROJECT_QA)
    log.debug("got redmine project %s", project)
    user = R.user.get('current')
    log.debug("got redmine user %s", user)
    tracker = None
    for t in project.trackers:
        if t['name'] == REDMINE_TRACKER_QA:
            tracker = t
    if tracker is None:
        log.error("could not find tracker in project: %s", REDMINE_TRACKER_QA)
    log.debug("got redmine tracker %s", tracker)

    # Use hard-coded custom field ids because there is apparently no way to
    # figure these out via the python library
    custom_fields = []
    custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_SHAMAN_BUILD, 'value': branch})
    custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_QA_RUNS, 'value': branch})
    if args.qa_release:
        custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_QA_RELEASE, 'value': args.qa_release})
    if args.qa_tags:
        custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_QA_TAGS, 'value': args.qa_tags})

    if not args.no_tag and tag:
        origin_url = f'{BASE_PROJECT}/{CI_REPO}/commits/{tag.name}'
    else:
        origin_url = f'{BASE_PROJECT}/{CI_REPO}/commits/{branch}'
    custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_GIT_BRANCH, 'value': origin_url})

    issue_kwargs = {
      "assigned_to_id": user['id'],
      "custom_fields": custom_fields,
      "description": '\n'.join(qa_tracker_description),
      "project_id": project['id'],
      "watcher_user_ids": [user['id']],
    }

    if args.qa_private:
        issue_kwargs['is_private'] = True

    if args.update_qa:
        issue = R.issue.get(args.update_qa)
        if issue.project.id != project.id:
            log.error(f"issue {issue.url} project {issue.project} does not match {project}")
            sys.exit(1)
        if issue.tracker.id != tracker.id:
            log.error(f"issue {issue.url} tracker {issue.tracker} does not match {tracker}")
            sys.exit(1)

        old_branch = "unknown"
        for cf in issue.custom_fields:
            if cf.id in (REDMINE_CUSTOM_FIELD_ID_SHAMAN_BUILD, REDMINE_CUSTOM_FIELD_ID_QA_RUNS):
                old_branch = cf.value
                if old_branch:
                    break

        old_prs = set()
        if hasattr(issue, 'description') and issue.description:
            for match in re.finditer(r'\* "PR #(\d+)":', issue.description):
                old_prs.add(int(match.group(1)))

        new_prs = set(int(p) for p in prs)
        added_prs = new_prs - old_prs
        removed_prs = old_prs - new_prs

        notes = f"""
            **Update Triggered**

            **Previous Branch:** `{old_branch}`
            **New Branch:** `{branch}`

            **Previous QA Links:**
            * "Shaman Build":https://shaman.ceph.com/builds/ceph/{old_branch}/
            * "Pulpito / Teuthology Results":https://pulpito.ceph.com/?branch={old_branch}

            """
        notes = textwrap.dedent(notes)
        if old_prs:
            notes += "**Previous PRs included in that run:**\n"
            for old_pr in sorted(old_prs):
                notes += get_pr_tracker_string(session, old_pr) + "\n"
        else:
            notes += "**Previous PRs included in that run:** None\n"

        issue_kwargs['notes'] = notes.strip()

        if args.dry_run:
            log.info(f"[DRY RUN] Would update redmine qa issue {issue.url} with kwargs: {issue_kwargs}")
            issue_url = issue.url
        else:
            log.debug("updating issue with kwargs: %s", issue_kwargs)
            if R.issue.update(issue.id, **issue_kwargs):
                log.info("updated redmine qa issue: %s", issue.url)
                issue_url = issue.url
            else:
                log.error(f"failed to update {issue}")
                sys.exit(1)

        for pr in added_prs:
            body = f"This PR has been added to [{issue.subject}]({issue_url})."
            if args.dry_run:
                log.info(f"[DRY RUN] Would post comment to added PR #{pr}: {body}")
            else:
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/comments"
                r = session.post(endpoint, auth=GithubBearerAuth(), data=json.dumps({'body':body}))
                if r.status_code == 201:
                    log.info(f"Successfully posted added comment to PR #{pr}")
                else:
                    log.error(f"Failed to post comment: {r.status_code} {r.text}")

        for pr in removed_prs:
            body = f"This PR has been removed from [{issue.subject}]({issue_url})."
            if args.dry_run:
                log.info(f"[DRY RUN] Would post comment to removed PR #{pr}: {body}")
            else:
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/comments"
                r = session.post(endpoint, auth=GithubBearerAuth(), data=json.dumps({'body':body}))
                if r.status_code == 201:
                    log.info(f"Successfully posted removed comment to PR #{pr}")
                else:
                    log.error(f"Failed to post comment: {r.status_code} {r.text}")

    elif args.create_qa:
        now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H:%M")
        default_subject = f"{base} integration testing by {USER} started {now_str}"
        issue_kwargs['subject'] = args.qa_subject if args.qa_subject else default_subject

        if args.dry_run:
            log.info(f"[DRY RUN] Would create redmine qa issue with kwargs: {issue_kwargs}")
            issue_url = f"{REDMINE_ENDPOINT}/issues/DRY_RUN_ID"
        else:
            log.debug("creating issue with kwargs: %s", issue_kwargs)
            issue = R.issue.create(**issue_kwargs)
            log.info("created redmine qa issue: %s", issue.url)
            issue_url = issue.url

        for pr in prs:
            log.debug(f"Posting QA Run in comment for ={pr}")
            subject = issue_kwargs['subject']
            body = f"This PR has been added to [{subject}]({issue_url})."
            if args.dry_run:
                log.info(f"[DRY RUN] Would post comment to PR #{pr}: {body}")
            else:
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/comments"
                r = session.post(endpoint, auth=GithubBearerAuth(), data=json.dumps({'body':body}))
                if r.status_code == 201:
                    log.info(f"Successfully posted comment to PR #{pr}")
                else:
                    log.error(f"Failed to post comment: {r.status_code} {r.text}")

def build_branch(args):
    base = args.base
    label = args.label
    merge_branch_name = args.merge_branch_name

    session = requests.Session()

    if label:
        # Check the label format
        if re.search(r'\bwip-(.*?)-testing\b', label) is None:
            log.error("Unknown Label '{lblname}'. Label Format: wip-<name>-testing".format(lblname=label))
            sys.exit(1)

        # Check if the Label exist in the repo
        endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/labels/{label}"
        get(session, endpoint, paging=False)

    G = git.Repo(args.git)

    try:
        c = resolve_ref(G, 'main', BASE_REMOTE_URL, args.always_fetch)
        githubmap_content = G.git.show(f"{c.hexsha}:.githubmap")
        comment = re.compile(r"\s*#")
        patt = re.compile(r"([\w-]+)\s+(.*)")
        for line in githubmap_content.splitlines():
            if comment.match(line):
                continue
            m = patt.match(line)
            if m:
                CONTRIBUTORS[m.group(1)] = m.group(2)
    except git.exc.GitCommandError as e:
        raise SystemExit(f"Could not fetch .githubmap from {BASE_REMOTE_URL}:main:\n{e}")

    R = None
    if args.create_qa or args.update_qa or args.audit or args.final_merge:
        log.info("connecting to %s", REDMINE_ENDPOINT)
        R = Redmine(REDMINE_ENDPOINT, username=REDMINE_USER, key=REDMINE_API_KEY)
        log.debug("connected")

    prs = args.prs
    if args.pr_label is not None:
        if args.pr_label == '' or args.pr_label.isspace():
            log.error("--pr-label must have a non-space value")
            sys.exit(1)
        payload = {'labels': args.pr_label, 'sort': 'created', 'direction': 'desc'}
        endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues"
        labeled_prs = []
        for l in get(session, endpoint, params=payload):
            labeled_prs.extend(l)
        if len(labeled_prs) == 0:
            log.error("Search for PRs matching label '{}' returned no results!".format(args.pr_label))
            sys.exit(1)
        for pr in labeled_prs:
            if pr['pull_request']:
                n = pr['number']
                log.info("Adding labeled PR #{} to PR list".format(n))
                prs.append(n)
    log.info("Will merge PRs: {}".format(prs))

    # PRE-FLIGHT: Auto-detect base from the first PR if necessary
    if prs and base is None:
        first_pr = prs[0]
        detected_base = get_pr_info(session, first_pr).get("base", {}).get("ref")
        
        if detected_base:
            log.info(f"Auto-detected target base from PR #{first_pr}: {detected_base}")
            base = detected_base
            if args.merge_branch_name is False:
                merge_branch_name = detected_base
        else:
            raise SystemExit(f"Could not auto-detect base for PR #{first_pr}. Use hard-coded --base")

    if args.integration:
        if not base or base == 'HEAD':
            log.error("--integration requires a valid base branch (auto-detected or provided via --base)")
            sys.exit(1)
        log.info(f"Integration workflow enabled. Applying defaults for base '{base}'...")
        args.branch_release = args.branch_release or base
        args.qa_release = args.qa_release or base
        args.credits = False
        args.always_fetch = True
        args.skip_conflict_check = True

    # Compute branch names now that integration flags and auto-detect have settled
    branch = datetime.datetime.utcnow().strftime(args.branch).format(user=USER)
    if args.branch_release:
        branch = branch + "-" + args.branch_release
    if args.branch_append:
        branch += f"-{args.branch_append}"
    if args.integration or merge_branch_name is False:
        merge_branch_name = branch

    if base == 'HEAD':
        log.info("Branch base is HEAD; not checking out!")
    else:
        log.info("Detaching HEAD onto base: {}".format(base))
        try:
            c = resolve_ref(G, base, BASE_REMOTE_URL, args.always_fetch)
            G.git.checkout(c)
        except git.exc.GitCommandError:
            log.info(f"Trying to checkout uninterpreted base {base}")
            c = G.commit(base)
            G.git.checkout(c)
        
        # So we know that we're not on an old test branch, detach HEAD onto ref:
        assert G.head.is_detached

    qa_tracker_description = []
    all_audits_passed = True

    for pr in prs:
        pr = int(pr)
        log.info("Merging PR #{pr}".format(pr=pr))

        response = get_pr_info(session, pr)
        detected_base = response.get("base", {}).get("ref")
        if base and detected_base and detected_base != base:
            log.warning(f"Base mismatch! PR #{pr} targets '{detected_base}' but expected '{base}'. Trusting provided --base.")

        remote_ref = "refs/pull/{pr}/head".format(pr=pr)
        remote_sha1 = response.get('head', {}).get('sha')
        
        ref_to_fetch = remote_sha1 if remote_sha1 else remote_ref
        tip = resolve_ref(G, ref_to_fetch, BASE_REMOTE_URL, args.always_fetch)
        log.info("Now have head for PR #%d: %s", pr, str(tip))

        qa_tracker_description.append(get_pr_tracker_string(session, pr, response))
        message = "Merge PR #%d into %s\n\n* %s:\n" % (pr, merge_branch_name, remote_ref)
        pr_commits = list(G.iter_commits(rev="HEAD.."+str(tip)))
        pr_commits.reverse() # chronological order for simulation

        audit_passed = True
        if args.final_merge or args.audit:
            audit_passed = verify_pr_readiness(G, session, R, pr, pr_commits, tip, base, args)
            if not audit_passed:
                all_audits_passed = False

        if not audit_passed and args.final_merge:
            log.error(f"Audit of PR #{pr} failed.")
            ans = input("Do you want to proceed with the final merge anyway? [y/N] ").strip().lower()
            if ans != 'y':
                log.error("Aborting final merge.")
                sys.exit(1)
            log.warning("Proceeding with final merge despite audit failure.")

        if args.audit:
            log.info(f"Audit of PR #{pr} {'passed' if audit_passed else 'failed'}. Skipping merge.")
            audit = args.audit_label
            if audit:
                if audit.queue:
                    if args.dry_run:
                        log.info(f"[DRY RUN] Would remove label {audit.queue} from PR #{pr}")
                    else:
                        req = session.delete(f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/labels/{audit.queue}", auth=GithubBearerAuth())
                        if req.status_code in (200, 204):
                            log.info(f"Removed label {audit.queue} from PR #{pr}")
                        else:
                            log.warning(f"Failed to remove label {audit.queue} from PR #{pr}: {req.status_code}")

                target_label = audit.passed if audit_passed else audit.failed
                if target_label:
                    if args.dry_run:
                        log.info(f"[DRY RUN] Would add label {target_label} to PR #{pr}")
                    else:
                        req = session.post(f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/labels", data=json.dumps([target_label]), auth=GithubBearerAuth())
                        if req.status_code == 200:
                            log.info(f"Added label {target_label} to PR #{pr}")
                        else:
                            raise SystemExit(f"Failed to add label {target_label} to PR #{pr}: {req.status_code}")

            # Skip merge
            continue

        for commit in reversed(pr_commits): # back to reverse-chronological for the message
            message = message + ("\t%s\n" % commit.summary)
            # Get tracker issues / bzs cited so the PTL can do updates
            short = commit.hexsha[:8]
            for m in BZ_MATCH.finditer(commit.message):
                log.info("[ {sha1} ] BZ cited: {cite}".format(sha1=short, cite=m.group(1)))
            for m in TRACKER_MATCH.finditer(commit.message):
                log.info("[ {sha1} ] Ceph tracker cited: {cite}".format(sha1=short, cite=m.group(1)))

        message = message + "\n"

        if args.credits:
            (addendum, new_contributors) = get_credits(session, pr, response)
            message += addendum
        else:
            new_contributors = []

        G.git.merge(tip.hexsha, '--no-ff', m=message)

        if new_contributors:
            # Check out the PR, add a commit adding to .githubmap
            log.info("adding new contributors to githubmap in merge commit")
            with open(git_dir + "/.githubmap", "a") as f:
                for c in new_contributors:
                    f.write("%s %s\n" % (c, new_contributors[c]))
            G.index.add([".githubmap"])
            G.git.commit("--amend", "--no-edit")

        if label:
            if args.dry_run:
                log.info(f"[DRY RUN] Would label PR #{pr} with {label}")
            else:
                req = session.post("https://api.github.com/repos/{project}/{repo}/issues/{pr}/labels".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), data=json.dumps([label]), auth=GithubBearerAuth())
                if req.status_code != 200:
                    log.error("PR #%d could not be labeled %s: %s" % (pr, label, req))
                    sys.exit(1)
                log.info("Labeled PR #{pr} {label}".format(pr=pr, label=label))

    if args.audit:
        log.info("Audit complete. Exiting without modifying branches or Redmine.")
        if not all_audits_passed:
            sys.exit(1)
        return

    message = """
    ceph-dev-pipeline: configure

    See documentation: https://github.com/ceph/ceph-build/tree/main/ceph-trigger-build#git-trailer-parameters

    """
    message = textwrap.dedent(message)
    trailer_commit = False
    if args.build_job:
        message += f"CEPH-BUILD-JOB: {args.build_job}\n"
        trailer_commit = True
    if args.distros:
        message += f"DISTROS: {' '.join(args.distros)}\n"
        trailer_commit = True
    if args.archs:
        message += f"ARCHS: {' '.join(args.archs)}\n"
        trailer_commit = True
    if args.flavors:
        message += f"FLAVORS: {' '.join(args.flavors)}\n"
        trailer_commit = True
    if trailer_commit:
        G.git.commit('--allow-empty', '-m', message)

    if args.stop_at_built:
        log.warning("Stopping execution (SIGSTOP) with built branch for further modification. Foreground when execution should resume (typically `fg`).")
        old_head = G.head.commit
        signal.raise_signal(signal.SIGSTOP)
        log.warning("Resuming execution.")
        new_head = G.head.commit
        if old_head != new_head:
            rev = f'{old_head}..{new_head}'
            for commit in G.iter_commits(rev=rev):
                qa_tracker_description.append(f'* "commit {commit}":{CI_REMOTE_URL}/commit/{commit} -- {commit.summary}')

    # If the branch is 'HEAD', leave HEAD detached (but use "main" for commit message)
    created_branch = False
    if branch == 'HEAD':
        log.info("Leaving HEAD detached; no branch anchors your commits")
    else:
        try:
            G.head.reference = G.create_head(branch)
            log.info("Checked out new branch {branch}".format(branch=branch))
            created_branch = True
        except:
            G.head.reference = G.create_head(branch, force=True)
            log.info("Checked out branch {branch}".format(branch=branch))

        if created_branch and not args.no_tag:
            # tag it for future reference.
            tag_name = "testing/%s" % branch
            if not args.dry_run:
                tag = git.refs.tag.Tag.create(G, tag_name)
                log.info("Created tag %s" % tag)
            else:
                log.info("[DRY RUN] Would create tag %s" % tag_name)
                class DummyTag:
                    def __init__(self, name):
                        self.name = name
                tag = DummyTag(tag_name)

    do_qa = args.create_qa or args.update_qa
    if args.push_ci or (not args.no_push_ci and do_qa):
        if not args.dry_run:
            G.git.push(CI_REMOTE_URL, branch) # for shaman
            if created_branch and not args.no_tag:
                G.git.push(CI_REMOTE_URL, tag.name) # for archival
        else:
            log.info("[DRY RUN] Would push branch %s to %s" % (branch, CI_REMOTE_URL))
            if created_branch and not args.no_tag:
                log.info("[DRY RUN] Would push tag %s to %s" % (tag.name, CI_REMOTE_URL))

    tag_obj = locals().get('tag') if created_branch and not args.no_tag else None
    manage_qa_tracker(args, R, session, branch, prs, tag_obj, qa_tracker_description, base, created_branch)

class SplitCommaAppendAction(argparse.Action):
    """
    Custom action to split comma-separated values and append each
    part to the destination list. Handles multiple uses of the argument.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        # 1. Get the list that's already stored in the namespace.
        #    'default=[]' in add_argument ensures this is always a list.
        current = getattr(namespace, self.dest)

        # 2. Split the new value(s) by comma
        #    Use strip() to remove any whitespace around items
        new_items = [item.strip() for item in values.split(',')]

        # 3. Extend the existing list with the new items
        if isinstance(current, list):
            current.extend(new_items)
        elif isinstance(current, set):
            current.update(new_items)
        else:
            raise NotImplementedError("type not supported")

def main():
    epilog_text = textwrap.dedent("""
        Quick Start Examples:
          1. Build an integration branch from labeled PRs and set up a QA ticket:
             $ ptl-tool.py --integration --pr-label wip-$USER-testing --create-qa

          2. Merge a specific PR for the main or release branch (leaves HEAD detached):
             $ ptl-tool.py --final-merge https://github.com/ceph/ceph/pull/12345

          For more examples, use the --examples switch.
    """)
    parser = argparse.ArgumentParser(
        description="Ceph PTL tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog_text
    )
    default_branch = TEST_BRANCH
    default_label = ''
    argv = sys.argv[1:]

    group = parser.add_argument_group('General Options')
    group.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    group.add_argument('--dry-run', dest='dry_run', action='store_true', help='print actions without modifying remote state')
    group.add_argument('--examples', dest='examples', action='store_true', help='show extended examples and usage')
    group.add_argument('--git-dir', dest='git', action='store', default=git_dir, help='git directory')

    group = parser.add_argument_group('GitHub PR Options')
    group.add_argument('--label', dest='label', action='store', default=default_label, help='label PRs for testing')
    group.add_argument('--pr-label', dest='pr_label', action='store', help='source PRs to merge via label')

    group = parser.add_argument_group('Branch Control Options')
    group.add_argument('--always-fetch', dest='always_fetch', action='store_true', help='always fetch commits from remote (bypass local cache)')
    group.add_argument('--base', dest='base', action='store', help='base for branch')
    group.add_argument('--branch', dest='branch', action='store', default=default_branch, help='branch to create ("HEAD" leaves HEAD detached; i.e. no branch is made)')
    group.add_argument('--branch-name-append', dest='branch_append', action='store', help='append string to branch name')
    group.add_argument('--branch-release', dest='branch_release', action='store', help='release name to embed in branch (for shaman)')
    group.add_argument('--merge-branch-name', dest='merge_branch_name', action='store', default=False, help='name of the branch for merge messages')
    group.add_argument('--no-credits', dest='credits', action='store_false', help='skip indication search (Reviewed-by, etc.)')
    group.add_argument('--no-tag', dest='no_tag', action='store_true', help='do not create a tag of the branch')
    group.add_argument('--stop-at-built', dest='stop_at_built', action='store_true', help='stop execution when branch is built')

    me_group = group.add_mutually_exclusive_group()
    me_group.add_argument('--final-merge', dest='final_merge', action='store_true', help='enable final upstream merge behavior (implies --branch HEAD)')
    me_group.add_argument('--integration', dest='integration', action='store_true', help='enable integration workflow: auto-sets --branch-release and --qa-release to the PR base, implies --no-credits, --always-fetch, and skips all verification checks')

    group = parser.add_argument_group('Build Control Options')
    group.add_argument('--archs', dest='archs', action=SplitCommaAppendAction, default=[], help='add arch(s) to build. Specify one or more times. Comma separated values are split.')
    group.add_argument('--build-job', dest='build_job', action='store', help='add ceph build job to execute in CI')
    group.add_argument('--debug-build', dest='debug_build', action='store_true', help='add \'debug\' flavor (build with CMAKE_BUILD_TYPE=Debug)')
    group.add_argument('--distros', dest='distros', action=SplitCommaAppendAction, default=set(), help='add distro(s) to build. Specify one or more times. Comma separated values are split.')
    group.add_argument('--flavors', dest='flavors', action=SplitCommaAppendAction, default=set(), help='add flavors(s) to build. Specify one or more times. Comma separated values are split.')

    group = parser.add_argument_group('QA Control Options')
    group.add_argument('--create-qa', dest='create_qa', action='store_true', help='create QA run ticket')
    group.add_argument('--qa-subject', dest='qa_subject', action='store', help='override default QA tracker subject')
    group.add_argument('--qa-private', dest='qa_private', action='store_true', help='make the QA run ticket private')
    group.add_argument('--qa-release', dest='qa_release', action='store', help='QA release for tracker (defaults to PR base when --integration)')
    group.add_argument('--qa-tags', dest='qa_tags', action='store', help='QA tags for tracker')
    group.add_argument('--update-qa', dest='update_qa', action='store', help='update QA run ticket')

    group = parser.add_argument_group('CI Repository Options')
    group.add_argument('--no-push-ci', dest='no_push_ci', action='store_true', help='don\'t push branch to ceph-ci repo (when making QA tickets)')
    group.add_argument('--push-ci', dest='push_ci', action='store_true', help='push branch and tag to CI repository (even when not making QA tickets)')

    group = parser.add_argument_group('Backport Verification')
    group.add_argument('--audit', dest='audit', action='store_true', help='run parity and conflict simulations')
    group.add_argument('--audit-label', dest='audit_label', type=parse_audit_labels, help='swap labels on success/failure. Format: "queue", "passed,failed", or "queue,passed,failed"')
    group.add_argument('--skip-conflict-check', dest='skip_conflict_check', action='store_true', help='skip conflict resolution simulation')
    group.add_argument('--ci-mode', dest='ci_mode', action='store_true', help='run non-interactively and post multiple separate reviews for failures')

    def parse_pr(value):
        m = re.search(r'/pull/(\d+)', value)
        if m:
            return int(m.group(1))
        try:
            return int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid PR format: {value}")

    group = parser.add_argument_group('PRs to Merge')
    group.add_argument('prs', metavar="PRs...", type=parse_pr, nargs='*', help='Pull Requests to Merge (numbers or URLs)')

    args = parser.parse_args(argv)

    # Make --audit-label redundant when --ci-mode is invoked
    if args.ci_mode and not args.audit_label:
        args.audit_label = parse_audit_labels("releng-audit-pass,releng-audit-fail")

    if args.examples:
        examples_text = textwrap.dedent("""
        Ceph PTL Tool - Advanced Examples
        =================================

        1. Integration Testing (The "Daily Driver"):
           Finds all PRs labeled 'wip-yourname-testing', auto-detects the base branch,
           merges them locally, creates a test branch with the correct release name,
           pushes it to ceph-ci, and creates a Redmine QA tracker ticket:
           $ ptl-tool.py --integration --pr-label wip-yourname-testing --create-qa

        2. Updating an Existing QA Run:
           If you already have a QA tracker ticket (e.g., #55555) and want to add or 
           remove PRs. Label the desired PRs on GitHub, then run:
           $ ptl-tool.py --integration --pr-label wip-yourname-testing --update-qa 55555

        3. Merging a Backport/Release PR:
           Merges a PR into a detached HEAD without creating a testing branch. 
           Useful for merging directly to a stable branch like 'quincy' or 'reef' locally
           before pushing upstream:
           $ ptl-tool.py --final-merge 123456
           $ git log # verify everything looks good
           $ git push upstream HEAD:main

        4. Dry-Run a Massive integration branch:
           Want to see what the tool *would* do without actually pushing branches,
           creating Redmine tickets, or leaving GitHub comments?
           $ ptl-tool.py --integration --pr-label wip-massive-test --create-qa --dry-run
        """)
        print(examples_text.strip())
        sys.exit(0)

    if args.audit_label and args.audit_label.queue:
        if args.pr_label:
            log.error("--audit-label with a queue label and --pr-label are mutually exclusive")
            sys.exit(1)
        args.pr_label = args.audit_label.queue

    if args.create_qa and args.update_qa:
        log.error("--create-qa and --update-qa are mutually exclusive switches")
        sys.exit(1)

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.debug_build:
        args.flavors.add('debug')

    if args.final_merge:
        args.branch = 'HEAD'

    if not GITHUB_TOKEN:
        log.error("Missing GitHub Personal Access Token.")
        log.error("Please create a file at ~/.github_token containing your token,")
        log.error("or set the PTL_TOOL_GITHUB_TOKEN environment variable.")
        sys.exit(1)

    if args.create_qa or args.update_qa or args.audit or args.final_merge:
        if Redmine is None:
            log.error("redmine library is not available so cannot create qa tracker ticket or audit")
            sys.exit(1)
        if not REDMINE_API_KEY:
            log.error("Missing Redmine API Key. Required for creating/updating QA tickets or auditing.")
            log.error("Please create a file at ~/.redmine_key containing your API key,")
            log.error("or set the PTL_TOOL_REDMINE_API_KEY environment variable.")
            sys.exit(1)

    return build_branch(args)

if __name__ == "__main__":
    main()
