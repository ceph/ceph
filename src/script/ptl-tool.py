#!/usr/bin/python3

# README:
#
# This tool's purpose is to make it easier to merge PRs into test branches and
# into main.
#
#
# == Getting Started ==
#
# You will probably want to setup a virtualenv for running this script:
#
#    (
#    virtualenv ~/ptl-venv
#    source ~/ptl-venv/bin/activate
#    pip3 install GitPython
#    pip3 install python-redmine
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
#  - PTL_TOOL_REDMINE_USER (your redmine username)
#  - PTL_TOOL_REDMINE_API_KEY (your redmine api key, or what is stored in ~/redmine_key)
#  - PTL_TOOL_USER (your desired username embedded in test branch names)
#
#
# You can use this tool to create a QA tracker ticket for you:
#
# $ python3 ptl-tool.py ... --create-qa --qa-release reef
#
# which will populate the ticket with all the usual information and also push a
# tagged version of your test branch to ceph-ci for posterity.

#
# ** Here are some basic exmples to get started: **
#
# Merging all PRs labeled 'wip-pdonnell-testing' into a new test branch:
#
# $ src/script/ptl-tool.py --pr-label wip-pdonnell-testing
# Adding labeled PR #18805 to PR list
# Adding labeled PR #18774 to PR list
# Adding labeled PR #18600 to PR list
# Will merge PRs: [18805, 18774, 18600]
# Detaching HEAD onto base: main
# Merging PR #18805
# Merging PR #18774
# Merging PR #18600
# Checked out new branch wip-pdonnell-testing-20171108.054517
# Created tag testing/wip-pdonnell-testing-20171108.054517
#
#
# Merging all PRs labeled 'wip-pdonnell-testing' into main:
#
# $ src/script/ptl-tool.py --pr-label wip-pdonnell-testing --branch main
# Adding labeled PR #18805 to PR list
# Adding labeled PR #18774 to PR list
# Adding labeled PR #18600 to PR list
# Will merge PRs: [18805, 18774, 18600]
# Detaching HEAD onto base: main
# Merging PR #18805
# Merging PR #18774
# Merging PR #18600
# Checked out branch main
#
# Now push to main:
# $ git push upstream main
# ...
#
#
# Merging all PRs labeled 'wip-pdonnell-testing' into a new test branch but
# NOT pushing that branch to ceph-ci repo (pushing to ceph-ci repo usually
# happens only when we use --create-qa or --update-qa):
#
# $ src/script/ptl-tool.py --pr-label wip-pdonnell-testing --branch main --no-push-ci
# Adding labeled PR #18805 to PR list
# Adding labeled PR #18774 to PR list
# Adding labeled PR #18600 to PR list
# Will merge PRs: [18805, 18774, 18600]
# Detaching HEAD onto base: main
# Merging PR #18805
# Merging PR #18774
# Merging PR #18600
# Checked out new branch wip-pdonnell-testing-20171108.054517
# Created tag testing/wip-pdonnell-testing-20171108.054517
#
#
# Merging PR #1234567 and #2345678 into a new test branch with a testing label added to the PR:
#
# $ src/script/ptl-tool.py 1234567 2345678 --label wip-pdonnell-testing
# Detaching HEAD onto base: main
# Merging PR #1234567
# Labeled PR #1234567 wip-pdonnell-testing
# Merging PR #2345678
# Labeled PR #2345678 wip-pdonnell-testing
# Deleted old test branch wip-pdonnell-testing-20170928
# Created branch wip-pdonnell-testing-20170928
# Created tag testing/wip-pdonnell-testing-20170928_03
#
#
# Merging PR #1234567 into main leaving a detached HEAD (i.e. do not update your repo's main branch) and do not label:
#
# $ src/script/ptl-tool.py --branch HEAD --merge-branch-name main 1234567
# Detaching HEAD onto base: main
# Merging PR #1234567
# Leaving HEAD detached; no branch anchors your commits
#
# Now push to main:
# $ git push upstream HEAD:main
#
#
# Merging PR #12345678 into luminous leaving a detached HEAD (i.e. do not update your repo's main branch) and do not label:
#
# $ src/script/ptl-tool.py --base luminous --branch HEAD --merge-branch-name luminous 12345678
# Detaching HEAD onto base: luminous
# Merging PR #12345678
# Leaving HEAD detached; no branch anchors your commits
#
# Now push to luminous:
# $ git push upstream HEAD:luminous
#
#
# Merging all PRs labelled 'wip-pdonnell-testing' into main leaving a detached HEAD:
#
# $ src/script/ptl-tool.py --base main --branch HEAD --merge-branch-name main --pr-label wip-pdonnell-testing
# Adding labeled PR #18192 to PR list
# Will merge PRs: [18192]
# Detaching HEAD onto base: main
# Merging PR #18192
# Leaving HEAD detached; no branch anchors your commit

# TODO
# Look for check failures?

import argparse
import datetime
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
    from redminelib import Redmine  # https://pypi.org/project/python-redmine/
except ImportError:
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
REDMINE_CUSTOM_FIELD_ID_SHAMAN_BUILD = 26
REDMINE_CUSTOM_FIELD_ID_QA_RUNS = 27
REDMINE_CUSTOM_FIELD_ID_QA_RELEASE = 28
REDMINE_CUSTOM_FIELD_ID_QA_TAGS = 3
REDMINE_CUSTOM_FIELD_ID_GIT_BRANCH = 29
REDMINE_ENDPOINT = "https://tracker.ceph.com"
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

SANDBOX_CFG = ['rerere.enabled=false', 'commit.gpgSign=false', 'core.hooksPath=/dev/null']

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

def gitauth():
    class GitHubBearerAuth(requests.auth.AuthBase):
        def __call__(self, r):
            if GITHUB_TOKEN:
                r.headers['Authorization'] = f'Bearer {GITHUB_TOKEN}'
            r.headers['Accept'] = 'application/vnd.github.v3+json'
            return r
    return GitHubBearerAuth()

def get(session, url, params=None, paging=True):
    if params is None:
        params = {}
    params['per_page'] = 100

    log.debug(f"Fetching {url}")
    response = session.get(url, auth=gitauth(), params=params)
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
            response = session.get(url, auth=gitauth(), params=new_params)
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
    if not always_fetch:
        try:
            c = G.commit(ref)
            log.debug(f"Resolved {ref} locally to {c.hexsha[:8]}.")
            return c
        except (git.exc.BadName, ValueError):
            pass
        
        # Check typical remote prefix fallbacks for branches
        if not re.match(r'^[0-9a-f]{40}$', ref) and not ref.startswith('refs/'):
            for local_alias in [f"upstream/{ref}", f"origin/{ref}"]:
                try:
                    c = G.commit(local_alias)
                    log.debug(f"Resolved {ref} locally via {local_alias} to {c.hexsha[:8]}.")
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

def post_draft_review(session, pr, initial_text, base=None):
    """
    Opens an editor with the draft text, previews it, and prompts the user to
    post it as a REQUEST_CHANGES review.
    """
    if base:
        rfa_msg = f"\n\nWhen you are ready for this to be reviewed and QA'd again, please add the `pdonnell-{base}-RFA` label to this PR."
        if rfa_msg not in initial_text:
            initial_text += rfa_msg
            
    editor = os.environ.get('EDITOR', 'vim')
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.md', delete=False) as tf:
        tf.write(initial_text)
        tf_path = tf.name
        
    try:
        while True:
            subprocess.run([editor, tf_path])
            with open(tf_path, 'r', encoding='utf-8') as f_read:
                final_text = f_read.read().strip()
                
            print("\n" + "="*80)
            print("DRAFT REVIEW PREVIEW (REQUEST_CHANGES):")
            print("-" * 80)
            print(final_text)
            print("="*80 + "\n")
            
            confirm = input(f"Post this review requesting changes to PR #{pr}? [y/e/N] (y=post, e=edit again, n=cancel): ").strip().lower()
            if confirm == 'y':
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}/reviews"
                r = session.post(endpoint, auth=gitauth(), json={'body': final_text, 'event': 'REQUEST_CHANGES'})
                if r.status_code in (200, 201):
                    log.info(f"Successfully posted review to PR #{pr}")
                    return True
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

def verify_commit_parity(G, session, pr, pr_commits, base):
    """
    Analyzes the local Git DAG to ensure all commits from the original main PRs 
    are present in the backport PR.
    """
    log.info("Verifying commit parity with original PR(s) locally...")
    bp_cherry_picks = []
    invalid_format_commits = []
    cp_regex = re.compile(r"\(cherry picked from commit ([0-9a-f]{40})\)")
    for commit in pr_commits:
        m = cp_regex.search(commit.message)
        is_cherry_pick = bool(m)
        if not is_cherry_pick and not commit.summary.startswith(f"{base}:"):
            invalid_format_commits.append(commit)

        if is_cherry_pick:
            bp_cherry_picks.append((commit, m.group(1)))
    
    missing_commits = []
    found_prs = set()
    bp_orig_shas = set(orig_sha for _, orig_sha in bp_cherry_picks)
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
                        bp_match = next((c for c, o_sha in bp_cherry_picks if o_sha == o_commit_sha), None)
                        
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
        log.info(f"Original PRs identified in this backport: {', '.join(found_prs)}")
        if len(found_prs) > 1:
            print("\033[91m" + "="*80)
            print("WARNING: Multiple original PRs detected in this backport!")
            print("Normally we expect exactly one main PR per backport.")
            print("Detected: " + ", ".join(found_prs))
            print("="*80 + "\033[0m")

    # Print Visualizer UI
    visualizer_text = ""
    visualizer_lines = []
    if found_prs or pr_commits:
        visualizer_lines.append("=" * 80)
        visualizer_lines.append("COMMIT PARITY VISUALIZER")
        visualizer_lines.append("=" * 80)
        
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
        
        # Print all backport commits exactly in chronological order
        current_pr = None
        for bp_c in pr_commits:
            if bp_c.hexsha in bp_to_source:
                pr_name, item = bp_to_source[bp_c.hexsha]
                
                # If PR block changes, flush the missing commits for the previous block
                if current_pr is not None and pr_name != current_pr:
                    if current_pr in unprinted_missing:
                        for m_sha, m_summary in unprinted_missing[current_pr]:
                            prefix = " " * (len(current_pr) + 1)
                            visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                        del unprinted_missing[current_pr]
                    visualizer_lines.append("") # Add visual grouping space between different PRs
                    
                prefix = f"{pr_name} " if pr_name != current_pr else " " * (len(pr_name) + 1)
                current_pr = pr_name
                visualizer_lines.append(format_parity_row(bp_c.hexsha, bp_c.summary, item['o_sha'], item['o_summary'], right_prefix=prefix))
            else:
                if current_pr is not None:
                    if current_pr in unprinted_missing:
                        for m_sha, m_summary in unprinted_missing[current_pr]:
                            prefix = " " * (len(current_pr) + 1)
                            visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                        del unprinted_missing[current_pr]
                    visualizer_lines.append("")
                current_pr = None # Reset so the next PR prints its name
                visualizer_lines.append(format_parity_row(bp_c.hexsha, bp_c.summary, None, None, is_extra=True))
        
        # Flush any remaining missing commits for the last processed PR block
        if current_pr is not None and current_pr in unprinted_missing:
            for m_sha, m_summary in unprinted_missing[current_pr]:
                prefix = " " * (len(current_pr) + 1)
                visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
            del unprinted_missing[current_pr]
            
        # Flush any missing commits for PRs that had NO commits successfully mapped
        for pr_name, missing_list in unprinted_missing.items():
            visualizer_lines.append("")
            first = True
            for m_sha, m_summary in missing_list:
                prefix = f"{pr_name} " if first else " " * (len(pr_name) + 1)
                visualizer_lines.append(format_parity_row(None, None, m_sha, m_summary, is_missing=True, right_prefix=prefix))
                first = False

        visualizer_lines.append("=" * 80)
        
        visualizer_text = "\n".join(visualizer_lines)
        print(visualizer_text)

    for commit in invalid_format_commits:
        log.error(f"Commit {commit.hexsha[:8]} invalid format. Must be cherry-pick or start with '{base}:'")
        ans = input("Do you want to allow this commit anyway? [y/N] ")
        if ans.lower() != 'y':
            sys.exit(1)

    visualizer_clean = re.sub(r'\033\[[0-9;]*m', '', visualizer_text) if visualizer_text else ""
    if missing_commits:
        # Generate markdown comment draft
        md_text = f"**Automated Backport Parity Review**\n\n"
        if len(found_prs) > 1:
            md_text += f":warning: **Multiple Original PRs Detected:** {', '.join(found_prs)}\n\n"
        md_text += "Discrepancies detected between the backport PR and the original source PR(s). Please review the mapping below:\n\n"
        md_text += f"```text\n{visualizer_clean}\n```"

        while True:
            ans = input("Parity mismatch! [p]roceed, [o]pen browser to investigate, [r]eview PR (request changes), [q]uit: ").strip().lower()
            if ans == 'p':
                break
            elif ans == 'q':
                log.error("Rejecting PR due to incomplete backport.")
                sys.exit(1)
            elif ans == 'r':
                if post_draft_review(session, pr, md_text, base=base):
                    log.error("Rejecting PR due to incomplete backport after posting review.")
                    sys.exit(1)
            elif ans == 'o':
                first_url = True
                for pr_name, o_sha, o_summary, m_sha in missing_commits:
                    urls_to_open = []
                    m_pr = re.search(r'#(\d+)', pr_name)
                    if m_pr:
                        urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{m_pr.group(1)}")
                    urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{m_sha}")
                    urls_to_open.append(f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{o_sha}")
                    
                    for url in urls_to_open:
                        if first_url:
                            webbrowser.open_new(url)
                            first_url = False
                        else:
                            webbrowser.open_new_tab(url)
                print("Opened URLs in a new browser window.")
            else:
                print("Invalid choice. Please enter p, o, r, or q.")
    elif analyzed_merges:
        print("\033[92mCommit parity check passed! All upstream commits from identified PRs are present.\033[0m")
        
        if len(found_prs) > 1:
            while True:
                ans = input("Multiple original PRs detected! Do you want to post a review requesting documentation? [y/N/o] (y=yes, n=no, o=open PRs in browser): ").strip().lower()
                if ans == 'o':
                    url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                    webbrowser.open_new(url)
                    print(f"Opened {url} in browser.")
                    for pr_str in found_prs:
                        m_pr = re.search(r'#(\d+)', pr_str)
                        if m_pr:
                            orig_url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{m_pr.group(1)}"
                            webbrowser.open_new_tab(orig_url)
                            print(f"Opened {orig_url} in browser.")
                elif ans == 'y':
                    md_text = f"**Automated Backport Parity Review - Multiple PRs Detected**\n\n"
                    md_text += f"This backport appears to pull commits from multiple `main` PRs including: {', '.join(found_prs)}.\n\n"
                    md_text += "This must be explicitly documented in the backport PR description. Furthermore, each backport tracker ticket associated with these `main` PRs must be linked to this PR.\n\n"
                    md_text += f"**Commit Parity Visualizer:**\n```text\n{visualizer_clean}\n```\n"
                    if post_draft_review(session, pr, md_text, base=base):
                        log.error("Rejecting PR pending documentation of multiple original PRs.")
                        sys.exit(1)
                    break
                else:
                    break

    return visualizer_text

def simulate_conflict_resolution(G, session, pr, pr_commits, base, always_fetch, visualizer_text):
    """
    Creates a temporary worktree to dry-run the cherry-pick sequence, verifying
    conflict resolutions dynamically.
    """
    wt_dir = tempfile.mkdtemp(prefix="ptl-worktree-")
    try:
        G.git.worktree('add', '--detach', wt_dir, base)
        wt_repo = git.Repo(wt_dir)
        
        cp_regex = re.compile(r"\(cherry picked from commit ([0-9a-f]{40})\)")
        auto_approve_conflicts = False
        first_conflict = True

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
                    sys.exit(1)
                    
                backport_tree = wt_repo.head.commit.tree.hexsha

                is_sneaky = (not has_conflict) and (clean_tree != backport_tree)

                if has_conflict or is_sneaky:
                    if auto_approve_conflicts:
                        log.info(f"Skipping approval and taking backport commit for {orig_sha[:8]}.")
                        continue

                    if first_conflict:
                        ans = input(f"Conflict or unapproved deviation detected in {c.hexsha[:8]}. Do you want to interactively review this PR? [Y/n/m]\n(y = yes, n = auto-approve remaining, m = skip to merge) ")
                        first_conflict = False
                        if ans.lower() == 'm':
                            log.info("Skipping ahead to merge.")
                            return
                        elif ans.lower() == 'n':
                            log.info("Auto-approving and skipping interactive checks for this PR.")
                            auto_approve_conflicts = True
                            continue

                    editor = os.environ.get('EDITOR', 'vim')
                    if has_conflict:
                        log.warning(f"Opening {editor} to inspect conflict in {c.hexsha[:8]} ...")
                    else:
                        log.warning(f"Opening {editor} to inspect unexplained deviation in clean cherry-pick {c.hexsha[:8]} ...")
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
                                
                                subprocess.run(cmd, pass_fds=pass_fds)
                                
                                for t in threads:
                                    t.join()
                                for fd in fds_to_close:
                                    os.close(fd)
                            except git.exc.GitCommandError as e:
                                log.warning(f"Could not generate patch comparison for {f}: {e}")
                    
                    prompt_text = "Does the PR properly explain and resolve this conflict/deviation? [y/N/s/m]\n(y = next check, n = abort, s = skip remaining checks, m = skip to merge) "
                    ans = input(prompt_text)
                    if ans.lower() == 'm':
                        log.info("Skipping ahead to merge.")
                        return
                    elif ans.lower() == 's':
                        log.info("Skipping remaining checks for this PR.")
                        auto_approve_conflicts = True
                    elif ans.lower() == 'y':
                        pass # Already applied backport commit
                    else:
                        log.error("Rejecting PR due to undocumented/unapproved change. Preparing draft review...")
                        visualizer_clean = re.sub(r'\033\[[0-9;]*m', '', visualizer_text) if visualizer_text else ""
                        diff_text = diff if 'diff' in locals() else "No range diff available."
                        
                        md_text = "**Automated Backport Parity Review - Backport Deviation Alert**\n\n"
                        md_text += "A conflict or unapproved deviation was detected during the simulation of this backport, and the changes appear to be incorrect or undocumented. Please review the highlighted files.\n\n"
                        md_text += "**Affected File(s):**\n"
                        for f_name in unmerged:
                            f_hash = hashlib.sha256(f_name.encode('utf-8')).hexdigest()
                            md_text += f"* `{f_name}`\n"
                            md_text += f"  * Original: https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{c.hexsha}#diff-{f_hash}\n"
                            md_text += f"  * Backport: https://github.com/{BASE_PROJECT}/{BASE_REPO}/commit/{commit.hexsha}#diff-{f_hash}\n"
                        
                        if visualizer_clean:
                            md_text += f"\n**Commit Parity Visualizer:**\n```text\n{visualizer_clean}\n```\n\n"
                            
                        md_text += f"**Range Diff:**\n<details><summary>Click to expand</summary>\n\n```diff\n{diff_text}\n```\n</details>\n\n"
                        
                        post_draft_review(session, pr, md_text)
                        sys.exit(1)
            else:
                log.info(f"Applying branch-specific commit {commit.hexsha[:8]} ...")
                wt_repo.git(c=SANDBOX_CFG).cherry_pick("--allow-empty", commit.hexsha)
        log.info("Verification of backport cherry-pick for PR #%d is complete.", pr)
    finally:
        log.info("Removing temporary worktree `%s'", wt_dir)
        G.git.worktree('remove', '--force', wt_dir)

def build_branch(args):
    base = args.base
    branch = datetime.datetime.utcnow().strftime(args.branch).format(user=USER)
    if args.branch_release:
        branch = branch + "-" + args.branch_release
    if args.branch_append:
        branch += f"-{args.branch_append}"
    label = args.label
    merge_branch_name = args.merge_branch_name
    if merge_branch_name is False:
        merge_branch_name = branch

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

    if args.create_qa or args.update_qa:
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

    # PRE-FLIGHT: Auto-detect base from the first PR if necessary and cache responses
    pr_responses = {}
    for pr in prs:
        log.info("Fetching information for PR #%d", pr)
        endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}"
        pr_responses[pr] = next(get(session, endpoint, paging=False))
        
    if prs and base is None:
        for pr in prs:
            detected_base = pr_responses[pr].get("base", {}).get("ref")
            log.debug("Detected base for PR #%d as `%s'", pr, detected_base)
            if detected_base:
                log.info(f"Auto-detected target base from PR #{pr}: {detected_base}")
                base = detected_base
                if args.merge_branch_name is False:
                    merge_branch_name = detected_base
            else:
                if base != detected_base:
                    raise SystemExit("base of each PR is not equal, use hard-coded --base")

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

    for pr in prs:
        pr = int(pr)
        log.info("Merging PR #{pr}".format(pr=pr))

        remote_ref = "refs/pull/{pr}/head".format(pr=pr)
        remote_sha1 = pr_responses[pr].get('head', {}).get('sha')
        
        ref_to_fetch = remote_sha1 if remote_sha1 else remote_ref
        tip = resolve_ref(G, ref_to_fetch, BASE_REMOTE_URL, args.always_fetch)
        log.info("Now have head for PR #%d: %s", pr, str(tip))

        response = pr_responses[pr]

        log.info("Performing trivial merge check for PR #%d...", pr)
        wt_dir = tempfile.mkdtemp(prefix="ptl-merge-check-")
        has_base_conflicts = False
        try:
            G.git.worktree('add', '--detach', wt_dir, G.head.commit)
            wt_repo = git.Repo(wt_dir)
            try:
                wt_repo.git(c=SANDBOX_CFG).merge(tip.hexsha, '--no-commit', '--no-ff')
            except git.exc.GitCommandError:
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
            while True:
                ans = input(f"PR #{pr} needs a rebase! Do you want to post a review requesting a rebase? [y/N/o] (y=yes, n=abort script, o=open PR in browser): ").strip().lower()
                if ans == 'o':
                    url = f"https://github.com/{BASE_PROJECT}/{BASE_REPO}/pull/{pr}"
                    webbrowser.open_new(url)
                    print(f"Opened {url} in browser.")
                elif ans == 'y':
                    md_text = "**Automated PR Review - Rebase Required**\n\n"
                    md_text += f"This PR currently has merge conflicts with the target base branch. Please rebase and resolve the conflicts."
                    if post_draft_review(session, pr, md_text):
                        log.error("Rejecting PR pending rebase.")
                        sys.exit(1)
                elif ans == 'n' or ans == '':
                    log.error(f"Aborting script due to unmergeable PR #{pr}.")
                    sys.exit(1)

        qa_tracker_description.append(f'* "PR #{pr}":{response["html_url"]} -- {response["title"].strip()}')

        message = "Merge PR #%d into %s\n\n* %s:\n" % (pr, merge_branch_name, remote_ref)

        pr_commits = list(G.iter_commits(rev="HEAD.."+str(tip)))
        pr_commits.reverse() # chronological order for simulation

        if base != 'main':
            visualizer_text = verify_commit_parity(G, session, pr, pr_commits, base)
            if not args.skip_conflict_check:
                simulate_conflict_resolution(G, session, pr, pr_commits, base, args.always_fetch, visualizer_text)

        if args.audit:
            log.info(f"Audit of PR #{pr} complete. Skipping merge.")
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
            req = session.post("https://api.github.com/repos/{project}/{repo}/issues/{pr}/labels".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), data=json.dumps([label]), auth=gitauth())
            if req.status_code != 200:
                log.error("PR #%d could not be labeled %s: %s" % (pr, label, req))
                sys.exit(1)
            log.info("Labeled PR #{pr} {label}".format(pr=pr, label=label))

    if args.audit:
        log.info("Audit complete. Exiting without modifying branches or Redmine.")
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
    if branch == 'HEAD':
        log.info("Leaving HEAD detached; no branch anchors your commits")
    else:
        created_branch = False
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
            tag = git.refs.tag.Tag.create(G, tag_name)
            log.info("Created tag %s" % tag)

    do_qa = args.create_qa or args.update_qa
    if args.push_ci or (not args.no_push_ci and do_qa):
        G.git.push(CI_REMOTE_URL, branch) # for shaman
        if created_branch and not args.no_tag:
            G.git.push(CI_REMOTE_URL, tag.name) # for archival

    if args.create_qa or args.update_qa:
        if not created_branch:
            log.error("branch already exists!")
            sys.exit(1)
        project = R.project.get(REDMINE_PROJECT_QA)
        log.debug("got redmine project %s", project)
        user = R.user.get('current')
        log.debug("got redmine user %s", user)
        for tracker in project.trackers:
            if tracker['name'] == REDMINE_TRACKER_QA:
                tracker = tracker
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

        if not args.no_tag:
            origin_url = f'{BASE_PROJECT}/{CI_REPO}/commits/{tag.name}'
        else:
            origin_url = f'{BASE_PROJECT}/{CI_REPO}/commits/{branch}'
        custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_GIT_BRANCH, 'value': origin_url})

        issue_kwargs = {
          "assigned_to_id": user['id'],
          "custom_fields": custom_fields,
          "description": '\n'.join(qa_tracker_description),
          "project_id": project['id'],
          "subject": branch,
          "watcher_user_ids": user['id'],
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

            log.debug("updating issue with kwargs: %s", issue_kwargs)
            notes = f"""
            Updating branch to {branch}.
            """
            if R.issue.update(issue.id, **issue_kwargs):
                log.info("updated redmine qa issue: %s", issue.url)
            else:
                log.error(f"failed to update {issue}")
                sys.exit(1)
        elif args.create_qa:
            log.debug("creating issue with kwargs: %s", issue_kwargs)
            issue = R.issue.create(**issue_kwargs)
            log.info("created redmine qa issue: %s", issue.url)

            for pr in prs:
                log.debug(f"Posting QA Run in comment for ={pr}")
                endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/issues/{pr}/comments"
                body = f"This PR is under test in [{issue.url}]({issue.url})."
                r = session.post(endpoint, auth=gitauth(), data=json.dumps({'body':body}))
                if r.status_code == 201:
                    log.info(f"Successfully posted comment to PR #{pr}")
                else:
                    log.error(f"Failed to post comment: {r.status_code} {r.text}")

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
    parser = argparse.ArgumentParser(description="Ceph PTL tool")
    default_branch = TEST_BRANCH
    default_label = ''
    argv = sys.argv[1:]

    group = parser.add_argument_group('General Options')
    group.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    group.add_argument('--git-dir', dest='git', action='store', default=git_dir, help='git directory')

    group = parser.add_argument_group('GitHub PR Options')
    group.add_argument('--label', dest='label', action='store', default=default_label, help='label PRs for testing')
    group.add_argument('--pr-label', dest='pr_label', action='store', help='source PRs to merge via label')

    group = parser.add_argument_group('Branch Control Options')
    group.add_argument('--always-fetch', dest='always_fetch', action='store_true', help='always fetch commits from remote (bypass local cache)')
    group.add_argument('--base', dest='base', action='store', help='base for branch')
    group.add_argument('--branch', dest='branch', action='store', default=default_branch, help='branch to create ("HEAD" leaves HEAD detached; i.e. no branch is made)')
    group.add_argument('--release-merge', dest='release_merge', action='store_true', help='enable release merge behavior (implies --branch HEAD)')
    group.add_argument('--branch-name-append', dest='branch_append', action='store', help='append string to branch name')
    group.add_argument('--branch-release', dest='branch_release', action='store', help='release name to embed in branch (for shaman)')
    group.add_argument('--merge-branch-name', dest='merge_branch_name', action='store', default=False, help='name of the branch for merge messages')
    group.add_argument('--no-credits', dest='credits', action='store_false', help='skip indication search (Reviewed-by, etc.)')
    group.add_argument('--no-tag', dest='no_tag', action='store_true', help='do not create a tag of the branch')
    group.add_argument('--stop-at-built', dest='stop_at_built', action='store_true', help='stop execution when branch is built')

    group = parser.add_argument_group('Build Control Options')
    group.add_argument('--archs', dest='archs', action=SplitCommaAppendAction, default=[], help='add arch(s) to build. Specify one or more times. Comma separated values are split.')
    group.add_argument('--build-job', dest='build_job', action='store', help='add ceph build job to execute in CI')
    group.add_argument('--debug-build', dest='debug_build', action='store_true', help='add \'debug\' flavor (build with CMAKE_BUILD_TYPE=Debug)')
    group.add_argument('--distros', dest='distros', action=SplitCommaAppendAction, default=set(), help='add distro(s) to build. Specify one or more times. Comma separated values are split.')
    group.add_argument('--flavors', dest='flavors', action=SplitCommaAppendAction, default=set(), help='add flavors(s) to build. Specify one or more times. Comma separated values are split.')

    group = parser.add_argument_group('QA Control Options')
    group.add_argument('--create-qa', dest='create_qa', action='store_true', help='create QA run ticket')
    group.add_argument('--qa-private', dest='qa_private', action='store_true', help='make the QA run ticket private')
    group.add_argument('--qa-release', dest='qa_release', action='store', help='QA release for tracker')
    group.add_argument('--qa-tags', dest='qa_tags', action='store', help='QA tags for tracker')
    group.add_argument('--update-qa', dest='update_qa', action='store', help='update QA run ticket')

    group = parser.add_argument_group('CI Repository Options')
    group.add_argument('--no-push-ci', dest='no_push_ci', action='store_true', help='don\'t push branch to ceph-ci repo (when making QA tickets)')
    group.add_argument('--push-ci', dest='push_ci', action='store_true', help='push branch and tag to CI repository (even when not making QA tickets)')

    group = parser.add_argument_group('Backport Verification')
    group.add_argument('--audit', dest='audit', action='store_true', help='run parity and conflict simulations without merging or modifying branches')
    group.add_argument('--skip-conflict-check', dest='skip_conflict_check', action='store_true', help='skip conflict resolution simulation')

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

    if args.create_qa and args.update_qa:
        log.error("--create-qa and --update-qa are mutually exclusive switches")
        sys.exit(1)

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.debug_build:
        args.flavors.add('debug')

    if args.release_merge:
        args.branch = 'HEAD'

    if not GITHUB_TOKEN:
        log.error("Missing GitHub Personal Access Token.")
        log.error("Please create a file at ~/.github_token containing your token,")
        log.error("or set the PTL_TOOL_GITHUB_TOKEN environment variable.")
        sys.exit(1)

    if args.create_qa or args.update_qa:
        if Redmine is None:
            log.error("redmine library is not available so cannot create qa tracker ticket")
            sys.exit(1)
        if not REDMINE_API_KEY:
            log.error("Missing Redmine API Key. Required for creating/updating QA tickets.")
            log.error("Please create a file at ~/.redmine_key containing your API key,")
            log.error("or set the PTL_TOOL_REDMINE_API_KEY environment variable.")
            sys.exit(1)

    return build_branch(args)

if __name__ == "__main__":
    main()
