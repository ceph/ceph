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
#  ~/.redmine_key -- Your redmine API key from right side of: https://tracker.ceph.com/my/account
#
#  ~/.github.key -- Your github API key: https://github.com/settings/tokens
#
# Some important environment variables:
#
#  - PTL_TOOL_GITHUB_USER (your github username)
#  - PTL_TOOL_GITHUB_API_KEY (your github api key, or what is stored in ~/.github.key)
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
import codecs
import datetime
from getpass import getuser
import git # https://github.com/gitpython-developers/gitpython
import itertools
import json
import logging
import os
import re
try:
    from redminelib import Redmine  # https://pypi.org/project/python-redmine/
except ModuleNotFoundError:
    Redmine = None
import requests
import signal
import sys

from os.path import expanduser

BASE_PROJECT = os.getenv("PTL_TOOL_BASE_PROJECT", "ceph")
BASE_REPO = os.getenv("PTL_TOOL_BASE_REPO", "ceph")
BASE_REMOTE_URL = os.getenv("PTL_TOOL_BASE_REMOTE_URL", f"https://github.com/{BASE_PROJECT}/{BASE_REPO}.git")
CI_REPO = os.getenv("PTL_TOOL_CI_REPO", "ceph-ci")
CI_REMOTE_URL = os.getenv("PTL_TOOL_CI_REMOTE_URL", f"git@github.com:{BASE_PROJECT}/{CI_REPO}.git")
GITDIR = os.getenv("PTL_TOOL_GITDIR", ".")
GITHUB_USER = os.getenv("PTL_TOOL_GITHUB_USER", os.getenv("PTL_TOOL_USER", getuser()))
GITHUB_API_KEY = None
try:
    with open(expanduser("~/.github.key")) as f:
        GITHUB_API_KEY = f.read().strip()
except FileNotFoundError:
    pass
GITHUB_API_KEY = os.getenv("PTL_TOOL_GITHUB_API_KEY", GITHUB_API_KEY)
INDICATIONS = [
    re.compile("(Reviewed-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Acked-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Tested-by: .+ <[\w@.-]+>)", re.IGNORECASE),
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

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

# find containing git dir
git_dir = GITDIR
max_levels = 6
while not os.path.exists(git_dir + '/.git'):
    git_dir += '/..'
    max_levels -= 1
    if max_levels < 0:
        break

CONTRIBUTORS = {}
NEW_CONTRIBUTORS = {}
with codecs.open(git_dir + "/.githubmap", encoding='utf-8') as f:
    comment = re.compile("\s*#")
    patt = re.compile("([\w-]+)\s+(.*)")
    for line in f:
        if comment.match(line):
            continue
        m = patt.match(line)
        CONTRIBUTORS[m.group(1)] = m.group(2)

BZ_MATCH = re.compile("(.*https?://bugzilla.redhat.com/.*)")
TRACKER_MATCH = re.compile("(.*https?://tracker.ceph.com/.*)")

def gitauth():
    return (GITHUB_USER, GITHUB_API_KEY)

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

def build_branch(args):
    base = args.base
    branch = datetime.datetime.utcnow().strftime(args.branch).format(user=USER)
    if args.branch_release:
        branch = branch + "-" + args.branch_release
    if args.debug_build:
        branch = branch + "-debug"
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

    if base == 'HEAD':
        log.info("Branch base is HEAD; not checking out!")
    else:
        log.info("Detaching HEAD onto base: {}".format(base))
        try:
            G.git.fetch(BASE_REMOTE_URL, base)
            # So we know that we're not on an old test branch, detach HEAD onto ref:
            c = G.commit('FETCH_HEAD')
        except git.exc.GitCommandError:
            log.debug("could not fetch %s from %s", base, BASE_REMOTE_URL)
            log.info(f"Trying to checkout uninterpreted base {base}")
            c = G.commit(base)
        G.git.checkout(c)
        assert G.head.is_detached

    qa_tracker_description = []

    for pr in prs:
        pr = int(pr)
        log.info("Merging PR #{pr}".format(pr=pr))

        remote_ref = "refs/pull/{pr}/head".format(pr=pr)
        try:
            G.git.fetch(BASE_REMOTE_URL, remote_ref)
        except git.exc.GitCommandError:
            log.error("could not fetch %s from %s", remote_ref, BASE_REMOTE_URL)
            sys.exit(1)
        tip = G.commit("FETCH_HEAD")

        endpoint = f"https://api.github.com/repos/{BASE_PROJECT}/{BASE_REPO}/pulls/{pr}"
        response = next(get(session, endpoint, paging=False))

        qa_tracker_description.append(f'* "PR #{pr}":{response["html_url"]} -- {response["title"].strip()}')

        message = "Merge PR #%d into %s\n\n* %s:\n" % (pr, merge_branch_name, remote_ref)

        for commit in G.iter_commits(rev="HEAD.."+str(tip)):
            message = message + ("\t%s\n" % commit.message.split('\n', 1)[0])
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

        if created_branch:
            # tag it for future reference.
            tag_name = "testing/%s" % branch
            tag = git.refs.tag.Tag.create(G, tag_name)
            log.info("Created tag %s" % tag)

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

        if not args.no_push_ci:
            G.git.push(CI_REMOTE_URL, branch) # for shaman
            G.git.push(CI_REMOTE_URL, tag.name) # for archival
        origin_url = f'{BASE_PROJECT}/{CI_REPO}/commits/{tag.name}'
        custom_fields.append({'id': REDMINE_CUSTOM_FIELD_ID_GIT_BRANCH, 'value': origin_url})

        issue_kwargs = {
          "assigned_to_id": user['id'],
          "custom_fields": custom_fields,
          "description": '\n'.join(qa_tracker_description),
          "project_id": project['id'],
          "subject": branch,
          "watcher_user_ids": user['id'],
        }

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
                log.debug(f"= {r}")

def main():
    parser = argparse.ArgumentParser(description="Ceph PTL tool")
    default_base = 'main'
    default_branch = TEST_BRANCH
    default_label = ''
    if len(sys.argv) > 1 and sys.argv[1] in SPECIAL_BRANCHES:
        argv = sys.argv[2:]
        default_branch = 'HEAD' # Leave HEAD detached
        default_base = default_branch
        default_label = False
    else:
        argv = sys.argv[1:]
    parser.add_argument('--base', dest='base', action='store', default=default_base, help='base for branch')
    parser.add_argument('--branch', dest='branch', action='store', default=default_branch, help='branch to create ("HEAD" leaves HEAD detached; i.e. no branch is made)')
    parser.add_argument('--branch-release', dest='branch_release', action='store', help='release name to embed in branch (for shaman)')
    parser.add_argument('--create-qa', dest='create_qa', action='store_true', help='create QA run ticket')
    parser.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    parser.add_argument('--debug-build', dest='debug_build', action='store_true', help='append -debug to branch name prompting ceph-build to build with CMAKE_BUILD_TYPE=Debug')
    parser.add_argument('--git-dir', dest='git', action='store', default=git_dir, help='git directory')
    parser.add_argument('--label', dest='label', action='store', default=default_label, help='label PRs for testing')
    parser.add_argument('--merge-branch-name', dest='merge_branch_name', action='store', default=False, help='name of the branch for merge messages')
    parser.add_argument('--no-credits', dest='credits', action='store_false', help='skip indication search (Reviewed-by, etc.)')
    parser.add_argument('--pr-label', dest='pr_label', action='store', help='label PRs for testing')
    parser.add_argument('--qa-release', dest='qa_release', action='store', help='QA release for tracker')
    parser.add_argument('--qa-tags', dest='qa_tags', action='store', help='QA tags for tracker')
    parser.add_argument('--stop-at-built', dest='stop_at_built', action='store_true', help='stop execution when branch is built')
    parser.add_argument('--update-qa', dest='update_qa', action='store', help='update QA run ticket')
    parser.add_argument('--no-push-ci', dest='no_push_ci', action='store_true',
                        help='don\'t push branch to ceph-ci repo')
    parser.add_argument('prs', metavar="PR", type=int, nargs='*', help='Pull Requests to merge')
    args = parser.parse_args(argv)

    if args.create_qa and args.update_qa:
        log.error("--create-qa and --update-qa are mutually exclusive switches")
        sys.exit(1)

    if args.debug:
        log.setLevel(logging.DEBUG)

    if (args.create_qa or args.update_qa) and Redmine is None:
        log.error("redmine library is not available so cannot create qa tracker ticket")
        sys.exit(1)

    return build_branch(args)

if __name__ == "__main__":
    main()
