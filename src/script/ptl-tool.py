#!/usr/bin/python3

# README:
#
# This tool's purpose is to make it easier to merge PRs into test branches and
# into master. Make sure you generate a Personal access token in GitHub and
# add it your ~/.github.key.
#
# Because developers often have custom names for the ceph upstream remote
# (https://github.com/ceph/ceph.git), You will probably want to export the
# PTL_TOOL_BASE_PATH environment variable in your shell rc files before using
# this script:
#
#     export PTL_TOOL_BASE_PATH=refs/remotes/<remotename>/
#
# and PTL_TOOL_BASE_REMOTE as the name of your Ceph upstream remote (default: "upstream"):
#
#     export PTL_TOOL_BASE_REMOTE=<remotename>
#
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
# Detaching HEAD onto base: master
# Merging PR #18805
# Merging PR #18774
# Merging PR #18600
# Checked out new branch wip-pdonnell-testing-20171108.054517
# Created tag testing/wip-pdonnell-testing-20171108.054517
#
#
# Merging all PRs labeled 'wip-pdonnell-testing' into master:
#
# $ src/script/ptl-tool.py --pr-label wip-pdonnell-testing --branch master
# Adding labeled PR #18805 to PR list
# Adding labeled PR #18774 to PR list
# Adding labeled PR #18600 to PR list
# Will merge PRs: [18805, 18774, 18600]
# Detaching HEAD onto base: master
# Merging PR #18805
# Merging PR #18774
# Merging PR #18600
# Checked out branch master
#
# Now push to master:
# $ git push upstream master
# ...
#
#
# Merging PR #1234567 and #2345678 into a new test branch with a testing label added to the PR:
#
# $ src/script/ptl-tool.py 1234567 2345678 --label wip-pdonnell-testing
# Detaching HEAD onto base: master
# Merging PR #1234567
# Labeled PR #1234567 wip-pdonnell-testing
# Merging PR #2345678
# Labeled PR #2345678 wip-pdonnell-testing
# Deleted old test branch wip-pdonnell-testing-20170928
# Created branch wip-pdonnell-testing-20170928
# Created tag testing/wip-pdonnell-testing-20170928_03
#
#
# Merging PR #1234567 into master leaving a detached HEAD (i.e. do not update your repo's master branch) and do not label:
#
# $ src/script/ptl-tool.py --branch HEAD --merge-branch-name master 1234567
# Detaching HEAD onto base: master
# Merging PR #1234567
# Leaving HEAD detached; no branch anchors your commits
#
# Now push to master:
# $ git push upstream HEAD:master
#
#
# Merging PR #12345678 into luminous leaving a detached HEAD (i.e. do not update your repo's master branch) and do not label:
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
# Merging all PRs labelled 'wip-pdonnell-testing' into master leaving a detached HEAD:
#
# $ src/script/ptl-tool.py --base master --branch HEAD --merge-branch-name master --pr-label wip-pdonnell-testing
# Adding labeled PR #18192 to PR list
# Will merge PRs: [18192]
# Detaching HEAD onto base: master
# Merging PR #18192
# Leaving HEAD detached; no branch anchors your commit


# TODO
# Look for check failures?
# redmine issue update: http://www.redmine.org/projects/redmine/wiki/Rest_Issues

import argparse
import codecs
import datetime
import getpass
import git
import itertools
import json
import logging
import os
import re
import requests
import sys

from os.path import expanduser

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

BASE_PROJECT = os.getenv("PTL_TOOL_BASE_PROJECT", "ceph")
BASE_REPO = os.getenv("PTL_TOOL_BASE_REPO", "ceph")
BASE_REMOTE = os.getenv("PTL_TOOL_BASE_REMOTE", "upstream")
BASE_PATH = os.getenv("PTL_TOOL_BASE_PATH", "refs/remotes/upstream/")
GITDIR = os.getenv("PTL_TOOL_GITDIR", ".")
USER = os.getenv("PTL_TOOL_USER", getpass.getuser())
with open(expanduser("~/.github.key")) as f:
    PASSWORD = f.read().strip()
TEST_BRANCH = os.getenv("PTL_TOOL_TEST_BRANCH", "wip-{user}-testing-%Y%m%d.%H%M%S")

SPECIAL_BRANCHES = ('master', 'luminous', 'jewel', 'HEAD')

INDICATIONS = [
    re.compile("(Reviewed-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Acked-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Tested-by: .+ <[\w@.-]+>)", re.IGNORECASE),
]

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

def get_credits(pr, pr_req):
    comments = requests.get("https://api.github.com/repos/{project}/{repo}/issues/{pr}/comments".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), auth=(USER, PASSWORD))
    if comments.status_code != 200:
        log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
        sys.exit(1)

    reviews = requests.get("https://api.github.com/repos/{project}/{repo}/pulls/{pr}/reviews".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), auth=(USER, PASSWORD))
    if reviews.status_code != 200:
        log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
        sys.exit(1)

    review_comments = requests.get("https://api.github.com/repos/{project}/{repo}/pulls/{pr}/comments".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), auth=(USER, PASSWORD))
    if review_comments.status_code != 200:
        log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
        sys.exit(1)

    credits = set()
    for comment in [pr_req.json()]+comments.json()+reviews.json()+review_comments.json():
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
    for review in reviews.json():
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
    label = args.label
    merge_branch_name = args.merge_branch_name
    if merge_branch_name is False:
        merge_branch_name = branch

    if label:
        #Check the label format
        if re.search(r'\bwip-(.*?)-testing\b', label) is None:
            log.error("Unknown Label '{lblname}'. Label Format: wip-<name>-testing".format(lblname=label))
            sys.exit(1)

        #Check if the Label exist in the repo
        res = requests.get("https://api.github.com/repos/{project}/{repo}/labels/{lblname}".format(lblname=label, project=BASE_PROJECT, repo=BASE_REPO), auth=(USER, PASSWORD))
        if res.status_code != 200:
            log.error("Label '{lblname}' not found in the repo".format(lblname=label))
            sys.exit(1)

    G = git.Repo(args.git)

    # First get the latest base branch and PRs from BASE_REMOTE
    remote = getattr(G.remotes, BASE_REMOTE)
    remote.fetch()

    prs = args.prs
    if args.pr_label is not None:
        if args.pr_label == '' or args.pr_label.isspace():
            log.error("--pr-label must have a non-space value")
            sys.exit(1)
        payload = {'labels': args.pr_label, 'sort': 'created', 'direction': 'desc'}
        labeled_prs = requests.get("https://api.github.com/repos/{project}/{repo}/issues".format(project=BASE_PROJECT, repo=BASE_REPO), auth=(USER, PASSWORD), params=payload)
        if labeled_prs.status_code != 200:
            log.error("Failed to load labeled PRs: {}".format(labeled_prs))
            sys.exit(1)
        labeled_prs = labeled_prs.json()
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
            base_path = args.base_path + base
            base = next(ref for ref in G.refs if ref.path == base_path)
        except StopIteration:
            log.error("Branch " + base + " does not exist!")
            sys.exit(1)

        # So we know that we're not on an old test branch, detach HEAD onto ref:
        base.checkout()

    for pr in prs:
        pr = int(pr)
        log.info("Merging PR #{pr}".format(pr=pr))

        remote_ref = "refs/pull/{pr}/head".format(pr=pr)
        fi = remote.fetch(remote_ref)
        if len(fi) != 1:
            log.error("PR {pr} does not exist?".format(pr=pr))
            sys.exit(1)
        tip = fi[0].ref.commit

        pr_req = requests.get("https://api.github.com/repos/ceph/ceph/pulls/{pr}".format(pr=pr), auth=(USER, PASSWORD))
        if pr_req.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=pr_req))
            sys.exit(1)

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
            (addendum, new_contributors) = get_credits(pr, pr_req)
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
            req = requests.post("https://api.github.com/repos/{project}/{repo}/issues/{pr}/labels".format(pr=pr, project=BASE_PROJECT, repo=BASE_REPO), data=json.dumps([label]), auth=(USER, PASSWORD))
            if req.status_code != 200:
                log.error("PR #%d could not be labeled %s: %s" % (pr, label, req))
                sys.exit(1)
            log.info("Labeled PR #{pr} {label}".format(pr=pr, label=label))

    # If the branch is 'HEAD', leave HEAD detached (but use "master" for commit message)
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
            tag = "testing/%s" % branch
            git.refs.tag.Tag.create(G, tag)
            log.info("Created tag %s" % tag)

def main():
    parser = argparse.ArgumentParser(description="Ceph PTL tool")
    default_base = 'master'
    default_branch = TEST_BRANCH
    default_label = ''
    if len(sys.argv) > 1 and sys.argv[1] in SPECIAL_BRANCHES:
        argv = sys.argv[2:]
        default_branch = 'HEAD' # Leave HEAD detached
        default_base = default_branch
        default_label = False
    else:
        argv = sys.argv[1:]
    parser.add_argument('--branch', dest='branch', action='store', default=default_branch, help='branch to create ("HEAD" leaves HEAD detached; i.e. no branch is made)')
    parser.add_argument('--merge-branch-name', dest='merge_branch_name', action='store', default=False, help='name of the branch for merge messages')
    parser.add_argument('--base', dest='base', action='store', default=default_base, help='base for branch')
    parser.add_argument('--base-path', dest='base_path', action='store', default=BASE_PATH, help='base for branch')
    parser.add_argument('--git-dir', dest='git', action='store', default=git_dir, help='git directory')
    parser.add_argument('--label', dest='label', action='store', default=default_label, help='label PRs for testing')
    parser.add_argument('--pr-label', dest='pr_label', action='store', help='label PRs for testing')
    parser.add_argument('--no-credits', dest='credits', action='store_false', help='skip indication search (Reviewed-by, etc.)')
    parser.add_argument('prs', metavar="PR", type=int, nargs='*', help='Pull Requests to merge')
    args = parser.parse_args(argv)
    return build_branch(args)

if __name__ == "__main__":
    main()
