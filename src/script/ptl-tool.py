#!/usr/bin/env python2

# README:
#
# This tool's purpose is to make it easier to merge PRs into Ceph.
#
# Because developers often have custom names for the ceph upstream remote
# (https://github.com/ceph/ceph.git), You will probably want to set the
# PTL_TOOL_BASE_PATH environment variable in your shell rc files before using
# this script:
#
#     PTL_TOOL_BASE_PATH=refs/remotes/upstream/
#
# and PTL_TOOL_BASE_REMOTE as the name of your Ceph upstream remote (default: "upstream"):
#
#     PTL_TOOL_BASE_REMOTE=origin
#
#
# ** Here are some basic exmples to get started: **
#
# Merging PR #1234567 and #2345678 into a new test branch with a testing label added to the PR:
#
# $ env PTL_TOOL_BASE_PATH=refs/remotes/upstream/ src/script/ptl-tool.py --base master 1234567 2345678
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
# $ env PTL_TOOL_BASE_PATH=refs/remotes/upstream/ src/script/ptl-tool.py --base master --branch HEAD --merge-branch-name --label - master 1234567
# Detaching HEAD onto base: master
# Merging PR #1234567
# Leaving HEAD detached; no branch anchors your commits
#
# Now push to master:
# $ git push upstream HEAD:master
#
#
# Merging PR #12345678 into luminous leaving a detached HEAD (i.e. do not update your repo's master branch) and do not label:
# $ env PTL_TOOL_BASE_PATH=refs/remotes/upstream/ src/script/ptl-tool.py --base luminous --branch HEAD --merge-branch-name luminous --label - 12345678
# Detaching HEAD onto base: luminous
# Merging PR #12345678
# Leaving HEAD detached; no branch anchors your commits
#
# Now push to luminous:
# $ git push upstream HEAD:luminous


# TODO
# Fetch PRs by label.
# Look for check failures?
# redmine issue update: http://www.redmine.org/projects/redmine/wiki/Rest_Issues

import argparse
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

BASE_REMOTE = os.getenv("PTL_TOOL_BASE_REMOTE", "upstream")
BASE_PATH = os.getenv("PTL_TOOL_BASE_PATH", "refs/remotes/upstream/heads/")
GITDIR = os.getenv("PTL_TOOL_GITDIR", ".")
USER = getpass.getuser()
with open(expanduser("~/.github.key")) as f:
    PASSWORD = f.read().strip()
BRANCH_PREFIX = "wip-%s-testing-" % USER
TESTING_LABEL = "wip-%s-testing" % USER
TESTING_BRANCH_NAME = BRANCH_PREFIX + datetime.datetime.now().strftime("%Y%m%d")

SPECIAL_BRANCHES = ('master', 'luminous', 'jewel', 'HEAD')

INDICATIONS = [
    re.compile("(Reviewed-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Acked-by: .+ <[\w@.-]+>)", re.IGNORECASE),
    re.compile("(Tested-by: .+ <[\w@.-]+>)", re.IGNORECASE),
]

CONTRIBUTORS = {}
NEW_CONTRIBUTORS = {}
with open(".githubmap") as f:
    comment = re.compile("\s*#")
    patt = re.compile("([\w-]+)\s+(.*)")
    for line in f:
        if comment.match(line):
            continue
        m = patt.match(line)
        CONTRIBUTORS[m.group(1)] = m.group(2)

def build_branch(args):
    base = args.base
    branch = args.branch
    label = args.label

    G = git.Repo(args.git)

    # First get the latest base branch and PRs from BASE_REMOTE
    remote = getattr(G.remotes, BASE_REMOTE)
    remote.fetch()

    if base == 'HEAD':
        log.info("Branch base is HEAD; not checking out!")
    else:
        log.info("Detaching HEAD onto base: {}".format(base))
        try:
            base_path = args.base_path + base
            base = filter(lambda r: r.path == base_path, G.refs)[0]
        except IndexError:
            log.error("Branch " + base + " does not exist!")
            sys.exit(1)

        # So we know that we're not on an old test branch, detach HEAD onto ref:
        base.checkout()

    for pr in args.prs:
        log.info("Merging PR #{pr}".format(pr=pr))
        pr = int(pr)
        remote_ref = "refs/pull/{pr}/head".format(pr=pr)
        fi = remote.fetch(remote_ref)
        if len(fi) != 1:
            log.error("PR {pr} does not exist?".format(pr=pr))
            sys.exit(1)
        tip = fi[0].ref.commit

        message = "Merge PR #%d into %s\n\n* %s:\n" % (pr, args.merge_branch_name, remote_ref)

        for commit in G.iter_commits(rev="HEAD.."+str(tip)):
            message = message + ("\t%s\n" % commit.message.split('\n', 1)[0])

        message = message + "\n"

        comments = requests.get("https://api.github.com/repos/ceph/ceph/issues/{pr}/comments".format(pr=pr), auth=(USER, PASSWORD))
        if comments.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            sys.exit(1)

        reviews = requests.get("https://api.github.com/repos/ceph/ceph/pulls/{pr}/reviews".format(pr=pr), auth=(USER, PASSWORD))
        if reviews.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            sys.exit(1)

        review_comments = requests.get("https://api.github.com/repos/ceph/ceph/pulls/{pr}/comments".format(pr=pr), auth=(USER, PASSWORD))
        if review_comments.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            sys.exit(1)

        indications = set()
        for comment in comments.json()+review_comments.json():
            for indication in INDICATIONS:
                for cap in indication.findall(comment["body"]):
                    indications.add(cap)

        new_new_contributors = {}
        for review in reviews.json():
            if review["state"] == "APPROVED":
                user = review["user"]["login"]
                try:
                    indications.add("Reviewed-by: "+CONTRIBUTORS[user])
                except KeyError as e:
                    try:
                        indications.add("Reviewed-by: "+NEW_CONTRIBUTORS[user])
                    except KeyError as e:
                        try:
                            name = raw_input("Need name for contributor \"%s\"; Reviewed-by: " % user)
                            name = name.strip()
                            if len(name) == 0:
                                continue
                            NEW_CONTRIBUTORS[user] = name
                            new_new_contributors[user] = name
                            indications.add("Reviewed-by: "+name)
                        except EOFError as e:
                            continue

        for indication in indications:
            message = message + indication + "\n"

        if new_new_contributors:
            # Check out the PR, add a commit adding to .githubmap
            HEAD = G.head.commit
            log.info("adding new contributors to githubmap on top of PR #%s" % pr)
            G.head.reset(commit=tip, index=True, working_tree=True)
            with open(".githubmap", "a") as f:
                for c in new_new_contributors:
                    f.write("%s %s\n" % (c, new_new_contributors[c]))
            G.index.add([".githubmap"])
            G.git.commit("-s", "-m", "githubmap: update contributors")
            c = G.head.commit
            G.head.reset(HEAD, index=True, working_tree=True)
        else:
            c = tip

        G.git.merge(c.hexsha, '--no-ff', m=message)

        if label and label != '-':
            req = requests.post("https://api.github.com/repos/ceph/ceph/issues/{pr}/labels".format(pr=pr), data=json.dumps([label]), auth=(USER, PASSWORD))
            if req.status_code != 200:
                log.error("PR #%d could not be labeled %s: %s" % (pr, label, req))
                sys.exit(1)
            log.info("Labeled PR #{pr} {label}".format(pr=pr, label=label))

    # If the branch is 'HEAD', leave HEAD detached (but use "master" for commit message)
    if branch == 'HEAD':
        log.info("Leaving HEAD detached; no branch anchors your commits")
    else:
        # Delete test branch if it already existed
        try:
            getattr(G.branches, branch).delete(
                    G, getattr(G.branches, branch), force=True)
            log.info("Deleted old test branch %s" % branch)
        except AttributeError:
            pass

        G.create_head(branch)
        log.info("Created branch {branch}".format(branch=branch))

        # tag it for future reference.
        for i in range(0, 100):
            if i == 0:
                name = "testing/%s" % branch
            else:
                name = "testing/%s_%02d" % (branch, i)
            try:
                git.refs.tag.Tag.create(G, name)
                log.info("Created tag %s" % name)
                break
            except:
                pass
            if i == 99:
                raise RuntimeException("ran out of numbers")

def main():
    parser = argparse.ArgumentParser(description="Ceph PTL tool")
    default_base = 'master'
    default_branch = TESTING_BRANCH_NAME
    default_label = TESTING_LABEL
    if len(sys.argv) > 1 and sys.argv[1] in SPECIAL_BRANCHES:
        argv = sys.argv[2:]
        default_branch = 'HEAD' # Leave HEAD deatched
        default_base = default_branch
        default_label = False
    else:
        argv = sys.argv[1:]
    parser.add_argument('--branch', dest='branch', action='store', default=default_branch, help='branch to create ("HEAD" leaves HEAD detached; i.e. no branch is made)')
    parser.add_argument('--merge-branch-name', dest='merge_branch_name', action='store', help='name of the branch for merge messages')
    parser.add_argument('--base', dest='base', action='store', default=default_base, help='base for branch')
    parser.add_argument('--base-path', dest='base_path', action='store', default=BASE_PATH, help='base for branch')
    parser.add_argument('--git-dir', dest='git', action='store', default=GITDIR, help='git directory')
    parser.add_argument('--label', dest='label', action='store', default=default_label, help='label PRs for testing')
    parser.add_argument('prs', metavar="PR", type=int, nargs='+', help='Pull Requests to merge')
    args = parser.parse_args(argv)
    if getattr(args, 'merge_branch_name') is None:
        setattr(args, 'merge_branch_name', args.branch)
    return build_branch(args)

if __name__ == "__main__":
    main()
