# TODO
# Look for check failures?
# redmine issue update: http://www.redmine.org/projects/redmine/wiki/Rest_Issues

import datetime
import getpass
import git
import json
import logging
import re
import requests
import sys
from os.path import expanduser

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

BASE = "refs/remotes/upstream/heads/%s"
USER = getpass.getuser()
with open(expanduser("~/.github.key")) as f:
    PASSWORD = f.read().strip()
BRANCH_PREFIX = "wip-%s-testing-" % USER
TESTING_LABEL = ["wip-%s-testing" % USER]

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

def build_branch(branch_name, pull_requests):
    repo = git.Repo(".")

    repo.remotes.upstream.fetch()

    # First get the latest base branch from upstream
    if branch_name == 'HEAD':
        log.info("Branch base is HEAD; not checking out!")
    else:
        if branch_name in SPECIAL_BRANCHES:
            base = BASE % branch_name
        else:
            base = BASE % "master"
        log.info("Branch base on {}".format(base))
        base = filter(lambda r: r.path == base, repo.refs)[0]

        # So we know that we're not on an old test branch, detach HEAD onto ref:
        base.checkout()

    for pr in pull_requests:
        log.info("Merging PR {pr}".format(pr=pr))
        pr = int(pr)
        r = filter(lambda r: r.path == "refs/remotes/upstream/pull/%d/head" % pr, repo.refs)[0]

        message = "Merge PR #%d into %s\n\n* %s:\n" % (pr, branch_name, r.path)

        for commit in repo.iter_commits(rev="HEAD.."+r.path):
            message = message + ("\t%s\n" % commit.message.split('\n', 1)[0])

        message = message + "\n"

        comments = requests.get("https://api.github.com/repos/ceph/ceph/issues/{pr}/comments".format(pr=pr), auth=(USER, PASSWORD))
        if comments.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            return

        reviews = requests.get("https://api.github.com/repos/ceph/ceph/pulls/{pr}/reviews".format(pr=pr), auth=(USER, PASSWORD))
        if reviews.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            return

        review_comments = requests.get("https://api.github.com/repos/ceph/ceph/pulls/{pr}/comments".format(pr=pr), auth=(USER, PASSWORD))
        if review_comments.status_code != 200:
            log.error("PR '{pr}' not found: {c}".format(pr=pr,c=comments))
            return

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
            HEAD = repo.head.commit
            log.info("adding new contributors to githubmap on top of PR #%s" % pr)
            r.checkout()
            with open(".githubmap", "a") as f:
                for c in new_new_contributors:
                    f.write("%s %s\n" % (c, new_new_contributors[c]))
            repo.index.add([".githubmap"])
            repo.git.commit("-s", "-m", "githubmap: update contributors")
            c = repo.head.commit
            repo.head.reset(HEAD, index=True, working_tree=True)
        else:
            c = r.commit

        repo.git.merge(c.hexsha, '--no-ff', m=message)

        if branch_name not in SPECIAL_BRANCHES:
            req = requests.post("https://api.github.com/repos/ceph/ceph/issues/{pr}/labels".format(pr=pr), data=json.dumps(TESTING_LABEL), auth=(USER, PASSWORD))
            if req.status_code != 200:
                log.error("PR #%d could not be labeled %s: %s" % (pr, wip, req))
                return

    # If the branch is master, leave HEAD detached (but use "master" for commit message)
    if branch_name not in SPECIAL_BRANCHES:
        # Delete test branch if it already existed
        try:
            getattr(repo.branches, branch_name).delete(
                    repo, getattr(repo.branches, branch_name), force=True)
            log.info("Deleted old test branch %s" % branch_name)
        except AttributeError:
            pass

        log.info("Creating branch {branch_name}".format(branch_name=branch_name))
        repo.create_head(branch_name)
        # tag it for future reference.
        name = "testing/%s" % branch_name
        log.info("Creating tag %s" % name)
        git.refs.tag.Tag.create(repo, name, force=True)

if __name__ == "__main__":
    if sys.argv[1] in SPECIAL_BRANCHES:
        branch_name = sys.argv[1]
        pull_requests = sys.argv[2:]
    else:
        branch_name = BRANCH_PREFIX + datetime.datetime.now().strftime("%Y%m%d")
        pull_requests = sys.argv[1:]
    build_branch(branch_name, pull_requests)
