# qa_triage

Triage the failed jobs of a Ceph [teuthology](https://github.com/ceph/teuthology) QA
run: find the ones that are already-known problems, and hand the rest to a person
with the evidence gathered and a draft tracker comment ready.

It lives in a repository of its own,
[aainscow/qa-triage](https://github.com/aainscow/qa-triage), with the library of
what it has learned, which everyone who uses it adds to. That repository is
private: its owner adds people as collaborators.

`qa_triage.sh` here clones it into `qa-triage/`, which git ignores, the first time,
brings it up to date each time after, and runs it, with this ceph clone for the
source checkouts and the fix check:

```
$ src/script/qa_triage/qa_triage.sh https://pulpito.ceph.com/<run>/
```

It clones over ssh, with your GitHub key; `QA_TRIAGE_REPO=https://github.com/aainscow/qa-triage.git`
clones over https instead. Its README, `qa-triage/README.md` once it is cloned, says
the rest.
