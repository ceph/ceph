# qa-triage-db

What qa_triage has learned about Ceph teuthology failures, shared by everyone who
uses it: `problems/` (a record for each known problem, named after its tracker
issue), `nodes/` (questions the tree of questions has learned, in Python) and
`labels/` (jobs whose answer someone settled, which every change is checked
against). qa_triage itself lives in ceph.git, under src/script/qa_triage.

Point qa_triage at it with the `problems`, `nodes` and `labels` settings.
