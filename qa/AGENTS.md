# qa/ — Integration Tests

Two approaches: **Teuthology** (distributed cluster tests, YAML suite definitions in `suites/`, Python tasks in `tasks/`) and **standalone** (shell scripts in `standalone/` using `ceph-helpers.sh` to run vstart clusters).

Unit tests are separate — see `src/test/AGENTS.md`. RGW test logic mostly lives in the external `s3-tests` repo; `qa/` RGW content is orchestration only.
