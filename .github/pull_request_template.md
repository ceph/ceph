



<!--
  - Please give your pull request a title like

      [component]: [short description]

  - Please use this format for each git commit message:

      [component]: [short description]

      [A longer multiline description]

      Fixes: [ticket URL on tracker.ceph.com, create one if necessary]
      Signed-off-by: [Your Name] <[your email]>

    For examples, use "git log".
-->

## Contribution Guidelines
- To sign and title your commits, please refer to [Submitting Patches to Ceph](https://github.com/ceph/ceph/blob/main/SubmittingPatches.rst).

- If you are submitting a fix for a stable branch (e.g. "quincy"), please refer to [Submitting Patches to Ceph - Backports](https://github.com/ceph/ceph/blob/master/SubmittingPatches-backports.rst) for the proper workflow.

- When filling out the below checklist, you may click boxes directly in the GitHub web UI.  When entering or editing the entire PR message in the GitHub web UI editor, you may also select a checklist item by adding an `x` between the brackets: `[x]`.  Spaces and capitalization matter when checking off items this way.

## Checklist
- Tracker (select at least one)
  - [ ] References tracker ticket
  - [ ] Very recent bug; references commit where it was introduced
  - [ ] New feature (ticket optional)
  - [ ] Doc update (no ticket needed)
  - [ ] Code cleanup (no ticket needed)
- Component impact
  - [ ] Affects [Dashboard](https://tracker.ceph.com/projects/dashboard/issues/new), opened tracker ticket
  - [ ] Affects [Orchestrator](https://tracker.ceph.com/projects/orchestrator/issues/new), opened tracker ticket
  - [ ] No impact that needs to be tracked
- Documentation (select at least one)
  - [ ] Updates relevant documentation
  - [ ] No doc update is appropriate
- Tests (select at least one)
  - [ ] Includes [unit test(s)](https://docs.ceph.com/en/latest/dev/developer_guide/tests-unit-tests/)
  - [ ] Includes [integration test(s)](https://docs.ceph.com/en/latest/dev/developer_guide/testing_integration_tests/)
  - [ ] Includes bug reproducer
  - [ ] No tests

<details>
<summary>Show available Jenkins commands</summary>

- `jenkins test classic perf` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-perf-classic/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-perf-pull-requests/config/definitions/ceph-perf-pull-requests.yml)
- `jenkins test crimson perf` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-perf-crimson/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-perf-pull-requests/config/definitions/ceph-perf-pull-requests.yml)
- `jenkins test signed` [Jenkins Job](https://jenkins.ceph.com/job/ceph-pr-commits/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-pr-commits/config/definitions/ceph-pr-commits.yml)
- `jenkins test make check` [Jenkins Job](https://jenkins.ceph.com/job/ceph-pull-requests/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-pull-requests/config/definitions/ceph-pull-requests.yml)
- `jenkins test make check arm64` [Jenkins Job](https://jenkins.ceph.com/job/ceph-pull-requests-arm64/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-pull-requests-arm64/config/definitions/ceph-pull-requests-arm64.yml)
- `jenkins test submodules` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-pr-submodules/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-pr-submodules/config/definitions/ceph-pr-commits.yml)
- `jenkins test dashboard` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-dashboard-pull-requests/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-dashboard-pull-requests/config/definitions/ceph-dashboard-pull-requests.yml)
- `jenkins test dashboard cephadm` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-dashboard-cephadm-e2e/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-dashboard-cephadm-e2e/config/definitions/ceph-dashboard-cephadm-e2e.yml)
- `jenkins test api` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-api/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-pr-api/config/definitions/ceph-pr-api.yml)
- `jenkins test docs` [ReadTheDocs](https://readthedocs.org/projects/ceph/) | [Github Workflow Definition](https://github.com/ceph/ceph/blob/main/.readthedocs.yml)
- `jenkins test ceph-volume all` [Jenkins Jobs](https://jenkins.ceph.com/view/ceph-volume%20PR/) | [Jenkins Jobs Definition](https://github.com/ceph/ceph-build/blob/main/ceph-volume-cephadm-prs/config/definitions/ceph-volume-pr.yml)
- `jenkins test windows` [Jenkins Job](https://jenkins.ceph.com/job/ceph-windows-pull-requests/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-windows-pull-requests/config/definitions/ceph-windows-pull-requests.yml)
- `jenkins test rook e2e` [Jenkins Job](https://jenkins.ceph.com/view/all/job/ceph-orchestrator-rook-e2e/) | [Jenkins Job Definition](https://github.com/ceph/ceph-build/blob/main/ceph-rook-e2e/config/definitions/ceph-orchestrator-rook-e2e.yml)
</details>
