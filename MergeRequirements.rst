===========================
Merge Requirements for Ceph
===========================

Ceph uses both unit and integration testing to gate merges. Our
requirements go beyond the Github Checklist. You may not merge a PR
until it has met the requirements for all relevant components.

Common code must meet the testing requirements of all components that
depend on it.

If you have the required access you may run QA yourself, but unless
all relevant test suites are solid green, you *may not* merge it before
the results are reviewed and approved by all relevant Tech Leads.

RGW
---

PRs to the RADOS Gateway must generally go through the RGW suite in
Teuthology. Once your PR is approved, you or the reviewer may tag it
`needs-qa`.

For PRs that only modify details of the build system rather than
making functional changes to code, an RGW lead may, at their option,
allow it to merge when the ceph-ci Jenkins builder has successfully
built all variants.
