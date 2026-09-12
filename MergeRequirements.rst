===========================
Merge Requirements for Ceph
===========================

Ceph uses both unit and integration testing to gate merges. Our
requirements go beyond the Github Checklist. You may not merge a PR
until it has met the requirements for all relevant components.

Common code must meet the testing requirements of all components that
depend on it.

RGW
---

PRs to the RADOS Gateway must generally go through RGW
Teuthology. Once your PR is approved, you or the reviewer may tag it
'needs-qa'. If it requires an update to `s3-tests`, instead please
mark it as `needs-separate-s3tests-qa`.

If you have teuthology access you may run the QA yourself, but unless
the run is solid green, you *may not* merge it before the results are
approved and reviewed by one of the RGW Tech Leads.

For PRs that only modify details of the build system rather than
making functional changes to code, an RGW lead may, at their option,
allow it to merge with a clean compile of all targets on Shaman.
