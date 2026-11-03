Stage 3 — Disclose
==================

Before the date
---------------

- [ ] At least one active release contains the fix, verified in the built
  artifacts; backports are in PRs for every active release except
  will-not-fix branches.
- [ ] Advisory verified: description, impact, affected and fixed versions,
  will-not-fix branches, remediation or workaround guidance, CVSS, credit,
  references, fixing PRs linked; nothing confidential.
- [ ] Disclosure date set and the security email list notified at least 7
  days ahead; any delay request decided by the Security Lead.
- [ ] ``DEFERRED`` or ``EMBARGOED``: no more than 90 days since the
  vulnerability was confirmed, or an exception approved by the Security Lead.
- [ ] ``EMBARGOED``: backports ready for stakeholders; a slip in the date is
  re-confirmed and re-announced at once.
- [ ] If an ``EMBARGOED`` case leaks before the date: stakeholders notified
  and the case moved to ``DEFERRED``, or to ``UNEMBARGOED`` if the leak makes
  it fully public; several days' notice given before disclosure unless the
  law requires earlier action (process.rst §4); postmortem filed.


On the date
-----------

- [ ] ``EMBARGOED``: private-fork PRs merged at release cut and the release
  shipped before anything is published.
- [ ] Published together: GHSA, CVE record (GitHub CNA, or submitted to
  MITRE), notices to ceph-announce@ceph.io and oss-security, docs and release
  notes. ``UNEMBARGOED``: release notes and bulletin cite the CVE; Ceph
  advisory if it is Ceph code.


Close
-----

- [ ] PR descriptions and trackers added to advisory; trackers made public
  per policy.
- [ ] Reporter notified with links and credit confirmed; trackers closed;
  30-day watch for corrections.
