Stage 3 — Disclose
==================

Before the date
---------------

- [ ] At least one supported release contain the fix, verified in the built artifacts; backports are in PRs for every supported release.
- [ ] Advisory verified: affected and fixed versions, CVSS, credit, references, fixing PRs linked; nothing confidential.
- [ ] Disclosure date set and the security mailing list notified at least 7 days ahead; any delay request decided by the Security Lead.
- [ ] ``EMBARGOED``: backports ready for stakeholders; a slip in the date is re-confirmed and re-announced at once.


On the date
-----------

- [ ] ``EMBARGOED``: private-fork PRs merged at release cut and the release shipped before anything is published.
- [ ] Published together: GHSA, CVE record (GitHub CNA, or submitted to MITRE), oss-security notice, docs and release notes. ``UNEMBARGOED``: release notes and bulletin cite the CVE; Ceph advisory if it is Ceph code.


Close
-----

- [ ] PR descriptions and trackers added to advisory; trackers made public per policy.
- [ ] Reporter notified with links and credit confirmed; trackers closed; 30-day watch for corrections.
- [ ] If details leak before the date: treat as ``UNEMBARGOED`` from that moment, publish what exists, and file a postmortem.
