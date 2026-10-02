Stage 3 — Disclose
==================

**Owner:** Security case owner; the Security Lead approves publication.
**Entry:** Stage 2 complete.


Before the date
---------------

- [ ] At least one upstream release and one downstream release contain the fix, verified in the built artifacts; backports are in PRs for every supported branch and downstream release.
- [ ] Advisory verified: affected and fixed versions, CVSS, credit, references, fixing PRs linked; nothing confidential.
- [ ] Disclosure date set and the security mailing list notified at least 7 days ahead; any delay request decided by the Security Lead.
- [ ] ``EMBARGOED``: backports ready for OpenStack and ODF; a slip in the date is re-confirmed and re-announced at once.


On the date
-----------

- [ ] ``EMBARGOED``: private-fork PRs merged at release cut and the release shipped before anything is published.
- [ ] Published together: GHSA, CVE record (GitHub CNA, or submitted to MITRE), oss-security notice, IBM bulletin, docs and release notes. ``UNEMBARGOED``: release notes and bulletin cite the CVE; Ceph advisory if it is Ceph code.
- [ ] ``UNEMBARGOED`` and ``DEFERRED``: Ceph program call names the downstream releases carrying the fix (ODF, OpenStack).


Close
-----

- [ ] CVE backfilled into PR descriptions and trackers; trackers made public per policy.
- [ ] Reporter notified with links and credit confirmed; trackers closed; 30-day watch for corrections.
- [ ] If details leak before the date: treat as ``UNEMBARGOED`` from that moment, publish what exists, and file a postmortem.
