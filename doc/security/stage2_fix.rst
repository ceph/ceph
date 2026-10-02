Stage 2 — Fix
=============


Every Tier
----------

- [ ] Root cause found; every supported release marked affected, not affected (with evidence of false positive) or will-not-fix (with approval).

  .. note::

     **Affected vs. Vulnerable.** Affected means the reported condition is present. Vulnerable implies a potential path to exploitation. The absence of a known exploit path, or the presence of mitigations, does not mean a component is unaffected: attacks may be chained, and architectural changes may bypass or remove existing mitigations.

- [ ] Fix merged on main; backports for every supported branch (``DEFERRED``: waivers recorded if WNF for a given stream).
- [ ] A PR in **every supported release** (``EMBARGOED`` prepared to release for the date).
- [ ] Fix verified on built packages (reproducer fails when applicable, CI green); release plan recorded: which releases carry it.


``UNEMBARGOED`` and ``DEFERRED``
---------------------------------

- [ ] Normal public PRs, CI and backport tooling (``UNEMBARGOED`` may cite the CVE).
- [ ] ``DEFERRED``: no CVE, GHSA or vulnerability wording in commits, PRs, tests or docs; describe the defect plainly.


``EMBARGOED``
-------------

- [ ] Code and PRs only in the GHSA private fork; nothing on ceph-ci, Shaman, public Teuthology or public branches.
- [ ] Builds only through `the embargoed CVE build process <https://github.com/ceph/ceph/blob/main/doc/dev/developer_guide/cve.rst>`_; every commit hygiene-checked before the release-cut merge.
- [ ] Release date confirmed in writing with security lead; this is the unembargo date; vendors/downstream projects aligned.
- [ ] Security mailing list notified of the targeted date; backports code ready for stakeholders.
