Stage 2 — Fix
=============


Every tier
----------

- [ ] Root cause found; every active release marked affected, not affected
  (with evidence of false positive) or will-not-fix (with Security Lead
  approval).

  .. note::

     **Affected vs. Vulnerable.** Affected means the reported condition is
     present. Vulnerable implies a potential path to exploitation. The absence
     of a known exploit path, or the presence of mitigations, does not mean a
     component is unaffected: attacks may be chained, and architectural
     changes may bypass or remove existing mitigations.

- [ ] Will-not-fix branches recorded in the GHSA so they are listed in the
  advisory.
- [ ] Fix merged on main; a backport PR in every active branch except
  will-not-fix branches.
- [ ] Fix verified on built packages (reproducer fails when applicable, CI
  green); release plan recorded: which releases carry it.


``UNEMBARGOED`` and ``DEFERRED``
---------------------------------

- [ ] Normal public PRs, CI and backport tooling (``UNEMBARGOED`` may cite the
  CVE).
- [ ] ``DEFERRED``: no CVE, GHSA or vulnerability wording in commits, PRs,
  tests or docs (see the prohibited terms in process.rst §6); describe the
  defect plainly.
- [ ] ``DEFERRED``: fix PRs linked in the GHSA.


``EMBARGOED``
-------------

- [ ] Code and PRs only in the GHSA private fork; nothing on ceph-ci, Shaman,
  public Teuthology or public branches.
- [ ] Builds only through `the embargoed CVE build process
  <https://github.com/ceph/ceph/blob/main/doc/dev/developer_guide/cve.rst>`_;
  every commit hygiene-checked before the release-cut merge.
- [ ] Disclosure date agreed with the release coordinator and confirmed in
  writing with the Security Lead; it coincides with a Ceph release containing
  the fix; vendors and downstream projects aligned.
- [ ] Backports prepared to release on the disclosure date.
- [ ] Security email list notified of the disclosure date at least seven days
  in advance; backport code ready for stakeholders.
