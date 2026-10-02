 Stage 1 — Decide: How will this be fixed?
==========================================

Intake
------

- [ ] Acknowledged within 3 business days; report and PoC stored in the private case location.
- [ ] Reporter recorded: contact, disclosure deadline, embargo handling, credit preference, and whether a CVE was already requested elsewhere.
- [ ] Preliminary rating (Low / Medium / High / Critical) and affected releases recorded.
- [ ] Checked whether it is already public, and whether it arrived under another organization's embargo (terms and date recorded).


Decide — first rule that matches
---------------------------------

- [ ] **Already public** (e.g. a standard dependency upgrade) → ``UNEMBARGOED``.
- [ ] **External Embargo** → ``EMBARGOED``.
- [ ] **Reporter approves deferred disclosure or does not object within 5 business days of the request** and the rating is Low / Medium (High / Critical with Security Lead approval), unless the Security Lead requires an embargo → ``DEFERRED``.
- [ ] **Otherwise** → ``EMBARGOED``, unless the rating is Low / Medium or the Security Lead finds an embargo impractical (→ ``DEFERRED``); participant list recorded and confidentiality reminder sent.


Set up and hand off
--------------------

- [ ] Trackers, cross-linked and tagged with the handling type: GHSA (unpublished for ``DEFERRED``; private fork for ``EMBARGOED``) with the CVE requested, Project Card, Redmine per supported branch.
- [ ] Decision record posted: type, rule, rating, approvals, external embargo, decided by, date.
- [ ] Reporter told the decision; assignee or engineering lead named → Stage 2.
