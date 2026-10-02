 Stage 1 — Decide: How will this be fixed?
==========================================

**Owner:** Security case owner; Security lead approves.
**Entry:** a vulnerability report is received.


Intake
------

- [ ] Acknowledged within 2–3 business days; report and PoC stored in the private case location.
- [ ] Reporter recorded: contact, disclosure deadline, credit preference, and whether a CVE was already requested elsewhere.
- [ ] Preliminary rating (Low / Moderate / Important / Critical) and affected releases recorded.
- [ ] Checked whether it is already public, and whether it arrived under another organization's embargo (terms and date recorded).


Decide — first rule that matches
---------------------------------

- [ ] **Already public** (e.g. a standard dependency upgrade) → ``UNEMBARGOED``.
- [ ] **Reporter approves deferred disclosure** and the rating is Low/Moderate (or the security program approves it for Important/Critical) and there is no external embargo → ``DEFERRED``.
- [ ] **Otherwise** → ``EMBARGOED``; participant list recorded and confidentiality reminder sent.


Set up and hand off
--------------------

- [ ] Trackers, cross-linked and tagged with the handling type: GHSA (unpublished for ``DEFERRED``; private fork for ``EMBARGOED``) with the CVE requested, Project Card, Redmine per supported branch.
- [ ] Decision record posted: type, rule, rating, approvals, external embargo, decided by, date.
- [ ] Reporter told the decision; assignee or engineering lead named → Stage 2.
