Stage 1 — Decide: How will this be fixed?
==========================================

Intake
------

- [ ] Acknowledged within 3 business days; report and PoC stored in the
  private case location.
- [ ] Reporter recorded: contact, disclosure deadline, embargo handling, credit
  preference, and whether a CVE was already requested elsewhere.
- [ ] Asked whether there is evidence of active exploitation. If yes, the
  security email list is informed immediately, whatever the tier (§4).
- [ ] Confirmed as a valid vulnerability by email; if not confirmed, the
  case is closed with no further action.
- [ ] Preliminary rating (Low / Medium / High / Critical), CVSS vector, CWE,
  and affected releases recorded.
- [ ] Checked whether it is already public, and whether it arrived under
  another organization's embargo (terms and date recorded).


Decide — first rule that matches
---------------------------------

- [ ] **Already public** (e.g. a standard dependency upgrade) →
  ``UNEMBARGOED``.
- [ ] **External embargo** → ``EMBARGOED``.
- [ ] **Reporter approves deferred disclosure or does not object within 5
  business days of the request** and the rating is Low / Medium (High /
  Critical with Security Lead approval), unless the Security Lead requires an
  embargo → ``DEFERRED``.
- [ ] **Otherwise** → ``EMBARGOED``, unless the rating is Low / Medium and the
  Security Lead finds an embargo impractical (→ ``DEFERRED``); participant list
  recorded and confidentiality reminder sent.


Set up and hand off
--------------------

- [ ] Trackers, cross-linked and tagged with the handling type: GHSA
  (unpublished for ``DEFERRED``; private fork for ``EMBARGOED``) with the CVE
  requested, Project Card, Redmine per active branch.
- [ ] Decision record posted: type, rule, rating, approvals, external embargo,
  date confirmed, decided by, date. For ``DEFERRED``: evidence of the
  reporter's approval or of no objection within 5 business days, and Security
  Lead approval for High / Critical.
- [ ] Reporter told the decision; assignee or engineering lead named → Stage 2.
