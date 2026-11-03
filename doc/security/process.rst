Ceph Vulnerability Handling
============================

If you have discovered a security vulnerability in Ceph, please report it to
security@ceph.io. Every report is acknowledged within three business days.

You may also open a `GitHub security advisory
<https://github.com/ceph/ceph/security/advisories/new>`_ for each vulnerability
you report and include its URL in your email. This is especially helpful if you
are reporting several vulnerabilities at once. For details on how to report,
what to expect, and how disclosures are handled, see the sections below.

You can reach the Ceph Project's `Security Lead
<https://github.com/ceph/ceph/blob/main/doc/security/securitylead.rst>`_ at
security@ceph.io. The Security Lead is designated by the CSC to coordinate
Ceph's security posture and is ultimately accountable for decisions about
Ceph's security.

1. Purpose
----------

Ceph maintains a vulnerability handling process for receiving, assessing,
fixing, and disclosing security vulnerabilities in the Ceph project. This
document defines that process from the initial report through public
disclosure. On an interim basis, this document also serves as the Ceph
project's cybersecurity policy for vulnerability handling under Article 24 of
the EU Cyber Resilience Act.

Vulnerabilities are handled based on their disclosure risk. Reports that are
already public follow the normal development process. Nonpublic Low and Medium
reports normally use deferred disclosure, which keeps the security context
private while allowing development in the open. Vulnerabilities that require
coordinated disclosure are handled under embargo.

Ceph has one vulnerability-handling service-level agreement: every report is
acknowledged within three business days. Remediation is prioritized according
to risk and follows the applicable development and release pipeline; fixes are
not subject to a fixed release deadline.

As an open-source project, Ceph receives reports ranging from already-public
dependency vulnerabilities to serious flaws that require coordinated
disclosure. The handling tier is selected at intake according to disclosure
risk. Full embargo is reserved for cases where keeping the fix private
materially protects users. Otherwise, the engineer follows the normal
development, build, CI, and release process.

**Severity alone does not determine an embargo.**

2. Stages
---------

Every vulnerability, whatever its tier, passes through the same three stages
from report to publication, so that each one is handled systematically. Each
stage's decisions and dates are recorded on the tracker and advisory, so the
handling of every case can be verified afterward.

**Decide.** A new report first goes through intake. The vulnerability is
acknowledged within three business days, and then is triaged and reviewed. The
responder updates the email thread with relevant information and may ask for
additional information. The responder also asks whether there is evidence of
active exploitation; if there is, the case follows the rules in §4. The
responder confirms via email that it is a valid vulnerability. If the report is
not confirmed, no further action is taken and the issue is closed. If
confirmed, the responder decides whether it warrants a CVE, and if so, assigns
a unique CVE identifier and shares it with the reporter. Next, the responder
assesses its CVSS score and CWE, and assigns it a tier: embargoed, deferred
disclosure, or unembargoed. Trackers are then created, shared, and assigned to
the responsible engineering lead, and a card is added to the Security Project
Board so the security team can follow the case. `Checklist For Decision
<https://github.com/ceph/ceph/blob/main/doc/security/stage1_decide.rst>`_ gives
the criteria for completing this stage.

**Fix.** The fix must land in main and in every active release's branch. With
Security Lead approval, an active branch may be marked will-not-fix; such
branches are listed in the advisory so vendors know where they need their own
fix. If the vulnerability is embargoed, all development happens in private
forks of the advisory; otherwise pull requests may be opened publicly.
Downstream vendors must be able to cherry-pick and pull in the fix before
disclosure. Active releases are the stable releases currently receiving
backports, as listed in `Ceph Releases
<https://docs.ceph.com/en/latest/releases/>`_. Vendors that support a release
beyond its upstream lifetime are responsible for their own fixes. `Checklist
For Fix
<https://github.com/ceph/ceph/blob/main/doc/security/stage2_fix.rst>`_ gives
the criteria for completing this stage.

**Disclose.** Once a disclosure date is set, the security email list is
notified at least seven days in advance. This gives stakeholders time to
prepare their releases and to decide whether they need time to mitigate
internally. Requests to delay disclosure are honored where possible, but the
decision rests with the Security Lead. Before disclosure, at least one release
must contain the fix, and every active release's branch, except branches marked
will-not-fix, must have a backport in a pull request so the fix can ship in
that branch's next release. The case may be held until the fix is in all
branches. On the disclosure date, the GitHub advisory is published, notices go
to ceph-announce@ceph.io and oss-security@lists.openwall.com, and the relevant
documentation is updated, all at the same time, with the release notes linked
in the public announcement. The advisory includes a description, the impact,
affected and fixed versions, any will-not-fix branches, and remediation or
workaround guidance. `Checklist For Disclosure
<https://github.com/ceph/ceph/blob/main/doc/security/stage3_disclose.rst>`_
gives the criteria for completing this stage.

3. The Tiers
------------

At intake, each vulnerability is assigned one of three handling tiers:
**Unembargoed**, **Deferred Disclosure**, or **Embargoed**. The tier determines
whether development occurs publicly or privately and when vulnerability details
are disclosed.


3.1 Unembargoed
~~~~~~~~~~~~~~~

This tier applies when the vulnerability is already public, such as a standard
dependency vulnerability or a flaw already publicly known to affect Ceph. There
is no security benefit to concealing the remediation.

The engineer follows the normal development, build, CI, and release
process. Commits, pull requests, and release notes may include the CVE
identifier and describe the security impact of the fix.

These fixes normally ship in the next applicable Ceph release. When the risk
warrants faster delivery, the Security Lead may arrange a hotfix for active
releases. A scheduled release is not delayed to coordinate disclosure; fixes
ship as soon as they are ready.


3.2 Deferred Disclosure
~~~~~~~~~~~~~~~~~~~~~~~~

Deferred disclosure is the default for vulnerabilities that are not yet public
and are rated Low or Medium by CVSS, unless one of the embargo criteria in §3.3
applies. High and Critical vulnerabilities may also be handled under deferred
disclosure with the approval of the Security Lead. Deferred-disclosure cases
are marked as deferred on the Redmine tracker, the Security Project Board card,
and the GitHub advisory. Teams must link the pull requests containing the fix
in the GitHub advisory. Evidence of deferred disclosure, namely the reporter's
approval or no objection within five business days, and Security Lead approval
for High and Critical vulnerabilities, is recorded in the decision record.
Downstream stakeholders learn of the case through the seven days' notice before
disclosure (condition 3 below), which may come after a release containing the
fix has shipped.

Deferred disclosure is **not** a formal embargo. Development occurs publicly
through normal pull requests, builds, and CI, while the vulnerability's
security context and advisory remain private until disclosure.

Commits, pull requests, tests, and documentation must not include the CVE
identifier or wording that identifies the change as a vulnerability fix (§4).
Prohibited terms are listed in §6.

The advisory remains private until all of the following conditions are met:

1. At least one Ceph active release contains the fix.
2. Every other active release, except branches marked will-not-fix, has a
   backport pull request containing the fix. An open backport pull request is
   sufficient because downstream vendors cherry-pick fixes directly from it
   (see §2, Fix).
3. Downstream stakeholders and vendors have received at least seven
   days' notice of the planned disclosure via the security email list.

Reporters may request full embargo handling in their initial report. If they
have not, the responder asks whether they want coordinated embargo handling. If
the reporter approves deferred disclosure, or does not object within five
business days of the request, the vulnerability proceeds under deferred
disclosure, subject to Security Lead approval for High and Critical
vulnerabilities. If the reporter of a Low or Medium vulnerability objects, the
Security Lead grants an embargo unless it is impractical. The Security Lead may
require an embargo in any case.

Deferred-disclosure fixes normally ship in the next applicable Ceph release.
When the risk warrants faster delivery, the Security Lead may arrange a hotfix
for active releases. A scheduled release is not delayed to coordinate
disclosure; fixes ship as soon as they are ready. Deferred disclosure should
not last more than 90 days from the date of vulnerability confirmation, except
under unusual circumstances.


3.3 Embargo (Coordinated Release)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An embargo keeps both the vulnerability and its remediation private until a
coordinated release and disclosure. Reporters and participants must not
disclose an embargoed vulnerability before it has been fixed and announced,
unless explicitly permitted by the Ceph security team or required by law
(see §4). This restriction holds until the agreed public disclosure date.

A vulnerability receives embargo handling when any of the following applies:

1. The report was received under an external embargo.
2. The reporter requested embargo handling. For Low or Medium vulnerabilities,
   the Security Lead may decline if an embargo is impractical (§3.2).
3. The Security Lead determines that an embargo is warranted based on the risk
   of disclosure before a fix is available.

The tiered embargo process was introduced in 2026. It uses private forks and
private builds and is stricter than Ceph's previous vulnerability-handling
process.

All development takes place in a private repository associated with the
vulnerability's GitHub Security Advisory. Builds follow `the embargoed CVE
build process
<https://github.com/ceph/ceph/blob/main/doc/dev/developer_guide/cve.rst>`_.
Vulnerability details, fixes, commits, builds, and related development remain
private until the agreed disclosure date.

If an embargoed vulnerability leaks, for example because it is unintentionally
fixed in the public repository, stakeholders and vendors are notified and the
case is moved to deferred disclosure (§3.2). If the leak makes the
vulnerability fully public, the case is moved to unembargoed (§3.1) instead.
In either case, downstream stakeholders and vendors are given several days'
notice before public disclosure, except where the law requires earlier action
(§4). A postmortem is filed for every leak.

The disclosure date is agreed with the release coordinator before being
announced to stakeholders, and must coincide with a Ceph release containing the
fix. No fix may ship publicly before that date. If the reporter has no date in
mind, the security team will coordinate one with list members and share the
agreed date with the reporter. Disclosure dates are not set on Fridays or
during holiday periods.

Once set, the planned date is announced on the security email list so
downstream stakeholders can prepare their releases and advisories. If the date
slips, the new date is confirmed and announced at once. Security
announcements are published to ceph-announce@ceph.io and
oss-security@lists.openwall.com (both low-traffic).

Requests to extend an embargo are considered individually by the Security
Lead. Embargoes should not be held for more than 90 days from the date of
vulnerability confirmation, except under unusual circumstances.


4. Rules for All Tiers
-----------------------

For deferred-disclosure and embargoed vulnerabilities, commits, pull requests,
tests, and documentation must not mention the CVE or describe the change as a
vulnerability fix until the advisory is published. Prohibited terms are listed
in §6. Unembargoed vulnerabilities may be referenced openly, but it is good
practice to say no more than necessary.

**Actively exploited vulnerabilities.** If there is evidence that a
vulnerability is being actively exploited, the security team informs the
security email list as soon as it is confirmed, whatever the tier, instead of
waiting for the seven days' notice before disclosure.

**Notifications required by law.** Nothing in this process prevents a
reporter, stakeholder, or vendor from notifying a CSIRT or ENISA where the law
requires it, such as under Article 14 of the EU Cyber Resilience Act, or from
making a voluntary notification under Article 15. Anyone who does so must
email the Security Lead at security@ceph.io before making the notification,
and may ask the CSIRT to delay further dissemination under Article 16(2). This
does not delay a notification beyond its legal deadline, but the email must
still be sent.

**Requests from authorities.** The Security Lead handles requests from market
surveillance authorities or CSIRTs about Ceph vulnerabilities.

5. Downstream Stakeholders
--------------------------

Downstream stakeholders, such as distributions, vendors, and large operators,
can receive advance notice of planned disclosures through the security email
list. Members get at least seven days' notice before a disclosure date and may
be given access to embargoed fixes so they can prepare their releases.

To request membership, email security@ceph.io with:

- Who you are and how you ship or run Ceph
- Contact addresses for the list
- Your GitHub ID
- Your agreement to the embargo terms in §3.3

Membership is approved by `the Security Working Group
<https://github.com/ceph/ceph/blob/main/doc/security/workinggroup.rst>`_.
Members must keep embargoed information confidential and must not disclose it.
Public disclosure is decided by the Security Lead and made by the Ceph security
team on the agreed date (§3.3). A confidential notification to a CSIRT or ENISA
that the law requires is not a disclosure, but members must email the Security
Lead before they make one. This does not delay a notification beyond its legal
deadline, but the email must still be sent (§4).

Vendors that find a vulnerability in Ceph, or write a fix for a will-not-fix
branch or a release past its upstream lifetime, are asked to report it or share
the fix with security@ceph.io.

6. Prohibited Terms
-------------------

Sample terms (not exhaustive) that must not appear in commits, pull requests,
tests, or documentation before disclosure: CVE/GHSA IDs, CVSS, vulnerability,
security fix, exploit, attack, embargo, DoS, RCE, privilege escalation, bypass.

7. Acknowledgements
--------------------

Your efforts and responsible disclosure are greatly appreciated and will be
publicly acknowledged by name, unless you prefer to remain anonymous. We do not
offer a bug bounty program, but we appreciate your contribution to open source
security!
