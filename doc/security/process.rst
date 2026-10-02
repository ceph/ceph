Ceph Vulnerability Handling
============================
If you have discovered a security vulnerability in Ceph, please report it to security@ceph.io. Every report is acknowledged within three business days. For details on how to report, what to expect, and how disclosures are handled, see the sections below.

1. Purpose
----------

Ceph maintains a vulnerability handling process for receiving, assessing, fixing, and disclosing security vulnerabilities in the Ceph project. This document defines that process from the initial report through public disclosure.

Vulnerabilities are handled based on their disclosure risk. Reports that are already public follow the normal development process. Nonpublic reports normally use deferred disclosure, which keeps the security context private while allowing development in the open. Vulnerabilities that require coordinated disclosure are handled under embargo.

Ceph has one vulnerability-handling service-level agreement: every report is acknowledged within two to three business days. Remediation is prioritized according to risk and follows the applicable development and release pipeline; fixes are not subject to a fixed release deadline.

As an open-source project, Ceph receives reports ranging from already-public dependency vulnerabilities to serious flaws that require coordinated disclosure. The handling tier is selected at intake according to disclosure risk. Full embargo is reserved for cases where keeping the fix private materially protects users. Otherwise, the engineer follows the normal development, build, CI, and release process.

**Severity alone does not require an embargo.**

2. Stages
---------

Every vulnerability, whatever its tier, passes through the same three stages from report to publication, so that each one is handled systematically.

**Decide.** A new report first goes through intake. The vulnerability is acknowledged within three business days, and then is triaged and reviewed. The responder updates the email thread with relevant information and may ask for additional information. The responder confirms via email that it is a valid vulnerability. If the report is not confirmed, no further action is taken and the issue is closed. If confirmed, the responder decides whether it warrants a CVE, and if so, assigns a unique CVE identifier and shares it with the reporter. Next, the responder assesses its CVSS score and CWE, and assigns it a tier: embargoed, deferred disclosure, or unembargoed. Trackers are then created, shared, and assigned to the responsible engineering lead, and a card is added to the Security Project Board so the security team can follow the case. `Checklist for Decision <https://github.com/ceph/ceph/blob/main/doc/security/stage1_decide.rst>`_ gives the criteria for completing this stage.

**Fix.** The fix must land in main and in every supported branch. If the vulnerability is embargoed, all development happens in private forks of the advisory; otherwise pull requests may be opened publicly. Downstream vendors must be able to cherry-pick and pull in the fix before disclosure.  `Checklist for fix <https://github.com/ceph/ceph/blob/main/doc/security/stage2_fix.rst>`_ gives the criteria for completing this stage.

**Disclose.** Once a disclosure date is set, the security mailing list is notified at least seven days in advance. This gives stakeholders time to prepare their releases and to decide whether they need time to mitigate internally. Requests to delay the unembargo are honored where possible, but the decision rests with the Security Lead. Before disclosure, at least one release must contain the fix, and every supported branch must have a backport in a pull request so the fix can ship in that branch's next release. The case may be held until the fix is in all branches. On the unembargo date, the GitHub advisory is published, a notice goes to oss-security, and the relevant documentation is updated, all at the same time, with the release notes linked in the public announcement.  `Checklist for disclosure <https://github.com/ceph/ceph/blob/main/doc/security/stage3_disclose.rst>`_ gives the criteria for completing this stage.

3. The Tiers
------------

At intake, each vulnerability is assigned one of three handling tiers: **Unembargoed**, **Deferred Disclosure**, or **Embargoed**. The tier determines whether development occurs publicly or privately and when vulnerability details are disclosed.


3.1 Unembargoed
~~~~~~~~~~~~~~~

This tier applies when the vulnerability is already public, such as a standard dependency vulnerability or a flaw already publicly known to affect Ceph. There is no security benefit to concealing the remediation.

The engineer follows the normal development, build, CI, and release process. Commits, pull requests, and release notes may include the CVE identifier and describe the security impact of the fix.

These fixes normally ship in the next applicable Ceph release. When the risk warrants faster delivery, the Security Lead may arrange a hotfix. A scheduled release is not delayed to coordinate disclosure; fixes ship as soon as they are ready.


3.2 Deferred Disclosure
~~~~~~~~~~~~~~~~~~~~~~~~

Deferred disclosure is the default for vulnerabilities that are not yet public unless one of the embargo criteria in §3.3 applies.

Deferred disclosure is **not** a formal embargo. Development occurs publicly through normal pull requests, builds, and CI, while the vulnerability's security context and advisory remain private until disclosure.

Public commits and pull requests must not include the CVE identifier or wording that identifies the change as a vulnerability fix. Prohibited terms are listed in Section 5.

The advisory remains private until all of the following conditions are met:

1. At least one Ceph release contains the fix.
2. Every other supported release has a backport pull request containing the fix.
3. Downstream stakeholders have received at least seven days' notice of the planned disclosure.

At least seven days before publication, Ceph sends the planned advisory and disclosure date to the security mailing list. A downstream vendor or other stakeholder that needs additional time must reply with its expected timeline or contact the Security Lead.

A reporter requesting full embargo handling must do so in the initial report. For vulnerabilities rated Critical or High by CVSS, the security responder asks the reporter to confirm that they want coordinated embargo handling. If the reporter does not respond within five business days, the vulnerability proceeds under deferred disclosure unless the Security Lead determines that an embargo is warranted. These fixes normally ship in the next applicable Ceph release. When the risk warrants faster delivery, the Security Lead may arrange a hotfix. A scheduled release is not delayed to coordinate disclosure; fixes ship as soon as they are ready.


3.3 Embargo (Coordinated Release)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An embargo keeps both the vulnerability and its remediation private until a coordinated release and disclosure. Reporters and participants must not disclose an embargoed vulnerability before it has been fixed and announced, unless explicitly permitted by the Ceph security team. This restriction holds until the agreed public disclosure date.

A vulnerability receives embargo handling when any of the following applies:

1. The report was received under an external embargo.
2. The vulnerability is rated Critical or High by CVSS, and the reporter requested embargo handling at intake.
3. The Security Lead determines that an embargo is warranted based on the risk of disclosure before a fix is available.

The tiered embargo process was introduced in 2026. It uses private forks and private builds and is stricter than Ceph's previous vulnerability-handling process.

All development takes place in a private repository associated with the vulnerability's GitHub Security Advisory. Builds follow `the embargoed CVE build process <https://github.com/ceph/ceph/blob/main/doc/dev/developer_guide/cve.rst>`_. Vulnerability details, fixes, commits, builds, and related development remain private until the agreed disclosure date. If a vulnerability is unintentionally already fixed in the public repository, downstream stakeholders/vendors will be notified, and the embargo status will be moved to deferred disclosure. If the vulnerability is fully public from this breach, several days are given to downstream stakeholders/vendors to prepare for updating before the public disclosure. 

The disclosure date is agreed with the release coordinator before being announced to stakeholders, and must coincide with a Ceph release containing the fix. No fix may ship publicly before that date. If the reporter has no date in mind, the security team will coordinate one with list members and share the agreed date with the reporter. Disclosure dates are not set on Fridays or during holiday periods. 

Once set, the planned date is announced on the security mailing list so downstream stakeholders can prepare their releases and advisories. Security announcements are published to ceph-announce@ceph.io and oss-security@lists.openwall.com (both low-traffic).

Requests to extend an embargo are considered individually by the Security Lead. Embargoes should not be held for more than 90 days from the date of vulnerability confirmation, except under unusual circumstances or with approval of the security lead. 



4. Rules for All Tiers
-----------------------

For deferred-disclosure and embargoed vulnerabilities, commits, pull requests, tests, and documentation must not mention the CVE or describe the change as a vulnerability fix until the advisory is published. Unembargoed vulnerabilities may be referenced openly, but it is good practice to say no more than necessary. If a vulnerability becomes public before disclosure, it is treated as unembargoed from that point, and a postmortem is filed on the breach.


5. Prohibited Words
--------------------

The following terms are prohibited in commits for deferred-disclosure vulnerabilities:

``security``, ``security fix``, ``vulnerability``, ``vuln``, ``vulnerable``, ``exploit``, ``exploitable``, ``attack``, ``attacker``, ``malicious``, ``embargo``, ``embargoed``, ``unembargo``, ``advisory``, ``disclosure``, ``coordinated disclosure``, ``PSIRT``, ``CNA``, ``MITRE``, ``NVD``, ``zero-day``, ``0day``, ``RCE``, ``leak``, ``unauthenticated``, ``buffer overflow``, ``out-of-bounds``, ``OOB``, ``use-after-free``, ``UAF``, ``double free``, ``race condition``, ``TOCTOU``, ``injection``, ``path traversal``, ``untrusted input``, ``tainted input``, ``sanitize``, ``bypass``, ``harden``, ``hardening``, ``urgent``, ``hotfix``, ``PoC``, ``proof of concept``, ``payload``, ``oss-security``, ``distros list``, ``CERT``, ``CVE``, ``CVSS``, ``CWE``

The list may be extended as needed.

6. Acknowledgements
--------------------
Your efforts and responsible disclosure are greatly appreciated and will be publicly acknowledged by name, unless you prefer to remain anonymous. We do not offer a bug bounty program, but we appreciate your contribution to open source security! 
