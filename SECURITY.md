# Security Policy

The information below, as well as information about past
vulnerabilities, can be found at

  https://docs.ceph.com/en/latest/security/

## Supported Versions

A new major Ceph release is made every year, and security and bug fixes
are backported to the last two releases.  For the current active
releases and the estimated end-of-life for each, please refer to

  https://docs.ceph.com/en/latest/releases/

## Reporting a Vulnerability

To report a vulnerability, please send email to security@ceph.io

* Please do not file a public ceph tracker issue for a vulnerability.
* We urge reporters to provide as much information as is practical 
  (a reproducer, versions affected, fix if available, etc.), as this
  can speed up the process considerably.
* Please let us know to whom credit should be given and with what
  affiliations.
* If this issue is not yet disclosed publicly and you have any
  disclosure date in mind, please share the same along with the
  report.

Although you are not required to, you may encrypt your message using
the following GPG key:

**6EEF26FFD4093B99: Ceph Security Team (security@ceph.io)**

**Download:** [MIT PGP Public Key Server](https://pgp.mit.edu/pks/lookup?op=vindex&search=0x6EEF26FFD4093B99)   
**Fingerprint:** A527 D019 21F9 7178 C232 66C1 6EEF 26FF D409 3B99

## Vulnerability Management Process

* The report will be acknowledged within three business days or less.
* The team will investigate and update the email thread with relevant
  information and may ask for additional information or guidance
  surrounding the reported issue.
* If the team does not confirm the report, no further action will be
  taken and the issue will be closed.
* If the team confirms the report, a unique CVE identifier will be
  assigned and shared with the reporter. The team will take action to
  fix the issue.
* If a reporter has no disclosure date in mind, a Ceph security team
  member will coordinate a release date (CRD) with the list members
  and share the mutually agreed disclosure date with the reporter.
* The vulnerability disclosure / release date is set excluding Friday and
  holiday periods.
* Embargoes are preferred for Critical and High impact
  issues. Embargo should not be held for more than 90 days from the
  date of vulnerability confirmation, except under unusual
  circumstances. For Low and Moderate issues with limited impact and
  an easy workaround or where an issue that is already public, a
  standard patch release process will be followed to fix the
  vulnerability once CVE is assigned.
* Medium and Low severity issues will be released as part of the next
  standard release cycle, with at least a 7 days advanced
  notification to the list members prior to the release date. The CVE
  fix details will be included in the release notes, which will be
  linked in the public announcement.
* Commits will be handled in a private repository for review and
  testing and a new patch version will be released from this private
  repository.
* If a vulnerability is unintentionally already fixed in the public
  repository, a few days are given to downstream stakeholders/vendors
  to prepare for updating before the public disclosure.
* An announcement will be made disclosing the vulnerability. The
  fastest place to receive security announcements is via the
  ceph-announce@ceph.io or oss-security@lists.openwall.com mailing
  lists.  (These lists are low-traffic).

If the report is considered embargoed, we ask you to not disclose the
vulnerability before it has been fixed and announced, unless you
received a response from the Ceph security team that you can do
so. This holds true until the public disclosure date that was agreed
upon by the list. Thank you for improving the security of Ceph and its
ecosystem. Your efforts and responsible disclosure are greatly
appreciated and will be acknowledged.
