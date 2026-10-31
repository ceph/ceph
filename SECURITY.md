# Security Policy

The information below, as well as information about past
vulnerabilities, can be found at

  https://docs.ceph.com/en/latest/security/

## Supported Versions

Security fixes are applied to the active releases, the stable releases
that currently receive backports. For the list of active releases and
the estimated end-of-life for each, please refer to

  https://docs.ceph.com/en/latest/releases/#active-releases

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
* If you want the report handled under embargo, with a coordinated
  release and disclosure, please say so in the report.

You may also open a [GitHub security advisory](https://github.com/ceph/ceph/security/advisories/new)
for each vulnerability you report and include its URL in your email.
This is especially helpful if you are reporting several
vulnerabilities at once.

## Vulnerability Management Process

The full process is documented at

  https://docs.ceph.com/en/latest/security/process/

* The report will be acknowledged within three business days.
* The team will investigate and update the email thread with relevant
  information and may ask for additional information.
* If the team does not confirm the report, no further action will be
  taken and the issue will be closed.
* If the team confirms the report, it decides whether the issue
  warrants a CVE and, if so, assigns a CVE identifier and shares it
  with the reporter.
* Each confirmed vulnerability is handled in one of three tiers,
  chosen at intake. Severity alone does not determine an embargo.
  * Unembargoed: the vulnerability is already public. The fix follows
    the normal development and release process and ships in the next
    applicable release, or in a hotfix if the risk warrants it.
  * Deferred disclosure: the default for vulnerabilities that are not
    yet public and are rated Low or Medium. The fix is developed in
    public without identifying it as a vulnerability fix, while the
    advisory stays private until a release contains the fix, backports
    for the other active releases are open, and downstream
    stakeholders have had at least seven days' notice.
  * Embargoed: used when the report came under an external embargo,
    the reporter requested it, or the Security Lead decides the risk
    warrants it. The vulnerability and its fix stay private until a
    coordinated release on an agreed disclosure date, which coincides
    with a Ceph release containing the fix and is not set on a Friday
    or during holiday periods.
* Neither deferred disclosure nor an embargo should last more than 90
  days from the date of vulnerability confirmation, except under
  unusual circumstances.
* Downstream stakeholders and vendors receive at least seven days'
  notice of a planned disclosure through the security email list.
  Membership can be requested from security@ceph.io.
* If an embargoed vulnerability is unintentionally fixed in the public
  repository, stakeholders and vendors are notified and given several
  days to prepare before the public disclosure.
* On the disclosure date the GitHub advisory is published and an
  announcement is made. The fastest place to receive security
  announcements is via the ceph-announce@ceph.io or
  oss-security@lists.openwall.com mailing lists.
* Where the law requires you to notify a CSIRT or ENISA, for example
  under the EU Cyber Resilience Act, please email the Security Lead at
  security@ceph.io before making the notification.

If the report is considered embargoed, we ask you to not disclose the
vulnerability before it has been fixed and announced, unless you
received a response from the Ceph security team that you can do so or
the law requires you to. This holds true until the public disclosure
date that was agreed upon by the list. Thank you for improving the
security of Ceph and its ecosystem. Your efforts and responsible
disclosure are greatly appreciated and will be publicly acknowledged
by name, unless you prefer to remain anonymous. We do not offer a bug
bounty program.
