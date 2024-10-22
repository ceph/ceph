Vulnerability Management Process
================================

#. The report will be acknowledged within three business days.
#. The team will investigate the reported issue and will update the email
   thread with relevant information. The team may ask for additional
   information regarding the reported issue.
#. If the team does not confirm the report, no further action will be
   taken and the issue will be closed.
#. If the report is confirmed by Ceph team members, a unique CVE identifier
   will be assigned to the report and then shared with the reporter. The Ceph
   security team will start working on a fix. 
#. If a reporter has no disclosure date in mind, a Ceph security team
   member will coordinate a release date (CRD) with the list members
   and share the mutually agreed disclosure date with the reporter.
#. The vulnerability disclosure / release date is set excluding Friday and
   holiday periods.
#. Embargoes are preferred for Critical and High impact
   issues. Embargo should not be held for more than 90 days from the
   date of vulnerability confirmation, except under unusual
   circumstances. For Low and Moderate issues with limited impact and
   an easy workaround or where an issue that is already public, a
   standard patch release process will be followed to fix the
   vulnerability once CVE is assigned.
#. Fixes for issues of "Medium" and "Low" severity will be released as part of
   the next standard release cycle. List members will receive seven days of
   advance notice prior to the release date of these fixes. The details of the
   CVE fix will be included in the release notes, and the release notes will be
   linked in the public announcement.
#. Commits will be handled in a private repository for review and
   testing and a new patch version will be released from this private
   repository.
#. If a vulnerability is unintentionally already fixed in the public
   repository, a few days are given to downstream stakeholders/vendors
   to prepare for updating before the public disclosure.
#. An announcement will be made disclosing the vulnerability. The
   fastest place to receive security announcements is via the
   `ceph-announce@ceph.io <ceph-announce@ceph.io>`_ or
   `oss-security@lists.openwall.com <oss-security@lists.openwall.com>`_ mailing
   lists.  (These lists are low-traffic).

If the report is considered embargoed, we ask you to not disclose the
vulnerability before it has been fixed and announced, unless you
received a response from the Ceph security team that you can do
so. This holds true until the public disclosure date that was agreed
upon by the list. Thank you for improving the security of Ceph and its
ecosystem. Your efforts and responsible disclosure are greatly
appreciated and will be acknowledged.
