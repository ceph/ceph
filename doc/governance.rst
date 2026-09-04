.. _governance:

============
 Governance
============

The Ceph project is established as Ceph a Series of LF Projects, LLC, and its
technical governance is set forth in the :ref:`technical-charter` (the
"Charter"), adopted February 12, 2026.

This document is the publicly documented set of operating procedures that the
Ceph Steering Committee (the "CSC") has adopted for itself under the authority
the Charter grants it in sections 2.b, 2.c, 2.e, 2.f and 2.g.  It records how
the CSC actually operates, which roles exist in the project, and who holds
them.  It is the governance documentation referred to by the Charter's
"CONTRIBUTING file", and is incorporated by reference from ``CONTRIBUTING.rst``
in the root of the Ceph source repository.

Relationship to the Technical Charter
=====================================

The Charter is the controlling document.  This document adds detail to the
Charter; it does not and cannot modify it.  In particular:

 * Where any provision of this document conflicts with the Charter, the
   Charter prevails and the conflicting provision has no effect.
 * Nothing in this document amends the Charter.  Amending the Charter requires
   a two-thirds vote of the entire CSC and approval by LF Projects, LLC
   (Charter section 8.a).
 * The order of precedence for the project's governing documents is: the
   Operating Agreement of LF Projects and the Series Agreement for the
   Project, then the policies of LF Projects
   (https://lfprojects.org/policies/), then the Charter, then this document,
   then the subordinate procedures described in :ref:`operating-procedures`.

Key principles
==============

 * Decision-making is consensus-driven by those who participate.  Votes are a
   tool of last resort when consensus cannot be reached.
 * Leadership roles are defined primarily by responsibility, not prestige or
   seniority.
 * It is normal and healthy for these roles to be passed on to others.
 * Everyone's role is ultimately to serve the users, and participation is
   voluntary.
 * The project operates in a transparent, open, collaborative, and ethical
   manner.  Discussions, proposals, timelines, decisions, and status are made
   open and easily visible to all (Charter section 4.e).
 * Participation is open to any individual or organization that meets the
   requirements for contributing, regardless of competitive interests
   (Charter section 4.d).

.. _csc:

Ceph Steering Committee
=======================

Mandate
-------

The CSC is responsible for all technical oversight of the Ceph open source
project (Charter section 2.a).  Its responsibilities include (Charter section
2.g):

 * coordinating the technical direction of the project;
 * approving project or system proposals, including incubation, deprecation,
   and changes to a sub-project's scope;
 * organizing and removing sub-projects;
 * creating sub-committees or working groups to focus on cross-project
   technical issues and requirements;
 * appointing representatives to work with other open source or open standards
   communities;
 * establishing community norms, workflows, issuing releases, and security
   issue reporting policies;
 * approving and implementing policies and processes for contributing, and
   coordinating with the Series Manager to resolve matters or concerns arising
   under section 7 of the Charter; and
 * seeking consensus and, where necessary, voting on technical matters
   relating to the code base that affect multiple projects.

In addition, and as documented by this document, the CSC:

 * elects the Ceph Executive Council, whose members serve as the CSC's
   Co-Chairs (see :ref:`cec`);
 * holds an annual election;
 * meets regularly to discuss and decide on tactical and strategic projects
   and improvements;
 * promotes Contributors to Maintainer, and removes Maintainers;
 * creates, modifies and eliminates project roles, and appoints or delegates
   the appointment of the people who hold them; and
 * amends this document.

The CSC delegates much of the day-to-day exercise of this mandate, but it
retains ultimate authority over every delegated power and may reverse or
withdraw any delegation at any time.

Membership
----------

The CSC has adopted an alternative approach to determining its voting members,
as permitted by Charter section 2.b: the voting members of the CSC are the
individuals listed in :ref:`csc-members` below, rather than the set of the
project's Maintainers.

 * Voting members are developers, users, and community members.
 * Members can be nominated by any existing member, and are added or removed
   by a supermajority vote of the CSC.
 * Anyone may attend CSC meetings as a non-voting participant.
 * Existing Ceph Leadership Team members were grandfathered in when the CSC
   was formed.
 * Membership is reflected by an email list, on the Ceph website, and in these
   documents.
 * Members may resign at any time.

Because CSC voting membership is decoupled from Maintainer status, being a
Maintainer does not confer a CSC vote, and CSC members are not automatically
Maintainers.

Meetings
--------

 * CSC meetings are open to the public and may be conducted electronically,
   via teleconference, or in person (Charter section 2.b).
 * A Co-Chair presides over meetings of the CSC (Charter section 2.f).
 * Meeting times, agendas, and notes are published so that the output of
   discussions and decisions is visible to the community.

.. _csc-voting:

Voting and decision making
--------------------------

The project aims to operate as a consensus-based community.  When a decision
requires a vote:

 * Voting members vote on a one-vote-per-voting-member basis (Charter section
   3.a).
 * Quorum for a CSC meeting requires at least fifty percent of all voting
   members of the CSC to be present.  The CSC may continue to meet without
   quorum, but cannot make decisions at that meeting (Charter section 3.b).
 * A decision taken by vote at a meeting requires a majority vote of those in
   attendance, provided quorum is met.  A decision made by electronic vote
   without a meeting requires a majority vote of all voting members of the CSC
   (Charter section 3.c).
 * If a vote cannot be resolved by the CSC, any voting member may refer the
   matter to the Series Manager for assistance in reaching a resolution
   (Charter section 3.d).

Two thresholds are set by the Charter itself and cannot be lowered:

 * Approving an inbound or outbound license exception requires a two-thirds
   vote of the entire CSC (Charter section 7.c).
 * Amending the Charter requires a two-thirds vote of the entire CSC and
   approval by LF Projects (Charter section 8.a).

The CSC additionally requires a supermajority for the following decisions.
These are self-imposed requirements that raise the bar above the Charter
minimum; they never lower it:

 * adding or removing a CSC voting member; and
 * amending this document.

.. note:: A "supermajority" is a 2/3 majority of votes cast on a particular
          item.  Abstaining does not bias a vote.  Where the Charter requires
          "two-thirds of the entire CSC", the threshold is computed against
          the full voting membership, not against the votes cast.

.. _csc-members:

Current Members
---------------

 * Adam C. Emerson <aemerson@redhat.com>
 * Adam King <adking@redhat.com>
 * Afreen Misbah <afreen@ibm.com>
 * Anthony D'Atri <anthony.datri@gmail.com>
 * Aviv Caro <Aviv.Caro@ibm.com>
 * Casey Bodley <cbodley@redhat.com>
 * Dan van der Ster <dan.vanderster@clyso.com>
 * David Orman <ormandj@1111systems.com>
 * Ernesto Puerta <epuertat@redhat.com>
 * Gaurav Sitlani <gsitlani@ibm.com>
 * Gregory Farnum <gfarnum@redhat.com>
 * Guillaume Abrioux <gabrioux@redhat.com>
 * Haomai Wang <haomai@xsky.com>
 * Igor Fedotov <igor.fedotov@croit.io>
 * Ilya Dryomov <idryomov@redhat.com>
 * Joseph Mundackal <jmundackal@bloomberg.net>
 * Josh Durgin <jdurgin@redhat.com>
 * João Eduardo Luis <joao@clyso.com>
 * Kyle Bader <kbader@ibm.com>
 * Laura Flores <lflores@ibm.com>
 * Mark Nelson <mark.nelson@clyso.com>
 * Matan Breizman <mbreizma@redhat.com>
 * Matt Benjamin <mbenjami@redhat.com>
 * Mike Perez <miperez@redhat.com>
 * Myoungwon Oh <ohmyoungwon@gmail.com>
 * Sage McTaggart <sagemct@ibm.com>
 * Neha Ojha <nojha@redhat.com>
 * Patrick Donnelly <pdonnell@ibm.com>
 * Radoslaw Zarzynski <rzarzyns@redhat.com>
 * Redouane Kachach <rkachach@redhat.com>
 * Venky Shankar <vshankar@redhat.com>
 * Vikhyat Umrao <vikhyat@ibm.com>
 * Xie Xingguo <xie.xingguo@zte.com.cn>
 * Yaarit Hatuka <yhatuka@ibm.com>
 * Yehuda Sadeh <yehuda@ui.com>
 * Yingxin Cheng <yingxin.cheng@intel.com>
 * Yuri Weinstein <yweinste@redhat.com>
 * Zac Dover <zac.dover@proton.me>

.. _cec:

Co-Chairs: the Ceph Executive Council
=====================================

Charter section 2.f permits the CSC to elect up to three Co-Chairs who share
the role of CSC Chair.  The CSC exercises that option through the Ceph
Executive Council: the three members elected to the Ceph Executive Council
automatically and by virtue of that election hold the formal office of
Co-Chair.  No separate election of Co-Chairs is held.  In this documentation
and elsewhere in the project, "Ceph Executive Council", "Council", and
"Co-Chairs" refer to the same three people.

Responsibilities and delegated powers
-------------------------------------

As Co-Chairs, the members of the Executive Council preside over meetings of
the CSC (Charter section 2.f).  The CSC further grants the Executive Council
the following powers, to be exercised subject to CSC oversight:

 * act as the arbiter in cases where a decision cannot be reached by
   consensus;
 * distribute key responsibilities amongst themselves or others, and delegate
   any of these powers;
 * serve as the point of contact for the project, and represent the project in
   public and to other organizations;
 * appoint representatives to work with other open source or open standards
   communities;
 * appoint and remove the holders of project roles described in
   :ref:`project-roles`, other than roles the CSC reserves to itself;
 * create working groups and sub-committees, which report back to the CSC;
 * approve the publication of releases, as described in
   :doc:`dev/release-process`; and
 * ensure things get done.

Limits on these powers
----------------------

The Executive Council's powers are delegated by the CSC and are revocable.
The Executive Council may not:

 * amend the Charter or this document;
 * add or remove CSC voting members;
 * promote or remove Maintainers, which requires a majority approval of the
   CSC (Charter section 2.c.iii);
 * approve license exceptions, which require a two-thirds vote of the entire
   CSC (Charter section 7.c); or
 * override a decision taken by a vote of the CSC.

Any decision of the Executive Council may be brought before the CSC, which
may reverse it by a vote taken under :ref:`csc-voting`.

Membership and composition
--------------------------

 * 3 people, elected by the CSC.
 * Terms are one year, with all members elected yearly (Charter section 2.f).
 * Members may resign at any time, and the CSC may vote to appoint a
   replacement for the remainder of the term.
 * The Co-Chairs may not all be employed by a single company or group of
   related companies.  In practice this means that at most two of the three
   members may share an employer.
 * If, during a term, more than two Co-Chairs become employed by the same
   company or group of related companies, the CSC decides which of those
   Co-Chairs ceases to serve as a Co-Chair (Charter section 2.f).

Elections
---------

Charter section 2.f requires that the process for electing Co-Chairs be
determined by the CSC and documented publicly.  That process is:

 * An election is held annually for all three seats.
 * Candidates self-nominate or are nominated by other members of the
   community.
 * Candidates are encouraged to discuss how they intend to delegate roles and
   responsibilities if elected.
 * The CSC's voting members elect the Council by ranked-choice vote; the three
   candidates selected by that vote take office as Co-Chairs.
 * Results are announced to the community and this document is updated.

Foundation liaison
------------------

The Executive Council represents the project at Ceph Foundation board
meetings.  The Council designates one of its members to serve a one-year term
as the primary communication contact between the project and the Ceph
Foundation, as required by Charter section 2.f, and informs the CSC of that
designation.

Current Members
---------------

* Dan van der Ster <dan.vanderster@clyso.com>
* Neha Ojha <nojha@redhat.com>
* Patrick Donnelly <pdonnell@ibm.com>

.. _project-roles:

Project roles
=============

The Charter provides for Contributors and Maintainers, and permits the CSC to
adopt, modify, refine, or eliminate roles and to create new ones, so long as
the roles are publicly documented (Charter sections 2.c and 2.e).  The roles
below are the roles the CSC has adopted.  Any additional role, and any change
to these roles, must be recorded in this document.

Unless stated otherwise, role holders are appointed by the Ceph Executive
Council, serve until they resign or are replaced, and are accountable to the
Executive Council and ultimately to the CSC.  Periodic rotation of these
responsibilities is encouraged.

Contributors
------------

Contributors include anyone in the technical community who contributes code,
documentation, or other technical artifacts to the project (Charter section
2.c.i).  Participation as a Contributor is open to anyone who abides by the
terms of the Charter (Charter section 2.d).

Contributions are subject to the project's intellectual property policy
(Charter section 7): inbound code contributions are made under a disjunctive
license choice of LGPL-2.1 OR LGPL-3.0, must carry a Developer Certificate of
Origin sign-off (https://developercertificate.org), and documentation is
contributed under the Creative Commons Attribution 4.0 International License.
See ``CONTRIBUTING.rst``, ``SubmittingPatches.rst``, and
:ref:`documenting_ceph` for the mechanics.

Maintainers
-----------

Maintainers are Contributors who have earned the ability to modify ("commit")
source code, documentation, or other technical artifacts in a project
repository (Charter section 2.c.ii).

 * A Contributor becomes a Maintainer by a majority approval of the CSC,
   normally on the recommendation of the relevant Component Team Lead
   (Charter section 2.c.iii).
 * A Maintainer may be removed by a majority approval of the CSC (Charter
   section 2.c.iii).
 * Maintainers are expected to review contributions in their area, uphold the
   project's contribution and licensing requirements, and keep the relevant
   entries in ``.github/CODEOWNERS`` accurate.

.. _ctl:

Component Team Leads
--------------------

A Component Team Lead manages a `component team`_ in Ceph.

Responsibilities:

 * ensure PRs for the component are reviewed and merged;
 * ensure severe bug fixes are backported;
 * run standups;
 * bug triage and scrubs; and
 * represent the component in cross-component discussions and in release
   planning.

Powers:

 * set review and merge priorities within the component;
 * recommend Contributors for promotion to Maintainer, and recommend the
   removal of Maintainers, for CSC approval; and
 * delegate any of the above within the component team.

Team leads are selected by the Executive Council, generally based on the
recommendation of team members and the outgoing lead.  Periodic rotation of
lead responsibility among team members is encouraged.

Documentation Lead
------------------

The Documentation Lead is responsible for the Ceph documentation as a whole.

Responsibilities:

 * maintain the documentation in the ``doc/`` tree and the toolchain that
   builds it;
 * review and merge documentation contributions, and help new contributors
   land their first documentation changes;
 * maintain the documentation style guide and the guidance in
   :ref:`documenting_ceph`; and
 * work with Component Team Leads and the Ceph Release Manager to ensure that
   user-visible changes and release notes are documented.

Powers:

 * set documentation standards and structure;
 * merge documentation changes, including changes outside any single
   component; and
 * delegate any of the above to other documentation contributors.

Ceph Release Manager
--------------------

The Ceph Release Manager is responsible for shepherding Ceph releases,
exercising the CSC's responsibility for issuing releases (Charter section
2.g.vi).

Responsibilities:

 * maintain the release schedule for the development branch and for the
   supported stable release series, as described in
   :ref:`ceph-releases-general`;
 * coordinate feature freeze, release candidates, and point releases with
   Component Team Leads and QE;
 * ensure that backports and tracker state for a release are complete and
   accurate, and that the release notes and changelog are prepared;
 * coordinate with the Security Lead on security releases, as described in
   :doc:`security/process`; and
 * announce releases and end-of-life dates to the community.

Powers:

 * declare feature freeze and set the content and timing of release
   candidates and point releases;
 * coordinate the build and signing steps described in :doc:`dev/release-process`,
   including work with the Build Lead; and
 * delegate any of the above.

Publication of a release is approved by the Executive Council.

Security Lead
-------------

The CSC designates a member as Security Lead, with responsibility for
coordinating the project's security posture, the intake and triage of
vulnerability reports, and the coordination of security releases.  See
:doc:`security/securitylead` for the full description of the role, and
:doc:`security/workinggroup` for the working group the Security Lead
coordinates.

Working groups and sub-committees
---------------------------------

The CSC may create sub-committees or working groups to focus on cross-project
technical issues and requirements (Charter section 2.g.iv), and has delegated
the creation of working groups to the Executive Council.  A working group
reports back to the CSC, and its scope, membership rules, and procedures are
documented in the Ceph documentation as a subordinate procedure (see
:ref:`operating-procedures`).

.. _operating-procedures:

Operating procedures
====================

Scope
-----

This document holds the project's governance-level procedures.  More detailed
operational procedures are maintained as subordinate documents in the Ceph
documentation, adopted by the CSC or by the role holder or working group to
which the CSC has delegated them.  Examples include:

 * ``CONTRIBUTING.rst`` and ``SubmittingPatches.rst`` — the contribution
   process;
 * :ref:`documenting_ceph` — the documentation process;
 * :ref:`ceph-releases-general` and :doc:`dev/release-process` — release cadence
   and release mechanics;
 * :doc:`security/process` and :doc:`security/workinggroup` — vulnerability
   handling.

Subordinate procedures are part of the project's publicly documented
governance and are subject to the same guardrails as this document.

Guardrails
----------

Any procedure adopted by the CSC, including this document, must be consistent
with the Charter, the Series Agreement for the project, the Operating
Agreement of LF Projects, the policies of LF Projects
(https://lfprojects.org/policies/), and the project's code of conduct.  A
CSC-adopted procedure may add detail to the Charter, and may impose stricter
requirements on the project than the Charter does, but it may not:

 * contradict the Charter;
 * lower any voting threshold or quorum requirement set by the Charter;
 * exclude any participant on any basis other than one that is reasonable and
   applied on a non-discriminatory basis to all Collaborators (Charter
   section 4.d);
 * alter the project's licensing or intellectual property terms, other than by
   the exception process in Charter section 7.c; or
 * reduce the transparency the Charter requires (Charter section 4.e).

A proposed procedure that could only take effect by changing the Charter must
instead be pursued as a Charter amendment under Charter section 8.a.

Amending this document
----------------------

 * Amendments to this document are proposed as a pull request against
   ``doc/governance.rst`` and announced to the CSC.
 * An amendment is adopted by a supermajority vote of the CSC voting members,
   taken at a meeting or electronically.
 * Subordinate procedures are adopted and amended by a majority vote of the
   CSC, or by the role holder or working group to which the CSC has delegated
   that procedure.
 * Purely factual updates — for example recording the outcome of an election,
   a role appointment, or a resignation — do not require a vote, and are made
   by or on behalf of the Executive Council.

Compliance and policies
=======================

 * **Code of conduct.**  The project's code of conduct is published at
   https://ceph.io/en/code-of-conduct/.  A project-specific code of conduct is
   subject to approval by the Series Manager; absent an approved
   project-specific code of conduct, the LF Projects Code of Conduct at
   https://lfprojects.org/policies applies to all Collaborators (Charter
   section 4.b).
 * **LF Projects policies.**  Contributors comply with the policies of LF
   Projects as adopted and amended, including those listed at
   https://lfprojects.org/policies/ (Charter section 4.a).
 * **Trademarks.**  LF Projects holds title to the Ceph trade and service
   marks.  Use of project trademarks must be fair use or in accordance with
   the applicable trademark usage guidelines (Charter section 5.a).
 * **Escalation.**  Concerns about compliance with the Charter, including
   potential violations of the transparency requirement, may be raised with
   the Series Manager (Charter sections 3.d and 4.e).

The Ceph Foundation
===================

The Ceph Foundation is organized as a directed fund under the Linux
Foundation and is tasked with supporting the Ceph project community
and ecosystem.  It has no direct control over the technical direction
of the Ceph open source project beyond offering feedback and input
into the collaborative development process.

For more information, see :ref:`foundation`.

.. _component team: https://ceph.io/en/community/team/
