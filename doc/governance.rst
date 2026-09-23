.. _governance:

====================================
 The Ceph Steering Committee Bylaws
====================================

The Ceph project is established as Ceph a Series of LF Projects, LLC, and is
governed by its :ref:`technical-charter`.  This document records the roles and
procedures that the Ceph Steering Committee (CSC) has adopted under the
Charter, and is part of the project's CONTRIBUTING file for the purposes of the
Charter.  Where the two conflict, the Charter prevails.

Key principles
==============

 * Decision-making is consensus-driven by those who participate.
 * Leadership roles are defined primarily by responsibility, not prestige or seniority.
 * It is normal and healthy for these roles to be passed on to others
 * Everyone's role is ultimately to serve the users and participation
   is voluntary.

Bodies
------

Ceph Executive Council
======================

The CSC elects three Co-Chairs (Charter section 2.f), who together form the
Ceph Executive Council.

Responsibilities
----------------

 * Preside over CSC meetings
 * Arbiter in cases where decisions cannot be reached by consensus
 * Distribute key responsibilities amongst themselves or others
 * Point of contact for the project
 * Representatives for Ceph foundation board meetings; one member is
   designated for a one-year term as the primary contact with the Ceph
   Foundation
 * Ensure things get done
   
Membership
----------

 * 3 people
 * Elected by the steering committee
 * Candidates self-nominate or are nominated by other members
 * Discussion of how roles/responsibilities may be delegated
 * Ranked-choice vote by the steering committee.  If the top three
   candidates are employed by the same company or group of related
   companies, the lowest ranked of them is passed over for the next
   candidate.
 * 1 year terms with all members elected yearly, before the current terms
   end
 * Members may resign at any time, and the steering committee may vote
   to appoint a replacement for the rest of their term
 * Members may not all be employed by the same company or group of related
   companies.  If that happens during a term, the steering committee decides
   which of them steps down.

Current Members
^^^^^^^^^^^^^^^

* Dan van der Ster <dan.vanderster@clyso.com>
* Neha Ojha <nojha@redhat.com>
* Patrick Donnelly <pdonnell@ibm.com>

.. _csc:

Ceph Steering Committee
=======================

Responsibilities
----------------

 * Elect executive council
 * Amend governance model by supermajority vote
 * Meet regularly to discuss and decide on tactical and strategic projects
   and improvements
 * Hold an annual election

Membership
----------

The CSC chooses its own voting members as described below.  This is the
alternative to Maintainer-based membership allowed by Charter section 2.b.

 * Members may be developers, users, or other community members
 * Members can be nominated and added/removed by existing members via a
   supermajority vote
 * Anyone may attend steering committee meetings as a non-voting participant
 * The list below is the authoritative membership; the CSC email list and
   the Ceph website follow it
 * Members may resign at any time by email to the Co-Chairs

.. note:: A "supermajority" is a 2/3 majority of votes on a particular item
          in an election. Abstaining does not bias a vote.

Voting
------

The CSC aims to decide by consensus.  When a vote is needed it follows
Charter section 3: one vote per member, quorum is half of all members, and a
decision needs a majority of those present at a meeting, or of all members
for an electronic vote.  A supermajority requirement above raises that bar;
it never lowers it.

Current Members
^^^^^^^^^^^^^^^

 * Adam C. Emerson <aemerson@redhat.com>
 * Adam King <adking@redhat.com>
 * Afreen Misbah <afreen@ibm.com>
 * Anthony D'Atri <anthony.datri@gmail.com>
 * Aviv Caro <Aviv.Caro@ibm.com>
 * Bill Scales <bill_scales@uk.ibm.com>
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
 * Neha Ojha <nojha@redhat.com>
 * Patrick Donnelly <pdonnell@ibm.com>
 * Radoslaw Zarzynski <rzarzyns@redhat.com>
 * Redouane Kachach <rkachach@redhat.com>
 * Sage McTaggart <sagemct@ibm.com>
 * Venky Shankar <vshankar@redhat.com>
 * Vikhyat Umrao <vikhyat@ibm.com>
 * Xie Xingguo <xie.xingguo@zte.com.cn>
 * Yaarit Hatuka <yhatuka@ibm.com>
 * Yehuda Sadeh <yehuda@ui.com>
 * Yingxin Cheng <yingxin.cheng@intel.com>
 * Yuri Weinstein <yweinste@redhat.com>
 * Zac Dover <zac.dover@proton.me>

.. _ctl:

Component Team Leads
====================

Responsibilities
----------------

 * Manage a `component team`_ in Ceph
 * Ensure PRs are reviewed and merged
 * Ensure severe bug fixes are backported
 * Run standups
 * Bug triage, scrubs
 * etc.

Team leads are selected by the executive council, generally based on
the recommendation by team members and outgoing lead.  Periodic
rotation of lead responsibility among team members is encouraged.

The Ceph Foundation
-------------------

The Ceph Foundation is organized as a directed fund under the Linux
Foundation and is tasked with supporting the Ceph project community
and ecosystem.  It has no direct control over the technical direction
of the Ceph open source project beyond offering feedback and input
into the collaborative development process.

For more information, see :ref:`foundation`.

.. _component team: https://ceph.io/en/community/team/
