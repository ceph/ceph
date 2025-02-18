.. _governance:

============
 Governance
============

The Ceph open source community is guided by a few different groups.

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

Responsibilities
----------------

 * Arbiter in cases where decisions cannot be reached by consensus
 * Distribute key responsibilities amongst themselves or others
 * Point of contact for the project
 * Representatives for Ceph foundation board meetings
 * Ensure things get done
   
Membership
----------

 * 3 people
 * Elected by the steering committee
 * Candidates self-nominate or are nominated by other members
 * Discussion of how roles/responsibilities may be delegated
 * Ranked-choice vote by the steering committee
 * 2 year terms, with one member being elected in even years, and the
   other two in odd years
 * Members may resign at any time, and the steering committee may vote
   to appoint a replacement for the rest of their term
 * members must involve >1 employer

Current Members
^^^^^^^^^^^^^^^

* Dan van der Ster <dan.vanderster@clyso.com>
* Josh Durgin <jdurgin@redhat.com>
* Neha Ojha <nojha@redhat.com>

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

 * Developers, users, community members
 * Members can be nominated and added/removed by existing members via a
   supermajority vote
 * Anyone may attend steering committee meetings as a non-voting participant
 * Existing Ceph Leadership Team members are grandfathered in
 * Membership reflected by an email list and on the Ceph website and
   docs

.. note:: A "supermajority" is a 2/3 majority of votes on a particular item
          in an election. Abstaining does not bias a vote.

Current Members
^^^^^^^^^^^^^^^

 * Adam King <adking@redhat.com>
 * Casey Bodley <cbodley@redhat.com>
 * Dan van der Ster <dan.vanderster@clyso.com>
 * David Orman <ormandj@1111systems.com>
 * Ernesto Puerta <epuertat@redhat.com>
 * Gregory Farnum <gfarnum@redhat.com>
 * Haomai Wang <haomai@xsky.com>
 * Ilya Dryomov <idryomov@redhat.com>
 * Igor Fedotov <igor.fedotov@croit.io>
 * Jeff Layton <jlayton@redhat.com>
 * Josh Durgin <jdurgin@redhat.com>
 * João Eduardo Luis <joao@clyso.com>
 * Ken Dreyer <kdreyer@redhat.com>
 * Mark Nelson <mark.nelson@clyso.com>
 * Matt Benjamin <mbenjami@redhat.com>
 * Mike Perez <miperez@redhat.com>
 * Myoungwon Oh <myoungwon.oh@samsung.com>
 * Neha Ojha <nojha@redhat.com>
 * Patrick Donnelly <pdonnell@ibm.com>
 * Sam Just <sjust@redhat.com>
 * Vikhyat Umrao <vikhyat@redhat.com>
 * Xie Xingguo <xie.xingguo@zte.com.cn>
 * Yehuda Sadeh <yehuda@redhat.com>
 * Yingxin Cheng <yingxin.cheng@intel.com>
 * Yuri Weinstein <yweinste@redhat.com>
 * Zac Dover <zac.dover@proton.me>
 * Laura Flores <lflores@redhat.com>
 * Venky Shankar <vshankar@redhat.com>
 * Guillaume Abrioux <gabrioux@redhat.com>
 * Anthony D'Atri <anthony.datri@gmail.com>
 * Joseph Mundackal <jmundackal@bloomberg.net>
 * Gaurav Sitlani <gsitlani@ibm.com>
 * Afreen Misbah <afreen@ibm.com>
 * Radoslaw Zarzynski <rzarzyns@redhat.com>
 * Matan Breizman <mbreizma@redhat.com>
 * Yaarit Hatuka <yhatuka@ibm.com>
 * Adam C. Emerson <aemerson@redhat.com>

.. _ctl:

Component Team Leads
====================

Responsibilities
----------------

 * Manage a `component team`_ in Ceph
 * Ensure PRs are reviewed and merged
 * Ensure severe bug fixes are backported
 * Maintain roadmap for the component
 * Update slides and present about component
 * Run standups
 * Bug triage, scrubs
 * etc.

Team leads are selected by the executive council, generally based on
the recommendation by team members and outgoing lead.  Periodic
rotation of lead responsibility among team members is encouraged.

Current Component Leads
^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Component
     - Description
   * - Adam King
     - Cephadm
     - Ceph's native orchestrator (install, upgrade...)
   * - Venky Shankar
     - CephFS
     - Shared filesystem
   * - Matan Breizman
     - Crimson
     - Next generation implementation of the OSD
   * - Yingxin Cheng
     - Seastore
     - Next generation storage underlying the OSD
   * - Afreen Misbah
     - Dashboard
     - Ceph's user interface for management and monitoring
   * - Radoslaw Zarzynski
     - RADOS
     - Ceph's low level storage foundation
   * - Ilya Dryomov
     - RBD
     - Block storage for containers and VMs
   * - Adam Emerson, Eric Ivancich
     - RGW
     - S3/Swift compatible object storage


General Leads
=============

These are general areas rather than specific subsystems within Ceph.
Each is unique.

Documentation
-------------

Responsibilities
^^^^^^^^^^^^^^^^
* Update the documentation to describe new capabilities.
* Solicit user feedback and respond to it.
* Maintain communications channels so that members of the upstream community
  have confidence that their documentation concerns are recognized and
  addressed.
* Correct the structure of sentences as needed.
* Ensure that the .rst files and the .md files that constitute the
  documentation are properly formatted and that Sphinx renders them as
  intended.
* Ensure that correct information is backported from the ``main`` branch to the
  documentation release branches.
* Communicate with component leads and other authorities to ensure the
  technical accuracy of the documentation. Alter the documentation as necessary
  when inaccuracies are discovered.
* Report persistent Jenkins-check failures to the #sepia channel.
* Enhance the ability of the community to contribute to documentation.
* Facilitate timely PR reviews.
* Facilitate the ceph.io blog site.
* Documentation kaizen: restructure and refactor documentation. Modernize
  obsolete content. Reflect the evolving Ceph landscape.

Current Lead: Zac Dover

Performance
-----------

Responsibilities
^^^^^^^^^^^^^^^^
* Monitor performance-related PRs
* Analyze the performance of new changes and releases
* Facilitate discussion around performance topics in the Performance Weekly meetings
* ...

Current Lead: Mark Nelson


The Ceph Foundation
-------------------

The Ceph Foundation is organized as a directed fund under the Linux
Foundation and is tasked with supporting the Ceph project community
and ecosystem.  It has no direct control over the technical direction
of the Ceph open source project beyond offering feedback and input
into the collaborative development process.

For more information, see :ref:`foundation`.

.. _component team: https://ceph.io/en/community/team/
