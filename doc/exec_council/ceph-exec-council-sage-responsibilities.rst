.. _ceph-exec-council-sage-responsibilities:

Sage Weil's Responsibilities
============================

.. warning:: This is an archived version of a document created by Sage Weil. It
   is included here exactly as Sage wrote it. It has not been corrected for
   grammar or spelling and is meant to be stored here as a historical artifact
   and not as a living part of the Ceph documentation.

* Mid-term vision

  * i.e., welcome/keynote for cephalocon
* Long term vision
* Sage industry/academy connections

  * CROSS

  * Universities
* Cross-cutting architecture

  * Ceph vs rook vs rhel vs openstack etc.
* Community focal point (user interaction, conference talks, mailing list, etc)
* Community management guidance

  * Regularly meet with Mike (turn into regular community call on the ceph calendar?)https://pad.ceph.com/p/sage-responsibilities

  * Coordinate blog posts, board meetings, t-shirts, events, website, other random community stuff

  * Event planning (cephalocon, ceph days, etc.)

  * Random roadmap presentations throughout the year
* Talk to prospective foundation members
* Talk to potential community partners

  * E.g., various arm partners
* Talk to the Ceph Foundation
  * run the board meetings

  * making sure those board members have the feeling they're valued
* Interface with Linux Foundation folks
  * Ceph training

  * Marketing WG

  * Conf/event organization

  * Ceph on Windows

  * Guiding docs

* Present in weekly leads call
* Present on dev list
* (sometimes) present on ceph-users list
* Make sure releases get named
* New release kickoff (e.g. https://github.com/ceph/ceph/pull/39039, object corpus update)

    [sebastian]: Can we split this up and move it to the component leads?

* Promote misc initiatives (e.g., telemetry)
* Keep CLT call productive, make sure it includes important/systemic/process issues
* Need to figure out OVH billing

    Who's paying that bill right now?

      The Linux Foundation by way of Sage's PayPal.  TLF doesn't have a credit card we can use that works and OVH doesn't accept wire transfers.  OVH charges Sage's PayPal and TLF reimburses him.

* access to a real cluster in the basement
* trello board up to date
* Teuthology is green

Governance
----------
* Council/Other name for subgroup to serve as face of Ceph / communication
  focal point

  * Ability to change governance process
  * Ensure CLT is functioning effectively, focus on whole project/cross-component
  * Responsible for making sure former Sage responsibilites above ^ are handled
  * Model 1

    * 3-person council/triad, chosen for each release cycle
    * responsible for spending some fraction of time on cross-project issues
    * CLT call run by rotating people
    * Council elected from among CLT? devs?
  * Model 2

    * Continue with existing CLT
    * Encourage leads to consider cross-project work
  * Model 3

    * BDFL
    * not scalable
    * not reliable long term

Proposals

    Patrick's proposal: https://pad.ceph.com/p/pdonnell-ceph-leadership-organization

    Greg's discussion and proposals: https://pad.ceph.com/p/gfarnum-organization-clt

    Sage's proposal: https://pad.ceph.com/p/sage-governance-clt

    Sebastian https://pad.ceph.com/p/sebastian-governance-clt (very minor changes, based on Sage)



Common threads

    Bodies

    [Executive] Council

    elected arbiters

    CLT / Congress

    team leads + senior developers + invited members

    Developers / committers

    [Foundation] Board

    Emphasis on consensus

    Council is used for conflict resolution, not deciders-in-chief

    Delegation of responsibilities/roles

    Council responsible for delegating explicit roles and/or responsibilities to ensure work remains on track

    No term limits


Greg

    mostly consensus-based

    who elects the council?

Patrick

    Roles/responsibilities to delegate

    limit influence of individual company


Key decisions / discussion areas

    Council size: 3 or 5?

    3: +1 +1 +1 +1 +1 +1 +1+1 +1+1+1

    5: 

    Nominees?

    Josh

    wrangle the testing

    Neha

    Sebastian

    pushing for more unsability

    Patrick

    Greg

    Dan

    fight for users

    Countil terms lengths / timing

    Who votes for Council members?

    CLT?  Committers?  _ + Users?

    How to choose nominees?  How do nominees make their case?

    How is CLT membership determined?

    ad hoc? developers? who shows up?

    members vote new members in

    members term out

    Naming

    CLT vs CC

    Executive Council vs Council vs __

    Ceph Foundation Board

    select new/formal Red Hat representative

    CLT/Council representative/role on board

    do they run the meeting?  is the council rep different than the CLT rep?  where does community manager fit in?


[sage] Proposal:

    Discuss proposals / issues today

    Decide on a minimal interim model going forward -- how to operate until Quincy is done

    At Cephalocon dev summit, have a retrospective to select a model going forward


Roles

    Events

    Cephalocon

    Community / Partner / Foundation board outreach

    primary interface with miperez

    build outreach, communications channels with users, partners, board members

    Quality

    pay attention to test coverage

    track ceph-users chatter

    bug triage, scrubs

    test infrastructure

    Release

    track features pending for next release

    ensure various components are communicating and on track

    maintain inter-component communication (e.g., is component feature X surfaced in dashboard?  how does it affect rook/cephadm?)

    regularly sync with component leads?


voting: https://civs1.civs.us/


References:
    Python governance proposals:
        https://www.python.org/dev/peps/pe-8016/ (accepted)
        https://www.python.org/dev/peps/pep-8012/ (rejected)
        https://www.python.org/dev/peps/pep-0013/ (current process)



