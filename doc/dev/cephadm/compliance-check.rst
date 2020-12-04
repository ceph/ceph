================
Compliance Check
================

The stability and reliability of a Ceph cluster is dependent not just upon the Ceph daemons, but
also the OS and hardware that Ceph is installed on. This document is intended to promote a design 
discussion for providing a "compliance" feature within mgr/cephadm, which would be responsible for
identifying common platform-related issues that could impact Ceph stability and operation.

The ultimate goal of these checks is to identify issues early and raise a healthcheck WARN
event, to alert the Administrator to the issue.

Prerequisites
=============
In order to effectively analyse the hosts that Ceph is deployed to, this feature requires a cache
of host-related metadata. The metadata is already available from cephadm's HostFacts class and the
``gather-facts`` cephadm command. For the purposes of this document, we will assume that this
data is available within the mgr/cephadm "cache" structure.

Some checks will require that the host status is also populated e.g. ONLINE, OFFLINE, MAINTENANCE

Administrator Interaction
=========================
Not all users will require this feature, and must be able to 'opt out'. For this reason,
mgr/cephadm must provide controls, such as the following;

.. code-block::

   ceph cephadm compliance enable | disable | status [--format json]
   ceph cephadm compliance ls [--format json]
   ceph cephadm compliance enable-check <name>
   ceph cephadm compliance disable-check <name>
   ceph cephadm compliance set-check-interval <int>
   ceph cephadm compliance get-check-interval

The status option would show the enabled/disabled state of the feature, along with the
check-interval.

The ``ls`` subcommand would show all checks in the following format;

``check-name status description``

Proposed Integration
====================
The compliance checks are not required to run all the time, but instead should run at discrete
intervals. The interval would be configurable under via the :code:`set-check-interval`
subcommand (default would be every 12 hours)


mgr/cephadm currently executes an event driven (time based) serve loop to act on deploy/remove and
reconcile activity. In order to execute the compliance checks, the compliance check code would be 
called from this main serve loop - when the :code:`set-check-interval` is met.


Proposed Checks
===============
All checks would push any errors to a list, so multiple issues can be escalated to the Admin at
the same time. The list below provides a description of each check, with the text following the 
name indicating a shortname version *(the shortname is the reference for command Interaction
when enabling or disabling a check)*


OS Consistency (OS)
___________________
* all hosts must use same vendor
* all hosts must be on the same major release (this check would only be applicable to distributions that
  offer a long-term-support strategy (RHEL, CentOS, SLES, Ubuntu etc)


*src: gather-facts output*

Linux Kernel Security Mode (LSM)
________________________________
* All hosts should have a consistent SELINUX/AppArmor configuration

*src: gather-facts output*

Services Check (SERVICES)
_________________________
Hosts that are in an ONLINE state should adhere to the following;

* all daemons (systemd units) should be enabled
* all daemons should be running (not dead)

*src: list_daemons output*

Support Status (SUPPORT)
________________________
If support status has been detected, it should be consistent across all hosts. At this point
support status is available only for Red Hat machines.

*src: gather-facts output*

Network : MTU (MTU)
________________________________
All network interfaces on the same Ceph network (public/cluster) should have the same MTU

*src: gather-facts output*

Network : LinkSpeed (LINKSPEED)
____________________________________________
All network interfaces on the same Ceph network (public/cluster) should have the same Linkspeed

*src: gather-facts output*

Network : Consistency (INTERFACE)
______________________________________________
All hosts with OSDs should have consistent network configuration - eg. if some hosts do
not separate cluster/public traffic but others do, that is an anomaly that would generate a
compliance check warning.

*src: gather-facts output*

Notification Strategy
=====================
If any of the checks fail, mgr/cephadm would raise a WARN level alert

Futures
=======
The checks highlighted here serve only as a starting point, and we should expect to expand
on the checks over time.
