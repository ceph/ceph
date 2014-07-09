=====================
 Add/Remove Monitors
=====================

With ``ceph-deploy``, adding and removing monitors is a simple task. You just
add or remove one or more monitors on the command line with one command. Before
``ceph-deploy``,  the process of `adding and removing monitors`_ involved
numerous manual steps. Using ``ceph-deploy`` imposes a restriction:  **you may
only install one monitor per host.**

.. note:: We do not recommend comingling monitors and OSDs on 
   the same host.

For high availability, you should run a production Ceph cluster with **AT
LEAST** three monitors. Ceph uses the Paxos algorithm, which requires a
consensus among the majority of monitors in a quorum. With Paxos, the monitors
cannot determine a majority for establishing a quorum with only two monitors. A
majority of monitors must be counted as such: 1:1, 2:3, 3:4, 3:5, 4:6, etc.

See `Monitor Config Reference`_ for details on configuring monitors.


Add a Monitor
=============

Once you create a cluster and install Ceph packages to the monitor host(s), you
may deploy the monitor(s) to the monitor host(s). When using ``ceph-deploy``,
the tool enforces a single monitor per host. ::

	ceph-deploy mon create {host-name [host-name]...}


.. note:: Ensure that you add monitors such that they may arrive at a consensus
   among a majority of monitors, otherwise other steps (like ``ceph-deploy gatherkeys``)
   will fail.

.. note::  When adding a monitor on a host that was not in hosts initially defined
   with the ``ceph-deploy new`` command, a ``public network`` statement needs
   to be added to the ceph.conf file.

Remove a Monitor
================

If you have a monitor in your cluster that you'd like to remove, you may use 
the ``destroy`` option. :: 

	ceph-deploy mon destroy {host-name [host-name]...}


.. note:: Ensure that if you remove a monitor, the remaining monitors will be 
   able to establish a consensus. If that is not possible, consider adding a 
   monitor before removing the monitor you would like to take offline.


.. _adding and removing monitors: ../../operations/add-or-rm-mons
.. _Monitor Config Reference: ../../configuration/mon-config-ref
