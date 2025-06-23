.. _stretch_mode:

================
Stretch Clusters
================


Stretch Clusters
================

A stretch cluster spreads hosts across geographically separated
data centers, with specific internal strategies to robustly localize
network traffic and respond to failures.
Stretch clusters have a higher
likelihood of (possibly asymmetric) network splits, and a higher likelihood of
temporary or complete loss of an entire data center (which can represent
one-third to one-half of the total cluster).

Ceph is designed with the expectation that all parts of its network and cluster
will be reliable and that failures will be distributed randomly across the
CRUSH topology. If a host or network switch goes down and causes the loss of
many OSDs, Ceph is designed so that the remaining OSDs and monitors will route
around such a loss. 

Sometimes this cannot be relied upon. If you have a "stretched-cluster"
deployment in which much of your cluster is behind a single network component,
you might need to use the explicit *stretch mode* to ensure data integrity.

We will here consider two standard configurations: a configuration with two
data centers (or, in clouds, two availability zones), and a configuration with
three data centers.

In the two-site configuration, Ceph arranges for each site to hold a copy of
the data, with a third site that has a tiebreaker (arbiter, witness)
monitor. This tiebreaker monitor picks a winner when a network connection
between sites fails and both data centers remain alive.

The tiebreaker monitor can be a VM. It can also have higher network latency
to the two main sites.

The standard Ceph configuration is able to survive many network failures or
data-center failures without compromising data availability. When enough
cluster components are brought back following a failure, the cluster will recover.
If you lose a data center but are still able to form a quorum of monitors and
still have replicas of all data available, Ceph will maintain availability. This
assumes that the cluster has enough copies to satisfy the pools' ``min_size``
configuration option, or (failing that) that the cluster has CRUSH rules in
place that will cause the cluster to re-replicate the data until the
``min_size`` configuration option has been met.

Stretch Cluster Issues
======================

Ceph does not compromise data integrity and data consistency
under any circumstances. When service is restored after a network failure or a
loss of Ceph nodes, Ceph will return to a state of normal function
without human intervention.  

Ceph does not permit the compromise of data integrity or data consistency, but
there are situations in which *data availability* is compromised. These
situations can occur even though there are sufficient replicas of data
available to satisfy consistency and sizing constraints. In some situations,
you might discover that your cluster does not satisfy those constraints.

The first category of these failures that we will discuss involves inconsistent
networks. If there is a netsplit (a failure that
splits the network into two conceptual islands that cannot communicate with
each other), Ceph might be unable to mark OSDs ``down``
and remove them from Placement Group (PG) acting sets. This failure to mark ODSs ``down``
will occur despite the fact that the primary PG is unable to replicate data (a
situation that, under normal non-netsplit circumstances, would result in the
marking of affected OSDs as ``down`` and their removal from the PG). When this
happens, Ceph will be unable to satisfy durability guarantees and
consequently IO will not be permitted.

The second category of failures that we will discuss are those in
which the constraints are not sufficient to guarantee the replication of data
across data centers, though it might seem that the data is correctly replicated.
For example, in a scenario in which there are two data
centers named Data Center A and Data Center B, and a CRUSH rule targets three
replicas and places a replica in each data center for a pool with ``min_size=2``,
the PG might go active with two replicas in Data Center A and zero replicas in
Data Center B. In this situation, the loss of Data Center A means
that the data is unavailable and Ceph and clients will not be able to operate on it. This
situation is difficult to avoid using only conventional CRUSH rules.

Individual Stretch Pools
========================
Setting individual ``stretch pool`` attributes allows for
specific pools to be distributed across two or more data centers.
This is done by executing the ``ceph osd pool stretch set`` command on each desired pool,
contrasted with a cluster-wide strategy with *stretch mode*.
See :ref:`setting_values_for_a_stretch_pool`

Use stretch mode when you have exactly two data centers and require a uniform
configuration across the entire cluster. Conversely, opt for a stretch pool
when you need only a particular pool to be replicated across more than two data centers,
providing a more granular level of control.

Limitations
-----------

Individual stretch pools do not support I/O operations during a netsplit
scenario between two or more zones. While the cluster remains accessible for
basic Ceph commands, I/O remains unavailable until the netsplit is
resolved. This is different from stretch mode, where the tiebreaker monitor
can isolate one CRUSH ``datacenter`` and serve I/O operations in degraded
mode during a netsplit. See :ref:`stretch_mode1`

Ceph is designed to tolerate multiple component failures. However, if more than 25% of
the OSDs in the cluster go down, Ceph may stop marking OSDs ``out``, which prevents rebalancing
and may result in PGs becoming ``inactive``. This behavior
is controlled by the ``mon_osd_min_in_ratio`` option.
The default value is ``0.75``, meaning that at least 75% of the OSDs
in the cluster must be ``active`` for any additional OSDs to be marked out.
This setting prevents too many OSDs from being marked out as this might lead to
cascading failures and an impactful thundering herd of data movement. This can
cause substantial client impact and long recovery times when OSDs return to
service. If Ceph stops marking OSDs ``out``, some PGs may fail to
rebalance to surviving OSDs, potentially leading to ``inactive`` PGs.
See https://tracker.ceph.com/issues/68338 for more information.

.. _stretch_mode1:

Stretch Mode
============

Stretch mode is designed to handle netsplit scenarios between two data centers as well
as the loss of one data center. It handles the netsplit scenario by choosing the surviving zone
that has the best connection to the tiebreaker Monitor. It handles the loss of one data center by
reducing the ``min_size`` of all pools to ``1``, allowing the cluster to continue operating
within the surviving data center. When the unavailable data center comes back, Ceph will
converge according to configured replication policy and return to normal operation.

Connectivity Monitor Election Strategy
---------------------------------------
When using stretch mode, the Monitor election strategy must be set to ``connectivity``.
This strategy tracks network connectivity between Monitors and is
used to determine which data center should be favored when the cluster
experiences netsplit.

See `Changing Monitor Elections`_

Stretch Peering Rule
--------------------
One critical behavior of stretch mode is its ability to prevent a PG from going ``active`` if the acting set
contains only replicas from a single data center. This safeguard is crucial for mitigating the risk of data
loss during site failures because if a PG were allowed to go ``active`` with replicas only at a single site,
writes could be acknowledged despite a lack of redundancy. In the event of a site failure, all data in the
affected PG would be lost.

Entering Stretch Mode
---------------------

To enable stretch mode, you must set the location of each monitor, correlating
with the CRUSH topology.

#. Place ``mon.a`` in your first data center:

   .. prompt:: bash $

      ceph mon set_location a datacenter=site1

#. Generate a CRUSH rule that places two copies in each data center.
   This requires editing the CRUSH map directly:

   .. prompt:: bash $

      ceph osd getcrushmap > crush.map.bin
      crushtool -d crush.map.bin -o crush.map.txt

#. Edit the ``crush.map.txt`` file to add a new rule. Here there is only one
   other rule (``id 1``), but you will likely need to use a different, unique rule ID. We
   have two ``datacenter`` buckets named ``site1`` and ``site2``:

   ::

      rule stretch_rule {
             id 1
             type replicated
             step take site1
             step chooseleaf firstn 2 type host
             step emit
             step take site2
             step chooseleaf firstn 2 type host
             step emit
     }

   .. warning:: If a CRUSH rule is defined in stretch mode cluster and the
      rule has multiple ``take`` steps, then ``MAX AVAIL`` for the pools
      associated with the CRUSH rule will report that the available size is all
      of the available space from the datacenter, not the available space for
      the pools associated with the CRUSH rule.
   
      For example, consider a cluster with two CRUSH rules, ``stretch_rule`` and
      ``stretch_replicated_rule``::

         rule stretch_rule {
              id 1
              type replicated
              step take DC1
              step chooseleaf firstn 2 type host
              step emit
              step take DC2
              step chooseleaf firstn 2 type host
              step emit
         }
         
         rule stretch_replicated_rule {
                 id 2
                 type replicated
                 step take default
                 step choose firstn 0 type datacenter
                 step chooseleaf firstn 2 type host
                 step emit
         }

      In the above example, ``stretch_rule`` will report an incorrect value for
      ``MAX AVAIL``. ``stretch_replicated_rule`` will report the correct value.
      This is because ``stretch_rule`` is defined in such a way that
      ``PGMap::get_rule_avail`` considers only the available capacity of a single
      ``datacenter``, and not (as would be correct) the total available capacity from
      both ``datacenters``.
      
      Here is a workaround. Instead of defining the stretch rule as defined in
      the ``stretch_rule`` above, define it as follows::

         rule stretch_rule {
           id 2
           type replicated
           step take default
           step choose firstn 0 type datacenter
           step chooseleaf firstn 2 type host
           step emit
         }

      See https://tracker.ceph.com/issues/56650 for more detail on this workaround.

   *The above procedure was developed in May and June of 2024 by Prashant Dhange.*

#. Compile and inject the CRUSH map to make the rule available to the cluster:

   .. prompt:: bash $

      crushtool -c crush.map.txt -o crush2.map.bin
      ceph osd setcrushmap -i crush2.map.bin

#. Run the Monitors in ``connectivity`` mode. See `Changing Monitor Elections`_.

   .. prompt:: bash $

      ceph mon set election_strategy connectivity

#. Direct the cluster to enter stretch mode. In this example, ``mon.e`` is the
   tiebreaker Monitor and we are splitting across CRUSH ``datacenters``. The tiebreaker
   monitor must be assigned a CRUSH ``datacenter`` that is neither ``site1`` nor
   ``site2``. This data center **should not** be predefined in your CRUSH map. Here 
   we are placing ``mon.e`` in a virtual data center named ``site3``:

   .. prompt:: bash $

      ceph mon set_location e datacenter=site3
      ceph mon enable_stretch_mode e stretch_rule datacenter

When stretch mode is enabled, PGs will become active only when they peer across
CRUSH ``datacenter``s (or across whichever CRUSH bucket type was specified),
assuming both are available. Pools will increase in size from the default ``3``
to ``4``, and two replicas will be placed at each site. OSDs will be allowed to
connect to Monitors only if they are in the same data center as the Monitors.
New Monitors will not be allowed to join the cluster if they do not specify a
CRUSH location.

If all OSDs and Monitors in one of the ``datacenter``s become inaccessible at once,
the cluster in the surviving ``datacenter`` enters  *degraded stretch mode*.
A health state warning will be
raised, pools' ``min_size`` will be reduced to ``1``, and the cluster will be
allowed to go active with the components and data at the single remaining site. Pool ``size``
does not change, so warnings will be raised that the PGs are undersized,
but a special stretch mode flag will prevent the OSDs from
creating extra copies in the remaining data center. This means that the data
center will keep only two copies, just as before.

When the inaccessible ``datacenter`` comes back, the cluster will enter *recovery
stretch mode*. This changes the warning and allows peering, but requires OSDs
only from the ``datacenter`` that was ``up`` throughout the duration of the
downtime. When all PGs are in a known state, and are neither degraded nor
undersized / incomplete, the cluster transitions back to regular stretch mode, ends the
warning, restores pools' ``min_size`` to its original value of ``2``, requires
PGs at both sites to peer, and no longer requires the site that was up throughout the
duration of the downtime when peering. This makes failover to the other site
possible, if needed.

.. _Changing Monitor elections: ../change-mon-elections

Exiting Stretch Mode
--------------------
To exit stretch mode, run the following command:

.. prompt:: bash $

   ceph mon disable_stretch_mode [{crush_rule}] --yes-i-really-mean-it


.. describe:: {crush_rule}

   The CRUSH rule to now use for all pools. If this
   is not specified, the pools will move to the default CRUSH rule.

   :Type: String
   :Required: No.

This command moves the cluster back to normal mode; the cluster will no longer
be in stretch mode.  All pools will be set with their prior ``size`` and
``min_size`` values. At this point the user is responsible for scaling down the
cluster to the desired number of OSDs if they choose to operate with fewer
OSDs.

Please note that the command will not execute when the cluster is in
recovery stretch mode. The command will only execute when the cluster
is in degraded stretch mode or healthy stretch mode.

Limitations of Stretch Mode 
===========================
When using stretch mode, OSDs must be located at exactly two sites. 

Two Monitors must be run in each data center, plus a tiebreaker in a third
(possibly in the cloud) for a total of five Monitors. While in stretch mode, OSDs
will connect only to Monitors within the data center in which they are located.
OSDs *DO NOT* connect to the tiebreaker monitor.

Erasure-coded pools cannot be used with stretch mode. Attempts to use erasure
coded pools with stretch mode will fail. Erasure coded pools cannot be created
while in stretch mode. 

To use stretch mode, you will need to create a CRUSH rule that provides two
replicas in each data center. Ensure that there are four total replicas: two in
each data center. If pools exist in the cluster that do not have the default
``size`` or ``min_size``, Ceph will not enter stretch mode. An example of such
a CRUSH rule is given above.

Because stretch mode runs with poos' ``min_size`` set to ``1``
, we recommend enabling stretch mode only when using OSDs on
SSDs. Hybrid HDD+SSD or HDD-only OSDs are not recommended
due to the long time it takes for them to recover after connectivity between
data centers has been restored. This reduces the potential for data loss.

.. warning:: CRUSH rules that specify a device class are not supported in stretch mode.
   For example, the following rule specifying the ``ssd`` device class will not work::

      rule stretch_replicated_rule {
                 id 2
                 type replicated class ssd
                 step take default
                 step choose firstn 0 type datacenter
                 step chooseleaf firstn 2 type host
                 step emit
      }

In the future, stretch mode could support erasure-coded pools,
enable deployments across more than two data centers,
and accommodate multiple CRUSH device classes.

Other commands
==============

Replacing a failed tiebreaker monitor
-------------------------------------

Deploy a new Monitor and run the following command:

.. prompt:: bash $

   ceph mon set_new_tiebreaker mon.<new_mon_name>

This command protests if the new Monitor is in the same CRUSH location as the
existing, non-tiebreaker monitors. This command will not remove the previous
tiebreaker monitor. If appropriate, you must remove the previous tiebreaker
Monitor manually.

Using "--set-crush-location" and not "ceph mon set_location"
------------------------------------------------------------

If you employ your own tooling for deploying Ceph, use the
``--set-crush-location`` option when booting Monitors instead of running ``ceph
mon set_location``. This option accepts only a single ``bucket=loc`` parameter, for
example ``ceph-mon --set-crush-location 'datacenter=a'``, and that parameter's
CRUSH bucket type must match the bucket type that was specified when running ``enable_stretch_mode``.

Forcing recovery stretch mode
-----------------------------

When in stretch degraded mode, the cluster will go into recovery mode
automatically when the disconnected data center comes back. If that does not
happen or you want to enable recovery mode early, run the following command:

.. prompt:: bash $

   ceph osd force_recovery_stretch_mode --yes-i-really-mean-it

Forcing normal stretch mode
---------------------------

When in recovery mode, the cluster should go back into normal stretch mode when
the PGs are healthy. If this fails to happen or if you want to force
cross-data-center peering early and are willing to risk data downtime (or have
verified separately that all the PGs can peer, even if they aren't fully
recovered), run the following command:

.. prompt:: bash $

   ceph osd force_healthy_stretch_mode --yes-i-really-mean-it

This command can be used to to remove the ``HEALTH_WARN`` state, which recovery
mode raises.
