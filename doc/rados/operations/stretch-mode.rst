.. _stretch_mode:

================
Stretch Clusters
================


Stretch Clusters
================

A stretch cluster is a cluster that has servers in geographically separated
data centers, distributed over a WAN. Stretch clusters have LAN-like high-speed
and low-latency connections, but limited links. Stretch clusters have a higher
likelihood of (possibly asymmetric) network splits, and a higher likelihood of
temporary or complete loss of an entire data center (which can represent
one-third to one-half of the total cluster).

Ceph is designed with the expectation that all parts of its network and cluster
will be reliable and that failures will be distributed randomly across the
CRUSH map. Even if a switch goes down and causes the loss of many OSDs, Ceph is
designed so that the remaining OSDs and monitors will route around such a loss. 

Sometimes this cannot be relied upon. If you have a "stretched-cluster"
deployment in which much of your cluster is behind a single network component,
you might need to use **stretch mode** to ensure data integrity.

We will here consider two standard configurations: a configuration with two
data centers (or, in clouds, two availability zones), and a configuration with
three data centers (or, in clouds, three availability zones).

In the two-site configuration, Ceph expects each of the sites to hold a copy of
the data, and Ceph also expects there to be a third site that has a tiebreaker
monitor. This tiebreaker monitor picks a winner if the network connection fails
and both data centers remain alive.

The tiebreaker monitor can be a VM. It can also have high latency relative to
the two main sites.

The standard Ceph configuration is able to survive MANY network failures or
data-center failures without ever compromising data availability. If enough
Ceph servers are brought back following a failure, the cluster *will* recover.
If you lose a data center but are still able to form a quorum of monitors and
still have all the data available, Ceph will maintain availability. (This
assumes that the cluster has enough copies to satisfy the pools' ``min_size``
configuration option, or (failing that) that the cluster has CRUSH rules in
place that will cause the cluster to re-replicate the data until the
``min_size`` configuration option has been met.)

Stretch Cluster Issues
======================

Ceph does not permit the compromise of data integrity and data consistency
under any circumstances. When service is restored after a network failure or a
loss of Ceph nodes, Ceph will restore itself to a state of normal functioning
without operator intervention.  

Ceph does not permit the compromise of data integrity or data consistency, but
there are situations in which *data availability* is compromised. These
situations can occur even though there are enough clusters available to satisfy
Ceph's consistency and sizing constraints. In some situations, you might
discover that your cluster does not satisfy those constraints.

The first category of these failures that we will discuss involves inconsistent
networks -- if there is a netsplit (a disconnection between two servers that
splits the network into two pieces), Ceph might be unable to mark OSDs ``down``
and remove them from the acting PG sets. This failure to mark ODSs ``down``
will occur, despite the fact that the primary PG is unable to replicate data (a
situation that, under normal non-netsplit circumstances, would result in the
marking of affected OSDs as ``down`` and their removal from the PG). If this
happens, Ceph will be unable to satisfy its durability guarantees and
consequently IO will not be permitted.

The second category of failures that we will discuss involves the situation in
which the constraints are not sufficient to guarantee the replication of data
across data centers, though it might seem that the data is correctly replicated
across data centers. For example, in a scenario in which there are two data
centers named Data Center A and Data Center B, and the CRUSH rule targets three
replicas and places a replica in each data center with a ``min_size`` of ``2``,
the PG might go active with two replicas in Data Center A and zero replicas in
Data Center B. In a situation of this kind, the loss of Data Center A means
that the data is lost and Ceph will not be able to operate on it. This
situation is surprisingly difficult to avoid using only standard CRUSH rules.


Stretch Mode
============
Stretch mode is designed to handle deployments in which you cannot guarantee the
replication of data across two data centers. This kind of situation can arise
when the cluster's CRUSH rule specifies that three copies are to be made, but 
then a copy is placed in each data center with a ``min_size`` of 2. Under such
conditions, a placement group can become active with two copies in the first
data center and no copies in the second data center. 


Entering Stretch Mode
---------------------

To enable stretch mode, you must set the location of each monitor, matching
your CRUSH map. This procedure shows how to do this.


#. Place ``mon.a`` in your first data center:

   .. prompt:: bash $

      ceph mon set_location a datacenter=site1

#. Generate a CRUSH rule that places two copies in each data center.
   This requires editing the CRUSH map directly:

   .. prompt:: bash $

      ceph osd getcrushmap > crush.map.bin
      crushtool -d crush.map.bin -o crush.map.txt

#. Edit the ``crush.map.txt`` file to add a new rule. Here there is only one
   other rule (``id 1``), but you might need to use a different rule ID. We
   have two data-center buckets named ``site1`` and ``site2``:

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

   .. warning:: If a CRUSH rule is defined for a stretch mode cluster and the
      rule has multiple "takes" in it, then ``MAX AVAIL`` for the pools
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
      ``PGMap::get_rule_avail`` considers only the available size of a single
      data center, and not (as would be correct) the total available size from
      both datacenters.
      
      Here is a workaround. Instead of defining the stretch rule as defined in
      the ``stretch_rule`` function above, define it as follows::

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

#. Inject the CRUSH map to make the rule available to the cluster:

   .. prompt:: bash $

      crushtool -c crush.map.txt -o crush2.map.bin
      ceph osd setcrushmap -i crush2.map.bin

#. Run the monitors in connectivity mode. See `Changing Monitor Elections`_.

   .. prompt:: bash $

      ceph mon set election_strategy connectivity

#. Command the cluster to enter stretch mode. In this example, ``mon.e`` is the
   tiebreaker monitor and we are splitting across data centers. The tiebreaker
   monitor must be assigned a data center that is neither ``site1`` nor
   ``site2``. This data center **should not** be defined in your CRUSH map, here 
   we are placing ``mon.e`` in a virtual data center called ``site3``:

   .. prompt:: bash $

      ceph mon set_location e datacenter=site3
      ceph mon enable_stretch_mode e stretch_rule datacenter

When stretch mode is enabled, PGs will become active only when they peer
across data centers (or across whichever CRUSH bucket type was specified),
assuming both are alive. Pools will increase in size from the default ``3`` to
``4``, and two copies will be expected in each site. OSDs will be allowed to
connect to monitors only if they are in the same data center as the monitors.
New monitors will not be allowed to join the cluster if they do not specify a
location.

If all OSDs and monitors in one of the data centers become inaccessible at once,
the surviving data center enters a "degraded stretch mode". A warning will be
issued, the ``min_size`` will be reduced to ``1``, and the cluster will be
allowed to go active with the data in the single remaining site. The pool size
does not change, so warnings will be generated that report that the pools are
too small -- but a special stretch mode flag will prevent the OSDs from
creating extra copies in the remaining data center. This means that the data
center will keep only two copies, just as before.

When the missing data center comes back, the cluster will enter a "recovery
stretch mode". This changes the warning and allows peering, but requires OSDs
only from the data center that was ``up`` throughout the duration of the
downtime. When all PGs are in a known state, and are neither degraded nor
incomplete, the cluster transitions back to regular stretch mode, ends the
warning, restores ``min_size`` to its original value (``2``), requires both
sites to peer, and no longer requires the site that was up throughout the
duration of the downtime when peering (which makes failover to the other site
possible, if needed).

.. _Changing Monitor elections: ../change-mon-elections

Limitations of Stretch Mode 
===========================
When using stretch mode, OSDs must be located at exactly two sites. 

Two monitors should be run in each data center, plus a tiebreaker in a third
(or in the cloud) for a total of five monitors. While in stretch mode, OSDs
will connect only to monitors within the data center in which they are located.
OSDs *DO NOT* connect to the tiebreaker monitor.

Erasure-coded pools cannot be used with stretch mode. Attempts to use erasure
coded pools with stretch mode will fail. Erasure coded pools cannot be created
while in stretch mode. 

To use stretch mode, you will need to create a CRUSH rule that provides two
replicas in each data center. Ensure that there are four total replicas: two in
each data center. If pools exist in the cluster that do not have the default
``size`` or ``min_size``, Ceph will not enter stretch mode. An example of such
a CRUSH rule is given above.

Because stretch mode runs with ``min_size`` set to ``1`` (or, more directly,
``min_size 1``), we recommend enabling stretch mode only when using OSDs on
SSDs (including NVMe OSDs). Hybrid HDD+SDD or HDD-only OSDs are not recommended
due to the long time it takes for them to recover after connectivity between
data centers has been restored. This reduces the potential for data loss.

In the future, stretch mode might support erasure-coded pools and might support
deployments that have more than two data centers.

Other commands
==============

Replacing a failed tiebreaker monitor
-------------------------------------

Turn on a new monitor and run the following command:

.. prompt:: bash $

   ceph mon set_new_tiebreaker mon.<new_mon_name>

This command protests if the new monitor is in the same location as the
existing non-tiebreaker monitors. **This command WILL NOT remove the previous
tiebreaker monitor.** Remove the previous tiebreaker monitor yourself.

Using "--set-crush-location" and not "ceph mon set_location"
------------------------------------------------------------

If you write your own tooling for deploying Ceph, use the
``--set-crush-location`` option when booting monitors instead of running ``ceph
mon set_location``. This option accepts only a single ``bucket=loc`` pair (for
example, ``ceph-mon --set-crush-location 'datacenter=a'``), and that pair must
match the bucket type that was specified when running ``enable_stretch_mode``.

Forcing recovery stretch mode
-----------------------------

When in stretch degraded mode, the cluster will go into "recovery" mode
automatically when the disconnected data center comes back. If that does not
happen or you want to enable recovery mode early, run the following command:

.. prompt:: bash $

   ceph osd force_recovery_stretch_mode --yes-i-really-mean-it

Forcing normal stretch mode
---------------------------

When in recovery mode, the cluster should go back into normal stretch mode when
the PGs are healthy. If this fails to happen or if you want to force the
cross-data-center peering early and are willing to risk data downtime (or have
verified separately that all the PGs can peer, even if they aren't fully
recovered), run the following command:

.. prompt:: bash $

   ceph osd force_healthy_stretch_mode --yes-i-really-mean-it

This command can be used to to remove the ``HEALTH_WARN`` state, which recovery
mode generates.
