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
No matter what happens, Ceph will not compromise on data integrity
and consistency. If there's a failure in your network or a loss of nodes and
you can restore service, Ceph will return to normal functionality on its own.

But there are scenarios where you lose data availibility despite having
enough servers available to satisfy Ceph's consistency and sizing constraints, or
where you may be surprised to not satisfy Ceph's constraints.
The first important category of these failures resolve around inconsistent
networks -- if there's a netsplit, Ceph may be unable to mark OSDs down and kick
them out of the acting PG sets despite the primary being unable to replicate data.
If this happens, IO will not be permitted, because Ceph can't satisfy its durability
guarantees.

The second important category of failures is when you think you have data replicated
across data centers, but the constraints aren't sufficient to guarantee this.
For instance, you might have data centers A and B, and your CRUSH rule targets 3 copies
and places a copy in each data center with a ``min_size`` of 2. The PG may go active with
2 copies in site A and no copies in site B, which means that if you then lose site A you
have lost data and Ceph can't operate on it. This situation is surprisingly difficult
to avoid with standard CRUSH rules.

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
             min_size 1
             max_size 10
             type replicated
             step take site1
             step chooseleaf firstn 2 type host
             step emit
             step take site2
             step chooseleaf firstn 2 type host
             step emit
     }

#. Inject the CRUSH map to make the rule available to the cluster:

   .. prompt:: bash $

      crushtool -c crush.map.txt -o crush2.map.bin
      ceph osd setcrushmap -i crush2.map.bin

#. Run the monitors in connectivity mode. See `Changing Monitor Elections`_.

#. Command the cluster to enter stretch mode. In this example, ``mon.e`` is the
   tiebreaker monitor and we are splitting across data centers. The tiebreaker
   monitor must be assigned a data center that is neither ``site1`` nor
   ``site2``. For this purpose you can create another data-center bucket named
   ``site3`` in your CRUSH and place ``mon.e`` there:

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

Stretch Mode Limitations
========================
As implied by the setup, stretch mode only handles 2 sites with OSDs.

While it is not enforced, you should run 2 monitors in each site plus
a tiebreaker, for a total of 5. This is because OSDs can only connect
to monitors in their own site when in stretch mode.

You cannot use erasure coded pools with stretch mode. If you try, it will
refuse, and it will not allow you to create EC pools once in stretch mode.

You must create your own CRUSH rule which provides 2 copies in each site, and
you must use 4 total copies with 2 in each site. If you have existing pools
with non-default size/min_size, Ceph will object when you attempt to
enable stretch mode.

Because it runs with ``min_size 1`` when degraded, you should only use stretch
mode with all-flash OSDs.  This minimizes the time needed to recover once
connectivity is restored, and thus minimizes the potential for data loss.

Hopefully, future development will extend this feature to support EC pools and
running with more than 2 full sites.

Other commands
==============
If your tiebreaker monitor fails for some reason, you can replace it. Turn on
a new monitor and run:

.. prompt:: bash $

   ceph mon set_new_tiebreaker mon.<new_mon_name>

This command will protest if the new monitor is in the same location as existing
non-tiebreaker monitors. This command WILL NOT remove the previous tiebreaker
monitor; you should do so yourself.

Also in 16.2.7, if you are writing your own tooling for deploying Ceph, you can use a new
``--set-crush-location`` option when booting monitors, instead of running
``ceph mon set_location``. This option accepts only a single "bucket=loc" pair, eg
``ceph-mon --set-crush-location 'datacenter=a'``, which must match the
bucket type you specified when running ``enable_stretch_mode``.


When in stretch degraded mode, the cluster will go into "recovery" mode automatically
when the disconnected data center comes back. If that doesn't work, or you want to
enable recovery mode early, you can invoke:

.. prompt:: bash $

   ceph osd force_recovery_stretch_mode --yes-i-really-mean-it

But this command should not be necessary; it is included to deal with
unanticipated situations.

When in recovery mode, the cluster should go back into normal stretch mode
when the PGs are healthy. If this doesn't happen, or you want to force the
cross-data-center peering early and are willing to risk data downtime (or have
verified separately that all the PGs can peer, even if they aren't fully
recovered), you can invoke:

.. prompt:: bash $

   ceph osd force_healthy_stretch_mode --yes-i-really-mean-it

This command should not be necessary; it is included to deal with
unanticipated situations. But you might wish to invoke it to remove
the ``HEALTH_WARN`` state which recovery mode generates.
