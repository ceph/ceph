Using the pg-upmap
==================

Starting in Luminous v12.2.z there is a new *pg-upmap* exception table
in the OSDMap that allows the cluster to explicitly map specific PGs to
specific OSDs.  This allows the cluster to fine-tune the data
distribution to, in most cases, perfectly distributed PGs across OSDs.

The key caveat to this new mechanism is that it requires that all
clients understand the new *pg-upmap* structure in the OSDMap.

Enabling
--------

To allow use of the feature, you must tell the cluster that it only
needs to support luminous (and newer) clients with::

  ceph osd set-require-min-compat-client luminous

This command will fail if any pre-luminous clients or daemons are
connected to the monitors.  You can see what client versions are in
use with::

  ceph features

Balancer module
-----------------

The new `balancer` module for ceph-mgr will automatically balance
the number of PGs per OSD.  See ``Balancer``


Offline optimization
--------------------

Upmap entries are updated with an offline optimizer built into ``osdmaptool``.

#. Grab the latest copy of your osdmap::

     ceph osd getmap -o om

#. Run the optimizer::

     osdmaptool om --upmap out.txt [--upmap-pool <pool>]
              [--upmap-max <max-optimizations>] [--upmap-deviation <max-deviation>]
              [--upmap-active]

   It is highly recommended that optimization be done for each pool
   individually, or for sets of similarly-utilized pools.  You can
   specify the ``--upmap-pool`` option multiple times.  "Similar pools"
   means pools that are mapped to the same devices and store the same
   kind of data (e.g., RBD image pools, yes; RGW index pool and RGW
   data pool, no).

   The ``max-optimizations`` value is the maximum number of upmap entries to
   identify in the run.  The default is `10` like the ceph-mgr balancer module,
   but you should use a larger number if you are doing offline optimization.
   If it cannot find any additional changes to make it will stop early
   (i.e., when the pool distribution is perfect).

   The ``max-deviation`` value defaults to `1`.  If an OSD PG count
   varies from the computed target number by less than or equal
   to this amount it will be considered perfect.

   The ``--upmap-active`` option simulates the behavior of the active
   balancer in upmap mode.  It keeps cycling until the OSDs are balanced
   and reports how many rounds and how long each round is taking.  The
   elapsed time for rounds indicates the CPU load ceph-mgr will be
   consuming when it tries to compute the next optimization plan.

#. Apply the changes::

     source out.txt

   The proposed changes are written to the output file ``out.txt`` in
   the example above.  These are normal ceph CLI commands that can be
   run to apply the changes to the cluster.


The above steps can be repeated as many times as necessary to achieve
a perfect distribution of PGs for each set of pools.

You can see some (gory) details about what the tool is doing by
passing ``--debug-osd 10`` and even more with ``--debug-crush 10``
to ``osdmaptool``.
