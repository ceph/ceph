.. _placement groups:

==================
 Placement Groups
==================

.. _pg-autoscaler:

Autoscaling placement groups
============================

Placement groups (PGs) are an internal implementation detail of how Ceph
distributes data. Autoscaling provides a way to manage PGs, and especially to
manage the number of PGs present in different pools.  When *pg-autoscaling* is
enabled, the cluster is allowed to make recommendations or automatic
adjustments with respect to the number of PGs for each pool (``pgp_num``) in
accordance with expected cluster utilization and expected pool utilization.

Each pool has a ``pg_autoscale_mode`` property that can be set to ``off``,
``on``, or ``warn``:

* ``off``: Disable autoscaling for this pool. It is up to the administrator to
  choose an appropriate ``pgp_num`` for each pool. For more information, see
  :ref:`choosing-number-of-placement-groups`.
* ``on``: Enable automated adjustments of the PG count for the given pool.
* ``warn``: Raise health checks when the PG count is in need of adjustment.

To set the autoscaling mode for an existing pool, run a command of the
following form:

.. prompt:: bash #

   ceph osd pool set <pool-name> pg_autoscale_mode <mode>

For example, to enable autoscaling on pool ``foo``, run the following command:

.. prompt:: bash #

   ceph osd pool set foo pg_autoscale_mode on

There is also a ``pg_autoscale_mode`` setting for any pools that are created
after the initial setup of the cluster. To change this setting, run a command
of the following form:

.. prompt:: bash #

   ceph config set global osd_pool_default_pg_autoscale_mode <mode>

You can disable or enable the autoscaler for all pools with the ``noautoscale``
flag. By default, this flag is set to ``off``, but you can set it to ``on`` by
running the following command:

.. prompt:: bash #

   ceph osd pool set noautoscale

To set the ``noautoscale`` flag to ``off``, run the following command:

.. prompt:: bash #

   ceph osd pool unset noautoscale

To get the value of the flag, run the following command:

.. prompt:: bash #

   ceph osd pool get noautoscale

Viewing PG scaling recommendations
----------------------------------

To view each pool, its relative utilization, and any recommended changes to the
PG count, run the following command:

.. prompt:: bash #

   ceph osd pool autoscale-status

The output will resemble the following::

   POOL    SIZE  TARGET SIZE  RATE  RAW CAPACITY   RATIO  TARGET RATIO  EFFECTIVE RATIO BIAS PG_NUM  NEW PG_NUM  AUTOSCALE BULK
   a     12900M                3.0        82431M  0.4695                                          8         128  warn      True
   c         0                 3.0        82431M  0.0000        0.2000           0.9884  1.0      1          64  warn      True
   b         0        953.6M   3.0        82431M  0.0347                                          8              warn      False

- **POOL** is the name of the pool. 

- **SIZE** is the amount of data stored in the pool. 
  
- **TARGET SIZE** (if present) is the amount of data that is expected to be
  stored in the pool, as specified by the administrator. The system uses the
  greater of the two values for its calculation.

- **RATE** is the multiplier for the pool that determines how much raw storage
  capacity is consumed. For example, a three-replica pool will have a ratio of
  3.0, and a ``k=4 m=2`` erasure-coded pool will have a ratio of 1.5.

- **RAW CAPACITY** is the total amount of raw storage capacity on the specific
  OSDs that are responsible for storing the data of the pool (and perhaps the
  data of other pools). 

- **RATIO** is the ratio of (1) the storage consumed by the pool to (2) the
  total raw storage capacity. In order words, RATIO is defined as (SIZE * RATE)
  / RAW CAPACITY.

- **TARGET RATIO** (if present) is the ratio of the expected storage of this
  pool (that is, the amount of storage that this pool is expected to consume,
  as specified by the administrator) to the expected storage of all other pools
  that have target ratios set.  If both ``target_size_bytes`` and
  ``target_size_ratio`` are specified, then ``target_size_ratio`` takes
  precedence.

- **EFFECTIVE RATIO** is the result of making two adjustments to the target
  ratio:

  #. Subtracting any capacity expected to be used by pools that have target
     size set.

  #. Normalizing the target ratios among pools that have target ratio set so
     that collectively they target cluster capacity. For example, four pools
     with target_ratio 1.0 would have an effective ratio of 0.25.

  The system's calculations use whichever of these two ratios (that is, the 
  target ratio and the effective ratio) is greater.

- **BIAS** is used as a multiplier to manually adjust a pool's PG in accordance
  with prior information about how many PGs a specific pool is expected to
  have.

- **PG_NUM** is either the current number of PGs associated with the pool or,
  if a ``pg_num`` change is in progress, the current number of PGs that the
  pool is working towards. 

- **NEW PG_NUM** (if present) is the value that the system is recommending the
  ``pg_num`` of the pool to be changed to. It is always a power of 2, and it is
  present only if the recommended value varies from the current value by more
  than the default factor of ``3``. To adjust this factor (in the following
  example, it is changed to ``2``), run the following command:

  .. prompt:: bash #

     ceph osd pool set threshold 2.0

- **AUTOSCALE** is the pool's ``pg_autoscale_mode`` and is set to ``on``,
  ``off``, or ``warn``.

- **BULK** determines whether the pool is ``bulk``. It has a value of ``True``
  or ``False``. A ``bulk`` pool is expected to be large and should initially
  have a large number of PGs so that performance does not suffer]. On the other
  hand, a pool that is not ``bulk`` is expected to be small (for example, a
  ``.mgr`` pool or a meta pool).

.. note::

   If the ``ceph osd pool autoscale-status`` command returns no output at all,
   there is probably at least one pool that spans multiple CRUSH roots.  This
   'spanning pool' issue can happen in scenarios like the following:
   when a new deployment auto-creates the ``.mgr`` pool on the ``default``
   CRUSH root, subsequent pools are created with rules that constrain them to a
   specific shadow CRUSH tree. For example, if you create an RBD metadata pool
   that is constrained to ``deviceclass = ssd`` and an RBD data pool that is
   constrained to ``deviceclass = hdd``, you will encounter this issue. To
   remedy this issue, constrain the spanning pool to only one device class. In
   the above scenario, there is likely to be a ``replicated-ssd`` CRUSH rule in
   effect, and the ``.mgr`` pool can be constrained to ``ssd`` devices by
   running the following commands:

   .. prompt:: bash #

      ceph osd pool set .mgr crush_rule replicated-ssd
      ceph osd pool set pool 1 crush_rule to replicated-ssd

   This intervention will result in a small amount of backfill, but
   typically this traffic completes quickly.


Automated scaling
-----------------

In the simplest approach to automated scaling, the cluster is allowed to
automatically scale ``pgp_num`` in accordance with usage. Ceph considers the
total available storage and the target number of PGs for the whole system,
considers how much data is stored in each pool, and apportions PGs accordingly.
The system is conservative with its approach, making changes to a pool only
when the current number of PGs (``pg_num``) varies by more than a factor of 3
from the recommended number.

The target number of PGs per OSD is determined by the ``mon_target_pg_per_osd``
parameter (default: 100), which can be adjusted by running the following
command:

.. prompt:: bash #

   ceph config set global mon_target_pg_per_osd 100

The autoscaler analyzes pools and adjusts on a per-subtree basis.  Because each
pool might map to a different CRUSH rule, and each rule might distribute data
across different devices, Ceph will consider the utilization of each subtree of
the hierarchy independently. For example, a pool that maps to OSDs of class
``ssd`` and a pool that maps to OSDs of class ``hdd`` will each have optimal PG
counts that are determined by how many of these two different device types
there are.

If a pool uses OSDs under two or more CRUSH roots (for example, shadow trees
with both ``ssd`` and ``hdd`` devices), the autoscaler issues a warning to the
user in the manager log. The warning states the name of the pool and the set of
roots that overlap each other. The autoscaler does not scale any pools with
overlapping roots because this condition can cause problems with the scaling
process. We recommend constraining each pool so that it belongs to only one
root (that is, one OSD class) to silence the warning and ensure a successful
scaling process.

If a pool is flagged ``bulk``, then the autoscaler starts the pool with a full
complement of PGs and then scales down the number of PGs only if the usage
ratio across the pool is uneven.  However, if a pool is not flagged ``bulk``,
then the autoscaler starts the pool with minimal PGs and creates additional PGs
only if there is more usage in the pool.

To create a pool that will be flagged ``bulk``, run the following command:

.. prompt:: bash #

   ceph osd pool create <pool-name> --bulk

To set or unset the ``bulk`` flag of an existing pool, run the following
command:

.. prompt:: bash #

   ceph osd pool set <pool-name> bulk <true/false/1/0>

To get the ``bulk`` flag of an existing pool, run the following command:

.. prompt:: bash #

   ceph osd pool get <pool-name> bulk

.. _specifying_pool_target_size:

Specifying expected pool size
-----------------------------

When a cluster or pool is first created, it consumes only a small fraction of
the total cluster capacity and appears to the system as if it should need only
a small number of PGs. However, in some cases, cluster administrators know
which pools are likely to consume most of the system capacity in the long run.
When Ceph is provided with this information, a more appropriate number of PGs
can be used from the beginning, obviating subsequent changes in ``pg_num`` and
the associated overhead cost of relocating data.

The *target size* of a pool can be specified in two ways: either in relation to
the absolute size (in bytes) of the pool, or as a weight relative to all other
pools that have ``target_size_ratio`` set.

For example, to tell the system that ``mypool`` is expected to consume 100 TB,
run the following command:

.. prompt:: bash #

   ceph osd pool set mypool target_size_bytes 100T

Alternatively, to tell the system that ``mypool`` is expected to consume a
ratio of 1.0 relative to other pools that have ``target_size_ratio`` set,
adjust the ``target_size_ratio`` setting of ``my pool`` by running the
following command:

.. prompt:: bash # 

   ceph osd pool set mypool target_size_ratio 1.0

If `mypool` is the only pool in the cluster, then it is expected to use 100% of
the total cluster capacity. However, if the cluster contains a second pool that
has ``target_size_ratio`` set to 1.0, then both pools are expected to use 50%
of the total cluster capacity.

The ``ceph osd pool create`` command has two command-line options that can be
used to set the target size of a pool at creation time: ``--target-size-bytes
<bytes>`` and ``--target-size-ratio <ratio>``.

Note that if the target-size values that have been specified are impossible
(for example, a capacity larger than the total cluster), then a health check
(``POOL_TARGET_SIZE_BYTES_OVERCOMMITTED``) will be raised.

If both ``target_size_ratio`` and ``target_size_bytes`` are specified for a
pool, then the latter will be ignored, the former will be used in system
calculations, and a health check (``POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO``)
will be raised.

Specifying bounds on a pool's PGs
---------------------------------

It is also possible to specify a minimum number of PGs for a pool.
This is useful for establishing a lower bound on the amount of
parallelism client will see when doing IO, even when a pool is mostly
empty.  Setting the lower bound prevents Ceph from reducing (or
recommending you reduce) the PG number below the configured number.

You can set the minimum or maximum number of PGs for a pool with:

.. prompt:: bash #

   ceph osd pool set <pool-name> pg_num_min <num>
   ceph osd pool set <pool-name> pg_num_max <num>

You can also specify the minimum or maximum PG count at pool creation
time with the optional ``--pg-num-min <num>`` or ``--pg-num-max
<num>`` arguments to the ``ceph osd pool create`` command.

.. _preselection:

A preselection of pg_num
========================

When creating a new pool with:

.. prompt:: bash #

   ceph osd pool create {pool-name} [pg_num]

it is optional to choose the value of ``pg_num``.  If you do not
specify ``pg_num``, the cluster can (by default) automatically tune it
for you based on how much data is stored in the pool (see above, :ref:`pg-autoscaler`).

Alternatively, ``pg_num`` can be explicitly provided.  However,
whether you specify a ``pg_num`` value or not does not affect whether
the value is automatically tuned by the cluster after the fact.  To
enable or disable auto-tuning:

  ceph osd pool set {pool-name} pg_autoscale_mode (on|off|warn)

The "rule of thumb" for PGs per OSD has traditionally be 100.  With
the additional of the balancer (which is also enabled by default), a
value of more like 50 PGs per OSD is probably reasonable.  The
challenge (which the autoscaler normally does for you), is to:

- have the PGs per pool proportional to the data in the pool, and
- end up with 50-100 PGs per OSDs, after the replication or
  erasuring-coding fan-out of each PG across OSDs is taken into
  consideration

How are Placement Groups used ?
===============================

A placement group (PG) aggregates objects within a pool because
tracking object placement and object metadata on a per-object basis is
computationally expensive--i.e., a system with millions of objects
cannot realistically track placement on a per-object basis.

.. ditaa::
           /-----\  /-----\  /-----\  /-----\  /-----\
           | obj |  | obj |  | obj |  | obj |  | obj |
           \-----/  \-----/  \-----/  \-----/  \-----/
              |        |        |        |        |
              +--------+--------+        +---+----+
              |                              |
              v                              v
   +-----------------------+      +-----------------------+
   |  Placement Group #1   |      |  Placement Group #2   |
   |                       |      |                       |
   +-----------------------+      +-----------------------+
               |                              |
               +------------------------------+
                             |
                             v
                  +-----------------------+
                  |        Pool           |
                  |                       |
                  +-----------------------+

The Ceph client will calculate which placement group an object should
be in. It does this by hashing the object ID and applying an operation
based on the number of PGs in the defined pool and the ID of the pool.
See `Mapping PGs to OSDs`_ for details.

The object's contents within a placement group are stored in a set of
OSDs. For instance, in a replicated pool of size two, each placement
group will store objects on two OSDs, as shown below.

.. ditaa::
   +-----------------------+      +-----------------------+
   |  Placement Group #1   |      |  Placement Group #2   |
   |                       |      |                       |
   +-----------------------+      +-----------------------+
        |             |               |             |
        v             v               v             v
   /----------\  /----------\    /----------\  /----------\
   |          |  |          |    |          |  |          |
   |  OSD #1  |  |  OSD #2  |    |  OSD #2  |  |  OSD #3  |
   |          |  |          |    |          |  |          |
   \----------/  \----------/    \----------/  \----------/


Should OSD #2 fail, another will be assigned to Placement Group #1 and
will be filled with copies of all objects in OSD #1. If the pool size
is changed from two to three, an additional OSD will be assigned to
the placement group and will receive copies of all objects in the
placement group.

Placement groups do not own the OSD; they share it with other
placement groups from the same pool or even other pools. If OSD #2
fails, the Placement Group #2 will also have to restore copies of
objects, using OSD #3.

When the number of placement groups increases, the new placement
groups will be assigned OSDs. The result of the CRUSH function will
also change and some objects from the former placement groups will be
copied over to the new Placement Groups and removed from the old ones.

Placement Groups Tradeoffs
==========================

Data durability and even distribution among all OSDs call for more
placement groups but their number should be reduced to the minimum to
save CPU and memory.

.. _data durability:

Data durability
---------------

After an OSD fails, the risk of data loss increases until the data it
contained is fully recovered. Let's imagine a scenario that causes
permanent data loss in a single placement group:

- The OSD fails and all copies of the object it contains are lost.
  For all objects within the placement group the number of replica
  suddenly drops from three to two.

- Ceph starts recovery for this placement group by choosing a new OSD
  to re-create the third copy of all objects.

- Another OSD, within the same placement group, fails before the new
  OSD is fully populated with the third copy. Some objects will then
  only have one surviving copies.

- Ceph picks yet another OSD and keeps copying objects to restore the
  desired number of copies.

- A third OSD, within the same placement group, fails before recovery
  is complete. If this OSD contained the only remaining copy of an
  object, it is permanently lost.

In a cluster containing 10 OSDs with 512 placement groups in a three
replica pool, CRUSH will give each placement groups three OSDs. In the
end, each OSDs will end up hosting (512 * 3) / 10 = ~150 Placement
Groups. When the first OSD fails, the above scenario will therefore
start recovery for all 150 placement groups at the same time.

The 150 placement groups being recovered are likely to be
homogeneously spread over the 9 remaining OSDs. Each remaining OSD is
therefore likely to send copies of objects to all others and also
receive some new objects to be stored because they became part of a
new placement group.

The amount of time it takes for this recovery to complete entirely
depends on the architecture of the Ceph cluster. Let say each OSD is
hosted by a 1TB SSD on a single machine and all of them are connected
to a 10Gb/s switch and the recovery for a single OSD completes within
M minutes. If there are two OSDs per machine using spinners with no
SSD journal and a 1Gb/s switch, it will at least be an order of
magnitude slower.

In a cluster of this size, the number of placement groups has almost
no influence on data durability. It could be 128 or 8192 and the
recovery would not be slower or faster.

However, growing the same Ceph cluster to 20 OSDs instead of 10 OSDs
is likely to speed up recovery and therefore improve data durability
significantly. Each OSD now participates in only ~75 placement groups
instead of ~150 when there were only 10 OSDs and it will still require
all 19 remaining OSDs to perform the same amount of object copies in
order to recover. But where 10 OSDs had to copy approximately 100GB
each, they now have to copy 50GB each instead. If the network was the
bottleneck, recovery will happen twice as fast. In other words,
recovery goes faster when the number of OSDs increases.

If this cluster grows to 40 OSDs, each of them will only host ~35
placement groups. If an OSD dies, recovery will keep going faster
unless it is blocked by another bottleneck. However, if this cluster
grows to 200 OSDs, each of them will only host ~7 placement groups. If
an OSD dies, recovery will happen between at most of ~21 (7 * 3) OSDs
in these placement groups: recovery will take longer than when there
were 40 OSDs, meaning the number of placement groups should be
increased.

No matter how short the recovery time is, there is a chance for a
second OSD to fail while it is in progress. In the 10 OSDs cluster
described above, if any of them fail, then ~17 placement groups
(i.e. ~150 / 9 placement groups being recovered) will only have one
surviving copy. And if any of the 8 remaining OSD fail, the last
objects of two placement groups are likely to be lost (i.e. ~17 / 8
placement groups with only one remaining copy being recovered).

When the size of the cluster grows to 20 OSDs, the number of Placement
Groups damaged by the loss of three OSDs drops. The second OSD lost
will degrade ~4 (i.e. ~75 / 19 placement groups being recovered)
instead of ~17 and the third OSD lost will only lose data if it is one
of the four OSDs containing the surviving copy. In other words, if the
probability of losing one OSD is 0.0001% during the recovery time
frame, it goes from 17 * 10 * 0.0001% in the cluster with 10 OSDs to 4 * 20 *
0.0001% in the cluster with 20 OSDs.

In a nutshell, more OSDs mean faster recovery and a lower risk of
cascading failures leading to the permanent loss of a Placement
Group. Having 512 or 4096 Placement Groups is roughly equivalent in a
cluster with less than 50 OSDs as far as data durability is concerned.

Note: It may take a long time for a new OSD added to the cluster to be
populated with placement groups that were assigned to it. However
there is no degradation of any object and it has no impact on the
durability of the data contained in the Cluster.

.. _object distribution:

Object distribution within a pool
---------------------------------

Ideally objects are evenly distributed in each placement group. Since
CRUSH computes the placement group for each object, but does not
actually know how much data is stored in each OSD within this
placement group, the ratio between the number of placement groups and
the number of OSDs may influence the distribution of the data
significantly.

For instance, if there was a single placement group for ten OSDs in a
three replica pool, only three OSD would be used because CRUSH would
have no other choice. When more placement groups are available,
objects are more likely to be evenly spread among them. CRUSH also
makes every effort to evenly spread OSDs among all existing Placement
Groups.

As long as there are one or two orders of magnitude more Placement
Groups than OSDs, the distribution should be even. For instance, 256
placement groups for 3 OSDs, 512 or 1024 placement groups for 10 OSDs
etc.

Uneven data distribution can be caused by factors other than the ratio
between OSDs and placement groups. Since CRUSH does not take into
account the size of the objects, a few very large objects may create
an imbalance. Let say one million 4K objects totaling 4GB are evenly
spread among 1024 placement groups on 10 OSDs. They will use 4GB / 10
= 400MB on each OSD. If one 400MB object is added to the pool, the
three OSDs supporting the placement group in which the object has been
placed will be filled with 400MB + 400MB = 800MB while the seven
others will remain occupied with only 400MB.

.. _resource usage:

Memory, CPU and network usage
-----------------------------

For each placement group, OSDs and MONs need memory, network and CPU
at all times and even more during recovery. Sharing this overhead by
clustering objects within a placement group is one of the main reasons
they exist.

Minimizing the number of placement groups saves significant amounts of
resources.

.. _choosing-number-of-placement-groups:

Choosing the number of Placement Groups
=======================================

.. note: It is rarely necessary to do this math by hand.  Instead, use the ``ceph osd pool autoscale-status`` command in combination with the ``target_size_bytes`` or ``target_size_ratio`` pool properties.  See :ref:`pg-autoscaler` for more information.

If you have more than 50 OSDs, we recommend approximately 50-100
placement groups per OSD to balance out resource usage, data
durability and distribution. If you have less than 50 OSDs, choosing
among the `preselection`_ above is best. For a single pool of objects,
you can use the following formula to get a baseline

  Total PGs = :math:`\frac{OSDs \times 100}{pool \: size}`

Where **pool size** is either the number of replicas for replicated
pools or the K+M sum for erasure coded pools (as returned by **ceph
osd erasure-code-profile get**).

You should then check if the result makes sense with the way you
designed your Ceph cluster to maximize `data durability`_,
`object distribution`_ and minimize `resource usage`_.

The result should always be **rounded up to the nearest power of two**.

Only a power of two will evenly balance the number of objects among
placement groups. Other values will result in an uneven distribution of
data across your OSDs. Their use should be limited to incrementally
stepping from one power of two to another.

As an example, for a cluster with 200 OSDs and a pool size of 3
replicas, you would estimate your number of PGs as follows

  :math:`\frac{200 \times 100}{3} = 6667`. Nearest power of 2: 8192

When using multiple data pools for storing objects, you need to ensure
that you balance the number of placement groups per pool with the
number of placement groups per OSD so that you arrive at a reasonable
total number of placement groups that provides reasonably low variance
per OSD without taxing system resources or making the peering process
too slow.

For instance a cluster of 10 pools each with 512 placement groups on
ten OSDs is a total of 5,120 placement groups spread over ten OSDs,
that is 512 placement groups per OSD. That does not use too many
resources. However, if 1,000 pools were created with 512 placement
groups each, the OSDs will handle ~50,000 placement groups each and it
would require significantly more resources and time for peering.

You may find the `PGCalc`_ tool helpful.


.. _setting the number of placement groups:

Set the Number of Placement Groups
==================================

To set the number of placement groups in a pool, you must specify the
number of placement groups at the time you create the pool.
See `Create a Pool`_ for details.  Even after a pool is created you can also change the number of placement groups with:

.. prompt:: bash # 

   ceph osd pool set {pool-name} pg_num {pg_num}

After you increase the number of placement groups, you must also
increase the number of placement groups for placement (``pgp_num``)
before your cluster will rebalance. The ``pgp_num`` will be the number of
placement groups that will be considered for placement by the CRUSH
algorithm. Increasing ``pg_num`` splits the placement groups but data
will not be migrated to the newer placement groups until placement
groups for placement, ie. ``pgp_num`` is increased. The ``pgp_num``
should be equal to the ``pg_num``.  To increase the number of
placement groups for placement, execute the following:

.. prompt:: bash #

   ceph osd pool set {pool-name} pgp_num {pgp_num}

When decreasing the number of PGs, ``pgp_num`` is adjusted
automatically for you.

Get the Number of Placement Groups
==================================

To get the number of placement groups in a pool, execute the following:

.. prompt:: bash #
   
   ceph osd pool get {pool-name} pg_num


Get a Cluster's PG Statistics
=============================

To get the statistics for the placement groups in your cluster, execute the following:

.. prompt:: bash #

   ceph pg dump [--format {format}]

Valid formats are ``plain`` (default) and ``json``.


Get Statistics for Stuck PGs
============================

To get the statistics for all placement groups stuck in a specified state,
execute the following:

.. prompt:: bash #

   ceph pg dump_stuck inactive|unclean|stale|undersized|degraded [--format <format>] [-t|--threshold <seconds>]

**Inactive** Placement groups cannot process reads or writes because they are waiting for an OSD
with the most up-to-date data to come up and in.

**Unclean** Placement groups contain objects that are not replicated the desired number
of times. They should be recovering.

**Stale** Placement groups are in an unknown state - the OSDs that host them have not
reported to the monitor cluster in a while (configured by ``mon_osd_report_timeout``).

Valid formats are ``plain`` (default) and ``json``. The threshold defines the minimum number
of seconds the placement group is stuck before including it in the returned statistics
(default 300 seconds).


Get a PG Map
============

To get the placement group map for a particular placement group, execute the following:

.. prompt:: bash #

   ceph pg map {pg-id}

For example: 

.. prompt:: bash #

   ceph pg map 1.6c

Ceph will return the placement group map, the placement group, and the OSD status:

.. prompt:: bash #

   osdmap e13 pg 1.6c (1.6c) -> up [1,0] acting [1,0]


Get a PGs Statistics
====================

To retrieve statistics for a particular placement group, execute the following:

.. prompt:: bash #

   ceph pg {pg-id} query


Scrub a Placement Group
=======================

To scrub a placement group, execute the following:

.. prompt:: bash #

   ceph pg scrub {pg-id}

Ceph checks the primary and any replica nodes, generates a catalog of all objects
in the placement group and compares them to ensure that no objects are missing
or mismatched, and their contents are consistent.  Assuming the replicas all
match, a final semantic sweep ensures that all of the snapshot-related object
metadata is consistent. Errors are reported via logs.

To scrub all placement groups from a specific pool, execute the following:

.. prompt:: bash #

   ceph osd pool scrub {pool-name}

Prioritize backfill/recovery of a Placement Group(s)
====================================================

You may run into a situation where a bunch of placement groups will require
recovery and/or backfill, and some particular groups hold data more important
than others (for example, those PGs may hold data for images used by running
machines and other PGs may be used by inactive machines/less relevant data).
In that case, you may want to prioritize recovery of those groups so
performance and/or availability of data stored on those groups is restored
earlier. To do this (mark particular placement group(s) as prioritized during
backfill or recovery), execute the following:

.. prompt:: bash #

   ceph pg force-recovery {pg-id} [{pg-id #2}] [{pg-id #3} ...]
   ceph pg force-backfill {pg-id} [{pg-id #2}] [{pg-id #3} ...]

This will cause Ceph to perform recovery or backfill on specified placement
groups first, before other placement groups. This does not interrupt currently
ongoing backfills or recovery, but causes specified PGs to be processed
as soon as possible. If you change your mind or prioritize wrong groups,
use:

.. prompt:: bash #

   ceph pg cancel-force-recovery {pg-id} [{pg-id #2}] [{pg-id #3} ...]
   ceph pg cancel-force-backfill {pg-id} [{pg-id #2}] [{pg-id #3} ...]

This will remove "force" flag from those PGs and they will be processed
in default order. Again, this doesn't affect currently processed placement
group, only those that are still queued.

The "force" flag is cleared automatically after recovery or backfill of group
is done.

Similarly, you may use the following commands to force Ceph to perform recovery
or backfill on all placement groups from a specified pool first:

.. prompt:: bash #

   ceph osd pool force-recovery {pool-name}
   ceph osd pool force-backfill {pool-name}

or:

.. prompt:: bash #

   ceph osd pool cancel-force-recovery {pool-name}
   ceph osd pool cancel-force-backfill {pool-name}

to restore to the default recovery or backfill priority if you change your mind.

Note that these commands could possibly break the ordering of Ceph's internal
priority computations, so use them with caution!
Especially, if you have multiple pools that are currently sharing the same
underlying OSDs, and some particular pools hold data more important than others,
we recommend you use the following command to re-arrange all pools's
recovery/backfill priority in a better order:

.. prompt:: bash #

   ceph osd pool set {pool-name} recovery_priority {value}

For example, if you have 10 pools you could make the most important one priority 10,
next 9, etc. Or you could leave most pools alone and have say 3 important pools
all priority 1 or priorities 3, 2, 1 respectively.

Revert Lost
===========

If the cluster has lost one or more objects, and you have decided to
abandon the search for the lost data, you must mark the unfound objects
as ``lost``.

If all possible locations have been queried and objects are still
lost, you may have to give up on the lost objects. This is
possible given unusual combinations of failures that allow the cluster
to learn about writes that were performed before the writes themselves
are recovered.

Currently the only supported option is "revert", which will either roll back to
a previous version of the object or (if it was a new object) forget about it
entirely. To mark the "unfound" objects as "lost", execute the following:

.. prompt:: bash #

   ceph pg {pg-id} mark_unfound_lost revert|delete

.. important:: Use this feature with caution, because it may confuse
   applications that expect the object(s) to exist.


.. toctree::
        :hidden:

        pg-states
        pg-concepts


.. _Create a Pool: ../pools#createpool
.. _Mapping PGs to OSDs: ../../../architecture#mapping-pgs-to-osds
.. _pgcalc: https://old.ceph.com/pgcalc/
