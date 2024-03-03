.. _placement groups:

==================
 Placement Groups
==================

Placement groups (PGs) are subsets of each logical Ceph pool. Placement groups
perform the function of placing objects (as a group) into OSDs. Ceph manages
data internally at placement-group granularity: this scales better than would
managing individual RADOS objects. A cluster that has a larger number of
placement groups (for example, 150 per OSD) is better balanced than an
otherwise identical cluster with a smaller number of placement groups.

Ceph’s internal RADOS objects are each mapped to a specific placement group,
and each placement group belongs to exactly one Ceph pool.

See Sage Weil's blog post `New in Nautilus: PG merging and autotuning
<https://ceph.io/en/news/blog/2019/new-in-nautilus-pg-merging-and-autotuning/>`_
for more information about the relationship of placement groups to pools and to
objects.

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
  total raw storage capacity. In order words, RATIO is defined as 
  (SIZE * RATE) / RAW CAPACITY.

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

- **NEW PG_NUM** (if present) is the value that the system recommends that the
  ``pg_num`` of the pool should be. It is always a power of two, and it
  is present only if the recommended value varies from the current value by
  more than the default factor of ``3``. To adjust this multiple (in the
  following example, it is changed to ``2``), run the following command:

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

.. _managing_bulk_flagged_pools:

Managing pools that are flagged with ``bulk``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

It is possible to specify both the minimum number and the maximum number of PGs
for a pool. 

Setting a Minimum Number of PGs and a Maximum Number of PGs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a minimum is set, then Ceph will not itself reduce (nor recommend that you
reduce) the number of PGs to a value below the configured value. Setting a
minimum serves to establish a lower bound on the amount of parallelism enjoyed
by a client during I/O, even if a pool is mostly empty. 

If a maximum is set, then Ceph will not itself increase (or recommend that you
increase) the number of PGs to a value above the configured value.

To set the minimum number of PGs for a pool, run a command of the following
form:

.. prompt:: bash #

   ceph osd pool set <pool-name> pg_num_min <num>

To set the maximum number of PGs for a pool, run a command of the following
form:

.. prompt:: bash #

   ceph osd pool set <pool-name> pg_num_max <num>

In addition, the ``ceph osd pool create`` command has two command-line options
that can be used to specify the minimum or maximum PG count of a pool at
creation time: ``--pg-num-min <num>`` and ``--pg-num-max <num>``.

.. _preselection:

Preselecting pg_num
===================

When creating a pool with the following command, you have the option to
preselect the value of the ``pg_num`` parameter:

.. prompt:: bash #

   ceph osd pool create {pool-name} [pg_num]

If you opt not to specify ``pg_num`` in this command, the cluster uses the PG
autoscaler to automatically configure the parameter in accordance with the
amount of data that is stored in the pool (see :ref:`pg-autoscaler` above).

However, your decision of whether or not to specify ``pg_num`` at creation time
has no effect on whether the parameter will be automatically tuned by the
cluster afterwards. As seen above, autoscaling of PGs is enabled or disabled by
running a command of the following form:
    
.. prompt:: bash #

   ceph osd pool set {pool-name} pg_autoscale_mode (on|off|warn)

Without the balancer, the suggested target is approximately 100 PG replicas on
each OSD. With the balancer, an initial target of 50 PG replicas on each OSD is
reasonable.

The autoscaler attempts to satisfy the following conditions:

- the number of PGs per OSD should be proportional to the amount of data in the
  pool
- there should be 50-100 PGs per pool, taking into account the replication
  overhead or erasure-coding fan-out of each PG's replicas across OSDs

Use of Placement Groups
=======================

A placement group aggregates objects within a pool. The tracking of RADOS
object placement and object metadata on a per-object basis is computationally
expensive. It would be infeasible for a system with millions of RADOS
objects to efficiently track placement on a per-object basis.

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

The Ceph client calculates which PG a RADOS object should be in. As part of
this calculation, the client hashes the object ID and performs an operation
involving both the number of PGs in the specified pool and the pool ID. For
details, see `Mapping PGs to OSDs`_.

The contents of a RADOS object belonging to a PG are stored in a set of OSDs.
For example, in a replicated pool of size two, each PG will store objects on
two OSDs, as shown below:

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


If OSD #2 fails, another OSD will be assigned to Placement Group #1 and then
filled with copies of all objects in OSD #1. If the pool size is changed from
two to three, an additional OSD will be assigned to the PG and will receive
copies of all objects in the PG.

An OSD assigned to a PG is not owned exclusively by that PG; rather, the OSD is
shared with other PGs either from the same pool or from other pools. In our
example, OSD #2 is shared by Placement Group #1 and Placement Group #2. If OSD
#2 fails, then Placement Group #2 must restore copies of objects (by making use
of OSD #3).

When the number of PGs increases, several consequences ensue. The new PGs are
assigned OSDs. The result of the CRUSH function changes, which means that some
objects from the already-existing PGs are copied to the new PGs and removed
from the old ones.

Factors Relevant To Specifying pg_num
=====================================

On the one hand, the criteria of data durability and even distribution across
OSDs weigh in favor of a high number of PGs. On the other hand, the criteria of
saving CPU resources and minimizing memory usage weigh in favor of a low number
of PGs.

.. _data durability:

Data durability
---------------

When an OSD fails, the risk of data loss is increased until replication of the
data it hosted is restored to the configured level. To illustrate this point,
let's imagine a scenario that results in permanent data loss in a single PG:

#. The OSD fails and all copies of the object that it contains are lost.  For
   each object within the PG, the number of its replicas suddenly drops from
   three to two.

#. Ceph starts recovery for this PG by choosing a new OSD on which to re-create
   the third copy of each object.

#. Another OSD within the same PG fails before the new OSD is fully populated
   with the third copy. Some objects will then only have one surviving copy.

#. Ceph selects yet another OSD and continues copying objects in order to
   restore the desired number of copies.

#. A third OSD within the same PG fails before recovery is complete. If this
   OSD happened to contain the only remaining copy of an object, the object is
   permanently lost.

In a cluster containing 10 OSDs with 512 PGs in a three-replica pool, CRUSH
will give each PG three OSDs.  Ultimately, each OSD hosts :math:`\frac{(512 *
3)}{10} = ~150` PGs. So when the first OSD fails in the above scenario,
recovery will begin for all 150 PGs at the same time.

The 150 PGs that are being recovered are likely to be homogeneously distributed
across the 9 remaining OSDs. Each remaining OSD is therefore likely to send
copies of objects to all other OSDs and also likely to receive some new objects
to be stored because it has become part of a new PG.

The amount of time it takes for this recovery to complete depends on the
architecture of the Ceph cluster. Compare two setups: (1) Each OSD is hosted by
a 1 TB SSD on a single machine, all of the OSDs are connected to a 10 Gb/s
switch, and the recovery of a single OSD completes within a certain number of
minutes. (2) There are two OSDs per machine using HDDs with no SSD WAL+DB and
a 1 Gb/s switch. In the second setup, recovery will be at least one order of
magnitude slower.

In such a cluster, the number of PGs has almost no effect on data durability.
Whether there are 128 PGs per OSD or 8192 PGs per OSD, the recovery will be no
slower or faster.

However, an increase in the number of OSDs can increase the speed of recovery.
Suppose our Ceph cluster is expanded from 10 OSDs to 20 OSDs.  Each OSD now
participates in only ~75 PGs rather than ~150 PGs. All 19 remaining OSDs will
still be required to replicate the same number of objects in order to recover.
But instead of there being only 10 OSDs that have to copy ~100 GB each, there
are now 20 OSDs that have to copy only 50 GB each. If the network had
previously been a bottleneck, recovery now happens twice as fast.

Similarly, suppose that our cluster grows to 40 OSDs. Each OSD will host only
~38 PGs. And if an OSD dies, recovery will take place faster than before unless
it is blocked by another bottleneck. Now, however, suppose that our cluster
grows to 200 OSDs. Each OSD will host only ~7 PGs. And if an OSD dies, recovery
will happen across at most :math:`\approx 21 = (7 \times 3)` OSDs
associated with these PGs. This means that recovery will take longer than when
there were only 40 OSDs. For this reason, the number of PGs should be
increased.

No matter how brief the recovery time is, there is always a chance that an
additional OSD will fail while recovery is in progress.  Consider the cluster
with 10 OSDs described above: if any of the OSDs fail, then :math:`\approx 17`
(approximately 150 divided by 9) PGs will have only one remaining copy. And if
any of the 8 remaining OSDs fail, then 2 (approximately 17 divided by 8) PGs
are likely to lose their remaining objects. This is one reason why setting
``size=2`` is risky.

When the number of OSDs in the cluster increases to 20, the number of PGs that
would be damaged by the loss of three OSDs significantly decreases. The loss of
a second OSD degrades only approximately :math:`4` or (:math:`\frac{75}{19}`)
PGs rather than :math:`\approx 17` PGs, and the loss of a third OSD results in
data loss only if it is one of the 4 OSDs that contains the remaining copy.
This means -- assuming that the probability of losing one OSD during recovery
is 0.0001% -- that the probability of data loss when three OSDs are lost is
:math:`\approx 17 \times 10 \times 0.0001%` in the cluster with 10 OSDs, and
only :math:`\approx 4 \times 20 \times 0.0001%` in the cluster with 20 OSDs.

In summary, the greater the number of OSDs, the faster the recovery and the
lower the risk of permanently losing a PG due to cascading failures. As far as
data durability is concerned, in a cluster with fewer than 50 OSDs, it doesn't
much matter whether there are 512 or 4096 PGs.

.. note::  It can take a long time for an OSD that has been recently added to
   the cluster to be populated with the PGs assigned to it.  However, no object
   degradation or impact on data durability will result from the slowness of
   this process since Ceph populates data into the new PGs before removing it
   from the old PGs.

.. _object distribution:

Object distribution within a pool
---------------------------------

Under ideal conditions, objects are evenly distributed across PGs. Because
CRUSH computes the PG for each object but does not know how much data is stored
in each OSD associated with the PG, the ratio between the number of PGs and the
number of OSDs can have a significant influence on data distribution.

For example, suppose that there is only a single PG for ten OSDs in a
three-replica pool. In that case, only three OSDs would be used because CRUSH
would have no other option. However, if more PGs are available, RADOS objects are
more likely to be evenly distributed across OSDs.  CRUSH makes every effort to
distribute OSDs evenly across all existing PGs.

As long as there are one or two orders of magnitude more PGs than OSDs, the
distribution is likely to be even. For example: 256 PGs for 3 OSDs, 512 PGs for
10 OSDs, or 1024 PGs for 10 OSDs.

However, uneven data distribution can emerge due to factors other than the
ratio of PGs to OSDs. For example, since CRUSH does not take into account the
size of the RADOS objects, the presence of a few very large RADOS objects can
create an imbalance. Suppose that one million 4 KB RADOS objects totaling 4 GB
are evenly distributed among 1024 PGs on 10 OSDs. These RADOS objects will
consume 4 GB / 10 = 400 MB on each OSD. If a single 400 MB RADOS object is then
added to the pool, the three OSDs supporting the PG in which the RADOS object
has been placed will each be filled with 400 MB + 400 MB = 800 MB but the seven
other OSDs will still contain only 400 MB.

.. _resource usage:

Memory, CPU and network usage
-----------------------------

Every PG in the cluster imposes memory, network, and CPU demands upon OSDs and
MONs. These needs must be met at all times and are increased during recovery.
Indeed, one of the main reasons PGs were developed was to share this overhead
by clustering objects together.

For this reason, minimizing the number of PGs saves significant resources.

.. _choosing-number-of-placement-groups:

Choosing the Number of PGs
==========================

.. note: It is rarely necessary to do the math in this section by hand.
   Instead, use the ``ceph osd pool autoscale-status`` command in combination
   with the ``target_size_bytes`` or ``target_size_ratio`` pool properties. For
   more information, see :ref:`pg-autoscaler`.

If you have more than 50 OSDs, we recommend approximately 50-100 PGs per OSD in
order to balance resource usage, data durability, and data distribution. If you
have fewer than 50 OSDs, follow the guidance in the `preselection`_ section.
For a single pool, use the following formula to get a baseline value:

  Total PGs = :math:`\frac{OSDs \times 100}{pool \: size}`

Here **pool size** is either the number of replicas for replicated pools or the
K+M sum for erasure-coded pools. To retrieve this sum, run the command ``ceph
osd erasure-code-profile get``.

Next, check whether the resulting baseline value is consistent with the way you
designed your Ceph cluster to maximize `data durability`_ and `object
distribution`_ and to minimize `resource usage`_.

This value should be **rounded up to the nearest power of two**.

Each pool's ``pg_num`` should be a power of two. Other values are likely to
result in uneven distribution of data across OSDs. It is best to increase
``pg_num`` for a pool only when it is feasible and desirable to set the next
highest power of two. Note that this power of two rule is per-pool; it is
neither necessary nor easy to align the sum of all pools' ``pg_num`` to a power
of two.

For example, if you have a cluster with 200 OSDs and a single pool with a size
of 3 replicas, estimate the number of PGs as follows:

  :math:`\frac{200 \times 100}{3} = 6667`. Rounded up to the nearest power of 2: 8192.

When using multiple data pools to store objects, make sure that you balance the
number of PGs per pool against the number of PGs per OSD so that you arrive at
a reasonable total number of PGs. It is important to find a number that
provides reasonably low variance per OSD without taxing system resources or
making the peering process too slow.

For example, suppose you have a cluster of 10 pools, each with 512 PGs on 10
OSDs. That amounts to 5,120 PGs distributed across 10 OSDs, or 512 PGs per OSD.
This cluster will not use too many resources. However, in a cluster of 1,000
pools, each with 512 PGs on 10 OSDs, the OSDs will have to handle ~50,000 PGs
each. This cluster will require significantly more resources and significantly
more time for peering.


.. _setting the number of placement groups:

Setting the Number of PGs
=========================

Setting the initial number of PGs in a pool must be done at the time you create
the pool. See `Create a Pool`_ for details. 

However, even after a pool is created, if the ``pg_autoscaler`` is not being
used to manage ``pg_num`` values, you can change the number of PGs by running a
command of the following form:

.. prompt:: bash # 

   ceph osd pool set {pool-name} pg_num {pg_num}

If you increase the number of PGs, your cluster will not rebalance until you
increase the number of PGs for placement (``pgp_num``). The ``pgp_num``
parameter specifies the number of PGs that are to be considered for placement
by the CRUSH algorithm. Increasing ``pg_num`` splits the PGs in your cluster,
but data will not be migrated to the newer PGs until ``pgp_num`` is increased.
The ``pgp_num`` parameter should be equal to the ``pg_num`` parameter. To
increase the number of PGs for placement, run a command of the following form:

.. prompt:: bash #

   ceph osd pool set {pool-name} pgp_num {pgp_num}

If you decrease the number of PGs, then ``pgp_num`` is adjusted automatically.
In releases of Ceph that are Nautilus and later (inclusive), when the
``pg_autoscaler`` is not used, ``pgp_num`` is automatically stepped to match
``pg_num``. This process manifests as periods of remapping of PGs and of
backfill, and is expected behavior and normal.

.. _rados_ops_pgs_get_pg_num:

Get the Number of PGs
=====================

To get the number of PGs in a pool, run a command of the following form:

.. prompt:: bash #

   ceph osd pool get {pool-name} pg_num


Get a Cluster's PG Statistics
=============================

To see the details of the PGs in your cluster, run a command of the following
form:

.. prompt:: bash #

   ceph pg dump [--format {format}]

Valid formats are ``plain`` (default) and ``json``.


Get Statistics for Stuck PGs
============================

To see the statistics for all PGs that are stuck in a specified state, run a
command of the following form:

.. prompt:: bash #

   ceph pg dump_stuck inactive|unclean|stale|undersized|degraded [--format <format>] [-t|--threshold <seconds>]

- **Inactive** PGs cannot process reads or writes because they are waiting for
  enough OSDs with the most up-to-date data to come ``up`` and ``in``.

- **Undersized** PGs contain objects that have not been replicated the desired
  number of times. Under normal conditions, it can be assumed that these PGs
  are recovering.

- **Stale** PGs are in an unknown state -- the OSDs that host them have not
  reported to the monitor cluster for a certain period of time (determined by
  ``mon_osd_report_timeout``).

Valid formats are ``plain`` (default) and ``json``. The threshold defines the
minimum number of seconds the PG is stuck before it is included in the returned
statistics (default: 300).


Get a PG Map
============

To get the PG map for a particular PG, run a command of the following form:

.. prompt:: bash #

   ceph pg map {pg-id}

For example: 

.. prompt:: bash #

   ceph pg map 1.6c

Ceph will return the PG map, the PG, and the OSD status. The output resembles
the following:

.. prompt:: bash #

   osdmap e13 pg 1.6c (1.6c) -> up [1,0] acting [1,0]


Get a PG's Statistics
=====================

To see statistics for a particular PG, run a command of the following form:

.. prompt:: bash #

   ceph pg {pg-id} query

Scrub a PG
==========

To force an immediate scrub of a PG, run a command of the following form:

.. prompt:: bash #

   ceph tell {pg-id} scrub

or

.. prompt:: bash #

   ceph tell {pg-id} deep-scrub

Ceph checks the primary and replica OSDs and generates a catalog of all objects in
the PG. For each object, Ceph compares all instances of the object (in the primary
and replica OSDs) to ensure that they are consistent. For shallow scrubs (initiated
by the first command format), only object metadata is compared. Deep scrubs
(initiated by the second command format) compare the contents of the objects as well.
If the replicas all match, a final semantic sweep takes place to ensure that all
snapshot-related object metadata is consistent.  Errors are reported in logs.

Scrubs initiated using the command format above are deemed high priority, and
are performed immediately. Such scrubs are not subject to any day-of-week or
time-of-day restrictions that are in effect for regular, periodic, scrubs.
They are not limited by 'osd_max_scrubs', and are not required to wait for
their replicas' scrub resources.

A second command format exists for initiating a scrub as-if it were a regular
scrub. This command format is as follows:

.. prompt:: bash #

   ceph tell {pg-id} schedule-scrub

or

.. prompt:: bash #

   ceph tell {pg-id} schedule-deep-scrub


To scrub all PGs from a specific pool, run a command of the following form:

.. prompt:: bash #

   ceph osd pool scrub {pool-name}


Prioritize backfill/recovery of PG(s)
=====================================

You might encounter a situation in which multiple PGs require recovery or
backfill, but the data in some PGs is more important than the data in others
(for example, some PGs hold data for images that are used by running machines
and other PGs are used by inactive machines and hold data that is less
relevant). In that case, you might want to prioritize recovery or backfill of
the PGs with especially important data so that the performance of the cluster
and the availability of their data are restored sooner. To designate specific
PG(s) as prioritized during recovery, run a command of the following form:

.. prompt:: bash #

   ceph pg force-recovery {pg-id} [{pg-id #2}] [{pg-id #3} ...]

To mark specific PG(s) as prioritized during backfill, run a command of the
following form:

.. prompt:: bash #

   ceph pg force-backfill {pg-id} [{pg-id #2}] [{pg-id #3} ...]

These commands instruct Ceph to perform recovery or backfill on the specified
PGs before processing the other PGs. Prioritization does not interrupt current
backfills or recovery, but places the specified PGs at the top of the queue so
that they will be acted upon next.  If you change your mind or realize that you
have prioritized the wrong PGs, run one or both of the following commands:

.. prompt:: bash #

   ceph pg cancel-force-recovery {pg-id} [{pg-id #2}] [{pg-id #3} ...]
   ceph pg cancel-force-backfill {pg-id} [{pg-id #2}] [{pg-id #3} ...]

These commands remove the ``force`` flag from the specified PGs, so that the
PGs will be processed in their usual order. As in the case of adding the
``force`` flag, this affects only those PGs that are still queued but does not
affect PGs currently undergoing recovery.

The ``force`` flag is cleared automatically after recovery or backfill of the
PGs is complete.

Similarly, to instruct Ceph to prioritize all PGs from a specified pool (that
is, to perform recovery or backfill on those PGs first), run one or both of the
following commands:

.. prompt:: bash #

   ceph osd pool force-recovery {pool-name}
   ceph osd pool force-backfill {pool-name}

These commands can also be cancelled. To revert to the default order, run one
or both of the following commands:

.. prompt:: bash #

   ceph osd pool cancel-force-recovery {pool-name}
   ceph osd pool cancel-force-backfill {pool-name}

.. warning:: These commands can break the order of Ceph's internal priority
   computations, so use them with caution! If you have multiple pools that are
   currently sharing the same underlying OSDs, and if the data held by certain
   pools is more important than the data held by other pools, then we recommend
   that you run a command of the following form to arrange a custom
   recovery/backfill priority for all pools:

.. prompt:: bash #

   ceph osd pool set {pool-name} recovery_priority {value}

For example, if you have twenty pools, you could make the most important pool  
priority ``20``, and the next most important pool priority ``19``, and so on.  

Another option is to set the recovery/backfill priority for only a proper
subset of pools. In such a scenario, three important pools might (all) be
assigned priority ``1`` and all other pools would be left without an assigned
recovery/backfill priority.  Another possibility is to select three important
pools and set their recovery/backfill priorities to ``3``, ``2``, and ``1``
respectively.

.. important:: Numbers of greater value have higher priority than numbers of
   lesser value when using ``ceph osd pool set {pool-name} recovery_priority
   {value}`` to set their recovery/backfill priority. For example, a pool with
   the recovery/backfill priority ``30`` has a higher priority than a pool with
   the recovery/backfill priority ``15``.

Reverting Lost RADOS Objects
============================

If the cluster has lost one or more RADOS objects and you have decided to
abandon the search for the lost data, you must mark the unfound objects
``lost``.

If every possible location has been queried and all OSDs are ``up`` and ``in``,
but certain RADOS objects are still lost, you might have to give up on those
objects. This situation can arise when rare and unusual combinations of
failures allow the cluster to learn about writes that were performed before the
writes themselves were recovered.

The command to mark a RADOS object ``lost`` has only one supported option:
``revert``. The ``revert`` option will either roll back to a previous version
of the RADOS object (if it is old enough to have a previous version) or forget
about it entirely (if it is too new to have a previous version). To mark the
"unfound" objects ``lost``, run a command of the following form:


.. prompt:: bash #

   ceph pg {pg-id} mark_unfound_lost revert|delete

.. important:: Use this feature with caution. It might confuse applications
   that expect the object(s) to exist.


.. toctree::
        :hidden:

        pg-states
        pg-concepts


.. _Create a Pool: ../pools#createpool
.. _Mapping PGs to OSDs: ../../../architecture#mapping-pgs-to-osds
