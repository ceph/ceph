.. _balancer:

Balancer Module
=======================

The *balancer* can optimize the allocation of placement groups (PGs) across
OSDs in order to achieve a balanced distribution. The balancer can operate
either automatically or in a supervised fashion.


Status
------

To check the current status of the balancer, run the following command:

   .. prompt:: bash $

      ceph balancer status


Automatic balancing
-------------------

When the balancer is in ``upmap`` mode, which is the default, the automatic
upmap balancing feature is enabled.  For more details, see :ref:`upmap`.
To disable the balancer, run the following command:

   .. prompt:: bash $

      ceph balancer off

The balancer mode can be changed from ``upmap`` mode to ``crush-compat`` mode.
``crush-compat`` mode is backward compatible with older clients.  In
``crush-compat`` mode, the balancer automatically makes small changes to the
data distribution in order to ensure that OSDs are utilized equally.

Additional modes include ``upmap-read`` and ``read``. ``upmap-read`` mode
combines the upmap balancer with the read balancer so that both writes
and reads are optimized. ``read`` mode can be used when only read optimization
is desired. For more details, see :ref:`read_balancer`.

Throttling
----------

If the cluster is degraded (that is, if an OSD has failed and the system hasn't
healed itself yet), then the balancer will not make any adjustments to the PG
distribution.

When the cluster is healthy, the balancer will incrementally move a small
fraction of unbalanced PGs in order to improve distribution.  This fraction
will not exceed a certain threshold that defaults to 5%. To adjust this
``target_max_misplaced_ratio`` threshold setting, run the following command:

   .. prompt:: bash $

      ceph config set mgr target_max_misplaced_ratio .07   # 7%

The balancer sleeps between runs. To set the number of seconds for this
interval of sleep, run the following command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/sleep_interval 60

To set the time of day (in HHMM format) at which automatic balancing begins,
run the following command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/begin_time 0000

To set the time of day (in HHMM format) at which automatic balancing ends, run
the following command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/end_time 2359

Automatic balancing can be restricted to certain days of the week.  To restrict
it to a specific day of the week or later (as with crontab, ``0`` is Sunday,
``1`` is Monday, and so on), run the following command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/begin_weekday 0

To restrict automatic balancing to a specific day of the week or earlier
(again, ``0`` is Sunday, ``1`` is Monday, and so on), run the following
command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/end_weekday 6

Automatic balancing can be restricted to certain pools. By default, the value
of this setting is an empty string, so that all pools are automatically
balanced.  To restrict automatic balancing to specific pools, retrieve their
numeric pool IDs (by running the :command:`ceph osd pool ls detail` command),
and then run the following command:

   .. prompt:: bash $

      ceph config set mgr mgr/balancer/pool_ids 1,2,3


Modes
-----

There are four supported balancer modes:

#. **crush-compat**. This mode uses the compat weight-set feature (introduced
   in Luminous) to manage an alternative set of weights for devices in the
   CRUSH hierarchy. When the balancer is operating in this mode, the normal
   weights should remain set to the size of the device in order to reflect the
   target amount of data intended to be stored on the device. The balancer will
   then optimize the weight-set values, adjusting them up or down in small
   increments, in order to achieve a distribution that matches the target
   distribution as closely as possible. (Because PG placement is a pseudorandom
   process, it is subject to a natural amount of variation; optimizing the
   weights serves to counteract that natural variation.)

   Note that this mode is *fully backward compatible* with older clients: when
   an OSD Map and CRUSH map are shared with older clients, Ceph presents the
   optimized weights as the "real" weights.

   The primary limitation of this mode is that the balancer cannot handle
   multiple CRUSH hierarchies with different placement rules if the subtrees of
   the hierarchy share any OSDs. (Such sharing of OSDs is not typical and,
   because of the difficulty of managing the space utilization on the shared
   OSDs, is generally not recommended.)

#. **upmap**. In Luminous and later releases, the OSDMap can store explicit
   mappings for individual OSDs as exceptions to the normal CRUSH placement
   calculation. These ``upmap`` entries provide fine-grained control over the
   PG mapping. This balancer mode optimizes the placement of individual PGs in
   order to achieve a balanced distribution.  In most cases, the resulting
   distribution is nearly perfect: that is, there is an equal number of PGs on
   each OSD (±1 PG, since the total number might not divide evenly).

   To use ``upmap``, all clients must be Luminous or newer.

#. **read**. In Reef and later releases, the OSDMap can store explicit
   mappings for individual primary OSDs as exceptions to the normal CRUSH
   placement calculation. These ``pg-upmap-primary`` entries provide fine-grained
   control over primary PG mappings. This mode optimizes the placement of individual
   primary PGs in order to achieve balanced reads, or primary PGs, in a cluster.
   In ``read`` mode, upmap behavior is not excercised, so this mode is best for
   uses cases in which only read balancing is desired.

   To use ``pg-upmap-primary``, all clients must be Reef or newer. For more
   details about client compatibility, see :ref:`read_balancer`.

#. **upmap-read**. This balancer mode combines optimization benefits of
   both ``upmap`` and ``read`` mode. Like in ``read`` mode, ``upmap-read``
   makes use of ``pg-upmap-primary``. As such, only Reef and later clients
   are compatible. For more details about client compatibility, see
   :ref:`read_balancer`.

   ``upmap-read`` is highly recommended for achieving the ``upmap`` mode's
   offering of balanced PG distribution as well as the ``read`` mode's
   offering of balanced reads.

The default mode is ``upmap``. The mode can be changed to ``crush-compat`` by running the following command:

   .. prompt:: bash $

      ceph balancer mode crush-compat

The mode can be changed to ``read`` by running the following command:

   .. prompt:: bash $

      ceph balancer mode read

The mode can be changed to ``upmap-read`` by running the following command:

   .. prompt:: bash $

      ceph balancer mode upmap-read

Supervised optimization
-----------------------

Supervised use of the balancer can be understood in terms of three distinct
phases:

#. building a plan
#. evaluating the quality of the data distribution, either for the current PG
   distribution or for the PG distribution that would result after executing a
   plan
#. executing the plan

To evaluate the current distribution, run the following command:

   .. prompt:: bash $

      ceph balancer eval

To evaluate the distribution for a single pool, run the following command:

   .. prompt:: bash $

      ceph balancer eval <pool-name>

To see the evaluation in greater detail, run the following command:

   .. prompt:: bash $

      ceph balancer eval-verbose ...

To instruct the balancer to generate a plan (using the currently configured
mode), make up a name (any useful identifying string) for the plan, and run the
following command:

   .. prompt:: bash $

      ceph balancer optimize <plan-name>

To see the contents of a plan, run the following command:

   .. prompt:: bash $

      ceph balancer show <plan-name>

To display all plans, run the following command:

   .. prompt:: bash $

      ceph balancer ls

To discard an old plan, run the following command:

   .. prompt:: bash $

      ceph balancer rm <plan-name>

To see currently recorded plans, examine the output of the following status
command:

   .. prompt:: bash $

      ceph balancer status

To see the status in greater detail, run the following command:

   .. prompt:: bash $

      ceph balancer status detail

To evaluate the distribution that would result from executing a specific plan,
run the following command:

   .. prompt:: bash $

      ceph balancer eval <plan-name>

If a plan is expected to improve the distribution (that is, the plan's score is
lower than the current cluster state's score), you can execute that plan by
running the following command:

   .. prompt:: bash $

      ceph balancer execute <plan-name>
