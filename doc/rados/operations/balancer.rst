
.. _balancer:

Balancer
========

The *balancer* can optimize the placement of PGs across OSDs in
order to achieve a balanced distribution, either automatically or in a
supervised fashion.

Status
------

The current status of the balancer can be checked at any time with::

  ceph balancer status


Automatic balancing
-------------------

The automatic balancing feature is enabled by default in ``upmap``
mode. Please refer to :ref:`upmap` for more details. The balancer can be
turned off with::

  ceph balancer off

The balancer mode can be changed to ``crush-compat`` mode, which is
backward compatible with older clients, and will make small changes to
the data distribution over time to ensure that OSDs are equally utilized.


Throttling
----------

No adjustments will be made to the PG distribution if the cluster is
degraded (e.g., because an OSD has failed and the system has not yet
healed itself).

When the cluster is healthy, the balancer will throttle its changes
such that the percentage of PGs that are misplaced (i.e., that need to
be moved) is below a threshold of (by default) 5%.  The
``target_max_misplaced_ratio`` threshold can be adjusted with::

  ceph config set mgr target_max_misplaced_ratio .07   # 7%

Set the number of seconds to sleep in between runs of the automatic balancer::

  ceph config set mgr mgr/balancer/sleep_interval 60

Set the time of day to begin automatic balancing in HHMM format::

  ceph config set mgr mgr/balancer/begin_time 0000

Set the time of day to finish automatic balancing in HHMM format::

  ceph config set mgr mgr/balancer/end_time 2359

Restrict automatic balancing to this day of the week or later. 
Uses the same conventions as crontab, 0 is Sunday, 1 is Monday, and so on::

  ceph config set mgr mgr/balancer/begin_weekday 0

Restrict automatic balancing to this day of the week or earlier. 
Uses the same conventions as crontab, 0 is Sunday, 1 is Monday, and so on::

  ceph config set mgr mgr/balancer/end_weekday 6

Pool IDs to which the automatic balancing will be limited. 
The default for this is an empty string, meaning all pools will be balanced. 
The numeric pool IDs can be gotten with the :command:`ceph osd pool ls detail` command::

  ceph config set mgr mgr/balancer/pool_ids 1,2,3


Modes
-----

There are currently two supported balancer modes:

#. **crush-compat**.  The CRUSH compat mode uses the compat weight-set
   feature (introduced in Luminous) to manage an alternative set of
   weights for devices in the CRUSH hierarchy.  The normal weights
   should remain set to the size of the device to reflect the target
   amount of data that we want to store on the device.  The balancer
   then optimizes the weight-set values, adjusting them up or down in
   small increments, in order to achieve a distribution that matches
   the target distribution as closely as possible.  (Because PG
   placement is a pseudorandom process, there is a natural amount of
   variation in the placement; by optimizing the weights we
   counter-act that natural variation.)

   Notably, this mode is *fully backwards compatible* with older
   clients: when an OSDMap and CRUSH map is shared with older clients,
   we present the optimized weights as the "real" weights.

   The primary restriction of this mode is that the balancer cannot
   handle multiple CRUSH hierarchies with different placement rules if
   the subtrees of the hierarchy share any OSDs.  (This is normally
   not the case, and is generally not a recommended configuration
   because it is hard to manage the space utilization on the shared
   OSDs.)

#. **upmap**.  Starting with Luminous, the OSDMap can store explicit
   mappings for individual OSDs as exceptions to the normal CRUSH
   placement calculation.  These `upmap` entries provide fine-grained
   control over the PG mapping.  This CRUSH mode will optimize the
   placement of individual PGs in order to achieve a balanced
   distribution.  In most cases, this distribution is "perfect," which
   an equal number of PGs on each OSD (+/-1 PG, since they might not
   divide evenly).

   Note that using upmap requires that all clients be Luminous or newer.

The default mode is ``upmap``.  The mode can be adjusted with::

  ceph balancer mode crush-compat

Supervised optimization
-----------------------

The balancer operation is broken into a few distinct phases:

#. building a *plan*
#. evaluating the quality of the data distribution, either for the current PG distribution, or the PG distribution that would result after executing a *plan*
#. executing the *plan*

To evaluate and score the current distribution::

  ceph balancer eval

You can also evaluate the distribution for a single pool with::

  ceph balancer eval <pool-name>

Greater detail for the evaluation can be seen with::

  ceph balancer eval-verbose ...
  
The balancer can generate a plan, using the currently configured mode, with::

  ceph balancer optimize <plan-name>

The name is provided by the user and can be any useful identifying string.  The contents of a plan can be seen with::

  ceph balancer show <plan-name>

All plans can be shown with::

  ceph balancer ls

Old plans can be discarded with::

  ceph balancer rm <plan-name>

Currently recorded plans are shown as part of the status command::

  ceph balancer status

The quality of the distribution that would result after executing a plan can be calculated with::

  ceph balancer eval <plan-name>

Assuming the plan is expected to improve the distribution (i.e., it has a lower score than the current cluster state), the user can execute that plan with::

  ceph balancer execute <plan-name>

