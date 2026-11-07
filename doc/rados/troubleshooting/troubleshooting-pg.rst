====================
 Troubleshooting PGs
====================

Placement Groups Never Get Clean
================================

Placement Groups (PGs) that remain in the ``active`` status, the
``active+remapped`` status or the ``active+degraded`` status and never achieve
an ``active+clean`` status might indicate a problem with the configuration of
the Ceph cluster.

In such a situation, review the settings in the :ref:`rados_config_pool_pg_crush_ref`
and make appropriate adjustments.

As a general rule, run your cluster with more than one OSD and a pool size
of greater than two object replicas.

.. _one-node-cluster:

One Node Cluster
----------------

Ceph no longer provides documentation for operating on a single node.  Systems
designed for distributed computing by definition do not run on a single node.
The mounting of client kernel modules on a single node that contains a Ceph
daemon may cause a deadlock due to issues with the Linux kernel itself (unless
VMs are used as clients). You can experiment with Ceph in a one-node
configuration, in spite of the limitations as described herein.

To create a cluster on a single node, you must change the
:confval:`osd_crush_chooseleaf_type` setting from the default of ``1`` (meaning
``host`` or ``node``) to ``0`` (meaning ``osd``) in your Ceph configuration
file before you create Monitors and OSDs. This tells Ceph that an OSD is
permitted to place another OSD on the same host. If you are trying to set up a
single-node cluster and :confval:`osd_crush_chooseleaf_type` is greater than ``0``,
Ceph will attempt to place the PGs of one OSD with the PGs of another OSD on
another node, chassis, rack, row, or datacenter depending on the setting.

.. tip:: DO NOT mount kernel clients directly on the same node as your Ceph
   Storage Cluster. Kernel conflicts can arise. However, you can mount kernel
   clients within virtual machines (VMs) on a single node.

If you are creating OSDs using a single disk, you must manually create
directories for the data first.


Fewer OSDs than Replicas
------------------------

If a number of OSDs are in an ``up`` and ``in`` state, but the placement groups are not
in an ``active+clean`` state, you may have an :confval:`osd_pool_default_size` set
to greater than the number of ``up`` and ``in`` state OSDs.

There are a few ways to address this situation. For example, if you want to operate your
cluster with :confval:`osd_pool_default_size` set to ``3`` in an ``active+degraded`` state with two replicas, you can set the
:confval:`osd_pool_default_min_size` to ``2`` so that you can write objects in an
``active+degraded`` state. You may also set the :confval:`osd_pool_default_size`
setting to ``2`` so that you have only two stored replicas (the original and
one replica). In such a case, the cluster should achieve an ``active+clean``
state.

.. note:: You can make the changes while the cluster is running. If you make
   the changes in your Ceph configuration file, you might need to restart your
   cluster.


Pool Size = 1
-------------

If you have :confval:`osd_pool_default_size` set to ``1``, you will have only one copy
of the object. OSDs rely on other OSDs to tell them which objects they should
have. If one OSD has a copy of an object and there is no second copy, then
there is no second OSD to tell the first OSD that it should have that copy. For
each placement group mapped to the first OSD (see ``ceph pg dump``), you can
force the first OSD to notice the placement groups it needs by running a
command of the following form:

.. prompt:: bash #

   ceph osd force-create-pg <pgid>


CRUSH Map Errors
----------------

If any placement groups in your cluster are ``unclean``, then there might be errors
in your CRUSH map.

.. _failures-pg-stuck:

Stuck Placement Groups
======================

It is normal for placement groups to enter ``degraded`` or ``peering`` states after
a component failure. Normally, these states reflect the expected progression
through the failure recovery process. However, a placement group that stays in
one of these states for a long time might be an indication of a larger problem.
For this reason, the Ceph Monitors will warn when placement groups get "stuck"
in a non-optimal state. Specifically, we check for:

* ``inactive`` The placement group has not been ``active`` for too long (that
  is, it hasn't been able to service read/write requests).

* ``unclean`` The placement group has not been ``clean`` for too long (that
  is, it hasn't been able to completely recover from a previous failure).

* ``stale`` The placement group status has not been updated by an OSD.
  This indicates that all nodes storing this placement group may be ``down``.

List stuck placement groups by running one of the following commands:

.. prompt:: bash #

   ceph pg dump_stuck stale
   ceph pg dump_stuck inactive
   ceph pg dump_stuck unclean

- Stuck ``stale`` placement groups usually indicate that key OSDs are
  not running.
- Stuck ``inactive`` placement groups usually indicate a peering problem (see
  :ref:`failures-osd-peering`).
- Stuck ``unclean`` placement groups usually indicate that something is
  preventing recovery from completing, possibly unfound objects (see
  :ref:`failures-osd-unfound`).


.. _failures-osd-peering:

Placement Group Down - Peering Failure
======================================

In certain cases, the OSD `peering` process can run into problems,
which can prevent a PG from becoming active and usable. In such a case, running
the command ``ceph health detail`` will report something similar to the following:

.. prompt:: bash #

   ceph health detail

.. code-block:: none

    HEALTH_ERR 7 pgs degraded; 12 pgs down; 12 pgs peering; 1 pgs recovering; 6 pgs stuck unclean; 114/3300 degraded (3.455%); 1/3 in osds are down
    ...
    pg 0.5 is down+peering
    pg 1.4 is down+peering
    ...
    osd.1 is down since epoch 69, last address 192.168.106.220:6801/8651

Query the cluster to determine exactly why the PG is marked ``down`` by running a command of the following form:

.. prompt:: bash #

   ceph pg 0.5 query

.. code-block:: javascript

 { "state": "down+peering",
   ...
   "recovery_state": [
        { "name": "Started/Primary/Peering/GetInfo",
          "enter_time": "2012-03-06 14:40:16.169679",
          "requested_info_from": []},
        { "name": "Started/Primary/Peering",
          "enter_time": "2012-03-06 14:40:16.169659",
          "probing_osds": [
                0,
                1],
          "blocked": "peering is blocked due to down osds",
          "down_osds_we_would_probe": [
                1],
          "peering_blocked_by": [
                { "osd": 1,
                  "current_lost_at": 0,
                  "comment": "starting or marking this osd lost may let us proceed"}]},
        { "name": "Started",
          "enter_time": "2012-03-06 14:40:16.169513"}
    ]
 }

The ``recovery_state`` section tells us that peering is blocked due to down
OSDs, specifically ``osd.1``. In this case, we can start that
particular OSD and recovery will proceed.

Alternatively, if there is a catastrophic failure of ``osd.1`` (for example, if
there has been a disk failure), the cluster can be informed that the OSD is
``lost`` and the cluster can be instructed that it must cope as best it can.

.. important:: Informing the cluster that an OSD has been lost is dangerous
   because the cluster cannot guarantee that the other copies of the data are
   consistent and up to date.

To report an OSD ``lost`` and to instruct Ceph to continue to attempt recovery
anyway, run a command of the following form:

.. prompt:: bash #

   ceph osd lost 1

Recovery will proceed.


.. _failures-osd-unfound:

Unfound Objects
===============

Under certain combinations of failures, Ceph may complain about ``unfound``
objects, as in this example:

.. prompt:: bash #

   ceph health detail

.. code-block:: none

   HEALTH_WARN 1 pgs degraded; 78/3778 unfound (2.065%)
   pg 2.4 is active+degraded, 78 unfound

This means that the storage cluster knows that some objects (or newer copies of
existing objects) exist, but it hasn't found copies of them.  Here is an
example of how this might come about for a PG whose data is on two OSDs, which
we will call "1" and "2":

* 1 goes down.
* 2 handles some writes, alone.
* 1 comes up.
* 1 and 2 re-peer, and the objects missing on 1 are queued for recovery.
* Before the new objects are copied, 2 goes down.

At this point, 1 knows that these objects exist, but there is no live OSD
that has a copy of the objects. In this case, I/O to those objects
will block, and the cluster will hope that the failed node comes back soon.
This is assumed to be preferable to returning an I/O error to the user.

.. note:: The situation described immediately above is one reason that setting
   ``size=2`` on a replicated pool and ``m=1`` on an erasure coded pool risks
   data loss.

Identify which objects are unfound by running a command of the following form:

.. prompt:: bash #

   ceph pg 2.4 list_unfound [starting offset, in json]

.. code-block:: json

  {
    "num_missing": 1,
    "num_unfound": 1,
    "objects": [
        {
            "oid": {
                "oid": "object",
                "key": "",
                "snapid": -2,
                "hash": 2249616407,
                "max": 0,
                "pool": 2,
                "namespace": ""
            },
            "need": "43'251",
            "have": "0'0",
            "flags": "none",
            "clean_regions": "clean_offsets: [], clean_omap: 0, new_object: 1",
            "locations": [
                "0(3)",
                "4(2)"
            ]
        }
    ],
    "state": "NotRecovering",
    "available_might_have_unfound": true,
    "might_have_unfound": [
        {
            "osd": "2(4)",
            "status": "osd is down"
        }
    ],
    "more": false
  }

If there are too many objects to list in a single result, the ``more`` field
will be true and you can query for more.  (Eventually the command line tool
will hide this from you, but not yet.)

Now you can identify which OSDs have been probed or might contain data.

At the end of the listing (before ``more: false``), ``might_have_unfound`` is
provided when ``available_might_have_unfound`` is true.  This is equivalent to
the output of ``ceph pg #.# query``.  This eliminates the need to use ``query``
directly.  The ``might_have_unfound`` information given behaves the same way as
that ``query`` does, which is described below.  The only difference is that
OSDs that have the status of ``already probed`` are ignored.

Use of ``query``:

.. prompt:: bash #

   ceph pg 2.4 query

.. code-block:: json

   "recovery_state": [
        { "name": "Started/Primary/Active",
          "enter_time": "2012-03-06 15:15:46.713212",
          "might_have_unfound": [
                { "osd": 1,
                  "status": "osd is down"}]}]

In this case, the cluster knows that ``osd.1`` might have data, but it is
``down``. Here is the full range of possible states:

* already probed
* querying
* OSD is down
* not queried (yet)

Sometimes it simply takes some time for the cluster to query possible
locations.

It is possible that there are other locations where the object might exist that
are not listed. For example: if an OSD is stopped and taken out of the cluster
and then the cluster fully recovers, and then through a subsequent set of
failures the cluster ends up with an unfound object, the cluster will ignore
the removed OSD. (This scenario, however, is unlikely.)

If all possible locations have been queried and objects are still lost, you may
have to give up on the lost objects. This, again, is possible only when unusual
combinations of failures have occurred that allow the cluster to learn about
writes that were performed before the writes themselves have been recovered. To
mark the "unfound" objects as "lost", run a command of the following form:

.. prompt:: bash #

   ceph pg 2.5 mark_unfound_lost revert|delete

Here the final argument (``revert|delete``) specifies how the cluster should
deal with lost objects.

The ``delete`` option will cause the cluster to forget about them entirely.

The ``revert`` option (which is not available for erasure coded pools) will
either roll back to a previous version of the object or (if it was a new
object) forget about the object entirely. Use ``revert`` with caution, as it
may confuse applications that expect the object to exist.


Homeless Placement Groups
=========================

It is possible that every OSD that has copies of a given placement group fails.
If this happens, then the subset of the object store that contains those
placement groups becomes unavailable and the monitor will receive no status
updates for those placement groups. The monitor marks as ``stale`` any
placement group whose primary OSD has failed. For example:

.. prompt:: bash #

   ceph health

.. code-block:: none

    HEALTH_WARN 24 pgs stale; 3/300 in osds are down

Identify which placement groups are ``stale`` and which were the last OSDs to
store the ``stale`` placement groups by running the following command:

.. prompt:: bash #

   ceph health detail

.. code-block:: none

   HEALTH_WARN 24 pgs stale; 3/300 in osds are down
   ...
   pg 2.5 is stuck stale+active+remapped, last acting [2,0]
   ...
   osd.10 is down since epoch 23, last address 192.168.106.220:6800/11080
   osd.11 is down since epoch 13, last address 192.168.106.220:6803/11539
   osd.12 is down since epoch 24, last address 192.168.106.220:6806/11861

This output indicates that placement group ``2.5`` (``pg 2.5``) was last managed by
``osd.0`` and ``osd.2``. Restart those OSDs to allow the cluster to recover
that placement group.


Only a Few OSDs Receive Data
============================

If only a few of the nodes in the cluster are receiving data, check the number
of placement groups in the pool as instructed in the :ref:`Placement Groups
<rados_ops_pgs_get_pg_num>` documentation. Since placement groups get mapped to
OSDs in an operation involving dividing the number of placement groups in the
cluster by the number of OSDs in the cluster, a small number of placement
groups (the remainder, in this operation) are sometimes not distributed across
the cluster. In situations like this, create a pool with a placement group
count that is a multiple of the number of OSDs. See :ref:`placement groups` for
details. See the :ref:`Pool, PG, and CRUSH Config Reference
<rados_config_pool_pg_crush_ref>` for instructions on changing the default
values used to determine how many placement groups are assigned to each pool.


Can't Write Data
================

If the cluster is up, but some OSDs are down and you cannot write data, make
sure that you have the minimum number of OSDs running in the pool. If you don't
have the minimum number of OSDs running in the pool, Ceph will not allow you to
write data to it because there is no guarantee that Ceph can replicate your
data. See :confval:`osd_pool_default_min_size` in the :ref:`Pool, PG, and CRUSH
Config Reference <rados_config_pool_pg_crush_ref>` for details.


PGs Inconsistent
================

A scrub marks a placement group ``inconsistent`` when it finds that the copies
of an object in the PG do not match, or that a copy cannot be read. The copies
are the replicas of the object in a replicated pool, or its shards in an
erasure-coded pool. The ``OSD_SCRUB_ERRORS`` and ``PG_DAMAGED`` health checks
report the PG (see :ref:`health-checks`):

.. prompt:: bash #

   ceph health detail

.. code-block:: none

   HEALTH_ERR 2 scrub errors; Possible data damage: 1 pg inconsistent
   [ERR] OSD_SCRUB_ERRORS: 2 scrub errors
   [ERR] PG_DAMAGED: Possible data damage: 1 pg inconsistent
       pg 0.6 is active+clean+inconsistent, acting [0,1,2]

An inconsistent PG still serves client reads and writes. In a replicated
pool, if a client reads an object whose copy on the primary OSD cannot be
read or fails its checksum, the primary recovers its copy from another OSD
and then completes the read. In an erasure-coded pool, the primary rebuilds
the data from the other shards instead, and the damaged shard stays damaged.

Writes do not check for damage. A write that replaces the whole object
replaces every copy, including the damaged one. A write that changes only
part of an object changes the same bytes in every copy, and the rest of a
damaged copy stays damaged. Such a write also removes the data digest from
the object info, so after it a scrub cannot use the digest to find the
damaged copy (see `How PG Repair Works`_).

Only a scrub updates the count of scrub errors behind the ``inconsistent``
flag, so the flag can stay set after the damaged copy is gone, for example
after a failed drive is replaced and backfill has rebuilt its copies. To clear
the flag, run a deep scrub of the PG (see `Requesting a Deep Scrub or
Repair`_).

To list the inconsistent PGs of a pool, run a command of the following form:

.. prompt:: bash #

   rados list-inconsistent-pg {pool}

.. code-block:: none

    ["0.6"]

To see the objects with errors in a PG, run a command of the following form:

.. prompt:: bash #

   rados list-inconsistent-obj {pgid} --format=json-pretty

For example, if the copy of the object ``foo`` on ``osd.2`` has been truncated
to zero bytes, the output for PG ``0.6`` looks like this:

.. code-block:: json

    {
        "epoch": 14,
        "inconsistents": [
            {
                "object": {
                    "name": "foo",
                    "nspace": "",
                    "locator": "",
                    "snap": "head",
                    "version": 1
                },
                "errors": [
                    "data_digest_mismatch",
                    "size_mismatch"
                ],
                "union_shard_errors": [
                    "data_digest_mismatch_info",
                    "size_mismatch_info"
                ],
                "selected_object_info": "0:602f83fe:::foo:head(16'1 client.4110.0:1 dirty|data_digest|omap_digest s 968 uv 1 dd e978e67f od ffffffff alloc_hint [0 0 0])",
                "shards": [
                    {
                        "osd": 0,
                        "errors": [],
                        "size": 968,
                        "omap_digest": "0xffffffff",
                        "data_digest": "0xe978e67f"
                    },
                    {
                        "osd": 1,
                        "errors": [],
                        "size": 968,
                        "omap_digest": "0xffffffff",
                        "data_digest": "0xe978e67f"
                    },
                    {
                        "osd": 2,
                        "errors": [
                            "data_digest_mismatch_info",
                            "size_mismatch_info"
                        ],
                        "size": 0,
                        "omap_digest": "0xffffffff",
                        "data_digest": "0xffffffff"
                    }
                ]
            }
        ]
    }

In this output:

* ``errors`` lists the ways in which the copies of the object differ from each
  other, without saying which copy is wrong:

  * ``data_digest_mismatch``: The data digest of the copy on ``osd.2``
    differs from the data digests of the copies on ``osd.0`` and ``osd.1``.
  * ``size_mismatch``: The copy on ``osd.2`` is ``0`` bytes long, and the
    copies on ``osd.0`` and ``osd.1`` are ``968`` bytes long.

* The ``errors`` of each entry in ``shards`` list what is wrong with that
  copy, and ``union_shard_errors`` combines them for all of the copies. Errors
  whose names end in ``_info`` compare the copy with ``selected_object_info``,
  the object info of the authoritative copy (see `How PG Repair Works`_):

  * ``data_digest_mismatch_info``: The data digest calculated from the copy on
    ``osd.2`` (``0xffffffff``) differs from the data digest recorded in the
    object info (``e978e67f``).
  * ``size_mismatch_info``: The size of the copy on ``osd.2`` (``0``) differs
    from the size recorded in the object info (``968``).

.. warning:: A ``read_error`` in the ``errors`` of a shard usually means that
   the drive behind that OSD has a media error. BlueStore also reports a
   checksum failure as a ``read_error``. Check the output of ``dmesg`` and
   ``smartctl`` on the host of that OSD before you repair the PG.

``rados list-inconsistent-snapset {pgid}`` lists errors in the snapshot
metadata of the objects in a PG.

No Scrub Information Available
------------------------------

``rados list-inconsistent-obj`` can report that it has no scrub information
for a PG that is inconsistent:

.. code-block:: none

   No scrub information available for pg 0.6
   error 2: (2) No such file or directory

The primary OSD of the PG keeps the details of the errors from the most recent
scrub, but the count of scrub errors is stored with the PG. The primary
discards the details when it restarts and whenever the PG peers again, for
example when one of its OSDs goes down or comes back up. The count of errors,
and with it the ``inconsistent`` flag, remains. To regenerate the details, run
a deep scrub of the PG, and run ``rados list-inconsistent-obj`` again after the
PG no longer shows ``scrubbing+deep`` in ``ceph health detail`` output:

.. prompt:: bash #

   ceph pg deep-scrub {pgid}

Additionally, while ``ceph pg repair`` runs, ``rados list-inconsistent-obj``
returns an empty list until the deep scrub that follows the repair has
finished.

Repairing an Inconsistent PG
----------------------------

Before you repair a PG, look at what is wrong with it:

* If the errors are ``read_error`` errors on one OSD whose drive is failing,
  you do not need ``ceph pg repair``. Replace the OSD as described in
  :ref:`rados-replacing-an-osd`. Backfill rebuilds the copies of that OSD from
  the good copies. Then run a deep scrub of the PG to clear the
  ``inconsistent`` flag.
* If the ``errors`` of the object show mismatches but no shard has errors,
  the copies differ and nothing shows which of them is wrong. In this case,
  ``ceph pg repair`` keeps the copy on the primary OSD, which might be the
  damaged one (see `How PG Repair Works`_).

To repair a PG, run a command of the following form:

.. prompt:: bash #

   ceph pg repair {pgid}

For example:

.. prompt:: bash #

   ceph pg repair 0.6

.. note:: PG IDs have the form ``N.xxxxx``, where ``N`` is the ID of the pool
   that contains the PG. The command ``ceph osd lspools`` lists the pool IDs.

How PG Repair Works
-------------------

``ceph pg repair`` runs a deep scrub of the PG, which reads every copy of every
object, and repairs the objects that have errors:

#. For each damaged object, Ceph chooses an authoritative copy. A copy cannot
   be authoritative if it cannot be read, if its object info is missing or
   corrupt, or if its size does not match its object info. Among the remaining
   copies, Ceph prefers the one with the newest version, then the one whose
   object info records more digests, and then the copy on the primary OSD.
   Ceph then compares each copy with the object info of the authoritative
   copy. The data and omap digests find the copies that differ, but do not
   choose the authoritative copy.

#. Ceph marks every copy that differs from the authoritative copy as missing,
   and recovers it from the authoritative copy as it would recover any missing
   copy. Meanwhile, the PG shows the ``repair`` state. In an erasure-coded
   pool, recovery rebuilds the damaged shards from the other shards. If all of
   the copies match each other but not the digest in their object info, Ceph
   corrects the digest in the object info and copies no data.

#. When recovery finishes, Ceph runs another deep scrub of the PG. If this
   scrub finds no errors, the ``inconsistent`` flag clears. If errors remain,
   the PG shows ``failed_repair``.

``ceph pg repair`` cannot repair an object when none of its copies can be
authoritative, for example when every copy has a read error. In this case, the
OSD log says ``failed to pick suitable object info``, and the PG shows
``failed_repair``. Errors in snapshot metadata are not repaired.

The object info of an object has no data digest after a write that changes
only part of the object, other than an append. In that case, Ceph cannot tell
from the digests which copy is wrong. A deep scrub records the data digest
again when all of the copies match and the object has not been modified for
:confval:`osd_deep_scrub_update_digest_min_age`.

Requesting a Deep Scrub or Repair
---------------------------------

Ceph starts a deep scrub that you request with ``ceph pg deep-scrub``, or a
repair that you request with ``ceph pg repair``, ahead of scheduled scrubs. It
does not wait for the ``noscrub`` or ``nodeep-scrub`` flags to be unset, for
the allowed scrub hours, for a free :confval:`osd_max_scrubs` slot, or for
recovery on the OSD to finish (see :confval:`osd_scrub_during_recovery`). It
does wait until the PG is ``active+clean``, and checks again every
:confval:`osd_scrub_retry_pg_state` seconds, so a PG that is recovering or
backfilling is scrubbed only after it is clean.

The deep scrub that follows a repair waits for the allowed scrub hours and for
a free :confval:`osd_max_scrubs` slot, and it honors
:confval:`osd_scrub_during_recovery`.

Automatic Repair
----------------

Ceph does not repair inconsistencies by itself unless
:confval:`osd_scrub_auto_repair` is set to ``true``. It is ``false`` by
default. When it is ``true``, a scheduled deep scrub repairs the errors that it
finds, and a scheduled shallow scrub that finds errors starts a deep scrub to
repair them. A scheduled scrub does not repair anything if it finds more than
:confval:`osd_scrub_auto_repair_num_errors` damaged objects. With
:confval:`osd_scrub_auto_repair` enabled, a deep scrub that you request with
``ceph pg deep-scrub`` also repairs the errors that it finds, however many
there are.


Erasure Coded PGs are not ``active+clean``
==========================================

If CRUSH fails to find enough OSDs to map to a PG, it will show as a
``2147483647`` which is ``ITEM_NONE`` or ``no OSD found``. For example::

     [2,1,6,0,5,8,2147483647,7,4]


Not enough OSDs
---------------

If the Ceph cluster has only eight OSDs and an erasure coded pool needs nine
OSDs, the cluster will show ``Not enough OSDs``. In this case, either add new
OSDs that the PG will then use automatically, or create
another erasure coded pool that requires fewer OSDs by running commands of the
following form:

.. prompt:: bash #

     ceph osd erasure-code-profile set myprofile k=5 m=3
     ceph osd pool create erasurepool erasure myprofile


CRUSH Constraints cannot be Satisfied
-------------------------------------

If the cluster has enough OSDs, it is possible that the CRUSH rule is imposing
constraints that cannot be satisfied. If there are ten OSDs on two hosts and
the CRUSH rule requires that no two OSDs from the same host are used in the
same PG, the mapping may fail because only two OSDs will be found. Check the
constraint by displaying ("dumping") the rule, as shown here:

.. prompt:: bash #

   ceph osd crush rule ls

.. code-block:: json

    [
        "replicated_rule",
        "erasurepool"]

.. prompt:: bash #

   ceph osd crush rule dump erasurepool

.. code-block:: json

    { "rule_id": 1,
      "rule_name": "erasurepool",
      "type": 3,
      "steps": [
            { "op": "take",
              "item": -1,
              "item_name": "default"},
            { "op": "chooseleaf_indep",
              "num": 0,
              "type": "host"},
            { "op": "emit"}]}


Resolve this problem by creating a new pool in which PGs are allowed to have
OSDs residing on the same host by running the following commands:

.. prompt:: bash #

   ceph osd erasure-code-profile set myprofile crush-failure-domain=osd
   ceph osd pool create erasurepool erasure myprofile


CRUSH Gives up too Soon
-----------------------

If the Ceph cluster has just enough OSDs to map the PG (for instance a cluster
with a total of nine OSDs and an erasure coded pool that requires nine OSDs per
PG), it is possible that CRUSH gives up before finding a mapping. To resolve
this problem, either:

* Lower the erasure coded pool requirements to use fewer OSDs per PG (this
  requires the creation of another pool, because erasure code profiles cannot
  be modified dynamically).

* Add more OSDs to the cluster (this does not require the erasure coded pool
  to be modified, because it will become clean automatically).

* Use a handmade CRUSH rule that tries more times to find a good mapping.
  This can be modified for an existing CRUSH rule by setting
  ``set_choose_tries`` to a value greater than the default. For more
  information, see :ref:`rados-crush-map-edits`.

* Use a multi-step retry (MSR) CRUSH rule (Squid or later releases). For more
  information, see :ref:`rados-crush-msr-rules`.

First, verify the problem by using  ``crushtool`` after extracting the crushmap
from the cluster. This ensures that your experiments do not modify the Ceph
cluster and that they operate only on local files:

.. prompt:: bash #

   ceph osd crush rule dump erasurepool

.. code-block:: json

    { "rule_id": 1,
      "rule_name": "erasurepool",
      "type": 3,
      "steps": [
            { "op": "take",
              "item": -1,
              "item_name": "default"},
            { "op": "chooseleaf_indep",
              "num": 0,
              "type": "host"},
            { "op": "emit"}]}

.. prompt:: bash #

    ceph osd getcrushmap > crush.map

.. code-block:: none

    got crush map from osdmap epoch 13

.. prompt:: bash #

    crushtool -i crush.map --test --show-bad-mappings \
       --rule 1 \
       --num-rep 9 \
       --min-x 1 --max-x $((1024 * 1024))

.. code-block:: none

    bad mapping rule 8 x 43 num_rep 9 result [3,2,7,1,2147483647,8,5,6,0]
    bad mapping rule 8 x 79 num_rep 9 result [6,0,2,1,4,7,2147483647,5,8]
    bad mapping rule 8 x 173 num_rep 9 result [0,4,6,8,2,1,3,7,2147483647]

Here, ``--num-rep`` is the number of OSDs that the erasure code CRUSH rule
needs, ``--rule`` is the value of the ``rule_id`` field that was displayed by
``ceph osd crush rule dump``. This test will simulate a number of PG placements
based on the CRUSH map. The exact count is based on ``[--min-x,--max-x]``. PG
placements are independent of each other, based only on the hash and bucket
algorithms. Any placement may fail on its own. If this test outputs nothing
then all mappings have been successful, indicating an issue other than CRUSH
mappings. If it does output bad mappings, as shown above, Ceph is unable to
consistently place PGs in the current topology. As long as not all mappings are
considered bad, the CRUSH rule can be configured to search longer for a viable
placement.


Changing the Value of ``set_choose_tries``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Decompile the CRUSH map to edit the CRUSH rule by running the following
   command:

   .. prompt:: bash #

      crushtool --decompile crush.map > crush.txt

   For illustrative purposes a simplified CRUSH map will be used in this
   example, simulating a single host with four disks of sizes 3×1 TiB and
   1×200 GiB.  The settings below are chosen specifically for this example and
   will diverge from the :ref:`CRUSH Map Tunables <crush-map-tunables>`
   generally found in production clusters. As defaults may change, please refer
   to the correct version of the documentation for your release of Ceph.

   ::

      tunable choose_local_tries 0
      tunable choose_local_fallback_tries 0
      # artificially low total tries, for illustration
      tunable choose_total_tries 10
      tunable chooseleaf_descend_once 1
      tunable chooseleaf_vary_r 1
      tunable chooseleaf_stable 1
      tunable straw_calc_version 1
      tunable allowed_bucket_algs 54

      # devices
      device 0 osd.0
      device 1 osd.1
      device 2 osd.2
      device 3 osd.3

      # types
      type 0 osd
      type 1 host
      type 2 chassis
      type 3 rack
      type 4 row
      type 5 pdu
      type 6 pod
      type 7 room
      type 8 datacenter
      type 9 zone
      type 10 region
      type 11 root

      # buckets
      host example {
              id -2
              alg straw2
              hash 0  # rjenkins1
              item osd.0 weight 1.00000
              item osd.1 weight 1.00000
              item osd.2 weight 1.00000
              item osd.3 weight 0.20000
      }
      root default {
              id -1
              alg straw2
              hash 0  # rjenkins1
              item example weight 3.20000
      }

      # rules
      rule ec {
              id 0
              type erasure
              step set_chooseleaf_tries 5
              # artificially low tries, for illustration
              step set_choose_tries 5
              step take default
              step choose indep 0 type osd
              step emit
      }

#. Add the following line to the rule::

      step set_choose_tries 100

   If the line does exist already, as in this example, only modify the value.
   Ensure that the rule in your ``crush.txt`` does resemble this after the
   change::

      rule ec {
              id 0
              type erasure
              step set_chooseleaf_tries 5
              step set_choose_tries 100
              step take default
              step choose indep 0 type osd
              step emit
      }

#. Recompile and retest the CRUSH rule:

   .. prompt:: bash #

      crushtool --compile crush.txt -o better-crush.map

#. When all mappings succeed, display a histogram of the number of tries that
   were necessary to find all of the mapping by using the
   ``--show-choose-tries`` option of the ``crushtool`` command, as in the
   following example:

   .. prompt:: bash #

      crushtool -i better-crush.map --test --show-bad-mappings \
       --show-choose-tries \
       --rule 0 \
       --num-rep 3 \
       --min-x 1 --max-x 10

   .. code-block:: none

     0:         0
     1:         0
     2:         4
     3:         3
     4:         1
     5:         1
     6:         1
     7:         0
     8:         0
     9:         0

.. note:: The total number of lines displayed equals the ``choose_total_tries``
   value of the CRUSH map. However the calculation done by ``crushtool`` will
   not be affected by the setting, only the output will be truncated. The
   ``--set-choose-total-tries`` flag can to be used to modify the value without
   modifying the CRUSH map.

The output is a histogram of the tries required for each placement. For
``--min-x 1`` and ``--max-x 10`` this totals to 10 PG placements. All of these
placements have been successful as is evident by the lack of the bad mapping
diagnostic messages. This output indicates that four PGs could be placed within
two tries, while one PG was only placed after four tries. Any failed placement
groups would be counted in the bucket in which it failed, for example in the
original ``crush.txt`` the eighth placement failed after the fifth try and
would have been counted in the fifth bucket together with one other mapping
which succeeded on the fifth try, visible in the histogram of the updated map
showing exactly one entry for five and six tries. As mentioned above, PG
placement is based solely on the CRUSH topology and the hash and bucket
algorithms. Running the original ``crush.txt`` with just ``--x 8`` instead of
the range will fail deterministically. This means that for evaluation of an
appropriate value for production much larger ranges should be used such as the
``1024 * 1024`` from an earlier example.

To find an appropriate value for tries, or to determine whether this is the
underlying issue with placement to begin with, setting a very high value such
as ``500`` and testing with a large sample size (large ``x`` range) can be used
to show the general distribution. From a statistical point of view taking the
last non-zero value as the maximum is very unlikely to cause any failed
placements in practice, however if a lower value is desired then the lower
value can be used at the chance of potentially hitting one of the rare cases in
which placement fails, requiring manual intervention.

