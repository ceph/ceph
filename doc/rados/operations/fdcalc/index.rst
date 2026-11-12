.. _fdcalc:

=======
FD Calc
=======

The failure domain calculator shows how much of a cluster's raw capacity a
replicated or erasure-coded pool can use, given the CRUSH weights of the
failure domains that the pool's CRUSH rule spreads data across. It also shows
what changes when an OSD or a whole failure domain fails.

Each PG puts each of its shards in a different failure domain. A shard is
a replica in a replicated pool, or one of the *k+m* data and coding shards
in an erasure-coded pool. A PG with *n* shards therefore needs at least *n*
failure domains, and no failure domain can hold more than 1/*n* of what
the pool stores. When a failure domain's CRUSH weight is more than 1/*n*
of the total, no placement can use the excess. The calculator calls this
capacity stranded.

.. raw:: html
   :file: fdcalc.html

Fields
======

Type, Size, k, and m
   A replicated pool with its size, or an erasure-coded pool with *k* data
   chunks and *m* coding chunks. The calculator warns when *m=1*. See
   :ref:`ecpool` for why.

Shards per failure domain
   Leave this at 1 unless you really know what you're doing. Values above 1
   model CRUSH rules that put several shards of a PG in one failure domain:
   the MSR rules described in :doc:`../crush-map`, or an erasure-code
   profile's ``crush-osds-per-failure-domain``.

full_ratio and backfillfull_ratio
   Ceph stops writes when an OSD reaches :confval:`mon_osd_full_ratio`, 0.95
   by default, and backfill stops when an OSD reaches
   :confval:`mon_osd_backfillfull_ratio`, 0.90 by default. ``ceph osd dump`` shows
   the values in effect.

Failure domains
   The CRUSH weight of each failure domain, from the ``WEIGHT`` column of
   ``ceph osd df tree``. A failure domain's weight is the sum of the weights
   of its OSDs, and an OSD's weight is by default its capacity in TiB.

What fails
   Nothing, an OSD of a given CRUSH weight in a chosen failure domain, or a
   whole failure domain.

Examples
   Each example fills in the fields above.

Results
=======

The result is marked:

- Red when there are fewer failure domains than shards in each PG, so the PGs
  can't be placed, or after a failure stay degraded.
- Yellow when there are exactly as many failure domains as shards. Every PG
  puts a shard in every failure domain, so the smallest one limits the pool,
  and when one is lost, the PGs stay degraded until it returns.
- Orange when raw capacity is stranded.
- Green otherwise.

**Balanced** is the most data that any placement can store before an OSD
reaches full_ratio. The :ref:`balancer <balancer>`, in its default ``upmap``
mode, approaches it.

**CRUSH alone** is how much data the pool holds, with no balancer, when the
first failure domain reaches full_ratio. CRUSH's straw2 algorithm draws each
failure domain in proportion to its weight, but draws again when it draws one
that the PG already uses, so a heavy failure domain is chosen less often than
its weight says, and light ones fill first.

After a failure, the calculator shows the new capacity, how much is newly
stranded, and the most data the pool can hold when the failure happens for
backfill to re-protect all of it before an OSD reaches backfillfull_ratio.

How the capacity is calculated
==============================

For a pool with *n* shards per PG and at most *r* shards of a PG in a failure
domain, let *X* be the amount of raw capacity that each shard position adds
up to across all of the pool's PGs. The pool uses *n* × *X* of raw
capacity, and a failure domain holds at most *r* × *X*. The calculator finds
the largest *X* for which:

.. code-block:: none

   n × X ≤ sum over failure domains of min(full_ratio × weight, r × X)

The pool then stores *X* of data if it is replicated, and *k* × *X* if it is
erasure coded. The calculator assumes that the OSDs within each failure
domain fill evenly.

For CRUSH alone, the calculator finds the probability that straw2 chooses
each failure domain for a PG, exactly for up to 16 failure domains and by
sampling for more.

See :ref:`pgcalc` to choose the number of PGs for each pool.
