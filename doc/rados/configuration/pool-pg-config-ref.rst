======================================
 Pool, PG and CRUSH Config Reference
======================================

.. index:: pools; configuration

The number of placement groups that the CRUSH algorithm assigns to each pool is
determined by the values of variables in the centralized configuration database
in the monitor cluster. 

Both containerized deployments of Ceph (deployments made using ``cephadm`` or
Rook) and non-containerized deployments of Ceph rely on the values in the
central configuration database in the monitor cluster to assign placement
groups to pools. 

Example Commands
----------------

To see the value of the variable that governs the number of placement groups in a given pool, run a command of the following form:

.. prompt:: bash

   ceph config get osd osd_pool_default_pg_num

To set the value of the variable that governs the number of placement groups in a given pool, run a command of the following form:

.. prompt:: bash

   ceph config set osd osd_pool_default_pg_num

Manual Tuning
-------------
In some cases, it might be advisable to override some of the defaults. For
example, you might determine that it is wise to set a pool's replica size and
to override the default number of placement groups in the pool. You can set
these values when running `pool`_ commands. 

See Also
--------

See :ref:`pg-autoscaler`.


.. literalinclude:: pool-pg.conf
   :language: ini


``mon_max_pool_pg_num``

:Description: The maximum number of placement groups per pool.
:Type: Integer
:Default: ``65536``


``mon_pg_create_interval``

:Description: Number of seconds between PG creation in the same
              Ceph OSD Daemon.

:Type: Float
:Default: ``30.0``


``mon_pg_stuck_threshold``

:Description: Number of seconds after which PGs can be considered as
              being stuck.

:Type: 32-bit Integer
:Default: ``300``

``mon_pg_min_inactive``

:Description: Raise ``HEALTH_ERR`` if the count of PGs that have been
              inactive longer than the ``mon_pg_stuck_threshold`` exceeds this
              setting. A non-positive number means disabled, never go into ERR.
:Type: Integer
:Default: ``1``


``mon_pg_warn_min_per_osd``

:Description: Raise ``HEALTH_WARN`` if the average number
              of PGs per ``in`` OSD is under this number. A non-positive number
              disables this.
:Type: Integer
:Default: ``30``


``mon_pg_warn_min_objects``

:Description: Do not warn if the total number of RADOS objects in cluster is below
              this number
:Type: Integer
:Default: ``1000``


``mon_pg_warn_min_pool_objects``

:Description: Do not warn on pools whose RADOS object count is below this number
:Type: Integer
:Default: ``1000``


``mon_pg_check_down_all_threshold``

:Description: Percentage threshold of ``down`` OSDs above which we check all PGs
              for stale ones.
:Type: Float
:Default: ``0.5``


``mon_pg_warn_max_object_skew``

:Description: Raise ``HEALTH_WARN`` if the average RADOS object count per PG
              of any pool is greater than ``mon_pg_warn_max_object_skew`` times
              the average RADOS object count per PG of all pools. Zero or a non-positive
              number disables this. Note that this option applies to ``ceph-mgr`` daemons.
:Type: Float
:Default: ``10``


``mon_delta_reset_interval``

:Description: Seconds of inactivity before we reset the PG delta to 0. We keep
              track of the delta of the used space of each pool, so, for
              example, it would be easier for us to understand the progress of
              recovery or the performance of cache tier. But if there's no
              activity reported for a certain pool, we just reset the history of
              deltas of that pool.
:Type: Integer
:Default: ``10``


``mon_osd_max_op_age``

:Description: Maximum op age before we get concerned (make it a power of 2).
              ``HEALTH_WARN`` will be raised if a request has been blocked longer
              than this limit.
:Type: Float
:Default: ``32.0``


``osd_pg_bits``

:Description: Placement group bits per Ceph OSD Daemon.
:Type: 32-bit Integer
:Default: ``6``


``osd_pgp_bits``

:Description: The number of bits per Ceph OSD Daemon for PGPs.
:Type: 32-bit Integer
:Default: ``6``


``osd_crush_chooseleaf_type``

:Description: The bucket type to use for ``chooseleaf`` in a CRUSH rule. Uses
              ordinal rank rather than name.

:Type: 32-bit Integer
:Default: ``1``. Typically a host containing one or more Ceph OSD Daemons.


``osd_crush_initial_weight``

:Description: The initial CRUSH weight for newly added OSDs.

:Type: Double
:Default: ``the size of a newly added OSD in TB``. By default, the initial CRUSH
          weight for a newly added OSD is set to its device size in TB.
          See `Weighting Bucket Items`_ for details.


``osd_pool_default_crush_rule``

:Description: The default CRUSH rule to use when creating a replicated pool.
:Type: 8-bit Integer
:Default: ``-1``, which means "pick the rule with the lowest numerical ID and 
          use that".  This is to make pool creation work in the absence of rule 0.


``osd_pool_erasure_code_stripe_unit``

:Description: Sets the default size, in bytes, of a chunk of an object
              stripe for erasure coded pools. Every object of size S
              will be stored as N stripes, with each data chunk
              receiving ``stripe unit`` bytes. Each stripe of ``N *
              stripe unit`` bytes will be encoded/decoded
              individually. This option can is overridden by the
              ``stripe_unit`` setting in an erasure code profile.

:Type: Unsigned 32-bit Integer
:Default: ``4096``


``osd_pool_default_size``

:Description: Sets the number of replicas for objects in the pool. The default
              value is the same as
              ``ceph osd pool set {pool-name} size {size}``.

:Type: 32-bit Integer
:Default: ``3``


``osd_pool_default_min_size``

:Description: Sets the minimum number of written replicas for objects in the
             pool in order to acknowledge a write operation to the client.  If
             minimum is not met, Ceph will not acknowledge the write to the
             client, **which may result in data loss**. This setting ensures
             a minimum number of replicas when operating in ``degraded`` mode.

:Type: 32-bit Integer
:Default: ``0``, which means no particular minimum. If ``0``,
          minimum is ``size - (size / 2)``.


``osd_pool_default_pg_num``

:Description: The default number of placement groups for a pool. The default
              value is the same as ``pg_num`` with ``mkpool``.

:Type: 32-bit Integer
:Default: ``32``


``osd_pool_default_pgp_num``

:Description: The default number of placement groups for placement for a pool.
              The default value is the same as ``pgp_num`` with ``mkpool``.
              PG and PGP should be equal (for now).

:Type: 32-bit Integer
:Default: ``8``


``osd_pool_default_flags``

:Description: The default flags for new pools.
:Type: 32-bit Integer
:Default: ``0``


``osd_max_pgls``

:Description: The maximum number of placement groups to list. A client
              requesting a large number can tie up the Ceph OSD Daemon.

:Type: Unsigned 64-bit Integer
:Default: ``1024``
:Note: Default should be fine.


``osd_min_pg_log_entries``

:Description: The minimum number of placement group logs to maintain
              when trimming log files.

:Type: 32-bit Int Unsigned
:Default: ``250``


``osd_max_pg_log_entries``

:Description: The maximum number of placement group logs to maintain
              when trimming log files.

:Type: 32-bit Int Unsigned
:Default: ``10000``


``osd_default_data_pool_replay_window``

:Description: The time (in seconds) for an OSD to wait for a client to replay
              a request.

:Type: 32-bit Integer
:Default: ``45``

``osd_max_pg_per_osd_hard_ratio``

:Description: The ratio of number of PGs per OSD allowed by the cluster before the
              OSD refuses to create new PGs. An OSD stops creating new PGs if the number
              of PGs it serves exceeds
              ``osd_max_pg_per_osd_hard_ratio`` \* ``mon_max_pg_per_osd``.

:Type: Float
:Default: ``2``

``osd_recovery_priority``

:Description: Priority of recovery in the work queue.

:Type: Integer
:Default: ``5``

``osd_recovery_op_priority``

:Description: Default priority used for recovery operations if pool doesn't override.

:Type: Integer
:Default: ``3``

.. _pool: ../../operations/pools
.. _Monitoring OSDs and PGs: ../../operations/monitoring-osd-pg#peering
.. _Weighting Bucket Items: ../../operations/crush-map#weightingbucketitems
