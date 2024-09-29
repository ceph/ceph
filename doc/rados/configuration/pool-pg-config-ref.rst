.. _rados_config_pool_pg_crush_ref:

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

.. confval:: mon_max_pool_pg_num
.. confval:: mon_pg_stuck_threshold
.. confval:: mon_pg_warn_min_per_osd
.. confval:: mon_pg_warn_min_objects
.. confval:: mon_pg_warn_min_pool_objects
.. confval:: mon_pg_check_down_all_threshold
.. confval:: mon_pg_warn_max_object_skew
.. confval:: mon_delta_reset_interval
.. confval:: osd_crush_chooseleaf_type
.. confval:: osd_crush_initial_weight
.. confval:: osd_pool_default_crush_rule
.. confval:: osd_pool_erasure_code_stripe_unit
.. confval:: osd_pool_default_size
.. confval:: osd_pool_default_min_size
.. confval:: osd_pool_default_pg_num
.. confval:: osd_pool_default_pgp_num
.. confval:: osd_pool_default_pg_autoscale_mode
.. confval:: osd_pool_default_flags
.. confval:: osd_max_pgls
.. confval:: osd_min_pg_log_entries
.. confval:: osd_max_pg_log_entries
.. confval:: osd_default_data_pool_replay_window
.. confval:: osd_max_pg_per_osd_hard_ratio

.. _pool: ../../operations/pools
.. _Monitoring OSDs and PGs: ../../operations/monitoring-osd-pg#peering
.. _Weighting Bucket Items: ../../operations/crush-map#weightingbucketitems
