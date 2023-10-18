.. _rados_config_pool_pg_crush_ref:

======================================
 Pool, PG and CRUSH Config Reference
======================================

.. index:: pools; configuration

Ceph uses default values to determine how many placement groups (PGs) will be
assigned to each pool. We recommend overriding some of the defaults.
Specifically, we recommend setting a pool's replica size and overriding the
default number of placement groups. You can set these values when running
`pool`_ commands. You can also override the defaults by adding new ones in the
``[global]`` section of your Ceph configuration file.


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
