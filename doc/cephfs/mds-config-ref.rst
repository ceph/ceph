======================
 MDS Config Reference
======================

.. confval:: mds_cache_memory_limit
.. confval:: mds_cache_reservation
.. confval:: mds_cache_mid
.. confval:: mds_dir_max_commit_size
.. confval:: mds_decay_halflife
.. confval:: mds_beacon_interval
.. confval:: mds_beacon_grace
.. confval:: mon_mds_blocklist_interval
.. confval:: mds_reconnect_timeout
.. confval:: mds_tick_interval
.. confval:: mds_dirstat_min_interval
.. confval:: mds_scatter_nudge_interval
.. confval:: mds_client_prealloc_inos

``mds_early_reply``

:Description: Determines whether the MDS should allow clients to see request 
              results before they commit to the journal.

:Type:  Boolean
:Default: ``true``


``mds_default_dir_hash``

:Description: The function to use for hashing files across directory fragments.
:Type:  32-bit Integer
:Default: ``2`` (i.e., rjenkins)


``mds_log_skip_corrupt_events``

:Description: Determines whether the MDS should try to skip corrupt journal 
              events during journal replay.
              
:Type:  Boolean
:Default:  ``false``


``mds_log_max_events``

:Description: The maximum events in the journal before we initiate trimming.
              Set to ``-1`` to disable limits.
              
:Type:  32-bit Integer
:Default: ``-1``


``mds_log_max_segments``

:Description: The maximum number of segments (objects) in the journal before 
              we initiate trimming. Set to ``-1`` to disable limits.

:Type:  32-bit Integer
:Default: ``128``


``mds_bal_sample_interval``

:Description: Determines how frequently to sample directory temperature 
              (for fragmentation decisions).
              
:Type:  Float
:Default: ``3``


``mds_bal_replicate_threshold``

:Description: The maximum temperature before Ceph attempts to replicate 
              metadata to other nodes.
              
:Type:  Float
:Default: ``8000``


``mds_bal_unreplicate_threshold``

:Description: The minimum temperature before Ceph stops replicating 
              metadata to other nodes.
              
:Type:  Float
:Default: ``0``


``mds_bal_split_size``

:Description: The maximum directory size before the MDS will split a directory 
              fragment into smaller bits.
              
:Type:  32-bit Integer
:Default: ``10000``


``mds_bal_split_rd``

:Description: The maximum directory read temperature before Ceph splits 
              a directory fragment.
              
:Type:  Float
:Default: ``25000``


``mds_bal_split_wr``

:Description: The maximum directory write temperature before Ceph splits 
              a directory fragment.
              
:Type:  Float
:Default: ``10000``


``mds_bal_split_bits``

:Description: The number of bits by which to split a directory fragment.
:Type:  32-bit Integer
:Default: ``3``


``mds_bal_merge_size``

:Description: The minimum directory size before Ceph tries to merge 
              adjacent directory fragments.
              
:Type:  32-bit Integer
:Default: ``50``


``mds_bal_interval``

:Description: The frequency (in seconds) of workload exchanges between MDSs.
:Type:  32-bit Integer
:Default: ``10``


``mds_bal_fragment_interval``

:Description: The delay (in seconds) between a fragment being eligible for split
              or merge and executing the fragmentation change.
:Type:  32-bit Integer
:Default: ``5``


``mds_bal_fragment_fast_factor``

:Description: The ratio by which frags may exceed the split size before
              a split is executed immediately (skipping the fragment interval)
:Type:  Float
:Default: ``1.5``

``mds_bal_fragment_size_max``

:Description: The maximum size of a fragment before any new entries
              are rejected with ENOSPC.
:Type:  32-bit Integer
:Default: ``100000``

``mds_bal_idle_threshold``

:Description: The minimum temperature before Ceph migrates a subtree 
              back to its parent.
              
:Type:  Float
:Default: ``0``


``mds_bal_max``

:Description: The number of iterations to run balancer before Ceph stops. 
              (used for testing purposes only)

:Type:  32-bit Integer
:Default: ``-1``


``mds_bal_max_until``

:Description: The number of seconds to run balancer before Ceph stops. 
              (used for testing purposes only)

:Type:  32-bit Integer
:Default: ``-1``


``mds_bal_mode``

:Description: The method for calculating MDS load. 

              - ``0`` = Hybrid.
              - ``1`` = Request rate and latency. 
              - ``2`` = CPU load.
              
:Type:  32-bit Integer
:Default: ``0``


``mds_bal_min_rebalance``

:Description: The minimum subtree temperature before Ceph migrates.
:Type:  Float
:Default: ``0.1``


``mds_bal_min_start``

:Description: The minimum subtree temperature before Ceph searches a subtree.
:Type:  Float
:Default: ``0.2``


``mds_bal_need_min``

:Description: The minimum fraction of target subtree size to accept.
:Type:  Float
:Default: ``0.8``


``mds_bal_need_max``

:Description: The maximum fraction of target subtree size to accept.
:Type:  Float
:Default: ``1.2``


``mds_bal_midchunk``

:Description: Ceph will migrate any subtree that is larger than this fraction 
              of the target subtree size.
              
:Type:  Float
:Default: ``0.3``


``mds_bal_minchunk``

:Description: Ceph will ignore any subtree that is smaller than this fraction 
              of the target subtree size.
              
:Type:  Float
:Default: ``0.001``


``mds_bal_target_removal_min``

:Description: The minimum number of balancer iterations before Ceph removes
              an old MDS target from the MDS map.
              
:Type:  32-bit Integer
:Default: ``5``


``mds_bal_target_removal_max``

:Description: The maximum number of balancer iterations before Ceph removes 
              an old MDS target from the MDS map.
              
:Type:  32-bit Integer
:Default: ``10``


``mds_replay_interval``

:Description: The journal poll interval when in standby-replay mode.
              ("hot standby")
              
:Type:  Float
:Default: ``1``


``mds_shutdown_check``

:Description: The interval for polling the cache during MDS shutdown.
:Type:  32-bit Integer
:Default: ``0``


``mds_thrash_exports``

:Description: Ceph will randomly export subtrees between nodes (testing only).
:Type:  32-bit Integer
:Default: ``0``


``mds_thrash_fragments``

:Description: Ceph will randomly fragment or merge directories.
:Type:  32-bit Integer
:Default: ``0``


``mds_dump_cache_on_map``

:Description: Ceph will dump the MDS cache contents to a file on each MDSMap.
:Type:  Boolean
:Default:  ``false``


``mds_dump_cache_after_rejoin``

:Description: Ceph will dump MDS cache contents to a file after 
              rejoining the cache (during recovery).
              
:Type:  Boolean
:Default:  ``false``


``mds_verify_scatter``

:Description: Ceph will assert that various scatter/gather invariants 
              are ``true`` (developers only).
              
:Type:  Boolean
:Default:  ``false``


``mds_debug_scatterstat``

:Description: Ceph will assert that various recursive stat invariants 
              are ``true`` (for developers only).
              
:Type:  Boolean
:Default:  ``false``


``mds_debug_frag``

:Description: Ceph will verify directory fragmentation invariants 
              when convenient (developers only).
              
:Type:  Boolean
:Default:  ``false``


``mds_debug_auth_pins``

:Description: The debug auth pin invariants (for developers only).
:Type:  Boolean
:Default:  ``false``


``mds_debug_subtrees``

:Description: The debug subtree invariants (for developers only).
:Type:  Boolean
:Default:  ``false``


``mds_kill_mdstable_at``

:Description: Ceph will inject MDS failure in MDSTable code 
              (for developers only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_kill_export_at``

:Description: Ceph will inject MDS failure in the subtree export code 
              (for developers only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_kill_import_at``

:Description: Ceph will inject MDS failure in the subtree import code 
              (for developers only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_kill_link_at``

:Description: Ceph will inject MDS failure in hard link code 
              (for developers only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_kill_rename_at``

:Description: Ceph will inject MDS failure in the rename code 
              (for developers only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_wipe_sessions``

:Description: Ceph will delete all client sessions on startup 
              (for testing only).
              
:Type:  Boolean
:Default: ``false``


``mds_wipe_ino_prealloc``

:Description: Ceph will delete ino preallocation metadata on startup 
              (for testing only).
              
:Type:  Boolean
:Default: ``false``


``mds_skip_ino``

:Description: The number of inode numbers to skip on startup 
              (for testing only).
              
:Type:  32-bit Integer
:Default: ``0``


``mds_min_caps_per_client``

:Description: Set the minimum number of capabilities a client may hold.
:Type: Integer
:Default: ``100``


``mds_max_ratio_caps_per_client``

:Description: Set the maximum ratio of current caps that may be recalled during MDS cache pressure.
:Type: Float
:Default: ``0.8``
