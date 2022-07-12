Client Configuration
====================

Updating Client Configuration
-----------------------------

Certain client configurations can be applied at runtime. To check if a configuration option can be applied (taken into affect by a client) at runtime, use the `config help` command::

   ceph config help debug_client
    debug_client - Debug level for client
    (str, advanced)                                                                                                                      Default: 0/5
    Can update at runtime: true

    The value takes the form 'N' or 'N/M' where N and M are values between 0 and 99.  N is the debug level to log (all values below this are included), and M is the level to gather and buffer in memory.  In the event of a crash, the most recent items <= M are dumped to the log file.

`config help` tells if a given configuration can be applied at runtime along with the defaults and a description of the configuration option.

To update a configuration option at runtime, use the `config set` command::

   ceph config set client debug_client 20/20

Note that this changes a given configuration for all clients.

To check configured options use the `config get` command::

   ceph config get client
    WHO    MASK LEVEL    OPTION                    VALUE     RO 
    client      advanced debug_client              20/20          
    global      advanced osd_pool_default_min_size 1            
    global      advanced osd_pool_default_size     3            

Client Config Reference
------------------------

.. confval:: client_acl_type
.. confval:: client_cache_mid
.. confval:: client_cache_size
.. confval:: client_caps_release_delay
.. confval:: client_debug_force_sync_read
.. confval:: client_dirsize_rbytes
.. confval:: client_max_inline_size
.. confval:: client_metadata
.. confval:: client_mount_gid
.. confval:: client_mount_timeout
.. confval:: client_mount_uid
.. confval:: client_mountpoint
.. confval:: client_oc
.. confval:: client_oc_max_dirty
.. confval:: client_oc_max_dirty_age
.. confval:: client_oc_max_objects
.. confval:: client_oc_size
.. confval:: client_oc_target_dirty
.. confval:: client_permissions
.. confval:: client_quota_df
.. confval:: client_readahead_max_bytes
.. confval:: client_readahead_max_periods
.. confval:: client_readahead_min
.. confval:: client_reconnect_stale
.. confval:: client_snapdir
.. confval:: client_tick_interval
.. confval:: client_use_random_mds
.. confval:: fuse_default_permissions
.. confval:: fuse_max_write
.. confval:: fuse_disable_pagecache

Developer Options
#################

.. important:: These options are internal. They are listed here only to complete the list of options.

.. confval:: client_debug_getattr_caps
.. confval:: client_debug_inject_tick_delay
.. confval:: client_inject_fixed_oldest_tid
.. confval:: client_inject_release_failure
.. confval:: client_trace
