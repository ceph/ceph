========================
 Client Config Reference
========================

``client acl type``

:Description: Set to ``"posix_acl"`` to enable POSIX ACLs.
:Type: String
:Default: ``""`` (N/A)

``client cache mid``

:Description: Set client cache mid-point.
:Type: Float
:Default: ``0.75``

``client_cache_size``

:Description: Set client cache object count maximum.
:Type: Integer
:Default: ``16384``

``client_caps_release_delay``

:Description: Delay between capability releases.
:Type: Integer
:Default: ``5`` (seconds)

``client_debug_force_sync_read``

:Description: Always read synchronously (go to osds).
:Type: Boolean
:Default: ``false``

``client_debug_getattr_caps``

:Description: Check if MDS reply contains wanted caps.
:Type: Boolean
:Default: ``false``

``client_dirsize_rbytes``

:Description: Use recursive size of directory for directory ``st_size`` stat member.
:Type: Boolean
:Default: ``true``

``client_max_inline_size``

:Description: Maximum size of inlined data stored in a file inode (rather than in a separate data object in RADOS). This setting only applies if the ``inline_data`` flag is set on the MDSMap.
:Type: Integer
:Default: ``4096``

``client_metadata``

:Description: Comma-delimited strings for client metadata sent to each MDS.
:Type: String
:Default: ``""`` (N/A)

``client_mount_gid``

:Description: Group-id of CephFS mount.
:Type: Integer
:Default: ``-1``

``client_mount_timeout``

:Description: Timeout for CephFS mount.
:Type: Float
:Default: ``300.0`` (seconds)

``client_mount_uid``

:Description: User-id of CephFS mount.
:Type: Integer
:Default: ``-1``

``client_mountpoint``

:Description: Directory to mount on the CephFS file system.
:Type: String
:Default: ``"/"``

``client_oc``

:Description: Enable object caching. **Setting currently ignored.**
:Type: Boolean
:Default: ``true``

``client_oc_max_dirty``

:Description: Maximum number of dirty bytes in object cache.
:Type: Integer
:Default: ``104857600`` (100MB)

``client_oc_max_dirty_age``

:Description: Maximum age of dirty data in the object cache before writeback.
:Type: Float
:Default: ``5.0`` (seconds)

``client_oc_max_objects``

:Description: Maximum number of objects in the object cache.
:Type: Integer
:Default: ``1000``

``client_oc_size``

:Description: Maximum size of cached data in the object cache.
:Type: Integer
:Default: ``209715200`` (200MB)

``client_oc_target_dirty``

:Description: Target size of dirty data. It is recommended to keep this low.
:Type: Integer
:Default: ``8388608`` (8MB)

``client_permissions``

:Description: Check client permissions on all I/O operations.
:Type: Boolean
:Default: ``true``

``client_quota``

:Description: Enable client quota checking.
:Type: Boolean
:Default: ``true``

``client_quota_df``

:Description: Report root directory quota for statfs operation.
:Type: Boolean
:Default: ``true``

``client_readahead_max_bytes``

:Description: Maximum bytes of readahead used for future read operations. Overridden by ``client_readahead_max_periods``.
:Type: Integer
:Default: ``0`` (unlimited)

``client_readahead_max_periods``

:Description: Number of file layout periods (object size * number of stripes) to readahead. Overrides ``client_readahead_max_bytes``.
:Type: Integer
:Default: ``4``

``client_readahead_min``

:Description: Minimum bytes to readahead.
:Type: Integer
:Default: ``131072`` (128KB)

``client_snapdir``

:Description: Name for the snapshot directory.
:Type: String
:Default: ``".snap"``

``client_tick_interval``

:Description: Interval between capability renewal and other upkeep.
:Type: Float
:Default: ``1.0`` (seconds)

``client_trace``

:Description: Trace file path for all file operations. The output is designed to be used by the Ceph `synthetic client <../man/8/ceph-syn>`.
:Type: String
:Default: ``""`` (disabled)

``client_use_random_mds``

:Description: Choose random MDS for each request.
:Type: Boolean
:Default: ``false``

Developer Options
#################

``client_inject_fixed_oldest_tid``

:Description:
:Type: Boolean
:Default: ``false``

``client_inject_release_failure``

:Description:
:Type: Boolean
:Default: ``false``

``client_debug_inject_tick_delay``

:Description: Add artificial delay between client ticks.
:Type: Integer
:Default: ``0``
