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

``client acl type``

:Description: Set the ACL type. Currently, only possible value is ``"posix_acl"`` to enable POSIX ACL, or an empty string. This option only takes effect when the ``fuse_default_permissions`` is set to ``false``.

:Type: String
:Default: ``""`` (no ACL enforcement)

``client cache mid``

:Description: Set client cache midpoint. The midpoint splits the least recently used lists into a hot and warm list.
:Type: Float
:Default: ``0.75``

``client cache size``

:Description: Set the number of inodes that the client keeps in the metadata cache.
:Type: Integer
:Default: ``16384``

``client caps release delay``

:Description: Set the delay between capability releases in seconds. The delay sets how many seconds a client waits to release capabilities that it no longer needs in case the capabilities are needed for another user space operation.
:Type: Integer
:Default: ``5`` (seconds)

``client debug force sync read``

:Description: If set to ``true``, clients read data directly from OSDs instead of using a local page cache.
:Type: Boolean
:Default: ``false``

``client dirsize rbytes``

:Description: If set to ``true``, use the recursive size of a directory (that is, total of all descendants).
:Type: Boolean
:Default: ``true``

``client max inline size``

:Description: Set the maximum size of inlined data stored in a file inode rather than in a separate data object in RADOS. This setting only applies if the ``inline_data`` flag is set on the MDS map.
:Type: Integer
:Default: ``4096``

``client metadata``

:Description: Comma-delimited strings for client metadata sent to each MDS, in addition to the automatically generated version, host name, and other metadata.
:Type: String
:Default: ``""`` (no additional metadata)

``client mount gid``

:Description: Set the group ID of CephFS mount.
:Type: Integer
:Default: ``-1``

``client mount timeout``

:Description: Set the timeout for CephFS mount in seconds.
:Type: Float
:Default: ``300.0``

``client mount uid``

:Description: Set the user ID of CephFS mount.
:Type: Integer
:Default: ``-1``

``client mountpoint``

:Description: Directory to mount on the CephFS file system. An alternative to the ``-r`` option of the ``ceph-fuse`` command.
:Type: String
:Default: ``"/"``

``client oc``

:Description: Enable object caching.
:Type: Boolean
:Default: ``true``

``client oc max dirty``

:Description: Set the maximum number of dirty bytes in the object cache.
:Type: Integer
:Default: ``104857600`` (100MB)

``client oc max dirty age``

:Description: Set the maximum age in seconds of dirty data in the object cache before writeback.
:Type: Float
:Default: ``5.0`` (seconds)

``client oc max objects``

:Description: Set the maximum number of objects in the object cache.
:Type: Integer
:Default: ``1000``

``client oc size``

:Description: Set how many bytes of data will the client cache.
:Type: Integer
:Default: ``209715200`` (200 MB)

``client oc target dirty``

:Description: Set the target size of dirty data. We recommend to keep this number low.
:Type: Integer
:Default: ``8388608`` (8MB)

``client permissions``

:Description: Check client permissions on all I/O operations.
:Type: Boolean
:Default: ``true``

``client quota``

:Description: Enable client quota checking if set to ``true``.
:Type: Boolean
:Default: ``true``

``client quota df``

:Description: Report root directory quota for the ``statfs`` operation.
:Type: Boolean
:Default: ``true``

``client readahead max bytes``

:Description: Set the maximum number of bytes that the client reads ahead for future read operations. Overridden by the ``client_readahead_max_periods`` setting.
:Type: Integer
:Default: ``0`` (unlimited)

``client readahead max periods``

:Description: Set the number of file layout periods (object size * number of stripes) that the client reads ahead. Overrides the ``client_readahead_max_bytes`` setting.
:Type: Integer
:Default: ``4``

``client readahead min``

:Description: Set the minimum number bytes that the client reads ahead.
:Type: Integer
:Default: ``131072`` (128KB)

``client reconnect stale``

:Description: Automatically reconnect stale session.
:Type: Boolean
:Default: ``false``

``client snapdir``

:Description: Set the snapshot directory name.
:Type: String
:Default: ``".snap"``

``client tick interval``

:Description: Set the interval in seconds between capability renewal and other upkeep.
:Type: Float
:Default: ``1.0`` (seconds)

``client use random mds``

:Description: Choose random MDS for each request.
:Type: Boolean
:Default: ``false``

``fuse default permissions``

:Description: When set to ``false``, ``ceph-fuse`` utility checks does its own permissions checking, instead of relying on the permissions enforcement in FUSE. Set to ``false`` together with the ``client acl type=posix_acl`` option to enable POSIX ACL.
:Type: Boolean
:Default: ``true``

``fuse max write``

:Description: Set the maximum number of bytes in a single write operation.  Because the FUSE default is 128kbytes, SO fuse_max_write default set to 0(The default does not take effect)
:Type: Integer
:Default: ``0``

``fuse disable pagecache``

:Description: If set to ``true``, kernel page cache is disabled for ceph-fuse mount. Because when multiple clients read/write to a file at the same time, reader may get stale data from page cache. (Due to limitation of fuse, ceph-fuse can't disable page cache dynamically)
:Type: Boolean
:Default: ``false``

Developer Options
#################

.. important:: These options are internal. They are listed here only to complete the list of options.

``client debug getattr caps``

:Description: Check if the reply from the MDS contains required capabilities.
:Type: Boolean
:Default: ``false``

``client debug inject tick delay``

:Description: Add artificial delay between client ticks.
:Type: Integer
:Default: ``0``

``client inject fixed oldest tid``

:Description:
:Type: Boolean
:Default: ``false``

``client inject release failure``

:Description:
:Type: Boolean
:Default: ``false``

``client trace``

:Description: The path to the trace file for all file operations. The output is designed to be used by the Ceph `synthetic client <../../man/8/ceph-syn>`_.
:Type: String
:Default: ``""`` (disabled)

