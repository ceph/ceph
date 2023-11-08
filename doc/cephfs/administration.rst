.. _cephfs-administration:

CephFS Administrative commands
==============================

File Systems
------------

.. note:: The names of the file systems, metadata pools, and data pools can
          only have characters in the set [a-zA-Z0-9\_-.].

These commands operate on the CephFS file systems in your Ceph cluster.
Note that by default only one file system is permitted: to enable
creation of multiple file systems use ``ceph fs flag set enable_multiple true``.

::

    ceph fs new <file system name> <metadata pool name> <data pool name>

This command creates a new file system. The file system name and metadata pool
name are self-explanatory. The specified data pool is the default data pool and
cannot be changed once set. Each file system has its own set of MDS daemons
assigned to ranks so ensure that you have sufficient standby daemons available
to accommodate the new file system.

::

    ceph fs ls

List all file systems by name.

::

    ceph fs lsflags <file system name>

List all the flags set on a file system.

::

    ceph fs dump [epoch]

This dumps the FSMap at the given epoch (default: current) which includes all
file system settings, MDS daemons and the ranks they hold, and the list of
standby MDS daemons.


::

    ceph fs rm <file system name> [--yes-i-really-mean-it]

Destroy a CephFS file system. This wipes information about the state of the
file system from the FSMap. The metadata pool and data pools are untouched and
must be destroyed separately.

::

    ceph fs get <file system name>

Get information about the named file system, including settings and ranks. This
is a subset of the same information from the ``ceph fs dump`` command.

::

    ceph fs set <file system name> <var> <val>

Change a setting on a file system. These settings are specific to the named
file system and do not affect other file systems.

::

    ceph fs add_data_pool <file system name> <pool name/id>

Add a data pool to the file system. This pool can be used for file layouts
as an alternate location to store file data.

::

    ceph fs rm_data_pool <file system name> <pool name/id>

This command removes the specified pool from the list of data pools for the
file system.  If any files have layouts for the removed data pool, the file
data will become unavailable. The default data pool (when creating the file
system) cannot be removed.

::

    ceph fs rename <file system name> <new file system name> [--yes-i-really-mean-it]

Rename a Ceph file system. This also changes the application tags on the data
pools and metadata pool of the file system to the new file system name.
The CephX IDs authorized to the old file system name need to be reauthorized
to the new name. Any on-going operations of the clients using these IDs may be
disrupted. Mirroring is expected to be disabled on the file system.

::

    fs swap <fs1-name> <fs1_id> <fs2-name> <fs2_id> [--swap-fscids=yes|no] [--yes-i-really-mean-it]

Swaps names of two Ceph file sytems and updates the application tags on all
pools of both FSs accordingly. Certain tools that track FSCIDs of the file
systems, besides the FS names, might get confused due to this operation. For
this reason, mandatory option ``--swap-fscids`` has been provided that must be
used to indicate whether or not FSCIDs must be swapped.

.. note:: FSCID stands for "File System Cluster ID".

Before the swap, mirroring should be disabled on both the CephFSs
(because the cephfs-mirror daemon uses the fscid internally and changing it
while the daemon is running could result in undefined behaviour), both the
CephFSs should be offline and the file system flag ``refuse_client_sessions``
must be set for both the CephFS.

The function of this API is to facilitate disaster recovery where a new file
system reconstructed from the previous one is ready to take over for the
possibly damaged file system. Instead of two ``fs rename`` operations, the
operator can use a swap so there is no FSMap epoch where the primary (or
production) named file system does not exist. This is important when Ceph is
monitored by automatic storage operators like (Rook) which try to reconcile
the storage system continuously. That operator may attempt to recreate the
file system as soon as it is seen to not exist.

After the swap, CephX credentials may need to be reauthorized if the existing
mounts should "follow" the old file system to its new name. Generally, for
disaster recovery, its desirable for the existing mounts to continue using
the same file system name. Any active file system mounts for either CephFSs
must remount. Existing unflushed operations will be lost. When it is judged
that one of the swapped file systems is ready for clients, run::

    ceph fs set <fs> joinable true
    ceph fs set <fs> refuse_client_sessions false

Keep in mind that one of the swapped file systems may be left offline for
future analysis if doing a disaster recovery swap.


Settings
--------

::

    ceph fs set <fs name> max_file_size <size in bytes>

CephFS has a configurable maximum file size, and it's 1TB by default.
You may wish to set this limit higher if you expect to store large files
in CephFS. It is a 64-bit field.

Setting ``max_file_size`` to 0 does not disable the limit. It would
simply limit clients to only creating empty files.


Maximum file sizes and performance
----------------------------------

CephFS enforces the maximum file size limit at the point of appending to
files or setting their size. It does not affect how anything is stored.

When users create a file of an enormous size (without necessarily
writing any data to it), some operations (such as deletes) cause the MDS
to have to do a large number of operations to check if any of the RADOS
objects within the range that could exist (according to the file size)
really existed.

The ``max_file_size`` setting prevents users from creating files that
appear to be eg. exabytes in size, causing load on the MDS as it tries
to enumerate the objects during operations like stats or deletes.


Taking the cluster down
-----------------------

Taking a CephFS cluster down is done by setting the down flag:
 
:: 
 
    ceph fs set <fs_name> down true
 
To bring the cluster back online:
 
:: 

    ceph fs set <fs_name> down false

This will also restore the previous value of max_mds. MDS daemons are brought
down in a way such that journals are flushed to the metadata pool and all
client I/O is stopped.


Taking the cluster down rapidly for deletion or disaster recovery
-----------------------------------------------------------------

To allow rapidly deleting a file system (for testing) or to quickly bring the
file system and MDS daemons down, use the ``ceph fs fail`` command:

::

    ceph fs fail <fs_name>

This command sets a file system flag to prevent standbys from
activating on the file system (the ``joinable`` flag).

This process can also be done manually by doing the following:

::

    ceph fs set <fs_name> joinable false

Then the operator can fail all of the ranks which causes the MDS daemons to
respawn as standbys. The file system will be left in a degraded state.

::

    # For all ranks, 0-N:
    ceph mds fail <fs_name>:<n>

Once all ranks are inactive, the file system may also be deleted or left in
this state for other purposes (perhaps disaster recovery).

To bring the cluster back up, simply set the joinable flag:

::

    ceph fs set <fs_name> joinable true


Daemons
-------

Most commands manipulating MDSs take a ``<role>`` argument which can take one
of three forms:

::

    <fs_name>:<rank>
    <fs_id>:<rank>
    <rank>

Commands to manipulate MDS daemons:

::

    ceph mds fail <gid/name/role>

Mark an MDS daemon as failed.  This is equivalent to what the cluster
would do if an MDS daemon had failed to send a message to the mon
for ``mds_beacon_grace`` second.  If the daemon was active and a suitable
standby is available, using ``ceph mds fail`` will force a failover to the
standby.

If the MDS daemon was in reality still running, then using ``ceph mds fail``
will cause the daemon to restart.  If it was active and a standby was
available, then the "failed" daemon will return as a standby.


::

    ceph tell mds.<daemon name> command ...

Send a command to the MDS daemon(s). Use ``mds.*`` to send a command to all
daemons. Use ``ceph tell mds.* help`` to learn available commands.

::

    ceph mds metadata <gid/name/role>

Get metadata about the given MDS known to the Monitors.

::

    ceph mds repaired <role>

Mark the file system rank as repaired. Unlike the name suggests, this command
does not change a MDS; it manipulates the file system rank which has been
marked damaged.


Required Client Features
------------------------

It is sometimes desirable to set features that clients must support to talk to
CephFS. Clients without those features may disrupt other clients or behave in
surprising ways. Or, you may want to require newer features to prevent older
and possibly buggy clients from connecting.

Commands to manipulate required client features of a file system:

::

    ceph fs required_client_features <fs name> add reply_encoding
    ceph fs required_client_features <fs name> rm reply_encoding

To list all CephFS features

::

    ceph fs feature ls

Clients that are missing newly added features will be evicted automatically.

Here are the current CephFS features and first release they came out:

+------------------+--------------+-----------------+
| Feature          | Ceph release | Upstream Kernel |
+==================+==============+=================+
| jewel            | jewel        | 4.5             |
+------------------+--------------+-----------------+
| kraken           | kraken       | 4.13            |
+------------------+--------------+-----------------+
| luminous         | luminous     | 4.13            |
+------------------+--------------+-----------------+
| mimic            | mimic        | 4.19            |
+------------------+--------------+-----------------+
| reply_encoding   | nautilus     | 5.1             |
+------------------+--------------+-----------------+
| reclaim_client   | nautilus     | N/A             |
+------------------+--------------+-----------------+
| lazy_caps_wanted | nautilus     | 5.1             |
+------------------+--------------+-----------------+
| multi_reconnect  | nautilus     | 5.1             |
+------------------+--------------+-----------------+
| deleg_ino        | octopus      | 5.6             |
+------------------+--------------+-----------------+
| metric_collect   | pacific      | N/A             |
+------------------+--------------+-----------------+
| alternate_name   | pacific      | PLANNED         |
+------------------+--------------+-----------------+

CephFS Feature Descriptions


::

    reply_encoding

MDS encodes request reply in extensible format if client supports this feature.


::

    reclaim_client

MDS allows new client to reclaim another (dead) client's states. This feature
is used by NFS-Ganesha.


::

    lazy_caps_wanted

When a stale client resumes, if the client supports this feature, mds only needs
to re-issue caps that are explicitly wanted.


::

    multi_reconnect

When mds failover, client sends reconnect messages to mds, to reestablish cache
states. If MDS supports this feature, client can split large reconnect message
into multiple ones.


::

    deleg_ino

MDS delegate inode numbers to client if client supports this feature. Having
delegated inode numbers is a prerequisite for client to do async file creation.


::

    metric_collect

Clients can send performance metric to MDS if MDS support this feature.

::

    alternate_name

Clients can set and understand "alternate names" for directory entries. This is
to be used for encrypted file name support.


Global settings
---------------


::

    ceph fs flag set <flag name> <flag val> [<confirmation string>]

Sets a global CephFS flag (i.e. not specific to a particular file system).
Currently, the only flag setting is 'enable_multiple' which allows having
multiple CephFS file systems.

Some flags require you to confirm your intentions with "--yes-i-really-mean-it"
or a similar string they will prompt you with. Consider these actions carefully
before proceeding; they are placed on especially dangerous activities.

.. _advanced-cephfs-admin-settings:

Advanced
--------

These commands are not required in normal operation, and exist
for use in exceptional circumstances.  Incorrect use of these
commands may cause serious problems, such as an inaccessible
file system.

::

    ceph mds rmfailed

This removes a rank from the failed set.

::

    ceph fs reset <file system name>

This command resets the file system state to defaults, except for the name and
pools. Non-zero ranks are saved in the stopped set.


::

    ceph fs new <file system name> <metadata pool name> <data pool name> --fscid <fscid> --force

This command creates a file system with a specific **fscid** (file system cluster ID).
You may want to do this when an application expects the file system's ID to be
stable after it has been recovered, e.g., after monitor databases are lost and
rebuilt. Consequently, file system IDs don't always keep increasing with newer
file systems.
