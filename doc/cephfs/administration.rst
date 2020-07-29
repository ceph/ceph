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

    fs new <file system name> <metadata pool name> <data pool name>

This command creates a new file system. The file system name and metadata pool
name are self-explanatory. The specified data pool is the default data pool and
cannot be changed once set. Each file system has its own set of MDS daemons
assigned to ranks so ensure that you have sufficient standby daemons available
to accommodate the new file system.

::

    fs ls

List all file systems by name.

::

    fs dump [epoch]

This dumps the FSMap at the given epoch (default: current) which includes all
file system settings, MDS daemons and the ranks they hold, and the list of
standby MDS daemons.


::

    fs rm <file system name> [--yes-i-really-mean-it]

Destroy a CephFS file system. This wipes information about the state of the
file system from the FSMap. The metadata pool and data pools are untouched and
must be destroyed separately.

::

    fs get <file system name>

Get information about the named file system, including settings and ranks. This
is a subset of the same information from the ``fs dump`` command.

::

    fs set <file system name> <var> <val>

Change a setting on a file system. These settings are specific to the named
file system and do not affect other file systems.

::

    fs add_data_pool <file system name> <pool name/id>

Add a data pool to the file system. This pool can be used for file layouts
as an alternate location to store file data.

::

    fs rm_data_pool <file system name> <pool name/id>

This command removes the specified pool from the list of data pools for the
file system.  If any files have layouts for the removed data pool, the file
data will become unavailable. The default data pool (when creating the file
system) cannot be removed.


Settings
--------

::

    fs set <fs name> max_file_size <size in bytes>

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
 
    fs set <fs_name> down true
 
To bring the cluster back online:
 
:: 

    fs set <fs_name> down false

This will also restore the previous value of max_mds. MDS daemons are brought
down in a way such that journals are flushed to the metadata pool and all
client I/O is stopped.


Taking the cluster down rapidly for deletion or disaster recovery
-----------------------------------------------------------------

To allow rapidly deleting a file system (for testing) or to quickly bring the
file system and MDS daemons down, use the ``fs fail`` command:

::

    fs fail <fs_name>

This command sets a file system flag to prevent standbys from
activating on the file system (the ``joinable`` flag).

This process can also be done manually by doing the following:

::

    fs set <fs_name> joinable false

Then the operator can fail all of the ranks which causes the MDS daemons to
respawn as standbys. The file system will be left in a degraded state.

::

    # For all ranks, 0-N:
    mds fail <fs_name>:<n>

Once all ranks are inactive, the file system may also be deleted or left in
this state for other purposes (perhaps disaster recovery).

To bring the cluster back up, simply set the joinable flag:

::

    fs set <fs_name> joinable true


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

    mds fail <gid/name/role>

Mark an MDS daemon as failed.  This is equivalent to what the cluster
would do if an MDS daemon had failed to send a message to the mon
for ``mds_beacon_grace`` second.  If the daemon was active and a suitable
standby is available, using ``mds fail`` will force a failover to the standby.

If the MDS daemon was in reality still running, then using ``mds fail``
will cause the daemon to restart.  If it was active and a standby was
available, then the "failed" daemon will return as a standby.


::

    tell mds.<daemon name> command ...

Send a command to the MDS daemon(s). Use ``mds.*`` to send a command to all
daemons. Use ``ceph tell mds.* help`` to learn available commands.

::

    mds metadata <gid/name/role>

Get metadata about the given MDS known to the Monitors.

::

    mds repaired <role>

Mark the file system rank as repaired. Unlike the name suggests, this command
does not change a MDS; it manipulates the file system rank which has been
marked damaged.


Minimum Client Version
----------------------

It is sometimes desirable to set the minimum version of Ceph that a client must be
running to connect to a CephFS cluster. Older clients may sometimes still be
running with bugs that can cause locking issues between clients (due to
capability release). CephFS provides a mechanism to set the minimum
client version:

::

    fs set <fs name> min_compat_client <release>

For example, to only allow Nautilus clients, use:

::

    fs set cephfs min_compat_client nautilus

Clients running an older version will be automatically evicted.

Enforcing minimum version of CephFS client is achieved by setting required
client features. Commands to manipulate required client features of a file
system:

::

    fs required_client_features <fs name> add reply_encoding
    fs required_client_features <fs name> rm reply_encoding

To list all CephFS features

::

    fs feature ls


CephFS features and first release they came out.

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
to re-issue caps that are explictly wanted.


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


Global settings
---------------


::

    fs flag set <flag name> <flag val> [<confirmation string>]

Sets a global CephFS flag (i.e. not specific to a particular file system).
Currently, the only flag setting is 'enable_multiple' which allows having
multiple CephFS file systems.

Some flags require you to confirm your intentions with "--yes-i-really-mean-it"
or a similar string they will prompt you with. Consider these actions carefully
before proceeding; they are placed on especially dangerous activities.


Advanced
--------

These commands are not required in normal operation, and exist
for use in exceptional circumstances.  Incorrect use of these
commands may cause serious problems, such as an inaccessible
file system.

::

    mds compat rm_compat

Removes an compatibility feature flag.

::

    mds compat rm_incompat

Removes an incompatibility feature flag.

::

    mds compat show

Show MDS compatibility flags.

::

    mds rmfailed

This removes a rank from the failed set.

::

    fs reset <file system name>

This command resets the file system state to defaults, except for the name and
pools. Non-zero ranks are saved in the stopped set.
