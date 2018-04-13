
CephFS Administrative commands
==============================

Filesystems
-----------

These commands operate on the CephFS filesystems in your Ceph cluster.
Note that by default only one filesystem is permitted: to enable
creation of multiple filesystems use ``ceph fs flag set enable_multiple true``.

::

    fs new <filesystem name> <metadata pool name> <data pool name>

::

    fs ls

::

    fs rm <filesystem name> [--yes-i-really-mean-it]

::

    fs reset <filesystem name>

::

    fs get <filesystem name>

::

    fs set <filesystem name> <var> <val>

::

    fs add_data_pool <filesystem name> <pool name/id>

::

    fs rm_data_pool <filesystem name> <pool name/id>


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
 
    mds set <fs_name> down true
 
To bring the cluster back online:
 
:: 

    mds set <fs_name> down false

This will also restore the previous value of max_mds. MDS daemons are brought
down in a way such that journals are flushed to the metadata pool and all
client I/O is stopped.


Taking the cluster down rapidly for deletion or disaster recovery
-----------------------------------------------------------------

To allow rapidly deleting a file system (for testing) or to quickly bring MDS
daemons down, the operator may also set a flag to prevent standbys from
activating on the file system. This is done using the ``joinable`` flag:

::

    fs set <fs_name> joinable false

Then the operator can fail all of the ranks which causes the MDS daemons to
respawn as standbys. The file system will be left in a degraded state.

::

    # For all ranks, 0-N:
    mds fail <fs_name>:<n>

Once all ranks are inactive, the file system may also be deleted or left in
this state for other purposes (perhaps disaster recovery).


Daemons
-------

Most commands manipulating MDSs take a ``<role>`` argument which can take one
of three forms:

::

    <fs_name>:<rank>
    <fs_id>:<rank>
    <rank>

Comamnds to manipulate MDS daemons:

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

::

    mds repaired <role>

::

    mds stat



Global settings
---------------

::

    fs dump

::

    fs flag set <flag name> <flag val> [<confirmation string>]

"flag name" must be one of ['enable_multiple']

Some flags require you to confirm your intentions with "--yes-i-really-mean-it"
or a similar string they will prompt you with. Consider these actions carefully
before proceeding; they are placed on especially dangerous activities.


Advanced
--------

These commands are not required in normal operation, and exist
for use in exceptional circumstances.  Incorrect use of these
commands may cause serious problems, such as an inaccessible
filesystem.

::

    mds compat rm_compat

::

    mds compat rm_incompat

::

    mds compat show

::

    mds set_state

::

    mds rmfailed

Legacy
------

These legacy commands are obsolete and no longer usable post-Luminous.

::

    mds add_data_pool # replaced by "fs add_data_pool"
    mds cluster_down  # replaced by "fs set cluster_down"
    mds cluster_up  # replaced by "fs set cluster_up"
    mds dump  # replaced by "fs get"
    mds getmap # replaced by "fs dump"
    mds newfs # replaced by "fs new"
    mds remove_data_pool # replaced by "fs rm_data_pool"
    mds set # replaced by "fs set"
    mds set_max_mds # replaced by "fs set max_mds"
    mds stop  # obsolete

