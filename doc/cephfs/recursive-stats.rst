.. _cephfs-recursive-stats:

Recursive statistics
====================

The MDS keeps recursive statistics for every directory in CephFS: how many
files and directories are beneath it, how many bytes the files beneath it
hold, and when anything beneath it last changed. Clients read these
statistics as *virtual extended attributes* (vxattrs) of the directory, so
finding the size of a directory tree takes one request instead of a walk of
the tree with a tool such as ``du``.

The statistics lag behind changes to the file system. See `Propagation
delay`_.

Statistics
----------

The following vxattrs count the whole subtree beneath a directory, at any
depth:

``ceph.dir.rbytes``
    The total size in bytes of the files in the subtree. A regular file
    counts with its size (``st_size``), which is its logical size: holes in
    sparse files count, and replication, erasure coding, and compression do
    not change it. A symbolic link counts with the length of the path that
    it points to. Directories add nothing.

``ceph.dir.rfiles``
    The number of files in the subtree: regular files, symbolic links, and
    special files such as device nodes, FIFOs, and sockets.

``ceph.dir.rsubdirs``
    The number of directories in the subtree, including the directory
    itself. An empty directory has an ``rsubdirs`` of ``1``.

``ceph.dir.rentries``
    The sum of ``rfiles`` and ``rsubdirs``. Because ``rsubdirs`` counts the
    directory itself, an empty directory has an ``rentries`` of ``1``.

``ceph.dir.rsnaps``
    The number of snapshots taken of the directory and of the directories in
    its subtree. The Linux kernel client supports this vxattr beginning with
    Linux 5.13.

``ceph.dir.rctime``
    The most recent change time (``ctime``) of the directory or of anything
    in its subtree, as seconds and nanoseconds since the Unix epoch, for
    example ``1764260775.123456789``. Creating, removing, or renaming an
    entry, writing to a file, or changing the attributes of a file or
    directory anywhere in the subtree moves ``rctime`` forward. Comparing
    the ``rctime`` values of directories is a fast way to find the subtrees
    that have changed, without walking the whole tree.

A file with more than one hard link counts once in ``rfiles`` and
``rbytes``, not once for each link.

The statistics describe the live file system. They do not count data that
exists only in snapshots: a file that is removed or truncated after a
snapshot is taken stops counting in ``rbytes``, even though the snapshot
still holds its data.

The following vxattrs count only the entries directly in the directory:

``ceph.dir.entries``
    The sum of ``files`` and ``subdirs``.

``ceph.dir.files``
    The number of entries in the directory that are not directories. Each
    hard link counts.

``ceph.dir.subdirs``
    The number of subdirectories in the directory. Unlike ``rsubdirs``, it
    does not count the directory itself.

These vxattrs exist only on directories.

Reading the statistics
----------------------

Read a statistic by naming it with ``getfattr -n``:

.. prompt:: bash $

   getfattr -n ceph.dir.rbytes projects

::

   # file: projects
   ceph.dir.rbytes="52613349376"

Add ``--only-value`` to print only the value. For example, the following
lists the subdirectories of the current directory by size, smallest first:

.. code-block:: bash

   for d in */; do
       echo "$(getfattr --only-value -n ceph.dir.rbytes "$d") $d"
   done | sort -n

``getfattr -d`` does not show these vxattrs, even with ``-m -``. The Linux
kernel client, ceph-fuse, and libcephfs leave all CephFS vxattrs out of the
list that ``listxattr(2)`` returns, so a tool that lists extended attributes
does not see them. Name each vxattr that you want to read.

Applications that use libcephfs read the same names with
``ceph_getxattr()``.

Each read asks the MDS that is authoritative for the directory for the
current value. That value can still lag behind recent changes, as described
in the next section.

Propagation delay
-----------------

The MDS does not update the statistics of every ancestor directory at the
moment that something changes. When a file or directory changes, the MDS
carries the change up through the ancestor directories, but not necessarily
all the way at once. To limit the cost of this work, it stops and leaves the
change pending at a directory that it cannot lock without waiting, at a
directory for which another MDS rank is authoritative, and, above the parent
directory, at a directory whose statistics it updated less than
:confval:`mds_dirstat_min_interval` seconds ago (default: ``1``).

Every :confval:`mds_tick_interval` seconds (default: ``5``), the MDS carries
further up the tree each pending change that has waited at least
:confval:`mds_scatter_nudge_interval` seconds (default: ``5``). The change
can stop again at a higher directory for the same reasons. Directories close
to a change therefore reflect it first, and a change deep in a busy tree can
take seconds to tens of seconds to reach the root of the file system.

Data that a client writes to a file counts in ``rbytes`` only after the
client reports the new size of the file to the MDS, which it does when it
flushes its capabilities for the file, for example on ``fsync(2)``.

Directory sizes
---------------

The size (``st_size``) that ``stat(2)`` and ``ls -l`` show for a directory
depends on the client:

ceph-fuse and libcephfs
    When :confval:`client_dirsize_rbytes` is ``true`` (the default), the size
    of a directory is its ``rbytes``. When it is ``false``, the size is the
    number of entries directly in the directory, the value of
    ``ceph.dir.entries``. The option takes effect when the client mounts the
    file system.

Linux kernel client
    With the ``rbytes`` mount option, the size of a directory is its
    ``rbytes``. With ``norbytes`` (the default), the size is the number of
    entries directly in the directory. With the ``dirstat`` mount option,
    reading a directory as a file, for example with ``cat``, prints its
    ``entries``, ``files``, ``subdirs``, ``rentries``, ``rfiles``,
    ``rsubdirs``, ``rbytes``, and ``rctime``. See :doc:`/man/8/mount.ceph`.

A directory size that changes with the files beneath it can confuse some
programs, such as ``rsync``.

Quotas
------

CephFS quotas are enforced with these statistics. A client compares the
``ceph.quota.max_bytes`` of a directory with its ``rbytes``, and the
``ceph.quota.max_files`` of a directory with its ``rentries``. The file
limit therefore counts directories too, including the directory that has
the quota. Because the statistics lag behind changes, writers can go past a
quota before they are stopped. See :doc:`/cephfs/quota`.

Repairing the statistics
------------------------

If the recursive statistics of a directory are wrong, for example after a
metadata recovery, a recursive scrub with the ``repair`` option recalculates
them:

.. prompt:: bash #

   ceph tell mds.<fs_name>:0 scrub start /path/to/dir recursive,repair

See :doc:`/cephfs/scrub`.
