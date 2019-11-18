
Application best practices for distributed file systems
=======================================================

CephFS is POSIX compatible, and therefore should work with any existing
applications that expect a POSIX file system.  However, because it is a
network file system (unlike e.g. XFS) and it is highly consistent (unlike
e.g. NFS), there are some consequences that application authors may
benefit from knowing about.

The following sections describe some areas where distributed file systems
may have noticeably different performance behaviours compared with
local file systems.


ls -l
-----

When you run "ls -l", the ``ls`` program
is first doing a directory listing, and then calling ``stat`` on every
file in the directory.

This is usually far in excess of what an application really needs, and
it can be slow for large directories.  If you don't really need all
this metadata for each file, then use a plain ``ls``.

ls/stat on files being extended
-------------------------------

If another client is currently extending files in the listed directory,
then an ``ls -l`` may take an exceptionally long time to complete, as
the lister must wait for the writer to flush data in order to do a valid
read of the every file's size.  So unless you *really* need to know the
exact size of every file in the directory, just don't do it!

This would also apply to any application code that was directly
issuing ``stat`` system calls on files being appended from
another node.

Very large directories
----------------------

Do you really need that 10,000,000 file directory?  While directory
fragmentation enables CephFS to handle it, it is always going to be
less efficient than splitting your files into more modest-sized directories.

Even standard userspace tools can become quite slow when operating on very
large directories. For example, the default behaviour of ``ls``
is to give an alphabetically ordered result, but ``readdir`` system
calls do not give an ordered result (this is true in general, not just
with CephFS).  So when you ``ls`` on a million file directory, it is
loading a list of a million names into memory, sorting the list, then writing
it out to the display.

Hard links
----------

Hard links have an intrinsic cost in terms of the internal housekeeping
that a file system has to do to keep two references to the same data.  In
CephFS there is a particular performance cost, because with normal files
the inode is embedded in the directory (i.e. there is no extra fetch of
the inode after looking up the path).

Working set size
----------------

The MDS acts as a cache for the metadata stored in RADOS.  Metadata
performance is very different for workloads whose metadata fits within
that cache.

If your workload has more files than fit in your cache (configured using 
``mds_cache_memory_limit`` settings), then make sure you test it
appropriately: don't test your system with a small number of files and then
expect equivalent performance when you move to a much larger number of files.

Do you need a file system?
--------------------------

Remember that Ceph also includes an object storage interface.  If your
application needs to store huge flat collections of files where you just
read and write whole files at once, then you might well be better off
using the :ref:`Object Gateway <object-gateway>`
