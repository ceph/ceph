========================
 Differences from POSIX
========================

CephFS aims to adhere to POSIX semantics wherever possible.  For
example, in contrast to many other common network file systems like
NFS, CephFS maintains strong cache coherency across clients.  The goal
is for processes communicating via the file system to behave the same
when they are on different hosts as when they are on the same host.

However, there are a few places where CephFS diverges from strict
POSIX semantics for various reasons:

- If a client is writing to a file and fails, its writes are not
  necessarily atomic. That is, the client may call write(2) on a file
  opened with O_SYNC with an 8 MB buffer and then crash and the write
  may be only partially applied.  (Almost all file systems, even local
  file systems, have this behavior.)
- In shared simultaneous writer situations, a write that crosses
  object boundaries is not necessarily atomic. This means that you
  could have writer A write "aa|aa" and writer B write "bb|bb"
  simultaneously (where | is the object boundary), and end up with
  "aa|bb" rather than the proper "aa|aa" or "bb|bb".
- Sparse files propagate incorrectly to the stat(2) st_blocks field.
  Because CephFS does not explicitly track which parts of a file are
  allocated/written, the st_blocks field is always populated by the
  file size divided by the block size.  This will cause tools like
  du(1) to overestimate consumed space.  (The recursive size field,
  maintained by CephFS, also includes file "holes" in its count.)
- When a file is mapped into memory via mmap(2) on multiple hosts,
  writes are not coherently propagated to other clients' caches.  That
  is, if a page is cached on host A, and then updated on host B, host
  A's page is not coherently invalidated.  (Shared writable mmap
  appears to be quite rare--we have yet to hear any complaints about this
  behavior, and implementing cache coherency properly is complex.)
- CephFS clients present a hidden ``.snap`` directory that is used to
  access, create, delete, and rename snapshots.  Although the virtual
  directory is excluded from readdir(2), any process that tries to
  create a file or directory with the same name will get an error
  code.  The name of this hidden directory can be changed at mount
  time with ``-o snapdirname=.somethingelse`` (Linux) or the config
  option ``client_snapdir`` (libcephfs, ceph-fuse).
- CephFS does not currently maintain the ``atime`` field. Most applications
  do not care, though this impacts some backup and data tiering
  applications that can move unused data to a secondary storage system.
  You may be able to work around this for some use cases, as CephFS does
  support setting ``atime`` via the ``setattr`` operation.

Perspective
-----------

People talk a lot about "POSIX compliance," but in reality most file
system implementations do not strictly adhere to the spec, including
local Linux file systems like ext4 and XFS.  For example, for
performance reasons, the atomicity requirements for reads are relaxed:
processes reading from a file that is also being written may see torn
results.

Similarly, NFS has extremely weak consistency semantics when multiple
clients are interacting with the same files or directories, opting
instead for "close-to-open".  In the world of network attached
storage, where most environments use NFS, whether or not the server's
file system is "fully POSIX" may not be relevant, and whether client
applications notice depends on whether data is being shared between
clients or not.  NFS may also "tear" the results of concurrent writers
as client data may not even be flushed to the server until the file is
closed (and more generally writes will be significantly more
time-shifted than CephFS, leading to less predictable results).

Regardless, these are all similar enough to POSIX, and applications still work
most of the time. Many other storage systems (e.g., HDFS) claim to be
"POSIX-like" but diverge significantly from the standard by dropping support
for things like in-place file modifications, truncate, or directory renames.

Bottom line
-----------

CephFS relaxes more than local Linux kernel file systems (for example, writes
spanning object boundaries may be torn).  It relaxes strictly less
than NFS when it comes to multiclient consistency, and generally less
than NFS when it comes to write atomicity.

In other words, when it comes to POSIX, ::

  HDFS < NFS < CephFS < {XFS, ext4}


Visibility to other clients
---------------------------

CephFS uses :doc:`capabilities <capabilities>` to keep every client's view
of the file system coherent. This section describes when a change that a
process makes on one host becomes visible to processes on other hosts.

Namespace changes, such as creating, unlinking, or renaming a file or a
directory, are made by the MDS. Before the MDS makes such a change, it
revokes the cached copies of the affected directory entries that other
clients hold. Other clients do not see the change until the MDS has recorded
it in its journal: a lookup or a directory listing that arrives earlier waits
for that.

ceph-fuse, libcephfs, and kernel clients mounted with ``wsync`` do not return
from a namespace operation until the MDS has made the change. After the call
returns, a process on any other host sees the change, and it sees the changes
of one process in the order in which that process made them.

Kernel clients mount with ``nowsync`` by default since kernel version 5.16.
With ``nowsync``, a client can create or unlink a file in a directory
without waiting for the MDS while it is the only client using that directory.
This needs no configuration: the MDS gives the client exclusive capabilities
on the directory after its first create or unlink there, and revokes them
when another client looks up or lists the directory. The client sends the
request to the MDS at once, but the call can return before the MDS has made
the change. Until then, a process on another host might not see the change,
and it might see a later change in another directory first. Other namespace
operations, such as mkdir(2), rmdir(2), and rename(2), always wait for the
MDS. Before you tell a process on another host that such a change has been
made, call fsync(2) on the directory, which waits until the MDS has journaled
the pending changes in it, or mount with ``wsync``.

File data follows the same model. While only one client has a file open, that
client can cache the file's data and buffer its writes. When a client on
another host opens the file or reads its attributes with stat(2), the MDS has
the first client write back its buffered data before letting the second
client proceed. On kernel clients, access(2) does not wait for this, because
it needs only the file's mode and ownership. Neither does statx(2) when its
mask leaves out the size, the block count, and the access, modification, and
change times. ceph-fuse fetches all of a file's attributes when it looks up a
file that it has not cached, so with ceph-fuse access(2) can wait as long as
stat(2). While clients on more than one host have the file open and any of
them has it open for writing, no client caches or buffers its data, and
every read and write goes to the OSDs. As a result, a read on any host returns
the data of a write(2) that has returned on another host, whether or not the
writer called fsync(2). fsync(2) protects the data against a failure of the
writing client, but other clients do not need it to see the data.

The exceptions are shared writable mmap(2), described above, and
:doc:`/cephfs/lazyio`, which lets applications that manage coherency
themselves keep caching and buffering data.


fsync() and error reporting
---------------------------

POSIX is somewhat vague about the state of an inode after fsync reports
an error. In general, CephFS uses the standard error-reporting
mechanisms in the client's kernel, and therefore follows the same
conventions as other file systems.

In modern Linux kernels (v4.17 or later), writeback errors are reported
once to every file description that is open at the time of the error. In
addition, unreported errors that occurred before the file description was
opened will also be returned on fsync.

See `PostgreSQL's summary of fsync() error reporting across operating systems
<https://wiki.postgresql.org/wiki/Fsync_Errors>`_ and `Matthew Wilcox's
presentation on Linux I/O error handling
<https://www.youtube.com/watch?v=74c19hwY2oE>`_ for more information.
