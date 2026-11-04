=================
Attribute caching
=================

CephFS clients cache inode attributes and directory contents only for as long
as the MDS lets them: the capabilities a client holds on an inode are what
makes the MDS tell it about changes, and the MDS takes them away as soon as
another client wants to change the inode.  A client that no longer holds them
goes back to the MDS for every ``stat()``, every lookup and every directory
listing.

That is what makes CephFS metadata as consistent as a local file system, and
it is also what makes a workload that walks a tree several clients are using
expensive: one round trip per file, every time.  NFS answers the same
questions out of a cache that it revalidates on a timer, with the
``acregmin``/``acregmax`` and ``acdirmin``/``acdirmax`` mount options.

Two options make a CephFS client do the same.  Both are off by default, and
neither has any effect while the client does hold the capabilities: nothing is
cached longer than it already was, and no extra request is sent to the MDS.

``client_attr_cache_timeout``
  How long ``stat()`` may be answered from the copy of the inode the client
  already has, counted from when the MDS last handed that copy out.  This
  covers the mode, owner, group, link count, size, timestamps and extended
  attributes.

``client_dentry_cache_timeout``
  How long lookups and ``readdir()`` may be answered from the names the client
  already has, counted from when it read them.  A name the client resolved
  on its own, without listing the directory, is cached like any other.
  Listing a directory out of the cache needs a listing: it only survives the
  loss of ``Fs`` if the client read the whole directory while it still held
  it, and getting ``Fs`` back always throws the cached listing away, because
  what happened while the MDS was not reporting changes is unknown.  Only
  names that exist are cached: whether a name does *not* exist is always
  asked of the MDS while the client does not hold ``Fs``, so a directory or
  file that another client just created can be used straight away.  This is
  what ``lookupcache=positive`` does for NFS.

Setting both to a minute is roughly what a default NFS mount does.

``ceph-fuse`` also lets the kernel cache lookups and attributes, for as long
as the client itself may still serve them: the interval, counted from when
the client last knew the answer to be current -- now, while it holds the
capabilities that keep it current, and otherwise when the MDS last told it.
Otherwise every path component of every ``stat()`` is a round trip through
FUSE, however warm the client's cache is. With both options at zero the
kernel caches nothing, as before.

The kernel then resolves cached paths without asking the client, and so
without the client's permission checks. With ``client_dentry_cache_timeout``
set, ``ceph-fuse`` therefore mounts with ``fuse_default_permissions``: the
kernel checks permissions itself, with the attributes it caches.

What is given up
================

Within the interval, a client can report:

* attributes another client has since changed -- a size that is behind, a mode
  that was tightened, an mtime that has moved on;
* a directory listing that does not yet contain a file another client
  created (looking the file up by name finds it);
* a directory that still contains a file another client removed, whose inode
  is then found to be gone when it is opened.

A client's own changes are always visible to itself; it is other clients that
are seen late.  Nothing is cached across a mount, and a blocklisted client
serves nothing from cache.

Operations that have to be right are not affected.  ``open()``, ``create()``,
``rename()``, ``unlink()`` and ``setattr()`` are carried out by the MDS as
before, so two clients still cannot create the same file.  The name an
operation opens, creates or replaces is resolved at the MDS, not out of the
cache, the way NFS resolves it at the server: ``open()``, file creation,
``mkdir()``, ``mknod()``, ``symlink()``, ``link()`` and the target of
``rename()``.  So a stale cached name neither opens a file another client has
removed or replaced, nor makes a create fail with ``EEXIST``.  An open by
inode (``ceph_ll_open()``, which is how ``ceph-fuse`` opens) of a file whose
name turns out to have been stale fails with ``ESTALE``, on which the kernel
looks the name up again.  ``lseek(SEEK_END)`` and the reads that check for
end of file ask the MDS as well.

Through ``ceph-fuse`` the kernel caches names as well, and before it
creates or replaces one it asks ``ceph-fuse`` about it again with an
ordinary lookup, even if it may still cache it.  The client resolves such a
lookup at the MDS too: one for a name it let the kernel cache, before that
has timed out.  A FUSE file system written against libcephfs gets the same
by handing the kernel ``ceph_ll_dentry_ttl()`` as the entry timeout.

Both options can be changed at runtime, and take effect on the next lookup or
``stat()``.

Where it helps
==============

* Walking or listing a large tree that other clients are also using, where the
  MDS keeps recalling capabilities and every ``stat()`` becomes a round trip.
* Gateways that re-export CephFS with a protocol whose own consistency model
  is no stronger, such as NFS, where the cost of keeping the CephFS caches
  coherent is paid for but never observed by the end client.

It does not help a client that holds more inodes than
``mds_max_caps_per_client`` allows.  To give capabilities back under recall,
the client trims those inodes from its cache altogether, so there is nothing
left to answer from, and every ``stat()`` of a trimmed inode goes to the MDS
as before.  Through ``ceph-fuse`` it helps nonetheless: the kernel goes on
answering out of what it was allowed to cache, for as long as it was
allowed to.

For the file data half of the same trade, see :doc:`/cephfs/lazyio`.
