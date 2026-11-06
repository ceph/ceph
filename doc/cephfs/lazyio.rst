======
LazyIO
======

LazyIO relaxes POSIX semantics. Buffered reads/writes are allowed even when a
file is opened by multiple applications on multiple clients. Applications are
responsible for managing cache coherency themselves.

Libcephfs supports LazyIO since Nautilus release.

Enable LazyIO
=============

LazyIO can be enabled in the following ways.

- ``client_force_lazyio`` option enables LAZY_IO globally for libcephfs and
  ceph-fuse mount. This is strictly a startup flag.

- ``ceph_lazyio(...)`` and ``ceph_ll_lazyio(...)`` enable LAZY_IO for file handle
  in libcephfs.

Using LazyIO
============

LazyIO includes two methods ``lazyio_propagate()`` and ``lazyio_synchronize()``.
With LazyIO enabled, writes may not be visible to other clients until
``lazyio_propagate()`` is called. Reads may come from local cache (irrespective of
changes to the file by other clients) until ``lazyio_synchronize()`` is called.

- ``lazyio_propagate(int fd, loff_t offset, size_t count)`` - Ensures that any
  buffered writes of the client, in the specific region (offset to offset+count),
  has been propagated to the shared file. If offset and count are both 0, the 
  operation is performed on the entire file. Currently only this is supported.

- ``lazyio_synchronize(int fd, loff_t offset, size_t count)`` - Ensures that the
  client is, in a subsequent read call, able to read the updated file with all 
  the propagated writes of the other clients. In CephFS this is facilitated by
  invalidating the file caches pertaining to the inode and hence forces the
  client to refetch/recache the data from the updated file. Also if the write cache
  of the calling client is dirty (not propagated), lazyio_synchronize() flushes it as well.

An example usage (utilizing libcephfs) is given below. This is a sample I/O loop for a
particular client/file descriptor in a parallel application:

::

        /* Client a (ca) opens the shared file file.txt */
        int fda = ceph_open(ca, "shared_file.txt", O_CREAT|O_RDWR, 0644); 

        /* Enable LazyIO for fda */
        ceph_lazyio(ca, fda, 1);

        for(i = 0; i < num_iters; i++) {
            char out_buf[] = "fooooooooo";
            
            ceph_write(ca, fda, out_buf, sizeof(out_buf), i);
            /* Propagate the writes associated with fda to the backing storage*/
            ceph_propagate(ca, fda, 0, 0);
            
            /* The barrier makes sure changes associated with all file descriptors
            are propagated so that there is certainty that the backing file 
            is up to date */
            application_specific_barrier();

            char in_buf[40];
            /* Calling ceph_lazyio_synchronize here will ascertain that ca will
            read the updated file with the propagated changes and not read
            stale cached data */
            ceph_lazyio_synchronize(ca, fda, 0, 0);
            ceph_read(ca, fda, in_buf, sizeof(in_buf), 0);

            /* A barrier is required here before returning to the next write
            phase so as to avoid overwriting the portion of the shared file still
            being read by another file descriptor */
            application_specific_barrier();
        }

Close-to-open consistency
=========================

``lazyio_propagate()`` and ``lazyio_synchronize()`` require an application that
knows when it shares a file and with whom.  Most applications do not, but many
of them are perfectly happy with the weaker consistency that NFS provides,
where a file that is written and closed is seen by the clients that open it
afterwards, and nothing is promised while the file stays open.

Setting ``client_close_to_open`` makes the client provide exactly that for
regular files, without any application changes:

* Data written to a file, and the size and mtime that describe it, reach the
  cluster before ``close()`` returns.
* An ``open()`` sees everything that other clients wrote and closed before it
  started.
* Taking an advisory lock (``flock()``, ``fcntl()``) revalidates like an open
  does, and releasing one flushes like a close does, so applications that
  coordinate with locks rather than with open and close keep working.
* Reads never serve data that is older than
  ``client_close_to_open_timeout`` (one minute by default, comparable to the
  ``acregmax`` mount option of NFS).  Setting it to zero leaves ``open()`` and
  locking as the only points where a cache is revalidated.  Through
  ``ceph-fuse``, reads the kernel answers out of its page cache never reach
  the client, so for them only ``open()`` and locking revalidate: the kernel
  page cache is dropped on an open that found the cache unprotected.

Only file data consistency changes.  Directory listings, dentries, inode
attributes, permissions and quota remain as consistent as they always were.

In exchange:

* While a file is open, a write by another client does not invalidate the
  cache, so reads may return data that is up to
  ``client_close_to_open_timeout`` old.
* Clients that write overlapping regions of the same file at the same time
  can lose each other's writes, because writeback happens at page or object
  granularity rather than in the byte ranges the application wrote.  Writers
  have to be serialized with close/open or with locks.
* ``mmap()`` of a shared file is not made coherent between clients.

Opens with ``O_DIRECT`` or ``O_SYNC`` keep the stricter semantics they asked
for, and ``O_APPEND`` is excluded as well, because clients that cache the end
of a file would silently overwrite each other's appends.  What any of these
descriptors write is still flushed when they are closed, so that the clients
that open the file afterwards see it.  Snapshots and directories are
unaffected.

The option only affects files opened after it was set, and it only makes a
difference for files that are open for writing on one client while other
clients have them open too.  In every other case the MDS keeps the client
caches coherent by itself and close-to-open costs nothing: no cache is dropped
and no extra request is sent to the MDS.

The most useful place for this mode is a gateway that re-exports CephFS with a
protocol that promises close-to-open to its own clients anyway, such as NFS
without delegations, where the stronger guarantees of CephFS are paid for but
never observed.

.. note:: Only libcephfs and ceph-fuse implement this.  The kernel client
   supports LazyIO itself, via ``ioctl(fd, CEPH_IOC_LAZYIO)``, together with
   ``sync_file_range(2)`` to propagate and ``posix_fadvise(2)``
   (``POSIX_FADV_DONTNEED``) to invalidate, but it has no close-to-open mode
   of its own.
