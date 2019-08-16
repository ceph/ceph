======
LazyIO
======

LazyIO relaxes POSIX semantics. Buffered reads/writes are allowed even when a
file is opened by multiple applications on multiple clients. Applications are
responsible for managing cache coherency themselves.

Libcephfs supports LazyIO since nautilus release.

Enable LazyIO
=============

LazyIO can be enabled by following ways.

- ``client_force_lazyio`` option enables LAZY_IO globally for libcephfs and
  ceph-fuse mount.

- ``ceph_lazyio(...)`` and ``ceph_ll_lazyio(...)`` enable LAZY_IO for file handle
  in libcephfs.

Using LazyIO
============

LazyIO includes two methods ``lazyio_propagate()`` and ``lazyio_synchronize()``.
With LazyIO enabled, writes may not be visble to other clients until
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
        ceph_lazyio(ca, fda, 1));

        for(i = 0; i < num_iters; i++) {
            char out_buf[] = "fooooooooo";
            
            ceph_write(ca, fda, out_buf, sizeof(out_buf), i);
            /* Propagate the writes associated with fda to the backing storage*/
            ceph_propagate(ca, fda, 0, 0);
            
            /* The barrier makes sure changes associated with all file descriptors
            are propagated so that there is certainty that the backing file 
            is upto date */
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
