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

- ``ioctl(fd, CEPH_IOC_LAZYIO, 1UL)`` enables LAZY_IO for file handle in
   ceph-fuse mount.
