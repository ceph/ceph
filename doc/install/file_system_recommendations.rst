===========================
File System Recommendations
===========================
Ceph OSDs depend on the Extended Attributes (XATTRS) of the underlying file system for:

- Internal object state
- Snapshot metadata
- RADOS Gateway Access Control Lists (ACLs). 

Ceph OSDs rely heavily upon the stability and performance of the underlying file system. The 
underlying file system must provide sufficient capacity for XATTRS. File system candidates for 
Ceph include B tree and B+ tree file systems such as: 

- ``btrfs``
- ``XFS``

.. warning:: XATTR limits.

   The RADOS Gateway's ACL and Ceph snapshots easily surpass the 4-kilobyte limit for XATTRs in ``ext4``, 
   causing the ``ceph-osd`` process to crash. So ``ext4`` is a poor file system choice if 
   you intend to deploy the RADOS Gateway or use snapshots.
  
.. tip:: Use `btrfs`

   The Ceph team believes that the best performance and stability will come from ``btrfs.`` 
   The ``btrfs`` file system has internal transactions that keep the local data set in a consistent state. 
   This makes OSDs based on ``btrfs`` simple to deploy, while providing scalability not 
   currently available from block-based file systems. The 64-kb XATTR limit for ``xfs``
   XATTRS is enough to accommodate RDB snapshot metadata and RADOS Gateway ACLs. So ``xfs`` is the second-choice 
   file system of the Ceph team. If you only plan to use RADOS and ``rbd`` without snapshots and without 
   ``radosgw``, the ``ext4`` file system should work just fine.
