=========================================
Hard Disk and File System Recommendations
=========================================

.. important:: Disable disk caching and asynchronous write.

Ceph aims for data safety, which means that when the application receives notice that data was 
written to the disk, that data was actually written to the disk and not still in a transient state in 
a buffer or cache pending a lazy write to the hard disk.

For data safety, you should mount your file system with caching disabled. Your file system should be
mounted with ``sync`` and NOT ``async``. For example, your ``fstab`` file would reflect ``sync``. :: 

	/dev/hda    /    xfs        sync    0   0

Use ``hdparm`` to disable write caching on the hard disk:: 

	$ hdparm -W 0 /dev/hda 0 	 

	
Ceph OSDs depend on the Extended Attributes (XATTRs) of the underlying file system for:

- Internal object state
- Snapshot metadata
- RADOS Gateway Access Control Lists (ACLs). 

Ceph OSDs rely heavily upon the stability and performance of the underlying file system. The 
underlying file system must provide sufficient capacity for XATTRs. File system candidates for 
Ceph include B tree and B+ tree file systems such as: 

- ``btrfs``
- ``XFS``

.. warning:: XATTR limits.

   The RADOS Gateway's ACL and Ceph snapshots easily surpass the 4-kilobyte limit for XATTRs in ``ext4``, 
   causing the ``ceph-osd`` process to crash. So ``ext4`` is a poor file system choice if 
   you intend to deploy the RADOS Gateway or use snapshots. Version 0.45 or newer uses ``leveldb`` to
   bypass this limitation.
  
.. tip:: Use ``xfs`` initially and ``btrfs`` when it is ready for production.

   The Ceph team believes that the best performance and stability will come from ``btrfs.`` 
   The ``btrfs`` file system has internal transactions that keep the local data set in a consistent state. 
   This makes OSDs based on ``btrfs`` simple to deploy, while providing scalability not 
   currently available from block-based file systems. The 64-kb XATTR limit for ``xfs``
   XATTRS is enough to accommodate RDB snapshot metadata and RADOS Gateway ACLs. So ``xfs`` is the second-choice 
   file system of the Ceph team in the long run, but ``xfs`` is currently more stable than ``btrfs``.  
   If you only plan to use RADOS and ``rbd`` without snapshots and without ``radosgw``, the ``ext4`` 
   file system should work just fine.

