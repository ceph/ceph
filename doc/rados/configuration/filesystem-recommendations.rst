===========================================
 Hard Disk and File System Recommendations
===========================================

.. index:: hard drive preparation

Hard Drive Prep
===============

Ceph aims for data safety, which means that when the :term:`Ceph Client`
receives notice that data was written to a storage drive, that data was actually
written to the storage drive. For old kernels (<2.6.33), disable the write cache
if the journal is on a raw drive. Newer kernels should work fine.

Use ``hdparm`` to disable write caching on the hard disk::

	sudo hdparm -W 0 /dev/hda 0

In production environments, we recommend running a :term:`Ceph OSD Daemon` with
separate drives for the operating system and the data. If you run data and an
operating system on a single disk, we recommend creating a separate partition
for your data.

.. index:: filesystems

Filesystems
===========

Ceph OSD Daemons rely heavily upon the stability and performance of the
underlying filesystem.

Recommended
-----------

We currently recommend ``XFS`` for production deployments.

Not recommended
---------------

We recommand *against* using ``btrfs`` due to the lack of a stable
version to test against and frequent bugs in the ENOSPC handling.

We recommend *against* using ``ext4`` due to limitations in the size
of xattrs it can store, and the problems this causes with the way Ceph
handles long RADOS object names.  Although these issues will generally
not surface with Ceph clusters using only short object names (e.g., an
RBD workload that does not include long RBD image names), other users
like RGW make extensive use of long object names and can break.

Starting with the Jewel release, the ``ceph-osd`` daemon will refuse
to start if the configured max object name cannot be safely stored on
``ext4``.  If the cluster is only being used with short object names
(e.g., RBD only), you can continue using ``ext4`` by setting the
following configuration option::

  osd max object name len = 256
  osd max object namespace len = 64

.. note:: This may result in difficult-to-diagnose errors if you try
          to use RGW or other librados clients that do not properly
          handle or politely surface any resulting ENAMETOOLONG
          errors.
