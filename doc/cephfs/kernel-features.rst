
Supported Features of the Kernel Driver
========================================
The kernel driver is developed separately from the core ceph code, and as
such it sometimes differs from the FUSE driver in feature implementation.
The following details the implementation status of various CephFS features
in the kernel driver.

Inline data
-----------
Inline data was introduced by the Firefly release. This feature is being
deprecated in mainline CephFS, and may be removed from a future kernel
release.

Linux kernel clients >= 3.19 can read inline data and convert existing
inline data to RADOS objects when file data is modified. At present,
Linux kernel clients do not store file data as inline data.

See `Experimental Features`_ for more information.

Quotas
------
Quota was first introduced by the hammer release. Quota disk format got renewed
by the Mimic release. Linux kernel clients >= 4.17 can support the new format
quota. At present, no Linux kernel client support the old format quota.

See `Quotas`_ for more information.

Multiple file systems within a Ceph cluster
-------------------------------------------
The feature was introduced by the Jewel release. Linux kernel clients >= 4.7
can support it.

See `Experimental Features`_ for more information.

Multiple active metadata servers
--------------------------------
The feature has been supported since the Luminous release. It is recommended to
use Linux kernel clients >= 4.14 when there are multiple active MDS.

Snapshots
---------
The feature has been supported since the Mimic release. It is recommended to
use Linux kernel clients >= 4.17 if snapshot is used.

.. _Experimental Features: ../experimental-features
.. _Quotas: ../quota
