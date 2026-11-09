
Supported Features of the Kernel Driver
========================================
The kernel driver is developed separately from the core Ceph code, and as
such it sometimes differs from the FUSE driver in feature implementation.
The following details the implementation status of various CephFS features
in the kernel driver.

Inline data
-----------
Inline data is deprecated: enabling it triggers a health warning, and it will
be removed in a future release.

Linux kernel clients >= 3.19 can read inline data and convert existing
inline data to RADOS objects when file data is modified. At present,
Linux kernel clients do not store file data as inline data.

See `Experimental Features`_ for more information.

Quotas
------
Linux kernel clients >= 4.17 support quotas.

See `Quotas`_ for more information.

Multiple file systems within a Ceph cluster
-------------------------------------------
Linux kernel clients >= 4.7 support multiple file systems.

See `Experimental Features`_ for more information.

Multiple active metadata servers
--------------------------------
It is recommended to use Linux kernel clients >= 4.14 when there are multiple
active MDS.

Snapshots
---------
It is recommended to use Linux kernel clients >= 4.17 if snapshot is used.

.. _Experimental Features: ../experimental-features
.. _Quotas: ../quota
