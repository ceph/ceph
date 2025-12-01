====================
 OS Recommendations
====================

Ceph Dependencies
=================

As a general rule, we recommend deploying Ceph on newer releases of Linux. 
We also recommend deploying on releases with long-term support.

Linux Kernel
------------

- **Ceph Kernel Client**

  If you are using the kernel client to map RBD block devices or mount
  CephFS, the general advice is to use a "stable" or "longterm
  maintenance" kernel series provided by either http://kernel.org or
  your Linux distribution on any client hosts.

  For RBD, if you choose to *track* long-term kernels, we recommend
  *at least* 4.19-based "longterm maintenance" kernel series.  If you can
  use a newer "stable" or "longterm maintenance" kernel series, do it.

  For CephFS, see the section about `Mounting CephFS using Kernel Driver`_
  for kernel version guidance.

  Older kernel client versions may not support your `CRUSH tunables`_ profile
  or other newer features of the Ceph cluster, requiring the storage cluster to
  be configured with those features disabled. For RBD, a kernel of version 5.3
  or CentOS 8.2 is the minimum necessary for reasonable support for RBD image
  features.

- **Ceph MS Windows Client**

  Ceph's MS Windows native client support is "best effort".  There is no
  full-time maintainer. As of July 2025 there are no plans to remove this
  client but the future is uncertain.

Platforms
=========

The chart below shows the platforms for which Ceph provides packages, and
the platforms on which Ceph has been tested. 

Ceph does not require a specific Linux distribution. Ceph can run on any
distribution that includes a supported kernel and supported system startup
framework, for example ``sysvinit`` or ``systemd``. Ceph is sometimes ported to
non-Linux systems but these are not supported by the core Ceph effort.

+---------------+-------------------+----------------+---------------+
|               | Tentacle (20.2.z) | Squid (19.2.z) | Reef (18.2.z) |
+===============+===================+================+===============+
| Centos 9      | A                 | A              |    A          |
+---------------+-------------------+----------------+---------------+
| Debian 12     | C                 | C              |    C          |
+---------------+-------------------+----------------+---------------+
| Ubuntu 20.04  |                   |                |    A          |
+---------------+-------------------+----------------+---------------+
| Ubuntu 22.04  | A                 | A              |    A          |
+---------------+-------------------+----------------+---------------+
| MS Windows    | D                 | D              | D             |
+---------------+-------------------+----------------+---------------+

- **A**: Ceph provides packages and has done comprehensive tests on the software in them.
- **B**: Ceph provides packages and has done basic tests on the software in them.
- **C**: Ceph provides packages only. No tests have been done on these releases.
- **D**: Client packages are available from an external site but are not maintained or tested by the core Ceph team.

Container Hosts
---------------

This table shows the operating systems that support Ceph's official container images.

+---------------+-------------------+----------------+------------------+
|               | Tentacle (20.2.z) | Squid (19.2.z) | Reef (18.2.z)    |
+===============+===================+================+==================+
| Centos 9      |      H            |      H         |        H         |
+---------------+-------------------+----------------+------------------+
| Ubuntu 22.04  |      H            |      H         |        H         |
+---------------+-------------------+----------------+------------------+

- **H**: Ceph tests this distribution as a container host.

.. _CRUSH Tunables: ../../rados/operations/crush-map#tunables

.. _Mounting CephFS using Kernel Driver: ../../cephfs/mount-using-kernel-driver#which-kernel-version
