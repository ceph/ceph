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


Platforms
=========

The chart below shows which Linux platforms Ceph provides packages for, and
which platforms Ceph has been tested on. 

Ceph does not require a specific Linux distribution. Ceph can run on any
distribution that includes a supported kernel and supported system startup
framework, for example ``sysvinit`` or ``systemd``. Ceph is sometimes ported to
non-Linux systems but these are not supported by the core Ceph effort.

+---------------+----------------+---------------+------------------+------------------+------------------+
|               | Squid (19.2.z) | Reef (18.2.z) | Quincy (17.2.z)  | Pacific (16.2.z) | Octopus (15.2.z) |
+===============+================+===============+==================+==================+==================+
| Centos 7      |                |               |                  |                  |      B           |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Centos 8      |                |               |                  |                  |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Centos 9      | A              |    A          |     A :sup:`1`   |                  |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Debian 10     |                |    C          |                  |         C        |      C           |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Debian 11     |                |    C          |     C            |         C        |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Debian 12     | C              |    C          |                  |                  |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+
| OpenSUSE 15.2 |                |    C          |                  |         C        |      C           |
+---------------+----------------+---------------+------------------+------------------+------------------+
| OpenSUSE 15.3 |                |    C          |     C            |                  |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Ubuntu 18.04  |                |               |                  |         C        |      C           |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Ubuntu 20.04  |                |    A          |     A            |         A        |      A           |
+---------------+----------------+---------------+------------------+------------------+------------------+
| Ubuntu 22.04  | A              |    A          |                  |                  |                  |
+---------------+----------------+---------------+------------------+------------------+------------------+

- **A**: Ceph provides packages and has done comprehensive tests on the software in them.
- **B**: Ceph provides packages and has done basic tests on the software in them.
- **C**: Ceph provides packages only. No tests have been done on these releases.
- **1**: Testing has been done on Centos 9 starting on version 17.2.8 for Quincy.

Container Hosts
---------------

This table shows the operating systems that support Ceph's official container images.

+---------------+----------------+------------------+------------------+
|               | Squid (19.2.z) | Reef (18.2.z)    | Quincy (17.2.z)  |
+===============+================+==================+==================+
| Centos 7      |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Centos 8      |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Centos 9      |      H         |        H         |        H         |
+---------------+----------------+------------------+------------------+
| Debian 10     |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Debian 11     |                |                  |                  |
+---------------+----------------+------------------+------------------+
| OpenSUSE 15.2 |                |                  |                  |
+---------------+----------------+------------------+------------------+
| OpenSUSE 15.3 |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Ubuntu 18.04  |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Ubuntu 20.04  |                |                  |                  |
+---------------+----------------+------------------+------------------+
| Ubuntu 22.04  |      H         |        H         |                  |
+---------------+----------------+------------------+------------------+

- **H**: Ceph tests this distribution as a container host.

.. note::
   **For Centos 7 Users** 
   
   ``Btrfs`` is no longer tested on Centos 7 in the Octopus release. We recommend using ``bluestore`` instead.

.. note:: See the list of QAed container hosts in the Ceph repository here:
   `List of Container Hosts
   <https://github.com/ceph/ceph/tree/main/qa/distros/supported-container-hosts>`_.


.. _CRUSH Tunables: ../../rados/operations/crush-map#tunables

.. _Mounting CephFS using Kernel Driver: ../../cephfs/mount-using-kernel-driver#which-kernel-version
