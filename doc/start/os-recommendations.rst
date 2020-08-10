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

  For RBD, if you choose to *track* long-term kernels, we currently recommend
  4.x-based "longterm maintenance" kernel series:

  - 4.19.z
  - 4.14.z

  For CephFS, see the section about `Mounting CephFS using Kernel Driver`_
  for kernel version guidance.

  Older kernel client versions may not support your `CRUSH tunables`_ profile
  or other newer features of the Ceph cluster, requiring the storage cluster
  to be configured with those features disabled.


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).

Octopus (15.2.z)
-----------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| CentOS   | 8        | N/A                | linux-4.18   |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 7        | N/A                | linux-3.10.0 | 4, 5    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 10       | Buster             | linux-4.19   |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 8        | Ootpa              | linux-4.18   |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 18.04    | Bionic Beaver      | linux-4.15   | 4       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| openSUSE | 15.2     | Leap               | linux-5.3    | 6       |            |
+----------+----------+--------------------+--------------+---------+------------+
| openSUSE |          | Tumbleweed         |              |         |            |
+----------+----------+--------------------+--------------+---------+------------+


Nautilus (14.2.z)
-----------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| CentOS   | 7        | N/A                | linux-3.10.0 | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 8.0      | Jessie             | linux-3.16.0 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 9.0      | Stretch            | linux-4.9    | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 16.04    | Xenial Xerus       | linux-4.4.0  | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 18.04    | Bionic Beaver      | linux-4.15   | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| openSUSE | 15.1     | Leap               | linux-4.12   | 6       |            |
+----------+----------+--------------------+--------------+---------+------------+

Luminous (12.2.z)
-----------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    |
+==========+==========+====================+==============+=========+============+
| CentOS   | 7        | N/A                | linux-3.10.0 | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 8.0      | Jessie             | linux-3.16.0 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 9.0      | Stretch            | linux-4.9    | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 22       | N/A                | linux-3.14.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 16.04    | Xenial Xerus       | linux-4.4.0  | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

Notes
-----

- **1**: The default kernel has an older version of ``btrfs`` that we do not
  recommend for ``ceph-osd`` storage nodes.  We recommend using ``bluestore``
  starting from Mimic, and ``XFS`` for previous releases with ``filestore``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.

- **3**: The default kernel regularly fails in QA when the ``btrfs``
  file system is used.  We recommend using ``bluestore`` starting from
  Mimic, and ``XFS`` for previous releases with ``filestore``.

- **4**: ``btrfs`` is no longer tested on this release. We recommend
  using ``bluestore``.

- **5**: Some additional features related to dashboard are not available.

- **6**: Building packages are built regularly, but not distributed by Ceph.

Testing
-------

- **B**: We build release packages for this platform. For some of these
  platforms, we may also continuously build all ceph branches and exercise
  basic unit tests.

- **I**: We do basic installation and functionality tests of releases on this
  platform.

- **C**: We run a comprehensive functional, regression, and stress test suite
  on this platform on a continuous basis. This includes development branches,
  pre-release, and released code.

.. _CRUSH Tunables: ../../rados/operations/crush-map#tunables

.. _Mounting CephFS using Kernel Driver: ../../cephfs/mount-using-kernel-driver#which-kernel-version
