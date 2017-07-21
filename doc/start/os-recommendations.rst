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

  - 4.9.z
  - 4.4.z

  For CephFS, see `CephFS best practices`_ for kernel version guidance.

  Older kernel client versions may not support your `CRUSH tunables`_ profile
  or other newer features of the Ceph cluster, requiring the storage cluster
  to be configured with those features disabled.


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).

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


Jewel (10.2.z)
--------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 7        | N/A                | linux-3.10.0 | 3       | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 8.0      | Jessie             | linux-3.16.0 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 22       | N/A                | linux-3.14.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

Hammer (0.94.z)
---------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 6        | N/A                | linux-2.6.32 | 1, 2    |            |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 7        | N/A                | linux-3.10.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    |            |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    |            |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

Firefly (0.80.z)
----------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 6        | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 7        | N/A                | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 19       | Schrödinger's Cat  | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 20       | Heisenbug          | linux-3.14.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6        | Santiago           | linux-2.6.32 | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

Notes
-----

- **1**: The default kernel has an older version of ``btrfs`` that we do not
  recommend for ``ceph-osd`` storage nodes.  We recommend using ``XFS``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.

- **3**: The default kernel regularly fails in QA when the ``btrfs``
  file system is used.  We do not recommend using ``btrfs`` for
  backing Ceph OSDs.


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

.. _CephFS best practices: ../../cephfs/best-practices
