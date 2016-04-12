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

  We currently recommend:

  - 4.1.4 or later
  - 3.16.3 or later (rbd deadlock regression in 3.16.[0-2])
  - *NOT* 3.15.* (rbd deadlock regression)
  - 3.14.*

  These are considered pretty old, but if you must:

  - 3.10.*

  Firefly (CRUSH_TUNABLES3) tunables are supported starting with 3.15.
  See `CRUSH Tunables`_ for more details.

- **B-tree File System (Btrfs)**

  If you use the ``btrfs`` file system with Ceph, we recommend using a
  recent Linux kernel (3.14 or later).

Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).

Infernalis (9.1.0)
------------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 7        | N/A                | linux-3.10.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 8.0      | Jessie             | linux-3.16.0 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 22       | N/A                | linux-3.14.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        | Maipo              | linux-3.10.0 |         | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

Hammer (0.94)
-------------

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

Firefly (0.80)
--------------

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
  recommend for ``ceph-osd`` storage nodes.  Upgrade to a recommended
  kernel or use ``XFS``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.


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
