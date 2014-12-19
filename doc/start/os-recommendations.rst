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

  - v3.16.3 or later (rbd deadlock regression in v3.16.[0-2])
  - *NOT* v3.15.* (rbd deadlock regression)
  - v3.14.*
  - v3.10.*

  These are considered pretty old, but if you must:

  - v3.6.6 or later in the v3.6 stable series
  - v3.4.20 or later in the v3.4 stable series

  firefly (CRUSH_TUNABLES3) tunables are supported starting with v3.15.
  See `CRUSH Tunables`_ for more details.

- **btrfs**

  If you use the ``btrfs`` file system with Ceph, we recommend using a
  recent Linux kernel (v3.14 or later).

glibc
-----

- **fdatasync(2)**: With Firefly v0.80 and beyond, use ``fdatasync(2)`` 
  instead of ``fsync(2)`` to improve performance.

- **syncfs(2)**: For non-btrfs filesystems 
  such as XFS and ext4 where more than one ``ceph-osd`` daemon is used on a 
  single server, Ceph performs significantly better with the ``syncfs(2)`` 
  system call (added in kernel 2.6.39 and glibc 2.14).  New versions of 
  Ceph (v0.55 and later) do not depend on glibc support.


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).



Firefly (0.80)
--------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 6        | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 7        |                    | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 19       | Schrödinger's Cat  | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 20       | Heisenbug          | linux-3.14.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6        |                    | linux-2.6.32 | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 7        |                    | linux-3.10.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 14.04    | Trusty Tahr        | linux-3.13.0 |         | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+

**NOTE**: Ceph also supports ``Quantal``, ``Raring`` and ``Saucy``. However, we 
recommend using LTS releases.



Emperor (0.72)
---------------

The Ceph Emperor release, version 0.72, is no longer supported, and Emperor users should update to Firefly (version 0.80).


Dumpling (0.67)
---------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18       | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 19       | Schrödinger's Cat  | linux-3.10.0 |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| RHEL     | 6.3      |                    | linux-2.6.32 | 1, 2    | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 13.04    | Raring Ringtail    | linux-3.8.5  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+

Argonaut (0.48), Bobtail (0.56), and Cuttlefish (0.61)
------------------------------------------------------

The Ceph Argonaut, Bobtail, and Cuttlefish releases are no longer supported,
and users should update to the latest stable release (Dumpling or Firefly).


Notes
-----

- **1**: The default kernel has an older version of ``btrfs`` that we do not
  recommend for ``ceph-osd`` storage nodes.  Upgrade to a recommended
  kernel or use ``XFS`` or ``ext4``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.

- **3**: The default kernel or installed version of ``glibc`` does not
  support the ``syncfs(2)`` system call.  Putting multiple
  ``ceph-osd`` daemons using ``XFS`` or ``ext4`` on the same host will
  not perform as well as they could.


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
