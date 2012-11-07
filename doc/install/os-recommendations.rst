====================
 OS Recommendations
====================

Ceph Dependencies
=================

As a general rule, we recommend deploying Ceph on newer releases of Linux. 

Linux Kernel
------------

- **Ceph Kernel Client:**  We recommend Linux kernel v3.6 or later.

- **btrfs**: If you use the ``btrfs`` file system with Ceph, we recommend using 
  a recent Linux kernel (v3.5 or later).

glibc
-----

- **syncfs(2)**: For non-btrfs filesystems such as XFS and ext4 where
  more than one ``ceph-osd`` daemon is used on a single server, Ceph
  performs signficantly better with the ``syncfs(2)`` system call
  (added in kernel 2.6.39 and glibc 2.14).


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions aside from the kernel and system initialization
package (i.e., sysvinit, upstart, systemd).

Argonaut (0.48)
---------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | TBA          |         | B          |
+----------+----------+--------------------+--------------+---------+------------+

Bobtail (0.55)
--------------

+----------+----------+--------------------+--------------+---------+------------+
| Distro   | Release  | Code Name          | Kernel       | Notes   | Testing    | 
+==========+==========+====================+==============+=========+============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  | 1, 2, 3 | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  | 1, 2    | B, I, C    |
+----------+----------+--------------------+--------------+---------+------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Debian   | 7.0      | Wheezy             | TBA          |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 | 1, 2, 3 | B, I       |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 17.0     | Beefy Miracle      | linux-3.3.4  | 1, 2    | B          |
+----------+----------+--------------------+--------------+---------+------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |         | B          |
+----------+----------+--------------------+--------------+---------+------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  | 2       | B          |
+----------+----------+--------------------+--------------+---------+------------+


Notes
-----

- **1**: The default kernel has an older version of ``btrfs`` that we do not
  recommend for ``ceph-osd`` storage nodes.  Upgrade to a recommended
  kernel or use ``XFS`` or ``ext4``.

- **2**: The default kernel has an old Ceph client that we do not recommend
  for kernel client (kernel RBD or the Ceph file system).  Upgrade to a
  recommended kernel.

- **3**: The installed version of ``glibc`` does not support the
  ``syncfs(2)`` system call.  Putting multiple ``ceph-osd`` daemons
  using ``XFS`` or ``ext4`` on the same host will not perform as well as
  they could.

Testing
-------

- **B**: We continuously build all branches on this platform and exercise basic
  unit tests.  We build release packages for this platform.

- **I**: We do basic installation and functionality tests of releases on this
  platform.

- **C**: We run a comprehensive functional, regression, and stress test suite
  on this platform on a continuous basis.   This includes development branches,
  pre-release, and released code.



