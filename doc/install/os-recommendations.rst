====================
 OS Recommendations
====================

Ceph Dependencies
=================

As a general rule, we recommend deploying Ceph on newer releases of Linux. 

- **Ceph Client:** For best results, we prefer newer kernels (e.g., 3.6.0). 
  **We do not recommend using Linux kernels prior to 3.0.0 for Argonaut (0.48) 
  and later.**

- **btrfs**: If you use the *btrfs* file system with Ceph, we recommend using 
  the newest releases (e.g., 3.4 or later).

- **syncfs(2)**: For non-btrfs filesystems such as XFS and ext4, Ceph requires
  the ``syncfs(2)`` system call (added in kernel 2.6.39).

- **glibc 2.14**: Ceph uses ``syncfs(2)``, and leverages the new RPC 
  implementation in ``glibc`` version 2.14.

.. note:: Ubuntu 11.10 Oneric doesn't include some of the required libraries, but
   Ubuntu 12.04 Precise does include all required libraries. Therefore, we 
   recommend using Ubuntu 12.04 Precise as the minimum Ubuntu distribution with 
   Ceph.


Argonaut (0.48)
===============


+----------+----------+--------------------+--------------+
| Distro   | Release  | Code Name          | Kernel       |
+==========+==========+====================+==============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 |
+----------+----------+--------------------+--------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  |
+----------+----------+--------------------+--------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  |
+----------+----------+--------------------+--------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  |
+----------+----------+--------------------+--------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 |
+----------+----------+--------------------+--------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  |
+----------+----------+--------------------+--------------+


Bobtail (0.54)
==============


+----------+----------+--------------------+--------------+
| Distro   | Release  | Code Name          | Kernel       |
+==========+==========+====================+==============+
| Ubuntu   | 11.04    | Natty Narwhal      | linux-2.6.38 |
+----------+----------+--------------------+--------------+
| Ubuntu   | 11.10    | Oneric Ocelot      | linux-3.0.0  |
+----------+----------+--------------------+--------------+
| Ubuntu   | 12.04    | Precise Pangolin   | linux-3.2.0  |
+----------+----------+--------------------+--------------+
| Ubuntu   | 12.10    | Quantal Quetzal    | linux-3.5.4  |
+----------+----------+--------------------+--------------+
| Debian   | 6.0      | Squeeze            | linux-2.6.32 |
+----------+----------+--------------------+--------------+
| Debian   | 7.0      | Wheezy             | linux-3.2.0  |
+----------+----------+--------------------+--------------+
| CentOS   | 6.3      | N/A                | linux-2.6.32 |
+----------+----------+--------------------+--------------+
| Fedora   | 17.0     | Beefy Miracle      | linux-3.3.4  |
+----------+----------+--------------------+--------------+
| Fedora   | 18.0     | Spherical Cow      | linux-3.6.0  |
+----------+----------+--------------------+--------------+
| OpenSuse | 12.2     | N/A                | linux-3.4.0  |
+----------+----------+--------------------+--------------+

