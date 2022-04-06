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
  4.x-based "longterm maintenance" kernel series or later:

  - 4.19.z
  - 4.14.z
  - 5.x

  For CephFS, see the section about `Mounting CephFS using Kernel Driver`_
  for kernel version guidance.

  Older kernel client versions may not support your `CRUSH tunables`_ profile
  or other newer features of the Ceph cluster, requiring the storage cluster
  to be configured with those features disabled.


Platforms
=========

The charts below show how Ceph's requirements map onto various Linux
platforms.  Generally speaking, there is very little dependence on
specific distributions outside of the kernel and system initialization
package (i.e., sysvinit, systemd).

+--------------+--------+------------------------+--------------------------------+-------------------+-----------------+
| Release Name | Tag    | CentOS                 | Ubuntu                         | OpenSUSE :sup:`C` | Debian :sup:`C` |
+==============+========+========================+================================+===================+=================+
| Quincy       | 17.2.z | 8 :sup:`A`             | 20.04 :sup:`A`                 | 15.3              | 11              |
+--------------+--------+------------------------+--------------------------------+-------------------+-----------------+
| Pacific      | 16.2.z | 8 :sup:`A`             | 18.04 :sup:`C`, 20.04 :sup:`A` | 15.2              | 10, 11          |
+--------------+--------+------------------------+--------------------------------+-------------------+-----------------+
| Octopus      | 15.2.z | 7 :sup:`B` 8 :sup:`A`  | 18.04 :sup:`C`, 20.04 :sup:`A` | 15.2              | 10              |
+--------------+--------+------------------------+--------------------------------+-------------------+-----------------+

- **A**: Ceph provides packages and has done comprehensive tests on the software in them.
- **B**: Ceph provides packages and has done basic tests on the software in them.
- **C**: Ceph provides packages only. No tests have been done on these releases.

.. note::
   **For Centos 7 Users** 
   
   ``Btrfs`` is no longer tested on Centos 7 in the Octopus release. We recommend using ``bluestore`` instead.

.. _CRUSH Tunables: ../../rados/operations/crush-map#tunables

.. _Mounting CephFS using Kernel Driver: ../../cephfs/mount-using-kernel-driver#which-kernel-version
