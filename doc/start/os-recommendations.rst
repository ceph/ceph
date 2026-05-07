.. _os-recommendations:

====================
 OS Recommendations
====================

Recommended Deployment Method: Containers via Cephadm
=====================================================

The Ceph project strongly prefers and natively supports container-based
releases deployed via :ref:`cephadm <cephadm>`. While legacy package-based
installations (via ``.deb`` or ``.rpm``) are still supported, containerized
deployments are preferred for all new and existing clusters. 
 
Container-based deployments offer significant advantages, particularly
regarding cluster upgrades. Because Ceph and its dependencies are packaged into
immutable container images, you avoid host-level OS package conflicts,
dependency nightmares, and broken libraries. This decoupled architecture allows you
to upgrade your Ceph cluster seamlessly, independently of the underlying host
operating system's package manager.

Ceph Dependencies
=================

As a general rule, we recommend deploying Ceph on newer releases of Linux.
We also recommend deploying on releases with long-term support.

Linux Kernel
------------

- **Ceph Kernel Client**

  If you are using the kernel client to map RBD block devices or mount
  CephFS, the general advice is to use a "stable" or "long-term
  maintenance" kernel series provided by either https://kernel.org or
  your Linux distribution on any client hosts.

  For RBD, if you choose to *track* long-term kernels, we recommend
  *at least* 4.19-based "long-term maintenance" kernel series.  If you can
  use a newer "stable" or "long-term maintenance" kernel series, do it.

  For CephFS, see the section about :ref:`Mounting CephFS using Kernel
  Driver <cephfs_which_kernel_version>` for kernel version guidance.

  Older kernel client versions may not support your :ref:`CRUSH
  tunables <crush-map-tunables>` profile or other newer features of the Ceph
  cluster, requiring the storage cluster to be configured with those features
  disabled. For RBD, a kernel of version 5.3 or CentOS 8.2 is the minimum
  necessary for reasonable support for RBD image features.

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

+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Distribution   | Distribution EOL        | Squid (19.2.z) | Tentacle (20.2.z) | Umbrella (21.x) | Vampire (22.x) | W (23.x)       | X (24.x)       |
|                |                         | EOL: Sept 2026 | EOL: May 2027     | EOL: May 2028   | EOL: May 2029  | EOL: May 2030  | EOL: May 2031  |
+================+=========================+================+===================+=================+================+================+================+
| CentOS 9       | `May 2027 <CentOS_>`_   | A              | A                 | Ae              |                |                |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Rocky 10       | `May 2035 <Rocky_>`_    |                | A in v20.2.2      | A               | A              | A              | A              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Rocky 11       |  May 2038*              |                |                   |                 |                |                | A              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Debian 12      | `Jun 2028 <Debian_b_>`_ | C              | C                 | C               |                |                |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Debian 13      | `Jun 2030 <Debian_t_>`_ |                |                   | C               | C              | C              | C              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 22.04   | `Jun 2027 <Ubuntu_>`_   | A              | A                 | A               |                |                |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 24.04   | `Jun 2029 <Ubuntu_>`_   |                | Upcoming          | A               | A              | A              |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 26.04   | `May 2031 <Ubuntu_>`_   |                |                   |                 | A              | A              | A              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 28.04   |  Jun 2033*              |                |                   |                 |                |                | A              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| MS Windows     |  Varies                 | D              | D                 | D               | D              | D              | D              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+


**Table legend:**

- **A**: Ceph provides packages and has done comprehensive tests on the software in them.
- **B**: Ceph provides packages and has done basic tests on the software in them.
- **C**: Ceph provides packages only. No tests have been done on these releases.
- **D**: Client packages are available from an external site but are not maintained or tested by the core Ceph team.
- **Ae**: It is expected that CentOS 9.stream will EOL before the Umbrella Ceph release is EOL. This means that CentOS 9.stream RPMs will no longer be generated for new minor releases of Umbrella when that occurs because CentOS deactivates its public repositories. It is strongly recommended to migrate to Rocky 10 or another supported distribution before that occurs.
  
.. note:: Dates marked with * are anticipated based on standard 10-year Enterprise Linux lifecycles and 5-year Ubuntu LTS lifecycles.

.. note:: Releases in the future are included for anticipated OS support and are not final.

.. warning:: Starting with CentOS 10 Stream and onwards, CentOS will no longer be built for or tested on by the upstream Ceph project.

Container Hosts
---------------

This table shows the operating systems that support Ceph's official
container images.

+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Distribution   | Distribution EOL        | Squid (19.2.z) | Tentacle (20.2.z) | Umbrella (21.x) | Vampire (22.x) | W (23.x)       | X (24.x)       |
|                |                         | EOL: Sept 2026 | EOL: May 2027     | EOL: May 2028   | EOL: May 2029  | EOL: May 2030  | EOL: May 2031  |
+================+=========================+================+===================+=================+================+================+================+
| CentOS 9       | `May 2027 <CentOS_>`_   | H              | H                 | H               |                |                |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Rocky 10       | `May 2035 <Rocky_>`_    |                | H in v20.2.2      | H               | H              | H              | H              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Rocky 11       |  May 2038*              |                |                   |                 |                |                | H              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 22.04   | `Jun 2027 <Ubuntu_>`_   | H              | H                 | H               |                |                |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 24.04   | `Jun 2029 <Ubuntu_>`_   |                | Upcoming          | H               | H              | H              |                |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 26.04   | `May 2031 <Ubuntu_>`_   |                |                   |                 | H              | H              | H              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+
| Ubuntu 28.04   |  Jun 2033*              |                |                   |                 |                |                | H              |
+----------------+-------------------------+----------------+-------------------+-----------------+----------------+----------------+----------------+

**Table legend:**

- **H**: Ceph tests this distribution as a container host.

.. warning:: This does not indicate that the container image is built on that distribution. It means the container image is tested to run on that distribution.


Umbrella Container Base Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting with the Umbrella release, the default base image for official Ceph
containers is Rocky Linux 10. Prior to Umbrella, CentOS 9 Stream had been used.

As a cluster administrator, you **do not** need to run Rocky Linux 10 as the
host operating system to use these containers. The Rocky 10 base image is used
strictly for static package management and to bundle Ceph's internal
dependencies within the isolated container boundary. Because ``cephadm``
leverages standard container runtimes (Podman or Docker), the Ceph container
will run smoothly on any supported container host OS (such as Ubuntu 24.04 or
CentOS 9), completely isolated from the host's native package manager.


Host Distribution Upgrades (Horizontal Paths)
=============================================

When managing the lifecycle of your hardware, you will eventually need to
upgrade the underlying host operating system to avoid hitting an End-of-Life
(EOL) situation where packages cannot be upgraded. It is highly recommended to
plan these horizontal upgrades when the same version of Ceph packages exists
for both the old and new host operating system.

Attempting to upgrade the host OS and the Ceph version simultaneously greatly
increases the risk of downtime and complicates troubleshooting. Ensure that
your current Ceph release supports both the old and new host operating systems
before beginning a horizontal node-by-node OS upgrade.

Anticipated Horizontal OS Upgrade Paths
---------------------------------------

+--------------------------------------+--------------------------------------+---------------------------+
| Current OS (EOL)                     | Target OS (EOL)                      | Ideal Ceph Release Window |
+======================================+======================================+===========================+
| CentOS 9 (`May 2027 <CentOS_>`_)     | Rocky 10 (`May 2035 <Rocky_>`_)      | Tentacle, Umbrella        |
+--------------------------------------+--------------------------------------+---------------------------+
| Ubuntu 22.04 (`Jun 2027 <Ubuntu_>`_) | Ubuntu 24.04 (`Jun 2029 <Ubuntu_>`_) | Tentacle, Umbrella        |
+--------------------------------------+--------------------------------------+---------------------------+
| Ubuntu 24.04 (`Jun 2029 <Ubuntu_>`_) | Ubuntu 26.04 (`May 2031 <Ubuntu_>`_) | Vampire, W                |
+--------------------------------------+--------------------------------------+---------------------------+
| Ubuntu 26.04 (`May 2031 <Ubuntu_>`_) | Ubuntu 28.04 (Jun 2033*)             | X, Y                      |
+--------------------------------------+--------------------------------------+---------------------------+
| Rocky 10 (`May 2035 <Rocky_>`_)      | Rocky 11 (May 2038*)                 | X, Y                      |
+--------------------------------------+--------------------------------------+---------------------------+


.. _CentOS: https://www.centos.org/cl-vs-cs/
.. _Debian_b: https://www.debian.org/releases/bookworm/
.. _Debian_t: https://www.debian.org/releases/trixie/
.. _Rocky: https://github.com/rocky-linux/wiki.rockylinux.org/blob/main/docs/rocky/version.md
.. _Ubuntu: https://ubuntu.com/about/release-cycle
