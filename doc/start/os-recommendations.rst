.. _os-recommendations:

====================
 OS Recommendations
====================

.. meta::
   :description: The operating systems, kernels, and container hosts that each Ceph release is built and tested on.
   :ceph-page-type: reference
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

The Linux distributions, kernels, and container hosts that each Ceph release
is built and tested on.

.. _start-platforms:

Platforms
=========

Ceph runs on any Linux distribution with a supported kernel and
``systemd``; only the distributions below get packages and testing.

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
- **C**: Ceph provides packages only. No tests have been done on these releases.
- **D**: Client packages are available from an external site but are not maintained or tested by the core Ceph team.
- **Ae**: CentOS 9 Stream is expected to reach EOL before Umbrella does; after that no new Umbrella RPMs are built for it. Move to Rocky 10 or another supported distribution first.

.. note:: Dates marked with * and columns for future releases are anticipated, not final.

.. warning:: Starting with CentOS 10 Stream and onwards, CentOS will no longer be built for or tested on by the upstream Ceph project.

Container Hosts
---------------

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

- **H**: Ceph tests its container image with this distribution as the host. The image itself is built on Rocky 10; see below.

Container Base Image
====================

Since Umbrella the container image is built on Rocky Linux 10 (CentOS 9
Stream before that). The host does not need to run Rocky 10; any
distribution marked H above works.

.. note:: ARM architecture containers provide a limited set of daemons.
   Check that the daemons you need are available before you plan an ARM
   deployment.

Kernel Version for Clients
==========================

.. list-table::
   :header-rows: 1
   :widths: 12 38 50

   * - Client
     - Minimum kernel
     - Note
   * - RBD
     - 5.3, or Enterprise Linux 8.2 (4.19 long-term at the very least)
     - Older kernels need :ref:`CRUSH tunables <crush-map-tunables>` and
       image features disabled on the cluster.
   * - CephFS
     - See :ref:`cephfs_which_kernel_version`
     -
   * - Both
     - Use a "stable" or "long-term maintenance" series from kernel.org or
       your distribution.
     -

The Windows client is best effort, with no full-time maintainer.

For the I/O scheduler setting per drive type, see
:ref:`Storage Devices <hardware-storage-devices>`.

Deployment Method
=================

Deploy Ceph as containers with :ref:`cephadm <cephadm>`. Package installs
(``.deb``, ``.rpm``) are still supported, but containers let you upgrade
Ceph independently of the host's packages.

Upgrading the Host OS
=====================

Upgrade the host OS one node at a time, on a Ceph release that supports both
the old and the new OS. Do not upgrade the OS and Ceph at the same time.

Upgrade Paths
-------------

+--------------------------------------+--------------------------------------+---------------------------+
| Current OS (EOL)                     | Target OS (EOL)                      | Do it on                  |
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

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`ceph-releases-general`
- :ref:`cephadm_deploying_new_cluster`

.. _CentOS: https://www.centos.org/cl-vs-cs/
.. _Debian_b: https://www.debian.org/releases/bookworm/
.. _Debian_t: https://www.debian.org/releases/trixie/
.. _Rocky: https://github.com/rocky-linux/wiki.rockylinux.org/blob/main/docs/rocky/version.md
.. _Ubuntu: https://wiki.ubuntu.com/Releases
