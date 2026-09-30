.. _install_storage_cluster:

==========================
 Installing Ceph Packages
==========================

.. meta::
   :description: Install the Ceph daemon packages with APT or DNF after adding the Ceph repository, or install a Ceph build from source.
   :ceph-page-type: procedure

Install the Ceph packages with APT or DNF, or install a Ceph build.
:term:`Hosts <Host>` that :term:`cephadm` manages do not need these packages;
see :ref:`cephadm_deploying_new_cluster`. Other deployment tools, such as
Juju, install the packages themselves.

Prerequisites
=============

- For APT or DNF, the Ceph repository, added as described in
  :ref:`packages`.
- For a build, a Ceph build; see :ref:`build-ceph`.
- Root access, or a user with ``sudo``.

Procedure
=========

Installing with APT
-------------------

#. Install the packages:

   .. prompt:: bash $

      sudo apt-get install ceph ceph-mds

   APT asks you to confirm, then shows a ``Setting up`` line for each package.
   The ``ceph`` package depends on ``ceph-mon``, ``ceph-mgr``, and
   ``ceph-osd``, the :term:`Monitor <Ceph Monitor>`,
   :term:`Manager <Ceph Manager>`, and :term:`OSD <Ceph OSD>` daemons. It only
   recommends ``ceph-mds``, the :term:`Metadata Server <Ceph Metadata Server>`,
   so the command names ``ceph-mds`` too.

.. _installing-with-rpm:

Installing with DNF
-------------------

#. Check that each section of ``ceph.repo`` sets ``priority=2``:

   .. prompt:: bash $

      grep priority /etc/yum.repos.d/ceph.repo

   The command prints ``priority=2`` once for each section. If it prints
   nothing, the file came from ``cephadm add-repo`` or from the
   ``ceph-release`` package, which set no priority; add ``priority=2`` to each
   section, as shown in :ref:`get-packages-rhel`.

#. Install the packages:

   .. prompt:: bash $

      sudo dnf install ceph

   DNF asks you to confirm. If it asks to import key ``0x460F3994``, check
   that the fingerprint is ``08B7 3419 AC32 B4E9 66C1 A330 E84A C2C0 460F
   3994``. ``Complete!`` is the last line of the output. The ``ceph`` package
   requires ``ceph-mon``, ``ceph-mgr``, ``ceph-osd``, and ``ceph-mds``.

.. _install-storage-cluster-build:

Installing a Build
------------------

.. warning:: ``sudo ninja install`` writes into ``/usr/local``, the CMake
   default prefix, as root and replaces any files that are already there. Do
   not install a build on a host that has Ceph packages installed: the
   packages are under ``/usr``, so the host would have two versions of each
   command.

#. From the build directory, install the build:

   .. prompt:: bash $

      sudo ninja install

   The output lists each file that it installs; the executables go in
   ``/usr/local/bin``.

#. Create the directory for the
   :ref:`Ceph configuration file <configuring-ceph>`:

   .. prompt:: bash $

      sudo mkdir -p /etc/ceph

   The command prints nothing on success. Put the configuration file in
   ``/etc/ceph/ceph.conf``. Ceph also looks in ``~/.ceph/ceph.conf`` and in
   the current directory, but not in ``/usr/local/bin``. ``ls /etc/ceph``
   then lists ``ceph.conf``.

Verification
============

- Check the installed version:

  .. prompt:: bash $

     ceph --version

  The output starts with ``ceph version`` and the version number, for
  example ``20.2.4``, followed by the commit ID and the release name, for
  example ``tentacle``.

Troubleshooting
===============

- **"Unable to locate package ceph" or "No match for argument: ceph".** The
  Ceph repository is not configured. Add it as described in :ref:`packages`.
- **The installed release is older than the one you added.** The packages
  came from the distribution's own repository. Check the Ceph repository with
  the Verification section of :ref:`packages`, then install again.
- **"nothing provides luarocks" or "nothing provides
  libtcmalloc.so.4()(64bit)".** EPEL is not enabled. See step 2 of
  :ref:`get-packages-rhel`.
- **"nothing provides lua-devel" or "nothing provides
  liblttng-ust.so.1()(64bit)".** The CRB repository is not enabled. See
  steps 3 and 4 of :ref:`get-packages-rhel`.
- **"No match for argument: yum-plugin-priorities".** Older guides install
  this plugin. It does not exist on EL 9 or EL 10, and DNF reads the
  ``priority`` setting of a repository without it, so skip that step.
- **"No match for argument: python-argparse".** Older guides install
  ``snappy``, ``gdisk``, ``python-argparse``, and ``gperftools-libs`` before
  Ceph. DNF installs the dependencies with ``ceph`` (``gperftools-libs`` from
  EPEL), and ``python-argparse`` does not exist on EL 9 or EL 10, so skip that
  step.
- **"ninja: error: loading 'build.ninja': No such file or directory".** The
  command did not run in the build directory. Change to the ``build``
  directory of the source tree, then run ``sudo ninja install`` again.

Next Steps
==========

- :ref:`manual-deployment`: create a cluster on hosts that have the packages.

Additional Resources
====================

- :ref:`packages`
- :ref:`os-recommendations`
