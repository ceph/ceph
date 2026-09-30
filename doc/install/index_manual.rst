.. _install-manual:

==========================
 Installing Ceph Manually
==========================

.. meta::
   :description: The manual path to a Ceph cluster without cephadm: get the software, install it on each node, deploy the cluster by hand, and upgrade it.
   :ceph-page-type: assembly

Get Ceph, install it on each :term:`Ceph Node`, and deploy a cluster by hand,
without :term:`cephadm`. The manual steps serve mainly as an example for
people who develop deployment scripts with Chef, Juju, Puppet, and similar
tools. For the recommended method, see :ref:`cephadm_deploying_new_cluster`.

.. toctree::
   :maxdepth: 1
   :hidden:

   get-packages
   get-tarballs
   clone-source
   build-ceph
   mirrors
   containers
   install-storage-cluster
   install-vm-cloud
   manual-deployment
   manual-freebsd-deployment

.. _get-software:

.. rubric:: Get the Software

- :ref:`packages`: add the Ceph repository for APT or DNF, the easiest and
  most common way to get Ceph, or download the pre-compiled packages from it.
- :doc:`get-tarballs`: the source code of a :term:`release <Ceph Release>`,
  to build Ceph yourself.
- :ref:`install-clone-source`: the source code of a branch, from GitHub or
  with Git.
- :doc:`build-ceph`: compile Ceph from source, or build its packages.
- :ref:`install-mirrors`: mirrors of ``download.ceph.com`` that serve the
  same packages and tarballs.
- :ref:`containers`: the container image that holds every Ceph daemon, and
  what each tag points to.

.. _install-software:

.. rubric:: Install the Software

- :ref:`install_storage_cluster`: install the packages on each node with APT
  or DNF, or install a build.
- :doc:`install-vm-cloud`: QEMU and ``libvirt``, for virtual machines and
  :term:`cloud platforms <Cloud Platforms>` that use
  :term:`Ceph Block Devices <Ceph Block Device>`.

.. _deploy-a-cluster-manually:

.. rubric:: Deploy a Cluster by Hand

- :ref:`manual-deployment`: create the first :term:`Monitor <Ceph Monitor>`,
  then add a :term:`Manager <Ceph Manager>`, :term:`OSDs <Ceph OSD>`, a
  :term:`Metadata Server <Ceph Metadata Server>`, and a
  :term:`RADOS Gateway <RGW>`.
- :ref:`manual-freebsd-deployment`: the Monitor, OSD, and Metadata Server
  steps on FreeBSD, where cephadm is not available.

.. _upgrade-software:

.. rubric:: Upgrade

Read the release notes of the new version before you upgrade: they give the
required upgrade sequence and any release-specific steps.

- :ref:`ceph-releases-index`: the release notes of each version, with its
  new features, bug fixes, and performance and security improvements.
- :ref:`ceph-releases-general`: the release cycle, and the releases from
  which an online, rolling upgrade is supported and tested.
- :doc:`/cephadm/upgrade`: upgrade a cluster that cephadm manages.

.. rubric:: Next Steps

- :ref:`rados-operations`: check the cluster's health, monitor it, manage
  :term:`pools <Pools>` and data placement, and add or replace hardware.

.. rubric:: Additional Resources

- :ref:`install-overview`: every installation method, including cephadm and
  Rook.
- :ref:`cephadm-adoption`: check whether an existing cluster can be
  converted to cephadm management, and convert it.
- :ref:`os-recommendations`: the distributions and kernels that each Ceph
  release is built and tested on.
