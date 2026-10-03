.. _install-overview:

=================
 Installing Ceph
=================

.. meta::
   :description: Choose how to install and deploy a Ceph cluster, with cephadm, Rook, another deployment tool, or by hand, and find the pages to plan, set up, and upgrade it.
   :ceph-page-type: assembly

Choose a deployment method, plan the hardware and operating system, then set
up the cluster.

.. toctree::
   :maxdepth: 1
   :hidden:

   index_manual
   windows-install
   windows-basic-config
   windows-troubleshooting

.. _recommended-methods:

.. rubric:: Choose a Deployment Method

.. list-table::
   :header-rows: 1
   :widths: 20 45 35

   * - Method
     - When to use it
     - Where to go
   * - :term:`cephadm` (recommended)
     - Installing and managing a cluster on Linux :term:`hosts <Host>` that
       have systemd, Python 3, and Podman or Docker. Cephadm supports only
       Octopus and newer :term:`releases <Ceph Release>`.
     - :ref:`cephadm_deploying_new_cluster`
   * - Rook (recommended for Kubernetes)
     - Running Ceph in Kubernetes, with storage resources and provisioning
       managed through Kubernetes APIs, or connecting a Kubernetes cluster
       to an existing (external) Ceph cluster
     - `rook.io <https://rook.io/>`__
   * - ceph-salt
     - Installing Ceph with Salt and cephadm
     - `github.com/ceph/ceph-salt <https://github.com/ceph/ceph-salt>`__
   * - Juju
     - Installing Ceph with Juju
     - `charmhub.io/ceph-mon <https://charmhub.io/ceph-mon>`__
   * - Puppet
     - Installing Ceph with Puppet
     - `github.com/openstack/puppet-ceph
       <https://github.com/openstack/puppet-ceph>`__
   * - OpenNebula HCI clusters
     - Deploying Ceph as the storage of OpenNebula 6.10 HCI clusters, on
       bare-metal servers in AWS or on premises (a legacy component that
       OpenNebula 7 no longer includes)
     - `docs.opennebula.io (6.10)
       <https://docs.opennebula.io/6.10/provision_clusters/hci_clusters/overview.html>`__
   * - Manual installation
     - Developing your own deployment scripts, or installing on hosts that
       cannot run cephadm, such as FreeBSD hosts
     - :ref:`install-manual`

Cephadm is fully integrated with the orchestrator API, so the CLI and
:term:`dashboard <Dashboard>` features that manage cluster deployment are
fully supported. Rook supports the orchestrator API but implements only some
of its commands: for example, ``ceph orch`` cannot create or remove OSDs in
a Rook cluster. Use the Rook operator for that.

.. rubric:: Plan

- :ref:`hardware-recommendations`: how to choose CPUs, memory, storage
  devices, and networks for a cluster.
- :ref:`os-recommendations`: the distributions, kernels, and container hosts
  that each Ceph release is built and tested on.
- :doc:`/cephadm/compatibility`: the Podman versions that work with each Ceph
  release, and the cephadm features that are still under development.

.. rubric:: Set Up

- :ref:`packages`: add the Ceph package repository for APT or DNF, use
  development builds, or download packages for hosts without internet
  access.
- :ref:`install-windows`: the :term:`Ceph client <Ceph Client>` for Windows,
  with which a host maps :term:`RBD` images as local disks and mounts
  :term:`CephFS` file systems. If something fails, see
  :ref:`install-windows-troubleshooting`.
- :ref:`install-windows-basic-config`: the configuration file and
  :term:`keyring <Keyring>` that the Windows client needs to connect to a
  cluster.

.. rubric:: Look Up

- :ref:`containers`: where Ceph publishes its container images, and what
  each tag points to.
- :ref:`install-mirrors`: mirrors of ``download.ceph.com`` for packages and
  tarballs, and how to run your own.
- :ref:`ceph-releases-index`: the releases that are maintained now, and the
  release notes of every release.

.. rubric:: Next Steps

- :doc:`/cephadm/upgrade`: upgrade a cluster that cephadm manages to a new
  release.
- :ref:`rados-operations`: check the cluster's health, monitor it, manage
  :term:`pools <Pools>` and data placement, and add or replace hardware.

.. rubric:: Additional Resources

- :ref:`Cephadm <cephadm>`: managing hosts and services, and the other tasks
  that cephadm performs after deployment.
- :doc:`Troubleshooting cephadm </cephadm/troubleshooting>`: what to check
  when a cephadm command fails or a service stops running.
- :ref:`start-here`: what Ceph is, and a single-host test cluster to learn
  on.
