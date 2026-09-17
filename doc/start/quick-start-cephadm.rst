.. _quick-start-cephadm:

======================================
 Deploying a Single-Host Test Cluster
======================================

.. meta::
   :description: Deploy a Ceph cluster on one machine with cephadm, for learning and testing.
   :ceph-page-type: procedure
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: cephadm

This procedure creates a working Ceph cluster on one machine. Use it to learn
Ceph or to test a change. A single-host cluster is not suitable for production
because the loss of the host means the loss of the data. To deploy a production
cluster, follow :ref:`cephadm_deploying_new_cluster`.

:Applies to: Squid, Tentacle
:Last reviewed: September 2026

Prerequisites
=============

- One Linux host, physical or virtual, on which you have root access.
- The software listed in :ref:`cephadm-host-requirements`: Python 3, systemd,
  Podman or Docker, time synchronization, and LVM2.
- ``ssh`` installed and running on the host.
- At least two unused storage devices of 5 GB or more on the host. A device is
  unused if it has no partitions, no LVM state, and no file system, and is not
  mounted.
- The IP address of the host.

.. warning:: Ceph erases every device that you give to it. Do not use a device
   that holds data you want to keep.

Procedure
=========

#. Install ``cephadm`` by following :ref:`get-cephadm`, then confirm that it
   runs:

   .. prompt:: bash #

      cephadm version

   The command prints the Ceph version that ``cephadm`` will deploy.

#. Bootstrap the cluster. Replace ``<mon-ip>`` with the IP address of the
   host:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip <mon-ip> --single-host-defaults

   The ``--single-host-defaults`` flag lets Ceph keep all copies of an object
   on one host, which a cluster does not allow by default. The command takes a
   few minutes. It ends by printing the address and the initial password of
   the Ceph Dashboard, followed by this line::

      Bootstrap complete.

#. Open a shell that has the Ceph commands available:

   .. prompt:: bash #

      cephadm shell

   Run the remaining commands in this shell.

#. List the storage devices that Ceph can see:

   .. prompt:: bash #

      ceph orch device ls

   Each device that Ceph can use shows ``Yes`` in the ``Available`` column.
   Note the hostname and the path of two available devices.

#. Create an :term:`OSD` on the first device. Replace ``<host>`` and ``<device-path>``
   with the values from the previous step, for example ``host1701:/dev/sdx``:

   .. prompt:: bash #

      ceph orch daemon add osd <host>:<device-path>

   The command reports the OSD that it created, for example::

      Created osd(s) 0 on host 'host1701'

#. Run the same command again for the second device.

Verification
============

Check the state of the cluster:

.. prompt:: bash #

   ceph status

The cluster is ready when the output shows ``health: HEALTH_OK``, one Monitor
in quorum, an active Manager, and two OSDs that are ``up`` and ``in``. It can
take a minute or two after the last OSD is created for the cluster to reach
``HEALTH_OK``.

Troubleshooting
===============

- **The bootstrap command stops with an error.** Check that the host meets
  the requirements in `Prerequisites`_, that ``ssh`` is running, and that
  ``<mon-ip>`` is an address of this host. See :doc:`/cephadm/troubleshooting`.
- **A device shows "No" in the "Available" column.** The device has
  partitions, LVM state, or a file system, or it is smaller than 5 GB. The
  conditions are listed in :ref:`cephadm-deploy-osds`.
- **The cluster stays in "HEALTH_WARN" with undersized or degraded placement
  groups.** The cluster has fewer than two OSDs, or it was bootstrapped without
  ``--single-host-defaults``. See :ref:`one-node-cluster`.

Next Steps
==========

- :ref:`Create and mount a block device <quick-rbd>` on the new cluster.
- :ref:`Deploy a production cluster <cephadm_deploying_new_cluster>` across
  several hosts.

Additional Resources
====================

- :ref:`ceph-cluster-components`
- :ref:`hardware-recommendations`
- :ref:`os-recommendations`
