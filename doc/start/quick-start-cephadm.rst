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
- The software listed in the :ref:`cephadm host requirements
  <cephadm-host-requirements>`: Python 3, systemd,
  Podman or Docker, time synchronization, and LVM2.
- ``ssh`` installed and running on the host.
- At least two unused storage devices on the host, each 6 GiB or larger (Ceph
  rejects devices under 5 GiB, and a disk labeled "5 GB" can fall short).
  Unused means no partition table, no LVM state, no file system, no label from
  an earlier OSD, and not mounted.
- The IP address of the host.

.. warning:: Ceph erases every device that you give to it. Do not use a device
   that holds data you want to keep.

Procedure
=========

#. Install :term:`cephadm` by following :ref:`get-cephadm`.

#. Confirm that ``cephadm`` runs:

   .. prompt:: bash #

      cephadm version

   The command prints the version of ``cephadm``.

#. Bootstrap the cluster, replacing ``<mon-ip>`` with the IP address of the
   host:

   .. prompt:: bash #

      cephadm bootstrap --mon-ip <mon-ip> --single-host-defaults

   ``--single-host-defaults`` lets the cluster keep its copies on one host.
   The command takes a few minutes. It prints the address and the initial
   password of the Ceph Dashboard, then a few hints, and ends with this
   line::

      Bootstrap complete.

#. Open a shell that has the Ceph commands available:

   .. prompt:: bash #

      cephadm shell

   Run the remaining commands in this shell. The prompt changes to
   ``[ceph: root@<host> /]#``.

#. List the storage devices that Ceph can see:

   .. prompt:: bash #

      ceph orch device ls

   Each device that Ceph can use shows ``Yes`` in the ``Available`` column.
   Note the hostname and the path of two available devices.

#. Create an :term:`OSD` on the first device, replacing ``<host>`` and
   ``<device-path>`` with values from the previous step, for example
   ``host1701:/dev/sdx``:

   .. prompt:: bash #

      ceph orch daemon add osd <host>:<device-path>

   The command reports the OSD that it created, for example::

      Created osd(s) 0 on host 'host1701'

#. Run the same command again for the second device. It reports ``Created
   osd(s) 1``.

Verification
============

- Check the state of the cluster:

  .. prompt:: bash #

     ceph status

  The cluster is ready when the output shows ``health: HEALTH_OK`` (no
  warnings), one Monitor in :term:`quorum<Quorum>`, an active Manager, and two
  OSDs that are ``up`` and ``in``. It can take a minute or two after the last
  OSD is created for the cluster to reach ``HEALTH_OK``.

Troubleshooting
===============

- **The bootstrap command stops with an error.** Check that the host meets
  the requirements in `Prerequisites`_, that ``ssh`` is running, and that
  ``<mon-ip>`` is an address of this host. See :doc:`cephadm troubleshooting
  </cephadm/troubleshooting>`.
- **A device shows "No" in the "Available" column.** The "Reject Reasons"
  column of the same output says why: usually a partition table, LVM state, a
  file system, or a size under 5 GiB. The conditions are listed in
  :ref:`cephadm-deploy-osds`.
- **The cluster stays in "HEALTH_WARN" with undersized or degraded placement
  groups.** Ceph cannot store the required number of copies. Either the
  cluster has fewer than two OSDs, or it was bootstrapped without
  ``--single-host-defaults``. See :ref:`one-node-cluster`.
- **"cephadm: command not found" after installation.** If you downloaded
  the ``cephadm`` binary, run it as ``./cephadm`` from the directory that
  holds it, or install it as a package as described in :ref:`get-cephadm`.
- **"ceph orch device ls" prints nothing.** The inventory can take a minute
  to fill after bootstrap. Run ``ceph orch device ls --refresh`` and try
  again.

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
