.. _start-here:

============
 Start Here
============

.. meta::
   :description: The starting point for readers who are new to Ceph: what to read, what to try, and where to look things up.
   :ceph-page-type: assembly

Read the Learn pages, then deploy a test cluster under Set Up. The Look Up
pages hold the numbers you need while you plan.

.. toctree::
   :maxdepth: 1
   :hidden:

   Beginner's Guide <beginners-guide>
   ceph-cluster-components
   quick-start-cephadm
   quick-rbd
   Hardware Recommendations <hardware-recommendations>
   minimum-hardware
   hardware-cpu-memory
   hardware-storage-devices
   hardware-networks
   OS Recommendations <os-recommendations>
   get-involved
   documenting-ceph

Learn
=====

Read these before you install anything.

- :doc:`Beginner's Guide <beginners-guide>`: what Ceph is, in plain language.
- :ref:`The Parts of a Ceph Cluster <ceph-cluster-components>`: the daemons
  in a cluster, what each one does, and how many you need.
- :ref:`Architecture <architecture>`: how Ceph stores data, places it with
  :term:`CRUSH`, and recovers from failures.
- :ref:`Hardware Recommendations <hardware-recommendations>`: how to choose
  CPUs, memory, storage devices, and networks for a cluster.

Set Up
======

- :ref:`Deploying a Single-Host Test Cluster <quick-start-cephadm>`: a working
  cluster on one machine, for learning and testing.
- :ref:`Creating and Mounting a Block Device <quick-rbd>`: a first block
  device on the new cluster.

Look Up
=======

- :ref:`Minimum Hardware per Daemon <minimum-hardware>`: the smallest
  configuration for each daemon.
- :ref:`CPU and Memory Sizing <hardware-cpu-memory>`: cores and RAM for each
  daemon, and the settings that control memory use.
- :ref:`Storage Devices <hardware-storage-devices>`: drive layout, HDD and
  SSD selection, controllers, and write caches.
- :ref:`Network Sizing <hardware-networks>`: link speeds, replication times,
  bonding, and management networks.
- :ref:`OS Recommendations <os-recommendations>`: the platforms that each Ceph
  release is built and tested on.
- :ref:`Ceph Releases <ceph-releases-general>`: the release cycle, and which
  releases are currently maintained.
- :doc:`Glossary </glossary>`: definitions of the terms used throughout this
  documentation.

Next Steps
==========

- Deploy a production cluster. See :ref:`cephadm_deploying_new_cluster`, or
  :ref:`install-overview` for the other installation methods, including Rook
  for Kubernetes.
- Set up the kind of storage that your applications need:
  :ref:`Ceph Block Device <ceph_block_device>`,
  :ref:`Ceph File System <ceph-file-system>`,
  :ref:`Ceph Object Gateway <object-gateway>`, or
  :ref:`Ceph CSI <ceph-csi>` for Kubernetes.

Additional Resources
====================

- :ref:`Ceph Community Channels <get-involved>`: mailing lists, chat channels, and
  community meetings.
- :ref:`Troubleshooting <rados_troubleshooting>`: what to check when a
  cluster is unhealthy.
- :ref:`Documenting Ceph <documenting_ceph>`: how to fix or improve this
  documentation.
