.. _hardware-networks:

================
 Network Sizing
================

.. meta::
   :description: Link speeds, replication times, bonding, VLANs, and BMC networking for a Ceph cluster.
   :ceph-page-type: reference
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

For the principles behind these recommendations, see
:ref:`Networks <hardware-recommendations-networks>`
on the Hardware Recommendations page. For how to configure the public and
cluster networks, see the :doc:`Network Configuration Reference
</rados/configuration/network-config-ref>`.

Link Speeds
===========

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Path
     - Recommendation
   * - Between Ceph hosts, between clients and the cluster, and between the
       cluster and compute stacks (OpenStack, CloudStack, and so on) over
       VLANs
     - At least 10 Gb/s. Clusters with substantial workload do well with
       25 Gb/s, and 25, 50, and 100 Gb/s links are common in production.
       Dense nodes often warrant 100 Gb/s links.
   * - Top-of-rack (TOR) switch uplinks to core or spine switches or routers
     - Fast and redundant, often at least 40 Gb/s.
   * - BMC (baseboard management controller) out-of-band management
     - 1 Gb/s is sufficient. BMCs rarely offer more than 1 Gb/s, so
       dedicated, inexpensive 1 Gb/s switches for BMC traffic waste fewer
       expensive ports on the host switches.

Notes:

- Bond links active/active across separate network switches, both for
  throughput and for tolerance of network failures and maintenance. Check
  that the bonding hash policy distributes traffic across links.
- 802.1Q VLANs need VLAN-capable NICs and switches.
- Put BMC traffic (iDRAC, CIMC, iLO; IPMI or Redfish) on its own
  out-of-band network. Hypervisor SSH, VM image uploads, and OS installs can
  load a shared network heavily.

Recovery Time by Link Speed
===========================

How long replication of a given amount of data takes, by link speed. Until
it finishes, a second failure can make data unavailable or lost.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Data to replicate
     - 1 Gb/s network
     - 10 Gb/s network
   * - 1 TiB
     - 3 hours
     - 20 minutes
   * - 10 TiB
     - 30 hours
     - 3 hours

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`minimum-hardware`
- :ref:`CPU and Memory Sizing <hardware-cpu-memory>`
- :ref:`Storage Devices <hardware-storage-devices>`
- :doc:`Network Configuration Reference </rados/configuration/network-config-ref>`
