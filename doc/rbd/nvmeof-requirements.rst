============================
NVME-oF Gateway Requirements
============================

We recommend that you provision at least two NVMe/TCP gateways on different
nodes to implement a highly-available Ceph NVMe/TCP solution.

We recommend at a minimum a single 10Gb Ethernet link in the Ceph public
network for the gateway. For hardware recommendations, see
:ref:`hardware-recommendations` .

.. note:: On the NVMe-oF gateway, the memory footprint is a function of the
   number of mapped RBD images and can grow to be large. Plan memory
   requirements accordingly based on the number RBD images to be mapped.
