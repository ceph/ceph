==========================================
Installing and Configuring NVMe-oF Targets
==========================================

Prerequisites
=============

-  A working Ceph Tentacle or later storage cluster, deployed with ``cephadm``

-  NVMe-oF gateways, which can either be colocated with OSD nodes or on dedicated nodes

-  Separate network subnets for NVME-oF front-end traffic and Ceph back-end traffic

Explanation
===========

The Ceph NVMe-oF gateway is both an NVMe-oF target and a Ceph client. Think of
it as a "translator" between Ceph's RBD interface and the NVME-oF protocol. The
Ceph NVMe-oF gateway can run on a standalone node or be colocated with other
daemons, for example on an OSD node. When colocating the Ceph NVMe-oF gateway
with other daemons, ensure that sufficient CPU and memory are available.
The steps below explain how to install and configure the Ceph NVMe/TCP gateway
for basic operation.


Installation
============

Complete the following steps to install the Ceph NVME-oF gateway:

#. Create a pool in which the gateways configuration can be managed:

   .. prompt:: bash #

      ceph osd pool create NVME-OF_POOL_NAME

#. Enable RBD on the NVMe-oF pool:

   .. prompt:: bash #
   
      rbd pool init NVME-OF_POOL_NAME

#. Deploy the NVMe-oF gateway daemons on a specific set of nodes, and with a unique group name:

   .. prompt:: bash #
   
      ceph orch apply nvmeof NVME-OF_POOL_NAME NVME_OF_GROUP_NAME --placement="host01, host02"

Configuration
=============

Use the ``ceph nvmeof`` command to configure NVMe-oF gateways.

.. note:: As an alternative to ``ceph nvmeof``, you can use the
   ``nvmeof-cli`` container image. Note that the container CLI
   uses different option names (for example, ``--rbd-image``
   instead of ``--rbd-image-name``). Pull it with
   ``podman pull quay.io/ceph/nvmeof-cli:latest`` and run commands
   in this form::

      podman run -it --rm quay.io/ceph/nvmeof-cli:latest \
        --server-address GATEWAY_IP --server-port 5500 <command>

#. Create an NVMe subsystem:

   .. prompt:: bash #

      ceph nvmeof subsystem add --nqn SUBSYSTEM_NQN

   The subsystem NQN is a user-defined string, for example
   ``nqn.2016-06.io.spdk:cnode1``.

#. Define the IP port on the gateway that will process the NVMe/TCP
   commands and I/O:

    a. On the install node, get the NVMe-oF gateway name:

       .. prompt:: bash #

          ceph orch ps | grep nvme

    b. Define the IP port for the gateway:

       .. prompt:: bash #

          ceph nvmeof listener add --nqn SUBSYSTEM_NQN --host-name HOST_NAME --traddr GATEWAY_IP --trsvcid 4420

#. Get the host NQN (NVMe Qualified Name) for each host:

   .. prompt:: bash #

      cat /etc/nvme/hostnqn

   .. prompt:: bash #

      esxcli nvme info get

#. Allow the initiator host to connect to the newly-created NVMe
   subsystem:

   .. prompt:: bash #

      ceph nvmeof host add --nqn SUBSYSTEM_NQN --host-nqn HOST_NQN

   To allow any host to connect, use ``*`` as the host NQN:

   .. prompt:: bash #

      ceph nvmeof host add --nqn SUBSYSTEM_NQN --host-nqn "*"

#. List all subsystems configured in the gateway:

   .. prompt:: bash #

      ceph nvmeof subsystem list

#. Create a new NVMe namespace:

   .. prompt:: bash #

      ceph nvmeof namespace add --nqn SUBSYSTEM_NQN --rbd-pool POOL_NAME --rbd-image-name IMAGE_NAME

#. List all namespaces in the subsystem:

   .. prompt:: bash #

      ceph nvmeof namespace list --nqn SUBSYSTEM_NQN

