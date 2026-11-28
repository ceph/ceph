==========================================
Installing and Configuring NVMe-oF Targets
==========================================

Prerequisites
=============

-  A working Ceph Tentacle or later storage cluster, deployed with ``cephadm``

-  NVMe-oF gateways, which can either be colocated with OSD nodes or on dedicated nodes

-  Separate network subnets for NVMe-oF front-end traffic and Ceph back-end traffic

Explanation
===========

The Ceph NVMe-oF gateway is both an NVMe-oF target and a Ceph client. Think of
it as a "translator" between Ceph's RBD interface and the NVMe-oF protocol. The
Ceph NVMe-oF gateway can run on a standalone node or be colocated with other
daemons, for example on an OSD node. When colocating the Ceph NVMe-oF gateway
with other daemons, ensure that sufficient CPU and memory are available.
The steps below explain how to install and configure the Ceph NVMe/TCP gateway
for basic operation.


Installation
============

If the cluster runs Tentacle 20.2.3 or later but was first deployed with an
earlier release, enable the ``nvmeof`` Manager module first:

.. prompt:: bash #

   ceph mgr module enable nvmeof

Deploy the NVMe-oF gateway daemons on a specific set of nodes, and with a
unique group name:

.. prompt:: bash #

   ceph orch apply nvmeof --group NVME_OF_GROUP_NAME --placement="host01 host02"

An optional ``--pool NVME-OF_POOL_NAME`` may be supplied to use a specific
RADOS pool for gateway group metadata. Beginning with Tentacle 20.2.3, a
``.nvmeof`` pool will be auto-created if necessary. This pool must be
dedicated; it cannot also host RBD images.

A pool named with ``--pool`` must be created before you deploy the gateways.
Before Tentacle 20.2.3, ``--pool`` is required. Without ``--pool``, the gateway
group uses the ``.nvmeof`` pool: give ``.nvmeof`` as ``NVME-OF_POOL_NAME`` in
the ``ceph nvme-gw`` commands below.

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

      ceph nvmeof subsystem add --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME

   The subsystem NQN is a user-defined string in NQN format, for example
   ``nqn.2016-06.io.spdk:cnode1``. The gateway appends a dot and the group
   name to the NQN, so that subsystems in different gateway groups do not
   share a name: in group ``group1``, the NQN becomes
   ``nqn.2016-06.io.spdk:cnode1.group1``. Any underscore in the group name
   becomes a hyphen in the NQN. Use that NQN in the commands below. On
   Tentacle 20.2.0, the group name is not appended.

#. Define the IP port on each gateway that will process the NVMe/TCP
   commands and I/O:

    a. On the install node, get the host name of each NVMe-oF gateway:

       .. prompt:: bash #

          ceph orch ps | grep nvme

    b. Define the IP port for each gateway, using its host name and IP
       address:

       .. prompt:: bash #

          ceph nvmeof listener add --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME --host-name HOST_NAME --traddr GATEWAY_IP --trsvcid 4420

   From Tentacle 20.2.2, the gateways can instead create the listeners
   themselves (auto-listeners). Give the subsystem a network mask in CIDR
   notation, for example ``192.0.2.0/24``:

   .. prompt:: bash #

      ceph nvmeof subsystem add_network --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME --network-mask NETWORK_MASK

   Each gateway in the group then adds a listener on port 4420 for each
   of its IP addresses in that network. The mask can also be given when
   the subsystem is created, by adding ``--network-mask NETWORK_MASK`` to
   ``ceph nvmeof subsystem add``. Use either ``listener add`` or a network
   mask for a subsystem, not both: on Umbrella and later, the gateway
   refuses to mix them.

#. Get the host NQN (NVMe Qualified Name) for each host:

   .. prompt:: bash #

      cat /etc/nvme/hostnqn

   .. prompt:: bash #

      esxcli nvme info get

#. Allow the initiator host to connect to the newly-created NVMe
   subsystem:

   .. prompt:: bash #

      ceph nvmeof host add --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME --host-nqn HOST_NQN

   To allow any host to connect, use ``*`` as the host NQN:

   .. warning:: With ``*``, any host that can reach a listener of the
      subsystem can connect to it and read and write its namespaces.
      The monitoring stack raises the ``NVMeoFGatewayOpenSecurity`` alert
      for such a subsystem. Use ``*`` only on an isolated test network.

   .. prompt:: bash #

      ceph nvmeof host add --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME --host-nqn "*"

#. List all subsystems configured in the gateway:

   .. prompt:: bash #

      ceph nvmeof subsystem list --gw-group NVME_OF_GROUP_NAME

#. Create a new NVMe namespace:

   .. prompt:: bash #

      ceph nvmeof namespace add --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME --rbd-pool POOL_NAME --rbd-image-name IMAGE_NAME

   ``POOL_NAME`` is a pool initialized for RBD with ``rbd pool init``, not
   the gateway group metadata pool, and ``IMAGE_NAME`` is an existing RBD
   image in that pool.

#. List all namespaces in the subsystem:

   .. prompt:: bash #

      ceph nvmeof namespace list --nqn SUBSYSTEM_NQN --gw-group NVME_OF_GROUP_NAME


.. _nvmeof-monitor-commands:

Monitor Commands for Gateway Groups
===================================

Besides the ``ceph nvmeof`` gateway CLI described above, Ceph provides
a separate ``ceph nvme-gw`` command family that is served by the
Monitors. The two operate at different layers: ``ceph nvmeof``
configures subsystems, listeners, hosts, and namespaces on the gateway
daemons, while ``ceph nvme-gw`` manages the gateway group state that
the Monitors track for high availability: group membership, ANA group
assignments, administrative state, and failover behavior.

Inspecting gateway groups
-------------------------

To display the state of the gateways in a group, run a command of the
following form:

.. prompt:: bash #

   ceph nvme-gw show NVME-OF_POOL_NAME NVME_OF_GROUP_NAME

The JSON output includes the number of gateways in the group, the ANA
group list, the total namespace count, and one entry per gateway
showing its ANA group, location, administrative state (``ENABLED`` or
``DISABLED``), availability (for example ``AVAILABLE``, ``CREATED``,
or ``DELETING``), listener count, and per-ANA-group state.

On Umbrella and later, to dump every gateway group in the cluster, run:

.. prompt:: bash #

   ceph nvme-gw show-all

To list all listeners in a group, including the listeners that
gateways create automatically rather than only the ones defined
through the ``ceph nvmeof`` CLI, run a command of the following form:

.. prompt:: bash #

   ceph nvme-gw listeners NVME-OF_POOL_NAME NVME_OF_GROUP_NAME

The output groups listeners by subsystem NQN and shows the address
family, address, service ID, and owning gateway of each listener.
Only gateways that are currently available contribute listeners to
the output. The ``listeners`` command was added in Tentacle 20.2.1.

Commands managed by the orchestrator
------------------------------------

The following commands change gateway group state and are normally
issued by cephadm, not by operators:

* ``ceph nvme-gw create`` and ``ceph nvme-gw delete`` register and
  remove a gateway in a group. cephadm runs them when NVMe-oF gateway
  daemons are deployed and removed. Do not run them manually against
  a cephadm-managed cluster: deleting a gateway triggers an immediate
  failover of its ANA groups.
* ``ceph nvme-gw enable`` and ``ceph nvme-gw disable`` set a single
  gateway's administrative state. They were added in Tentacle 20.2.2.
  From Tentacle 20.2.5, cephadm uses them to quiesce gateways when a
  host enters maintenance and to bring them back afterwards. Disabling
  an available gateway takes it out of service and temporarily
  suppresses failovers while its ANA groups move, so disabling all the
  gateways in a group removes NVMe-oF access to its namespaces.

Locations and disaster recovery
-------------------------------

Gateways can be assigned a *location*, an availability domain label
such as a site or rack, which the Monitors use for disaster-recovery
handling. These commands were added in Tentacle 20.2.2. They require
the ``nvmeof_beacon_diff`` Monitor feature and fail cleanly when it is
not available:

.. prompt:: bash #

   ceph nvme-gw set-location GATEWAY_ID NVME-OF_POOL_NAME NVME_OF_GROUP_NAME LOCATION

When all the gateways in a location have become unavailable, for
example after the loss of a site, their ANA groups fail over to
gateways in the other locations. To keep the ANA groups there while
the location is repaired, declare the location to be in a disaster
state. Its gateways then take no ANA group back, and no ANA group over
from another location, even when some of them return:

.. prompt:: bash #

   ceph nvme-gw disaster-set NVME-OF_POOL_NAME NVME_OF_GROUP_NAME LOCATION

The command is rejected while any gateway in the location is still
available, which prevents declaring a disaster during a transient
outage. Once the location has recovered, clear the disaster state to
permit failback:

.. prompt:: bash #

   ceph nvme-gw disaster-clear NVME-OF_POOL_NAME NVME_OF_GROUP_NAME LOCATION

.. warning:: ``disaster-clear`` moves live I/O paths back into the
   location. Use ``disaster-set`` and ``disaster-clear`` only as part
   of a planned disaster-recovery procedure.

Once all the Monitors run Tentacle 20.2.2 or later, they switch every
gateway group to differential beacons, in which gateways send only the
subsystems that changed since their previous beacon. To turn
differential beacons off, or on again with ``enable``, run:

.. prompt:: bash #

   ceph nvme-gw set beacon-diff disable

With differential beacons, the Monitors also check the order of the
beacons. A beacon out of order from an available gateway sets the
gateway to ``CREATED``, which takes its ANA groups off it, and holds
back failovers in its gateway group for 7 seconds. The setting does
not affect the location commands. Leave it at the default unless
directed otherwise.
