.. _adding-and-removing-monitors:

==========================
 Adding/Removing Monitors
==========================

.. note:: Most procedures on this page are for clusters that are not
   managed by cephadm. In a cephadm-managed cluster, Monitors are
   added and removed through the orchestrator: see
   :ref:`deploy_additional_monitors`. Using the add and remove procedures
   below on a cephadm-managed cluster will conflict with the orchestrator's
   own management of the daemons. Two sections also serve cephadm-managed
   clusters: `Using cephadm to change the public network`_, and the monitor
   map steps of :ref:`rados-mon-remove-from-unhealthy`, which
   :ref:`cephadm-restore-quorum` uses.

It is possible to add Monitors to a running cluster as long as redundancy is
maintained. To bootstrap a Monitor, see :ref:`manual-deployment`
or :ref:`dev-monitor-bootstrap`.

.. _adding-monitors:

Adding Monitors
===============

Ceph Monitors serve as the single source of truth for the cluster map. It is
possible to run a cluster with only one Monitor, but for a production cluster
it is recommended to have at least three Monitors provisioned and in quorum.
Ceph Monitors use a variation of the `Paxos`_ algorithm to maintain consensus
about maps and about other critical information across the cluster. Due to the
nature of Paxos, Ceph is able to maintain quorum (and thus establish
consensus) only if a majority of the Monitors are ``active``.

It is best to run an odd number of Monitors. This is because a cluster that is
running an odd number of Monitors is more resilient than a cluster running an
even number. For example, in a two-Monitor deployment, no failures can be
tolerated if quorum is to be maintained; in a three-Monitor deployment, one
failure can be tolerated; in a four-Monitor deployment, one failure can be
tolerated; and in a five-Monitor deployment, two failures can be tolerated. The
majority rule, not the odd number, prevents what is called the *split brain*
phenomenon. In short, Ceph is able to operate only if a majority of Monitors
are ``active`` and able to communicate with each other. For example: there must
be a single Monitor, two out of two Monitors, two out of three Monitors, three
out of five Monitors, or the like.

For small or non-critical deployments of multi-node Ceph clusters, it is
recommended to deploy three Monitors. For larger clusters or for clusters that
are intended to survive a double failure, it is recommended to deploy five
Monitors. Only in rare circumstances is there any justification for deploying
seven or more Monitors.

It is possible to run a Monitor on a host that also runs OSDs, but never on
the same drive as an OSD (see :ref:`hardware-storage-devices`). A host that
runs both is a shared failure domain: if it crashes, is rebooted, or is taken
down for maintenance, the cluster loses a Monitor and that host's OSDs at the
same time. For this reason, it is recommended to run Monitors and Managers on
dedicated hosts.

.. note:: A *majority* of Monitors in your cluster must be able to 
   reach each other in order for quorum to be established.

Deploying your Hardware
-----------------------

Some operators choose to add a new Monitor host at the same time that they add
a new Monitor. For details on the minimum recommendations for Monitor hardware,
see :ref:`minimum-hardware`. Before adding a Monitor host to the cluster,
make sure that there is an up-to-date version of Linux installed.

Add the newly installed Monitor host to a rack in your cluster, connect the
host to the network, and make sure that the host has network connectivity.


Installing the Required Software
--------------------------------

In manually deployed clusters, it is necessary to install Ceph packages
manually. For details, see :ref:`install_storage_cluster`.

.. _adding-monitor-manual:

Adding a Monitor (Manual)
-------------------------

The procedure in this section creates a ``ceph-mon`` data directory, retrieves
both the monitor map and the monitor keyring, and adds a ``ceph-mon`` daemon to
the cluster. If the cluster has one Monitor, this procedure leaves it with two,
and both must be running for the cluster to keep quorum. Repeat the procedure
until the cluster has three Monitors, so that it keeps quorum if one Monitor
fails.

This is a good point at which to define the new Monitor's ``id``. Monitors have
often been named with single letters (``a``, ``b``, ``c``, etc.), but you are
free to define the ``id`` however you see fit. In this document, ``{mon-id}``
refers to the ``id`` exclusive of the ``mon.`` prefix: for example, if
``mon.a`` has been chosen as the ``id`` of a Monitor, then ``{mon-id}`` is
``a``.

#. Create a data directory, owned by the ``ceph`` user, on the machine that
   will host the new Monitor:

   .. prompt:: bash $

      ssh {new-mon-host}
      sudo -u ceph mkdir /var/lib/ceph/mon/ceph-{mon-id}

#. Create a temporary directory ``{tmp}`` that will contain the files needed
   during this procedure. This directory should be different from the data
   directory created in the previous step. Because this is a temporary
   directory, it can be removed after the procedure is complete:

   .. prompt:: bash $

      mkdir {tmp}

#. Retrieve the keyring for your Monitors (``{tmp}`` is the path to the
   retrieved keyring and ``{key-filename}`` is the name of the file that
   contains the retrieved Monitor key):

   .. prompt:: bash $

      ceph auth get mon. -o {tmp}/{key-filename}

#. Retrieve the monitor map (``{tmp}`` is the path to the retrieved monitor map
   and ``{map-filename}`` is the name of the file that contains the retrieved
   monitor map):

   .. prompt:: bash $

      ceph mon getmap -o {tmp}/{map-filename}

#. Prepare the Monitor's data directory, which was created in the first step.
   The following command must specify the path to the monitor map (from which
   the new Monitor learns the cluster's ``fsid`` and the names and addresses
   of the existing Monitors) and the path to the monitor keyring. Run it as
   the ``ceph`` user, which must be able to read both files in ``{tmp}``:

   .. prompt:: bash $

      sudo -u ceph ceph-mon -i {mon-id} --mkfs --monmap {tmp}/{map-filename} --keyring {tmp}/{key-filename}

#. Set the address that the new Monitor binds to: add ``public_addr = {ip}``
   to a ``[mon.{mon-id}]`` section of ``/etc/ceph/ceph.conf`` on its host, or
   make sure that ``public_network`` in that file includes the host's address.
   Give the IP address without a port, so that the Monitor binds both the
   msgr2 port (3300) and the v1 port (6789). Then start the Monitor and enable
   it at boot. It joins the cluster by itself:

   .. prompt:: bash $

      sudo systemctl start ceph-mon@{mon-id}
      sudo systemctl enable ceph-mon@{mon-id}

.. _removing-monitors:

Removing Monitors
=================

When Monitors are removed from a cluster, it is important to remember
that Ceph Monitors use Paxos to maintain consensus about the cluster
map. Such consensus is possible only if the number of Monitors is sufficient
to establish quorum.

.. _removing-monitor-manual:

Removing a Monitor (Manual)
---------------------------

The procedure in this section removes a ``ceph-mon`` daemon from the cluster.
The procedure might result in a Ceph cluster that contains a number of Monitors
insufficient to maintain quorum, so plan carefully. When replacing an old
Monitor with a new Monitor, add the new Monitor first, wait for quorum to be
established, and then remove the old Monitor. This ensures that quorum is not
lost.


#. Stop the Monitor:

   .. prompt:: bash $

      sudo systemctl stop ceph-mon@{mon-id}

#. Remove the Monitor from the cluster:

   .. prompt:: bash $

      ceph mon rm {mon-id}

#. Remove the Monitor's address from ``mon_host`` in the ``[global]`` section
   of ``ceph.conf`` on every host, and remove its ``[mon.{mon-id}]`` section if
   it has one.

.. _rados-mon-remove-from-unhealthy: 

Removing Monitors from an Unhealthy Cluster
-------------------------------------------

The procedure in this section removes a ``ceph-mon`` daemon from an unhealthy
cluster (for example, a cluster whose Monitors are unable to form a quorum).

#. Stop all ``ceph-mon`` daemons on all Monitor hosts:

   .. prompt:: bash $

      ssh {mon-host}
      sudo systemctl stop ceph-mon.target

   Repeat this step on every Monitor host.

#. Identify a surviving Monitor and log in to the Monitor's host:

   .. prompt:: bash $

      ssh {mon-host}

#. Extract a copy of the ``monmap`` file by running a command of the following
   form:

   .. prompt:: bash $

      sudo ceph-mon -i {mon-id} --extract-monmap {map-path}

   Here is a more concrete example, for a Monitor whose ``{mon-id}`` is the
   short host name, with ``/tmp/monmap`` as the ``{map-path}``:

   .. prompt:: bash $

      sudo ceph-mon -i $(hostname -s) --extract-monmap /tmp/monmap

#. Remove the non-surviving or otherwise problematic Monitors:

   .. prompt:: bash $

      sudo monmaptool {map-path} --rm {mon-id}

   For example, suppose that there are three Monitors (``mon.a``, ``mon.b``,
   and ``mon.c``) and that only ``mon.a`` will survive:

   .. prompt:: bash $

      sudo monmaptool /tmp/monmap --rm b
      sudo monmaptool /tmp/monmap --rm c

#. Inject the edited monmap, from which the non-surviving Monitors have been
   removed, into each surviving Monitor:

   .. prompt:: bash $

      sudo ceph-mon -i {mon-id} --inject-monmap {map-path}

   Continuing with the above example, inject a map into Monitor ``mon.a`` by
   running the following command:

   .. prompt:: bash $

      sudo ceph-mon -i a --inject-monmap /tmp/monmap

#. Give each surviving Monitor's data back to the ``ceph`` user, which the
   ``ceph-mon@`` unit runs as. Running ``ceph-mon`` as root leaves files owned
   by root in the store:

   .. prompt:: bash $

      sudo chown -R ceph:ceph /var/lib/ceph/mon/ceph-{mon-id}

#. Start only the surviving Monitors.

#. Verify that the Monitors form a quorum by running the command ``ceph -s``.

#. The data directory of the removed Monitors is in ``/var/lib/ceph/mon``:
   either archive this data directory in a safe location or delete this data
   directory. However, do not delete it unless you are confident that the
   remaining Monitors are healthy and sufficiently redundant. Make sure that
   there is enough room for the live DB to expand and compact, and make sure
   that there is also room for an archived copy of the DB. The archived copy
   can be compressed.

.. _change-monitor-address:

Changing a Monitor's IP Address
===============================

.. important:: Existing Monitors are not supposed to change their IP addresses.

Monitors are critical components of a Ceph cluster. The entire system can work
properly only if the Monitors maintain quorum, and quorum can be established
only if the Monitors have discovered each other by means of their IP addresses.
Ceph has strict requirements on the discovery of Monitors.

Although the ``ceph.conf`` file is used by Ceph clients and other Ceph daemons
to discover Monitors, the monitor map is used by Monitors to discover each
other. This is why it is necessary to obtain the current ``monmap`` at the time
a new Monitor is created: as can be seen above in :ref:`adding-monitor-manual`,
the ``monmap`` is one of the arguments given to the ``ceph-mon -i {mon-id}
--mkfs`` command. The following sections explain the consistency requirements
for Ceph Monitors, and also explain a number of safe ways to change a Monitor's
IP address.


Consistency Requirements
------------------------

When a Monitor discovers other Monitors in the cluster, it always refers to the
local copy of the Monitor map. Using the Monitor map instead of using the
``ceph.conf`` file avoids errors that could break the cluster (for example,
typos or other slight errors in ``ceph.conf`` when a Monitor address or port is
specified). Because Monitors use monitor maps for discovery and because they
share monitor maps with Ceph clients and other Ceph daemons, the monitor map
provides Monitors with a strict guarantee that their consensus is valid.

Strict consistency also applies to updates to the monmap. As with any other
updates on the Monitor, changes to the monmap always run through a distributed
consensus algorithm called `Paxos`_. The Monitors must agree on each update to
the monmap, such as adding or removing a Monitor, to ensure that each Monitor
in the quorum has the same version of the monmap. Each change to the monmap
creates a new version (epoch), and each version is stored as a complete map. A
Monitor that has an older version of the monmap receives the current one from
the other Monitors when it probes them, and so catches up with the current
state of the cluster.

There are additional advantages to using the monitor map rather than
``ceph.conf`` when Monitors discover each other. Because ``ceph.conf`` is not
automatically updated and distributed, its use would bring certain risks:
Monitors might use an outdated ``ceph.conf`` file, might fail to recognize a
specific Monitor, might fall out of quorum, and might develop a situation in
which `Paxos`_ is unable to accurately ascertain the current state of the
system. Because of these risks, any changes to an existing Monitor's IP address
must be made with great care.

.. _operations_add_or_rm_mons_changing_mon_ip:

Changing a Monitor's IP address (Preferred Method)
--------------------------------------------------

If a Monitor's IP address is changed only in the ``ceph.conf`` file, there is
no guarantee that the other Monitors in the cluster will receive the update.
For this reason, the preferred method to change a Monitor's IP address is as
follows: add a new Monitor with the desired IP address (as described in
:ref:`adding-monitor-manual`), make sure that the new Monitor successfully joins the
quorum, remove the Monitor that is using the old IP address, and update the
``ceph.conf`` file to ensure that clients and other daemons are made aware of
the new Monitor's IP address.

For example, suppose that there are three Monitors in place, ``mon.a`` on
``host01``, ``mon.b`` on ``host02``, and ``mon.c`` on ``host03``, and that
``ceph.conf`` lists their addresses::

    [global]
        mon_host = 10.0.0.1,10.0.0.2,10.0.0.3

To move ``mon.c`` to ``host04`` with the IP address ``10.0.0.4``: (1) follow
the steps in :ref:`adding-monitor-manual` to add a new Monitor ``mon.d`` on
``host04``, (2) make sure that ``mon.d`` is running before removing ``mon.c``
or else quorum will be broken, and (3) follow the steps in
:ref:`removing-monitor-manual` to remove ``mon.c``. To move all three Monitors to new IP
addresses, repeat this process.


Changing a Monitor's IP address (Advanced Method)
-------------------------------------------------

There are cases in which the method outlined in
:ref:`operations_add_or_rm_mons_changing_mon_ip` cannot be used. For example,
it might be necessary to move the cluster's Monitors to a different network, to
a different part of the datacenter, or to a different datacenter altogether. It
is still possible to change the Monitors' IP addresses, but a different method
must be used.


For such cases, a new monitor map with updated IP addresses for every Monitor
in the cluster must be generated and injected on each Monitor. Although this
method is not particularly easy, such a major migration is unlikely to be a
routine task. As stated at the beginning of this section, existing Monitors are
not supposed to change their IP addresses.

Continue with the Monitor configuration in the example from
:ref:`operations_add_or_rm_mons_changing_mon_ip`. Suppose that all of the
Monitors are to be moved from the ``10.0.0.x`` range to the ``10.1.0.x`` range,
and that these networks are unable to communicate. Carry out the following
procedure:

#. Retrieve the monitor map (``{tmp}`` is the path to the retrieved monitor
   map, and ``{filename}`` is the name of the file that contains the retrieved
   monitor map):

   .. prompt:: bash $

      ceph mon getmap -o {tmp}/{filename}

#. Check the contents of the monitor map:

   .. prompt:: bash $

      monmaptool --print {tmp}/{filename}

   The output takes the following form. Squid 19.2.6 and later, Tentacle
   20.2.4 and later, and Umbrella also print four ``auth_`` lines after the
   Monitor lines.

   ::

    monmaptool: monmap file {tmp}/{filename}
    epoch 1
    fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
    last_changed 2026-10-22T10:48:54.628829+0000
    created 2026-10-22T10:48:54.628829+0000
    min_mon_release 19 (squid)
    election_strategy: 1
    0: [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0] mon.a
    1: [v2:10.0.0.2:3300/0,v1:10.0.0.2:6789/0] mon.b
    2: [v2:10.0.0.3:3300/0,v1:10.0.0.3:6789/0] mon.c

#. Remove the existing Monitors from the monitor map:

   .. prompt:: bash $

      monmaptool --rm a --rm b --rm c {tmp}/{filename}

   ::

    monmaptool: monmap file {tmp}/{filename}
    monmaptool: removing a
    monmaptool: removing b
    monmaptool: removing c
    monmaptool: writing epoch 1 to {tmp}/{filename} (0 monitors)

#. Add the new Monitor locations to the monitor map:

   .. prompt:: bash $

      monmaptool --addv a '[v2:10.1.0.1:3300/0,v1:10.1.0.1:6789/0]' --addv b '[v2:10.1.0.2:3300/0,v1:10.1.0.2:6789/0]' --addv c '[v2:10.1.0.3:3300/0,v1:10.1.0.3:6789/0]' {tmp}/{filename}

   ::

      monmaptool: monmap file {tmp}/{filename}
      monmaptool: writing epoch 1 to {tmp}/{filename} (3 monitors)

#. Check the new contents of the monitor map:

   .. prompt:: bash $

       monmaptool --print {tmp}/{filename}

   ::

    monmaptool: monmap file {tmp}/{filename}
    epoch 1
    fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
    last_changed 2026-10-22T10:48:54.628829+0000
    created 2026-10-22T10:48:54.628829+0000
    min_mon_release 19 (squid)
    election_strategy: 1
    0: [v2:10.1.0.1:3300/0,v1:10.1.0.1:6789/0] mon.a
    1: [v2:10.1.0.2:3300/0,v1:10.1.0.2:6789/0] mon.b
    2: [v2:10.1.0.3:3300/0,v1:10.1.0.3:6789/0] mon.c

#. Install the Monitors and their stores at the new location, then copy the
   modified monitor map to each new Monitor.

#. Make sure all of your Monitors have been stopped. Never inject into a
   Monitor while the Monitor daemon is running.

#. Inject the monitor map:

   .. prompt:: bash $

      sudo ceph-mon -i {mon-id} --inject-monmap {tmp}/{filename}

#. Give each Monitor's data back to the ``ceph`` user, which the ``ceph-mon@``
   unit runs as. Running ``ceph-mon`` as root leaves files owned by root in
   the store:

   .. prompt:: bash $

      sudo chown -R ceph:ceph /var/lib/ceph/mon/ceph-{mon-id}

#. Restart all of the Monitors.

Migration to the new location is now complete. The Monitors should operate
successfully.

Using cephadm to change the public network
==========================================

Overview
--------

The procedure in this overview section provides only the broad outlines of
using ``cephadm`` to change the public network.

#. Create backups of all keyrings, configuration files, and the current monmap.

#. Stop the cluster, and keep its daemons from starting at boot, by running the
   following command on every host:

   .. prompt:: bash #

      systemctl disable --now ceph-{FSID}.target

   When the move is complete, run ``systemctl enable ceph-{FSID}.target`` on
   every host.

#. Move the servers and power them on.

#. Change the network setup as desired.


Example Procedure 
-----------------

.. note:: In this procedure, the "old network" has addresses of the form
   ``10.10.10.0/24`` and the "new network" has addresses of the form
   ``192.168.160.0/24``.

#. Enter the shell of the first Monitor:

   .. prompt:: bash #

      cephadm shell --name mon.reef1

#. Extract the current monmap from ``mon.reef1``:

   .. prompt:: bash #
      
      ceph-mon -i reef1 --extract-monmap monmap

#. Print the content of the monmap:

   .. prompt:: bash #

      monmaptool --print monmap

   ::

      monmaptool: monmap file monmap
      epoch 5
      fsid 2851404a-d09a-11ee-9aaa-fa163e2de51a
      last_changed 2024-02-21T09:32:18.292040+0000
      created 2024-02-21T09:18:27.136371+0000
      min_mon_release 18 (reef)
      election_strategy: 1
      0: [v2:10.10.10.11:3300/0,v1:10.10.10.11:6789/0] mon.reef1
      1: [v2:10.10.10.12:3300/0,v1:10.10.10.12:6789/0] mon.reef2
      2: [v2:10.10.10.13:3300/0,v1:10.10.10.13:6789/0] mon.reef3

#. Remove Monitors with old addresses:

   .. prompt:: bash #

      monmaptool --rm reef1 --rm reef2 --rm reef3 monmap

#. Add Monitors with new addresses:

   .. prompt:: bash #

      monmaptool --addv reef1 [v2:192.168.160.11:3300/0,v1:192.168.160.11:6789/0] --addv reef2 [v2:192.168.160.12:3300/0,v1:192.168.160.12:6789/0] --addv reef3 [v2:192.168.160.13:3300/0,v1:192.168.160.13:6789/0] monmap
  
#. Verify that the changes to the monmap have been made successfully:

   .. prompt:: bash #

      monmaptool --print monmap 

   ::

      monmaptool: monmap file monmap
      epoch 5
      fsid 2851404a-d09a-11ee-9aaa-fa163e2de51a
      last_changed 2024-02-21T09:32:18.292040+0000
      created 2024-02-21T09:18:27.136371+0000
      min_mon_release 18 (reef)
      election_strategy: 1
      0: [v2:192.168.160.11:3300/0,v1:192.168.160.11:6789/0] mon.reef1
      1: [v2:192.168.160.12:3300/0,v1:192.168.160.12:6789/0] mon.reef2
      2: [v2:192.168.160.13:3300/0,v1:192.168.160.13:6789/0] mon.reef3

#. Inject the new monmap into the Ceph cluster:

   .. prompt:: bash #

      ceph-mon -i reef1 --inject-monmap monmap

#. Repeat the steps above for all other Monitors in the cluster.

#. Update ``/var/lib/ceph/{FSID}/mon.{MON}/config``.

#. Start the Monitors.

#. Update ``public_network`` in the configuration database, in the section
   where it is set. Run ``ceph config dump | grep public_network`` to see
   where it is set, then change it there, for example:

   .. prompt:: bash #

      ceph config set mon public_network 192.168.160.0/24

#. Update the configuration files of the Managers
   (``/var/lib/ceph/{FSID}/mgr.{mgr}/config``) and start them. Orchestrator
   will now be available, but it will attempt to connect to the old network
   because the host list contains the old addresses.

#. Update the host addresses by running commands of the following form:

   .. prompt:: bash #

      ceph orch host set-addr reef1 192.168.160.11
      ceph orch host set-addr reef2 192.168.160.12
      ceph orch host set-addr reef3 192.168.160.13

#. Wait a few minutes for the orchestrator to connect to each host.

#. Reconfigure the OSDs so that their config files are automatically updated:
   
   .. prompt:: bash #
    
      ceph orch reconfig osd.<service_id>

   The ``<service_id>`` value can be obtained from
   the output of the command ``ceph orch ls osd``.

*The above procedure was developed by Eugen Block and was successfully tested
in February 2024 on Ceph version 18.2.1 (Reef).*

.. _Paxos: https://en.wikipedia.org/wiki/Paxos_(computer_science)
