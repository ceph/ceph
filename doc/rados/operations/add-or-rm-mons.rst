.. _adding-and-removing-monitors:

==========================
 Adding/Removing Monitors
==========================

When you have a cluster up and running, you may add or remove monitors
from the cluster at runtime. To bootstrap a monitor, see `Manual Deployment`_
or `Monitor Bootstrap`_.

.. _adding-monitors:

Adding Monitors
===============

Ceph monitors are lightweight processes that are the single source of truth
for the cluster map. You can run a cluster with 1 monitor but we recommend at least 3 
for a production cluster. Ceph monitors use a variation of the
`Paxos`_ algorithm to establish consensus about maps and other critical
information across the cluster. Due to the nature of Paxos, Ceph requires
a majority of monitors to be active to establish a quorum (thus establishing
consensus).

It is advisable to run an odd number of monitors. An
odd number of monitors is more resilient than an
even number. For instance, with a two monitor deployment, no
failures can be tolerated and still maintain a quorum; with three monitors,
one failure can be tolerated; in a four monitor deployment, one failure can
be tolerated; with five monitors, two failures can be tolerated.  This avoids
the dreaded *split brain* phenomenon, and is why an odd number is best.
In short, Ceph needs a majority of
monitors to be active (and able to communicate with each other), but that
majority can be achieved using a single monitor, or 2 out of 2 monitors,
2 out of 3, 3 out of 4, etc.

For small or non-critical deployments of multi-node Ceph clusters, it is
advisable to deploy three monitors, and to increase the number of monitors
to five for larger clusters or to survive a double failure.  There is rarely
justification for seven or more.

Since monitors are lightweight, it is possible to run them on the same 
host as OSDs; however, we recommend running them on separate hosts,
because `fsync` issues with the kernel may impair performance.
Dedicated monitor nodes also minimize disruption since monitor and OSD
daemons are not inactive at the same time when a node crashes or is
taken down for maintenance.

Dedicated
monitor nodes also make for cleaner maintenance by avoiding both OSDs and
a mon going down if a node is rebooted, taken down, or crashes.

.. note:: A *majority* of monitors in your cluster must be able to 
   reach each other in order to establish a quorum.

Deploy your Hardware
--------------------

If you are adding a new host when adding a new monitor,  see `Hardware
Recommendations`_ for details on minimum recommendations for monitor hardware.
To add a monitor host to your cluster, first make sure you have an up-to-date
version of Linux installed (typically Ubuntu 16.04 or RHEL 7). 

Add your monitor host to a rack in your cluster, connect it to the network
and ensure that it has network connectivity.

.. _Hardware Recommendations: ../../../start/hardware-recommendations

Install the Required Software
-----------------------------

For manually deployed clusters, you must install Ceph packages
manually. See `Installing Packages`_ for details.
You should configure SSH to a user with password-less authentication
and root permissions.

.. _Installing Packages: ../../../install/install-storage-cluster


.. _Adding a Monitor (Manual):

Adding a Monitor (Manual)
-------------------------

This procedure creates a ``ceph-mon`` data directory, retrieves the monitor map
and monitor keyring, and adds a ``ceph-mon`` daemon to your cluster.  If
this results in only two monitor daemons, you may add more monitors by
repeating this procedure until you have a sufficient number of ``ceph-mon`` 
daemons to achieve a quorum.

At this point you should define your monitor's id.  Traditionally, monitors 
have been named with single letters (``a``, ``b``, ``c``, ...), but you are 
free to define the id as you see fit.  For the purpose of this document, 
please take into account that ``{mon-id}`` should be the id you chose, 
without the ``mon.`` prefix (i.e., ``{mon-id}`` should be the ``a`` 
on ``mon.a``).

#. Create the default directory on the machine that will host your 
   new monitor:

   .. prompt:: bash $

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-id}

#. Create a temporary directory ``{tmp}`` to keep the files needed during 
   this process. This directory should be different from the monitor's default 
   directory created in the previous step, and can be removed after all the 
   steps are executed:

   .. prompt:: bash $

	mkdir {tmp}

#. Retrieve the keyring for your monitors, where ``{tmp}`` is the path to 
   the retrieved keyring, and ``{key-filename}`` is the name of the file 
   containing the retrieved monitor key:

   .. prompt:: bash $

      ceph auth get mon. -o {tmp}/{key-filename}

#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{map-filename}`` is the name of the file 
   containing the retrieved monitor map:

   .. prompt:: bash $

      ceph mon getmap -o {tmp}/{map-filename}

#. Prepare the monitor's data directory created in the first step. You must 
   specify the path to the monitor map so that you can retrieve the 
   information about a quorum of monitors and their ``fsid``. You must also 
   specify a path to the monitor keyring:
   
   .. prompt:: bash $

      sudo ceph-mon -i {mon-id} --mkfs --monmap {tmp}/{map-filename} --keyring {tmp}/{key-filename}
	

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, via either the
   ``--public-addr {ip}`` or ``--public-network {network}`` argument.
   For example:
   
   .. prompt:: bash $

      ceph-mon -i {mon-id} --public-addr {ip:port}

.. _removing-monitors:

Removing Monitors
=================

When you remove monitors from a cluster, consider that Ceph monitors use 
Paxos to establish consensus about the master cluster map. You must have 
a sufficient number of monitors to establish a quorum for consensus about 
the cluster map.

.. _Removing a Monitor (Manual):

Removing a Monitor (Manual)
---------------------------

This procedure removes a ``ceph-mon`` daemon from your cluster.   If this
procedure results in only two monitor daemons, you may add or remove another
monitor until you have a number of ``ceph-mon`` daemons that can achieve a 
quorum.

#. Stop the monitor:

   .. prompt:: bash $

      service ceph -a stop mon.{mon-id}
	
#. Remove the monitor from the cluster:

   .. prompt:: bash $

      ceph mon remove {mon-id}
	
#. Remove the monitor entry from ``ceph.conf``. 

.. _rados-mon-remove-from-unhealthy: 

Removing Monitors from an Unhealthy Cluster
-------------------------------------------

This procedure removes a ``ceph-mon`` daemon from an unhealthy
cluster, for example a cluster where the monitors cannot form a
quorum.


#. Stop all ``ceph-mon`` daemons on all monitor hosts:

   .. prompt:: bash $

      ssh {mon-host}
      systemctl stop ceph-mon.target

   Repeat for all monitor hosts.

#. Identify a surviving monitor and log in to that host:

   .. prompt:: bash $

      ssh {mon-host}

#. Extract a copy of the monmap file:

   .. prompt:: bash $

      ceph-mon -i {mon-id} --extract-monmap {map-path}

   In most cases, this command will be:

   .. prompt:: bash $

      ceph-mon -i `hostname` --extract-monmap /tmp/monmap

#. Remove the non-surviving or otherwise problematic monitors:

   .. prompt:: bash $

      monmaptool {map-path} --rm {mon-id}

   For example, suppose that there are three monitors |---| ``mon.a``, ``mon.b``,
   and ``mon.c`` |---| and that only ``mon.a`` will survive:

   .. prompt:: bash $

      monmaptool /tmp/monmap --rm b
      monmaptool /tmp/monmap --rm c

#. Inject the surviving map that includes the removed monitors into the
   monmap of the surviving monitor(s):

   .. prompt:: bash $

      ceph-mon -i {mon-id} --inject-monmap {map-path}

   Continuing with the above example, inject a map into monitor ``mon.a`` by
   running the following command:

   .. prompt:: bash $

      ceph-mon -i a --inject-monmap /tmp/monmap


#. Start only the surviving monitors.

#. Verify that the monitors form a quorum by running the command ``ceph -s``.

#. The data directory of the removed monitors is in ``/var/lib/ceph/mon``:
   either archive this data directory in a safe location or delete this data
   directory. However, do not delete it unless you are confident that the
   remaining monitors are healthy and sufficiently redundant. Make sure that
   there is enough room for the live DB to expand and compact, and make sure
   that there is also room for an archived copy of the DB. The archived copy
   can be compressed.

.. _Changing a Monitor's IP address:

Changing a Monitor's IP Address
===============================

.. important:: Existing monitors are not supposed to change their IP addresses.

Monitors are critical components of a Ceph cluster. The entire system can work
properly only if the monitors maintain quorum, and quorum can be established
only if the monitors have discovered each other by means of their IP addresses.
Ceph has strict requirements on the discovery of monitors.

Although the ``ceph.conf`` file is used by Ceph clients and other Ceph daemons
to discover monitors, the monitor map is used by monitors to discover each
other. This is why it is necessary to obtain the current ``monmap`` at the time
a new monitor is created: as can be seen above in `Adding a Monitor (Manual)`_,
the ``monmap`` is one of the arguments required by the ``ceph-mon -i {mon-id}
--mkfs`` command. The following sections explain the consistency requirements
for Ceph monitors, and also explain a number of safe ways to change a monitor's
IP address.


Consistency Requirements
------------------------

When a monitor discovers other monitors in the cluster, it always refers to the
local copy of the monitor map. Using the monitor map instead of using the
``ceph.conf`` file avoids errors that could break the cluster (for example,
typos or other slight errors in ``ceph.conf`` when a monitor address or port is
specified). Because monitors use monitor maps for discovery and because they
share monitor maps with Ceph clients and other Ceph daemons, the monitor map
provides monitors with a strict guarantee that their consensus is valid.

Strict consistency also applies to updates to the monmap. As with any other
updates on the monitor, changes to the monmap always run through a distributed
consensus algorithm called `Paxos`_. The monitors must agree on each update to
the monmap, such as adding or removing a monitor, to ensure that each monitor
in the quorum has the same version of the monmap. Updates to the monmap are
incremental so that monitors have the latest agreed upon version, and a set of
previous versions, allowing a monitor that has an older version of the monmap
to catch up with the current state of the cluster.

There are additional advantages to using the monitor map rather than
``ceph.conf`` when monitors discover each other. Because ``ceph.conf`` is not
automatically updated and distributed, its use would bring certain risks:
monitors might use an outdated ``ceph.conf`` file, might fail to recognize a
specific monitor, might fall out of quorum, and might develop a situation in
which `Paxos`_ is unable to accurately ascertain the current state of the
system. Because of these risks, any changes to an existing monitor's IP address
must be made with great care.

.. _operations_add_or_rm_mons_changing_mon_ip:

Changing a Monitor's IP address (Preferred Method)
--------------------------------------------------

If a monitor's IP address is changed only in the ``ceph.conf`` file, there is
no guarantee that the other monitors in the cluster will receive the update.
For this reason, the preferred method to change a monitor's IP address is as
follows: add a new monitor with the desired IP address (as described in `Adding
a Monitor (Manual)`_), make sure that the new monitor successfully joins the
quorum, remove the monitor that is using the old IP address, and update the
``ceph.conf`` file to ensure that clients and other daemons are made aware of
the new monitor's IP address.

For example, suppose that there are three monitors in place:: 

    [mon.a]
        host = host01
        addr = 10.0.0.1:6789
    [mon.b]
        host = host02
        addr = 10.0.0.2:6789
    [mon.c]
        host = host03
        addr = 10.0.0.3:6789

To change ``mon.c`` so that its name is ``host04`` and its IP address is
``10.0.0.4``: (1) follow the steps in `Adding a Monitor (Manual)`_ to add a new
monitor ``mon.d``, (2) make sure that ``mon.d`` is  running before removing
``mon.c`` or else quorum will be broken, and (3) follow the steps in `Removing
a Monitor (Manual)`_ to remove ``mon.c``. To move all three monitors to new IP
addresses, repeat this process.

Changing a Monitor's IP address (Advanced Method)
-------------------------------------------------

There are cases in which the method outlined in :ref"`<Changing a Monitor's IP
Address (Preferred Method)> operations_add_or_rm_mons_changing_mon_ip` cannot
be used. For example, it might be necessary to move the cluster's monitors to a
different network, to a different part of the datacenter, or to a different
datacenter altogether. It is still possible to change the monitors' IP
addresses, but a different method must be used.

For such cases, a new monitor map with updated IP addresses for every monitor
in the cluster must be generated and injected on each monitor. Although this
method is not particularly easy, such a major migration is unlikely to be a
routine task. As stated at the beginning of this section, existing monitors are
not supposed to change their IP addresses.

Continue with the monitor configuration in the example from :ref"`<Changing a
Monitor's IP Address (Preferred Method)>
operations_add_or_rm_mons_changing_mon_ip` . Suppose that all of the monitors
are to be moved from the ``10.0.0.x`` range to the ``10.1.0.x`` range, and that
these networks are unable to communicate. Carry out the following procedure:

#. Retrieve the monitor map (``{tmp}`` is the path to the retrieved monitor
   map, and ``{filename}`` is the name of the file that contains the retrieved
   monitor map):

   .. prompt:: bash $

      ceph mon getmap -o {tmp}/{filename}

#. Check the contents of the monitor map:

   .. prompt:: bash $

      monmaptool --print {tmp}/{filename}

   ::    

    monmaptool: monmap file {tmp}/{filename}
    epoch 1
    fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
    last_changed 2012-12-17 02:46:41.591248
    created 2012-12-17 02:46:41.591248
    0: 10.0.0.1:6789/0 mon.a
    1: 10.0.0.2:6789/0 mon.b
    2: 10.0.0.3:6789/0 mon.c

#. Remove the existing monitors from the monitor map:

   .. prompt:: bash $

      monmaptool --rm a --rm b --rm c {tmp}/{filename}

   ::

    monmaptool: monmap file {tmp}/{filename}
    monmaptool: removing a
    monmaptool: removing b
    monmaptool: removing c
    monmaptool: writing epoch 1 to {tmp}/{filename} (0 monitors)

#. Add the new monitor locations to the monitor map:

   .. prompt:: bash $

      monmaptool --add a 10.1.0.1:6789 --add b 10.1.0.2:6789 --add c 10.1.0.3:6789 {tmp}/{filename}

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
    last_changed 2012-12-17 02:46:41.591248
    created 2012-12-17 02:46:41.591248
    0: 10.1.0.1:6789/0 mon.a
    1: 10.1.0.2:6789/0 mon.b
    2: 10.1.0.3:6789/0 mon.c

At this point, we assume that the monitors (and stores) have been installed at
the new location. Next, propagate the modified monitor map to the new monitors,
and inject the modified monitor map into each new monitor.

#. Make sure all of your monitors have been stopped. Never inject into a
   monitor while the monitor daemon is running.

#. Inject the monitor map:

   .. prompt:: bash $

      ceph-mon -i {mon-id} --inject-monmap {tmp}/{filename}

#. Restart all of the monitors.

Migration to the new location is now complete. The monitors should operate
successfully.



.. _Manual Deployment: ../../../install/manual-deployment
.. _Monitor Bootstrap: ../../../dev/mon-bootstrap
.. _Paxos: https://en.wikipedia.org/wiki/Paxos_(computer_science)

.. |---|   unicode:: U+2014 .. EM DASH
   :trim:
