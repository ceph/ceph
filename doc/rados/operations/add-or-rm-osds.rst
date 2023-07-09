======================
 Adding/Removing OSDs
======================

When a cluster is up and running, it is possible to add or remove OSDs. 

Adding OSDs
===========

OSDs can be added to a cluster in order to expand the cluster's capacity and
resilience. Typically, an OSD is a Ceph ``ceph-osd`` daemon running on one
storage drive within a host machine. But if your host machine has multiple
storage drives, you may map one ``ceph-osd`` daemon for each drive on the
machine.

It's a good idea to check the capacity of your cluster so that you know when it
approaches its capacity limits. If your cluster has reached its ``near full``
ratio, then you should add OSDs to expand your cluster's capacity.

.. warning:: Do not add an OSD after your cluster has reached its ``full
   ratio``. OSD failures that occur after the cluster reaches its ``near full
   ratio`` might cause the cluster to exceed its ``full ratio``.


Deploying your Hardware
-----------------------

If you are also adding a new host when adding a new OSD, see `Hardware
Recommendations`_ for details on minimum recommendations for OSD hardware. To
add an OSD host to your cluster, begin by making sure that an appropriate 
version of Linux has been installed on the host machine and that all initial
preparations for your storage drives have been carried out. For details, see
`Filesystem Recommendations`_.

Next, add your OSD host to a rack in your cluster, connect the host to the
network, and ensure that the host has network connectivity. For details, see
`Network Configuration Reference`_.


.. _Hardware Recommendations: ../../../start/hardware-recommendations
.. _Filesystem Recommendations: ../../configuration/filesystem-recommendations
.. _Network Configuration Reference: ../../configuration/network-config-ref

Installing the Required Software
--------------------------------

If your cluster has been manually deployed, you will need to install Ceph
software packages manually. For details, see `Installing Ceph (Manual)`_.
Configure SSH for the appropriate user to have both passwordless authentication
and root permissions.

.. _Installing Ceph (Manual): ../../../install


Adding an OSD (Manual)
----------------------

The following procedure sets up a ``ceph-osd`` daemon, configures this OSD to
use one drive, and configures the cluster to distribute data to the OSD. If
your host machine has multiple drives, you may add an OSD for each drive on the
host by repeating this procedure.

As the following procedure will demonstrate, adding an OSD involves creating a
metadata directory for it, configuring a data storage drive, adding the OSD to
the cluster, and then adding it to the CRUSH map.

When you add the OSD to the CRUSH map, you will need to consider the weight you
assign to the new OSD. Since storage drive capacities increase over time, newer
OSD hosts are likely to have larger hard drives than the older hosts in the
cluster have and therefore might have greater weight as well.

.. tip:: Ceph works best with uniform hardware across pools. It is possible to
   add drives of dissimilar size and then adjust their weights accordingly.
   However, for best performance, consider a CRUSH hierarchy that has drives of
   the same type and size. It is better to add larger drives uniformly to
   existing hosts. This can be done incrementally, replacing smaller drives
   each time the new drives are added.

#. Create the new OSD by running a command of the following form. If you opt
   not to specify a UUID in this command, the UUID will be set automatically
   when the OSD starts up. The OSD number, which is needed for subsequent
   steps, is found in the command's output:

   .. prompt:: bash $

      ceph osd create [{uuid} [{id}]]

   If the optional parameter {id} is specified it will be used as the OSD ID.
   However, if the ID number is already in use, the command will fail.

   .. warning:: Explicitly specifying the ``{id}`` parameter is not
      recommended. IDs are allocated as an array, and any skipping of entries
      consumes extra memory. This memory consumption can become significant if
      there are large gaps or if clusters are large. By leaving the ``{id}``
      parameter unspecified, we ensure that Ceph uses the smallest ID number
      available and that these problems are avoided.

#. Create the default directory for your new OSD by running commands of the
   following form:

   .. prompt:: bash $

      ssh {new-osd-host}
      sudo mkdir /var/lib/ceph/osd/ceph-{osd-number}

#. If the OSD will be created on a drive other than the OS drive, prepare it
   for use with Ceph. Run commands of the following form:

   .. prompt:: bash $

      ssh {new-osd-host}
      sudo mkfs -t {fstype} /dev/{drive}
      sudo mount -o user_xattr /dev/{hdd} /var/lib/ceph/osd/ceph-{osd-number}

#. Initialize the OSD data directory by running commands of the following form:

   .. prompt:: bash $

      ssh {new-osd-host}
      ceph-osd -i {osd-num} --mkfs --mkkey

   Make sure that the directory is empty before running ``ceph-osd``.

#. Register the OSD authentication key by running a command of the following
   form:

   .. prompt:: bash $

      ceph auth add osd.{osd-num} osd 'allow *' mon 'allow rwx' -i /var/lib/ceph/osd/ceph-{osd-num}/keyring

   This presentation of the command has ``ceph-{osd-num}`` in the listed path
   because many clusters have the name ``ceph``. However, if your cluster name
   is not ``ceph``, then the string ``ceph`` in ``ceph-{osd-num}`` needs to be
   replaced with your cluster name. For example, if your cluster name is
   ``cluster1``, then the path in the command should be
   ``/var/lib/ceph/osd/cluster1-{osd-num}/keyring``.

#. Add the OSD to the CRUSH map by running the following command. This allows
   the OSD to begin receiving data. The ``ceph osd crush add`` command can add
   OSDs to the CRUSH hierarchy wherever you want. If you specify one or more
   buckets, the command places the OSD in the most specific of those buckets,
   and it moves that bucket underneath any other buckets that you have
   specified. **Important:** If you specify only the root bucket, the command
   will attach the OSD directly to the root, but CRUSH rules expect OSDs to be
   inside of hosts. If the OSDs are not inside hosts, the OSDS will likely not
   receive any data.

   .. prompt:: bash $

      ceph osd crush add {id-or-name} {weight}  [{bucket-type}={bucket-name} ...]

   Note that there is another way to add a new OSD to the CRUSH map: decompile
   the CRUSH map, add the OSD to the device list, add the host as a bucket (if
   it is not already in the CRUSH map), add the device as an item in the host,
   assign the device a weight, recompile the CRUSH map, and set the CRUSH map.
   For details, see `Add/Move an OSD`_. This is rarely necessary with recent
   releases (this sentence was written the month that Reef was released).


.. _rados-replacing-an-osd:

Replacing an OSD
----------------

.. note:: If the procedure in this section does not work for you, try the
   instructions in the ``cephadm`` documentation:
   :ref:`cephadm-replacing-an-osd`.

Sometimes OSDs need to be replaced: for example, when a disk fails, or when an
administrator wants to reprovision OSDs with a new back end (perhaps when
switching from Filestore to BlueStore). Replacing an OSD differs from `Removing
the OSD`_ in that the replaced OSD's ID and CRUSH map entry must be kept intact
after the OSD is destroyed for replacement.


#. Make sure that it is safe to destroy the OSD:

   .. prompt:: bash $

      while ! ceph osd safe-to-destroy osd.{id} ; do sleep 10 ; done

#. Destroy the OSD:

   .. prompt:: bash $

      ceph osd destroy {id} --yes-i-really-mean-it

#. *Optional*: If the disk that you plan to use is not a new disk and has been
   used before for other purposes, zap the disk:

   .. prompt:: bash $

      ceph-volume lvm zap /dev/sdX

#. Prepare the disk for replacement by using the ID of the OSD that was
   destroyed in previous steps:

   .. prompt:: bash $

      ceph-volume lvm prepare --osd-id {id} --data /dev/sdX

#. Finally, activate the OSD:

   .. prompt:: bash $

      ceph-volume lvm activate {id} {fsid}

Alternatively, instead of carrying out the final two steps (preparing the disk
and activating the OSD), you can re-create the OSD by running a single command
of the following form:

   .. prompt:: bash $

      ceph-volume lvm create --osd-id {id} --data /dev/sdX

Starting the OSD
----------------

After you add an OSD to Ceph, the OSD is in your configuration. However,
it is not yet running. The OSD is ``down`` and ``in``. You must start
your new OSD before it can begin receiving data. You may use
``service ceph`` from your admin host or start the OSD from its host
machine:

   .. prompt:: bash $

      sudo systemctl start ceph-osd@{osd-num}


Once you start your OSD, it is ``up`` and ``in``.


Observe the Data Migration
--------------------------

Once you have added your new OSD to the CRUSH map, Ceph  will begin rebalancing
the server by migrating placement groups to your new OSD. You can observe this
process with  the `ceph`_ tool. :

   .. prompt:: bash $

      ceph -w

You should see the placement group states change from ``active+clean`` to
``active, some degraded objects``, and finally ``active+clean`` when migration
completes. (Control-c to exit.)

.. _Add/Move an OSD: ../crush-map#addosd
.. _ceph: ../monitoring



Removing OSDs (Manual)
======================

When you want to reduce the size of a cluster or replace hardware, you may
remove an OSD at runtime. With Ceph, an OSD is generally one Ceph ``ceph-osd``
daemon for one storage drive within a host machine. If your host has multiple
storage drives, you may need to remove one ``ceph-osd`` daemon for each drive.
Generally, it's a good idea to check the capacity of your cluster to see if you
are reaching the upper end of its capacity. Ensure that when you remove an OSD
that your cluster is not at its ``near full`` ratio.

.. warning:: Do not let your cluster reach its ``full ratio`` when
   removing an OSD. Removing OSDs could cause the cluster to reach
   or exceed its ``full ratio``.


Take the OSD out of the Cluster
-----------------------------------

Before you remove an OSD, it is usually ``up`` and ``in``.  You need to take it
out of the cluster so that Ceph can begin rebalancing and copying its data to
other OSDs. :

   .. prompt:: bash $

      ceph osd out {osd-num}


Observe the Data Migration
--------------------------

Once you have taken your OSD ``out`` of the cluster, Ceph  will begin
rebalancing the cluster by migrating placement groups out of the OSD you
removed. You can observe  this process with  the `ceph`_ tool. :

   .. prompt:: bash $

      ceph -w

You should see the placement group states change from ``active+clean`` to
``active, some degraded objects``, and finally ``active+clean`` when migration
completes. (Control-c to exit.)

.. note:: Sometimes, typically in a "small" cluster with few hosts (for
   instance with a small testing cluster), the fact to take ``out`` the
   OSD can spawn a CRUSH corner case where some PGs remain stuck in the
   ``active+remapped`` state. If you are in this case, you should mark
   the OSD ``in`` with:

   .. prompt:: bash $

      ceph osd in {osd-num}

   to come back to the initial state and then, instead of marking ``out``
   the OSD, set its weight to 0 with:

   .. prompt:: bash $

      ceph osd crush reweight osd.{osd-num} 0

   After that, you can observe the data migration which should come to its
   end. The difference between marking ``out`` the OSD and reweighting it
   to 0 is that in the first case the weight of the bucket which contains
   the OSD is not changed whereas in the second case the weight of the bucket
   is updated (and decreased of the OSD weight). The reweight command could
   be sometimes favoured in the case of a "small" cluster.



Stopping the OSD
----------------

After you take an OSD out of the cluster, it may still be running.
That is, the OSD may be ``up`` and ``out``. You must stop
your OSD before you remove it from the configuration: 

   .. prompt:: bash $

      ssh {osd-host}
      sudo systemctl stop ceph-osd@{osd-num}

Once you stop your OSD, it is ``down``.


Removing the OSD
----------------

This procedure removes an OSD from a cluster map, removes its authentication
key, removes the OSD from the OSD map, and removes the OSD from the
``ceph.conf`` file. If your host has multiple drives, you may need to remove an
OSD for each drive by repeating this procedure.

#. Let the cluster forget the OSD first. This step removes the OSD from the CRUSH
   map, removes its authentication key. And it is removed from the OSD map as
   well. Please note the :ref:`purge subcommand <ceph-admin-osd>` is introduced in Luminous, for older
   versions, please see below:

   .. prompt:: bash $

      ceph osd purge {id} --yes-i-really-mean-it

#. Navigate to the host where you keep the master copy of the cluster's
   ``ceph.conf`` file:

   .. prompt:: bash $

      ssh {admin-host}
      cd /etc/ceph
      vim ceph.conf

#. Remove the OSD entry from your ``ceph.conf`` file (if it exists)::

	[osd.1]
		host = {hostname}

#. From the host where you keep the master copy of the cluster's ``ceph.conf``
   file, copy the updated ``ceph.conf`` file to the ``/etc/ceph`` directory of
   other hosts in your cluster.

If your Ceph cluster is older than Luminous, instead of using ``ceph osd
purge``, you need to perform this step manually:


#. Remove the OSD from the CRUSH map so that it no longer receives data. You may
   also decompile the CRUSH map, remove the OSD from the device list, remove the
   device as an item in the host bucket or remove the host  bucket (if it's in the
   CRUSH map and you intend to remove the host), recompile the map and set it.
   See `Remove an OSD`_ for details:

   .. prompt:: bash $

      ceph osd crush remove {name}

#. Remove the OSD authentication key:

   .. prompt:: bash $

      ceph auth del osd.{osd-num}

   The value of ``ceph`` for ``ceph-{osd-num}`` in the path is the
   ``$cluster-$id``.  If your cluster name differs from ``ceph``, use your
   cluster name instead.

#. Remove the OSD:

   .. prompt:: bash $

      ceph osd rm {osd-num}

   for example:

   .. prompt:: bash $

      ceph osd rm 1

.. _Remove an OSD: ../crush-map#removeosd
