.. _adding-and-removing-osds:

======================
 Adding/Removing OSDs
======================

.. note:: The procedures on this page are for clusters that are not
   managed by cephadm. In a cephadm-managed cluster, OSDs are added
   and removed through the orchestrator: see
   :ref:`cephadm-deploy-osds` and :ref:`cephadm-osd-removal`. Using
   the procedures below on a cephadm-managed cluster will conflict
   with the orchestrator's own management of the daemons.

When a cluster is up and running, it is possible to add or remove OSDs.

Adding OSDs
===========

OSDs can be added to a cluster in order to expand the cluster's capacity and
resilience. Each OSD is a ``ceph-osd`` daemon that manages one unit of
underlying storage, usually an entire HDD or SSD, so a host with several
storage drives usually runs one OSD for each drive.

It's a good idea to check the capacity of your cluster so that you know when it
approaches its capacity limits. If your cluster has reached its nearfull ratio
(``nearfull_ratio`` in the output of ``ceph osd dump``), add OSDs to expand its
capacity. See :ref:`storage-capacity`.

.. warning:: Do not let your cluster reach its full ratio (``full_ratio``)
   before adding an OSD. OSD failures that occur after the cluster reaches its
   nearfull ratio might cause the cluster to exceed its full ratio.


Deploying your Hardware
-----------------------

If you are also adding a new host when adding a new OSD, see
:ref:`minimum-hardware` for the minimum OSD hardware. To
add an OSD host to your cluster, begin by making sure that an appropriate 
version of Linux has been installed on the host machine and that all initial
preparations for your storage drives have been carried out.

Next, add your OSD host to a rack in your cluster, connect the host to the
network, and ensure that the host has network connectivity. For details, see
`Network Configuration Reference`_.


.. _Network Configuration Reference: ../../configuration/network-config-ref

Installing the Required Software
--------------------------------

If your cluster has been manually deployed, you will need to install Ceph
software packages manually. For details, see :ref:`install_storage_cluster`.


Adding an OSD (Manual)
----------------------

.. seealso:: The :ref:`manual-deployment-adding-osds` section of the
   Manual Deployment guide documents the same task using
   ``ceph-volume``, which automates most of the steps below, and a
   long form based on the newer ``ceph osd new`` command. Prefer
   those procedures where possible.

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

   ``ceph osd create`` is deprecated in favor of ``ceph osd new``, and it
   refuses an ID that belongs to a destroyed OSD.

   If the optional parameter {id} is specified it will be used as the OSD ID.
   However, if the ID number is already in use, the command will fail.

   If you give a UUID to ``ceph osd create``, also give it to
   ``ceph-osd --mkfs`` with ``--osd-uuid {uuid}``. Otherwise the Monitors
   ignore the OSD when it starts.

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

#. Link the OSD's drive into the directory as ``block``, and let the ``ceph``
   user open it. Use a drive other than the OS drive: an OSD on the OS drive is
   slow, and reimaging the OS takes the OSD data with it. Run commands of the
   following form:

   .. warning:: The ``ceph-osd --mkfs`` step below writes a new BlueStore to
      ``/dev/{drive}`` and destroys all data on it.

   .. prompt:: bash $

      ssh {new-osd-host}
      sudo ln -s /dev/{drive} /var/lib/ceph/osd/ceph-{osd-number}/block
      sudo chown ceph:ceph /dev/{drive}

   Without a ``block`` link, ``ceph-osd --mkfs`` stores the OSD in a 100 GiB
   file (``bluestore_block_size``) inside the directory. The device is owned
   by ``root`` again after the host reboots, and the OSD then cannot open it:
   run the ``chown`` command again after each reboot, or create the OSD with
   ``ceph-volume``, which sets the ownership each time it activates the OSD.

#. Initialize the OSD data directory, then give the directory to the ``ceph``
   user, which the ``ceph-osd@`` unit runs as. The OSD has no key yet, so it
   cannot fetch its configuration from the Monitors, and the command needs
   ``--no-mon-config``. Make sure that the directory is empty, apart from the
   ``block`` link, before you run these commands:

   .. prompt:: bash $

      ssh {new-osd-host}
      sudo ceph-osd -i {osd-num} --mkfs --mkkey --no-mon-config
      sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-{osd-num}

#. Register the OSD authentication key by running a command of the following
   form:

   .. prompt:: bash $

      sudo ceph auth add osd.{osd-num} osd 'allow *' mon 'allow profile osd' mgr 'allow profile osd' -i /var/lib/ceph/osd/ceph-{osd-num}/keyring

   These are the capabilities that ``ceph osd new`` gives a new OSD. The
   keyring file is readable only by its owner, so the command needs ``sudo``.

   This presentation of the command assumes the default cluster name,
   ``ceph``. Custom cluster names are deprecated (see
   :ref:`Naming Clusters (deprecated) <ceph-runtime-config>`), and in Tentacle
   and later releases the packaged ``ceph-osd@`` unit no longer passes
   ``--cluster``, so it starts only OSDs of a cluster named ``ceph``.

#. Add the OSD to the CRUSH map by running the following command. This allows
   the OSD to begin receiving data. The ``ceph osd crush add`` command can add
   OSDs to the CRUSH hierarchy wherever you want. It places the OSD in the most
   specific bucket that you name. If that bucket does not exist, the command
   creates it, together with each missing bucket that you name above it, under
   the first named bucket that exists. The command does not move a bucket that
   already exists; to move one, use ``ceph osd crush move``.
   **Important:** If you specify only the root bucket, the command
   will attach the OSD directly to the root, but CRUSH rules expect OSDs to be
   inside of hosts. If the OSDs are not inside hosts, the OSDs will likely not
   receive any data.

   .. prompt:: bash $

      ceph osd crush add {id-or-name} {weight} {bucket-type}={bucket-name} [{bucket-type}={bucket-name} ...]

   Give at least one ``{bucket-type}={bucket-name}`` pair: the command rejects
   a call without one.

   When the OSD starts, it checks its place in the CRUSH map. Unless
   ``osd_crush_update_on_start`` is ``false``, an OSD that is not in the
   lowest-level bucket of its CRUSH location (usually its host) moves itself
   there. The CRUSH location is the value of ``crush_location``, or the output
   of ``crush_location_hook``, or, if neither is set,
   ``root=default host={short-hostname}`` (see :ref:`rados-crush-map`). To keep
   a location that you set with ``ceph osd crush add``, set ``crush_location``
   to that location for the OSD.

   Note that there is another way to add a new OSD to the CRUSH map: decompile
   the CRUSH map, add the OSD to the device list, add the host as a bucket (if
   it is not already in the CRUSH map), add the device as an item in the host,
   assign the device a weight, recompile the CRUSH map, and set the CRUSH map.
   For details, see :ref:`rados-crush-map-edits`. This is rarely necessary.


.. _rados-replacing-an-osd:

Replacing an OSD
----------------

.. note:: On a cluster managed by cephadm, replace OSDs with the orchestrator
   instead: see :ref:`cephadm-replacing-an-osd`.

Sometimes OSDs need to be replaced: for example, when a disk fails, or when an
administrator wants to reprovision OSDs with a new back end (perhaps when
switching from Filestore to BlueStore). Replacing an OSD differs from `Removing
the OSD`_ in that the replaced OSD's ID and CRUSH map entry must be kept intact
after the OSD is destroyed for replacement.


#. If the OSD is still ``in``, mark it ``out`` so that its data moves to
   other OSDs:

   .. prompt:: bash $

      ceph osd out {id}

#. Make sure that it is safe to destroy the OSD:

   .. prompt:: bash $

      while ! ceph osd safe-to-destroy osd.{id} ; do sleep 10 ; done

#. If the OSD is still running, stop it on its host. ``ceph osd destroy``
   refuses an OSD that is not ``down``:

   .. prompt:: bash $

      sudo systemctl stop ceph-osd@{id}

#. Destroy the OSD:

   .. warning:: This step removes the OSD's CephX keys, its config-key data,
      and its lockbox keys. On an encrypted OSD, the config-key data holds
      the encryption key, so the data on the OSD can never be read again.

   .. prompt:: bash $

      ceph osd destroy {id}

#. *Optional*: If the disk that you plan to use is not new (for example, if it
   held an OSD before), zap it. ``--destroy`` also removes the volume groups
   and logical volumes on the disk, so that ``ceph-volume`` can use the disk
   for another OSD (see :ref:`ceph-volume-lvm-zap`):

   .. warning:: This destroys all data on ``/dev/sdX``.

   .. prompt:: bash $

      ceph-volume lvm zap --destroy /dev/sdX

#. Prepare the disk for replacement by using the ID of the OSD that was
   destroyed in previous steps:

   .. prompt:: bash $

      ceph-volume lvm prepare --osd-id {id} --data /dev/sdX

#. Finally, activate the OSD. ``{osd-fsid}`` is the OSD's own fsid, which
   ``ceph-volume lvm list`` shows as ``osd fsid``; it is not the cluster
   ``fsid``:

   .. prompt:: bash $

      ceph-volume lvm activate {id} {osd-fsid}

Alternatively, instead of carrying out the final two steps (preparing the disk
and activating the OSD), you can re-create the OSD by running a single command
of the following form:

.. prompt:: bash $

   ceph-volume lvm create --osd-id {id} --data /dev/sdX

Starting the OSD
----------------

After an OSD is added to Ceph, it is in the OSD map, but until it is started it
is ``down`` and ``in``: it is not running and receives no data. To start the
OSD, run a command of the following form on its host:

.. prompt:: bash $

   sudo systemctl start ceph-osd@{osd-num}

After the OSD is started, it is considered ``up`` and ``in``.

Observing the Data Migration
----------------------------

After the new OSD has been added to the CRUSH map, Ceph begins rebalancing the
cluster by migrating placement groups (PGs) to the new OSD. To observe this
process by using the `ceph`_ tool, run the following command:

.. prompt:: bash $

   ceph -w

Or:

.. prompt:: bash $

   watch ceph status

The PG states change from ``active+clean`` to states such as
``active+remapped+backfill_wait`` and ``active+remapped+backfilling``, and
``ceph status`` reports misplaced objects while the data moves. The PGs return
to ``active+clean`` when migration completes. When you are finished observing,
press Ctrl-C to exit.

.. _ceph: ../monitoring


Removing OSDs (Manual)
======================

It is possible to remove an OSD manually while the cluster is running: you
might want to do this in order to reduce the size of the cluster or when
replacing hardware. Each OSD manages one unit of underlying storage, usually an
entire HDD or SSD, so a host with several storage drives usually runs several
OSDs, and you might need to remove one OSD for each drive.

.. warning:: Before you begin the process of removing an OSD, make sure that
   your cluster is not near its full ratio (``full_ratio``). Otherwise the act
   of removing OSDs might cause the cluster to reach or exceed its full ratio.


Taking the OSD ``out`` of the Cluster
-------------------------------------

OSDs are typically ``up`` and ``in`` before they are removed from the cluster.
Before the OSD can be removed from the cluster, the OSD must be taken ``out``
of the cluster so that Ceph can begin rebalancing and copying its data to other
OSDs. To take an OSD ``out`` of the cluster, run a command of the following
form:

.. prompt:: bash $

   ceph osd out {osd-num}


Observing the Data Migration
----------------------------

After the OSD has been taken ``out`` of the cluster, Ceph begins rebalancing
the cluster by migrating placement groups out of the OSD that was removed. To
observe this process by using the `ceph`_ tool, run the following command:

.. prompt:: bash $

   ceph -w

The PG states change from ``active+clean`` to states such as
``active+remapped+backfill_wait`` and ``active+remapped+backfilling``, and
``ceph status`` reports misplaced objects while the data moves. The PGs return
to ``active+clean`` when migration completes. When you are finished observing,
press Ctrl-C to exit.

.. note:: Under certain conditions, the action of taking ``out`` an OSD
   might lead CRUSH to encounter a corner case in which some PGs remain stuck
   in the ``active+remapped`` state. This problem sometimes occurs in small
   clusters with few hosts (for example, in a small testing cluster). To
   address this problem, mark the OSD ``in`` by running a command of the
   following form:

   .. prompt:: bash $

      ceph osd in {osd-num}

   After the OSD has come back to its initial state, do not mark the OSD
   ``out`` again. Instead, set the OSD's weight to ``0`` by running a command
   of the following form:

   .. prompt:: bash $

      ceph osd crush reweight osd.{osd-num} 0

   After the OSD has been reweighted, observe the data migration and confirm
   that it has completed successfully. The difference between marking an OSD
   ``out`` and reweighting the OSD to ``0`` has to do with the bucket that
   contains the OSD. When an OSD is marked ``out``, the weight of the bucket is
   not changed. But when an OSD is reweighted to ``0``, the weight of the
   bucket is updated (namely, the weight of the OSD is subtracted from the
   overall weight of the bucket). When operating small clusters, it can
   sometimes be preferable to use the above reweight command.


Stopping the OSD
----------------

After you take an OSD ``out`` of the cluster, the OSD might still be running.
In such a case, the OSD is ``up`` and ``out``. Before it is removed from the
cluster, the OSD must be stopped by running commands of the following form:

.. prompt:: bash $

   ssh {osd-host}
   sudo systemctl stop ceph-osd@{osd-num}

After the OSD has been stopped, it is ``down``.


Removing the OSD
----------------

The following procedure removes an OSD from the cluster map, removes the OSD's
authentication key, removes the OSD from the OSD map, and removes the OSD from
the ``ceph.conf`` file. If your host has multiple drives, it might be necessary
to remove an OSD from each drive by repeating this procedure.

#. Begin by having the cluster forget the OSD. This step removes the OSD from
   the CRUSH map, removes the OSD's authentication key, and removes the OSD
   from the OSD map. The :ref:`purge subcommand <ceph-admin-osd>` first checks
   that the OSD is safe to destroy, and stops if it is not:

   .. warning:: This step deletes everything that the Monitors store about the
      OSD, including its ID and its CRUSH position. ``--force`` (or its older
      synonym ``--yes-i-really-mean-it``) skips the safety check and can cause
      permanent data loss.

   .. prompt:: bash $

      ceph osd purge {id}


#. Navigate to the host where the master copy of the cluster's
   ``ceph.conf`` file is kept:

   .. prompt:: bash $

      ssh {admin-host}
      cd /etc/ceph
      vim ceph.conf

#. Remove the OSD entry from your ``ceph.conf`` file (if such an entry
   exists)::

    [osd.1]
        host = {hostname}

#. Copy the updated ``ceph.conf`` file from the location on the host where the
   master copy of the cluster's ``ceph.conf`` is kept to the ``/etc/ceph``
   directory of the other hosts in your cluster.

.. _ceph_osd_purge_procedure_pre_luminous:
