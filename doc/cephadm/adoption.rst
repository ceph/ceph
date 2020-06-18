.. _cephadm-adoption:

Converting an existing cluster to cephadm
=========================================

Cephadm allows you to convert an existing Ceph cluster that
has been deployed with ceph-deploy, ceph-ansible, DeepSea, or similar tools.

Limitations
-----------

* Cephadm only works with BlueStore OSDs.  If there are FileStore OSDs
  in your cluster you cannot manage them.

Preparation
-----------

#. Get the ``cephadm`` command line tool on each host in the existing
   cluster.  See :ref:`get-cephadm`.

#. Prepare each host for use by ``cephadm``::

     # cephadm prepare-host

#. Determine which Ceph version you will use.  You can use any Octopus (15.2.z)
   release or later.  For example, ``docker.io/ceph/ceph:v15.2.0``.  The default
   will be the latest stable release, but if you are upgrading from an earlier
   release at the same time be sure to refer to the upgrade notes for any
   special steps to take while upgrading.

   The image is passed to cephadm with::

     # cephadm --image $IMAGE <rest of command goes here>

#. Cephadm can provide a list of all Ceph daemons on the current host::

     # cephadm ls

   Before starting, you should see that all existing daemons have a
   style of ``legacy`` in the resulting output.  As the adoption
   process progresses, adopted daemons will appear as style
   ``cephadm:v1``.


Adoption process
----------------

#. Ensure the ceph configuration is migrated to use the cluster config database.
   If the ``/etc/ceph/ceph.conf`` is identical on each host, then on one host::

     # ceph config assimilate-conf -i /etc/ceph/ceph.conf

   If there are config variations on each host, you may need to repeat
   this command on each host.  You can view the cluster's
   configuration to confirm that it is complete with::

     # ceph config dump

#. Adopt each monitor::

     # cephadm adopt --style legacy --name mon.<hostname>

   Each legacy monitor should stop, quickly restart as a cephadm
   container, and rejoin the quorum.

#. Adopt each manager::

     # cephadm adopt --style legacy --name mgr.<hostname>

#. Enable cephadm::

     # ceph mgr module enable cephadm
     # ceph orch set backend cephadm

#. Generate an SSH key::

     # ceph cephadm generate-key
     # ceph cephadm get-pub-key > ceph.pub

#. Install the cluster SSH key on each host in the cluster::

     # ssh-copy-id -f -i ceph.pub root@<host>

   .. note::
     It is also possible to import an existing ssh key. See
     :ref:`ssh errors <cephadm-ssh-errors>` in the troubleshooting
     document for instructions describing how to import existing
     ssh keys.

#. Tell cephadm which hosts to manage::

     # ceph orch host add <hostname> [ip-address]

   This will perform a ``cephadm check-host`` on each host before
   adding it to ensure it is working.  The IP address argument is only
   required if DNS does not allow you to connect to each host by its
   short name.

#. Verify that the adopted monitor and manager daemons are visible::

     # ceph orch ps

#. Adopt all OSDs in the cluster::

     # cephadm adopt --style legacy --name <name>

   For example::

     # cephadm adopt --style legacy --name osd.1
     # cephadm adopt --style legacy --name osd.2

#. Redeploy MDS daemons by telling cephadm how many daemons to run for
   each file system.  You can list file systems by name with ``ceph fs
   ls``.  For each file system::

     # ceph orch apply mds <fs-name> [--placement=<placement>]

   For example, in a cluster with a single file system called `foo`::

     # ceph fs ls
     name: foo, metadata pool: foo_metadata, data pools: [foo_data ]
     # ceph orch apply mds foo 2

   Wait for the new MDS daemons to start with::

     # ceph orch ps --daemon-type mds

   Finally, stop and remove the legacy MDS daemons::

     # systemctl stop ceph-mds.target
     # rm -rf /var/lib/ceph/mds/ceph-*

#. Redeploy RGW daemons.  Cephadm manages RGW daemons by zone.  For each
   zone, deploy new RGW daemons with cephadm::

     # ceph orch apply rgw <realm> <zone> [--subcluster=<subcluster>] [--port=<port>] [--ssl] [--placement=<placement>]

   where *<placement>* can be a simple daemon count, or a list of
   specific hosts (see :ref:`orchestrator-cli-placement-spec`).

   Once the daemons have started and you have confirmed they are functioning,
   stop and remove the old legacy daemons::

     # systemctl stop ceph-rgw.target
     # rm -rf /var/lib/ceph/radosgw/ceph-*

   For adopting single-site systems without a realm, see also
   :ref:`rgw-multisite-migrate-from-single-site`.

#. Check the ``ceph health detail`` output for cephadm warnings about
   stray cluster daemons or hosts that are not yet managed.
