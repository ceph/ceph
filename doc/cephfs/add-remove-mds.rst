============================
 Add/Remove Metadata Server
============================

You must deploy at least one metadata server daemon to use CephFS.  Instructions are given here for setting up an MDS manually, but you might prefer to use another tool such as ceph-deploy or ceph-ansible.

See `MDS Config Reference`_ for details on configuring metadata servers.


Add a Metadata Server
=====================

#. Create an mds data point ``/var/lib/ceph/mds/ceph-{$id}``.

#. Edit ``ceph.conf`` and add MDS section. ::

	[mds.{$id}]
	host = {hostname}

#. Create the authentication key, if you use CephX. ::

	$ sudo ceph auth get-or-create mds.{$id} mon 'profile mds' mgr 'profile mds' mds 'allow *' osd 'allow *' > /var/lib/ceph/mds/ceph-{$id}/keyring

#. Start the service. ::

	$ sudo service ceph start mds.{$id}

#. The status of the cluster shows: ::

	mds: cephfs_a-1/1/1 up  {0=c=up:active}, 3 up:standby

Remove a Metadata Server
========================

.. note:: Ensure that if you remove a metadata server, the remaining metadata
   servers will be able to service requests from CephFS clients. If that is not
   possible, consider adding a metadata server before destroying the metadata
   server you would like to take offline.

If you have a metadata server in your cluster that you'd like to remove, you may use
the following method.

#. Create a new Metadata Server as shown in the above section.

#. Stop the old Metadata Server and start using the new one. ::

	$ ceph mds fail <mds name>

#. Remove the ``/var/lib/ceph/mds/ceph-{$id}`` directory on the old Metadata server.

.. _MDS Config Reference: ../mds-config-ref
