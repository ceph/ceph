============================
 Add/Remove Metadata Server
============================

.. important:: You must deploy at least one metadata server to use CephFS.
    There is experimental support for running multiple metadata servers.
    Do not run multiple active metadata servers in production.

See `MDS Config Reference`_ for details on configuring metadata servers.


Add a Metadata Server
=====================

#. Create an mds data point ``/var/lib/ceph/mds/mds.<your-mds-id>``.

#. Edit ``ceph.conf`` and add MDS section. ::

	[mds.<your-mds-id>]
	host = {hostname}

#. Create the authentication key, if you use CephX. ::

	$ sudo ceph auth get-or-create mds.<your-mds-id> mon 'profile mds' mgr 'profile mds' mds 'allow *' osd 'allow *' > /var/lib/ceph/mds/ceph-<your-mds-id>/keying

#. Start the service. ::

	$ sudo service ceph start mds.<your-mds-id>

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

#. Remove the ``/var/lib/ceph/mds/mds.<your-mds-id>`` directory on the old Metadata server.

.. _MDS Config Reference: ../mds-config-ref
