============================
 Add/Remove Metadata Server
============================

.. important:: You must deploy at least one metadata server to use CephFS.
    There is experimental support for running multiple metadata servers.
    Do not run multiple active metadata servers in production.

See `MDS Config Reference`_ for details on configuring metadata servers.


Add a Metadata Server
=====================

Once you deploy monitors and OSDs you may deploy the metadata server(s). ::

	ceph-deploy mds create {host-name}[:{daemon-name}] [{host-name}[:{daemon-name}] ...]

You may specify a daemon instance a name (optional) if you would like to run
multiple daemons on a single server.


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

          ceph mds fail <mds name>

#. Remove the ``/var/lib/ceph/mds/<your-mds-id>`` directory on the old Metadata server.

.. _MDS Config Reference: ../../../cephfs/mds-config-ref
