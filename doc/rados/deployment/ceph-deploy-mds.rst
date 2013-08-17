============================
 Add/Remove Metadata Server
============================

With ``ceph-deploy``, adding and removing metadata servers is a simple task. You
just add or remove one or more metadata servers on the command line with one
command.

.. important:: You must deploy at least one metadata server to use CephFS.
    There is experimental support for running multiple metadata servers.
    Do not run multiple metadata servers in production.

See `MDS Config Reference`_ for details on configuring metadata servers.


Add a Metadata Server
=====================

Once you deploy monitors and OSDs you may deploy the metadata server(s). ::

	ceph-deploy mds create {host-name}[:{daemon-name}] [{host-name}[:{daemon-name}] ...]

You may specify a daemon instance a name (optional) if you would like to run
multiple daemons on a single server.


Remove a Metadata Server
========================

Coming soon...

.. If you have a metadata server in your cluster that you'd like to remove, you may use 
.. the ``destroy`` option. :: 

..	ceph-deploy mds destroy {host-name}[:{daemon-name}] [{host-name}[:{daemon-name}] ...]

.. You may specify a daemon instance a name (optional) if you would like to destroy
.. a particular daemon that runs on a single server with multiple MDS daemons.
 
.. .. note:: Ensure that if you remove a metadata server, the remaining metadata
   servers will be able to service requests from CephFS clients. If that is not
   possible, consider adding a metadata server before destroying the metadata 
   server you would like to take offline.


.. _MDS Config Reference: ../../../cephfs/mds-config-ref
