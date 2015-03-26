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

Add a Metadata Server [Manualy]
=====================

Once you deploy monitors and OSDs you may deploy the metadata server:
1. Add it in config file (ceph.conf) ::

	[mds]
	mds data = /var/lib/ceph/mds/$cluster-$id
	keyring = /var/lib/ceph/mds/$cluster-$id/$cluster.keyring
	
	[mds.a]
	host = {hostname}

2. Create working directory ::

	$ sudo mkdir /var/lib/ceph/mds/ceph-a
	
3. Create the authentication key (only if you use cephX) ::

	$ sudo ceph auth get-or-create mds.a mds 'allow ' osd 'allow *' mon 'allow rwx' > /var/lib/ceph/mds/ceph-a/ceph.keyring

4. Start mds daemon ::

	 $ sudo service ceph start mds.0
	 
5. Check ::
	
	$ ceph mds stat
	e22: 1/1/1 up {0=a=up:active}

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

Remove a Metadata Server [Manualy]
========================

1. Remove mds section in your config file ::

	[mds.a]
	host = {hostname}
	
2. Remove mds from cluster ::

	$ sudo ceph mds rm mds.a
