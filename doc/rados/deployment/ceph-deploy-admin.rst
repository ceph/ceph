=============
 Admin Tasks
=============

Once you have set up a cluster with ``ceph-deploy``, you may 
provide the client admin key and the Ceph configuration file
to another host so that a user on the host may use the ``ceph``
command line as an administrative user.


Create an Admin Host
====================

To enable a host to execute ceph commands with administrator
priveleges, use the ``admin`` command. ::

	ceph-deploy admin {host-name [host-name]...}
	

Deploy Config File
==================

To send an updated copy of the Ceph configuration file to hosts
in your cluster, use the ``config`` command. ::

	ceph-deploy config {host-name [host-name]...}
	
.. tip:: With a base name and increment host-naming convention, 
   it is easy to deploy configuration files via simple scripts
   (e.g., ``ceph-deploy config hostname{1,2,3,4,5}``).

