=================
 Troubleshooting
=================


Mount 5 Error
=============

A mount 5 error typically occurs if a MDS server is laggy or if it crashed.
Ensure at least one MDS is up and running, and the cluster is ``active +
healthy``. 


Mount 12 Error
==============

A mount 12 error with ``cannot allocate memory`` usually occurs if you  have a
version mismatch between the :term:`Ceph Client` version and the :term:`Ceph
Storage Cluster` version. Check the versions using:: 

	ceph -v
	
If the Ceph Client is behind the Ceph cluster, try to upgrade it:: 

	sudo apt-get update && sudo apt-get install ceph-common 

You may need to uninstall, autoclean and autoremove ``ceph-common`` 
and then reinstall it so that you have the latest version.