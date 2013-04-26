=================
 Keys Management
=================


Gather Keys
===========

Before you can provision a host to run OSDs or metadata servers, you must gather
monitor keys and the OSD and MDS bootstrap keyrings. To gather keys, enter the
following:: 

	ceph-deploy gatherkeys {monitor-host}


.. note:: To retreive the keys, you specify a host that has a
   Ceph monitor. 


Forget Keys
===========

When you are no longer using ``ceph-deploy`` (or if you are recreating a
cluster),  you should delete the keys in the local directory of your admin host.
To delete keys, enter the following:: 

	ceph-deploy forgetkeys

