=================
 Keys Management
=================


Gather Keys
===========

Before you can provision a host to run OSDs or metadata servers, you must gather
monitor keys and the OSD and MDS bootstrap keyrings. To gather keys, enter the
following:: 

	ceph-deploy gatherkeys {monitor-host}


.. note:: To retrieve the keys, you specify a host that has a
   Ceph monitor. 

.. note:: If you have specified multiple monitors in the setup of the cluster,
   make sure, that all monitors are up and running. If the monitors haven't
   formed quorum, ``ceph-create-keys`` will not finish and the keys aren't 
   generated.

Forget Keys
===========

When you are no longer using ``ceph-deploy`` (or if you are recreating a
cluster),  you should delete the keys in the local directory of your admin host.
To delete keys, enter the following:: 

	ceph-deploy forgetkeys

