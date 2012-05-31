===============
 Storage Pools
===============

Ceph stores data in 'pools' within the OSDs. When you first deploy a cluster 
without specifying pools, Ceph uses the default pools for storing data. 
To organize data into pools, see the `rados`_ command for details.

You can list, create, and remove pools. You can also view the pool utilization
statistics. 

List Pools
----------
To list your cluster's pools, execute:: 

	rados lspools

The default pools include:

- ``data``
- ``metadata``
- ``rbd``

Create a Pool
-------------
To create a pool, execute:: 

	rados mkpool {pool_name}
	
Remove a Pool
-------------
To remove a pool, execute::

	rados rmpool {pool_name}

Show Pool Stats
---------------
To show a pool's utilization statistics, execute:: 

	rados df
	
.. _rados: http://ceph.com/docs/master/man/8/rados/