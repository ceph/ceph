====================
 RADOS RDB Commands
====================
The ``rbd`` command enables you to create, list, introspect and remove block
device images. You can also use it to clone images, create snapshots,
rollback an image to a snapshot, view a snapshot, etc. For details on using
the ``rbd`` command, see `RBD – Manage RADOS Block Device (RBD) Images`_ for
details. 


Creating a Block Device Image
-----------------------------
Before you can add a block device to a Ceph client, you must create an image for
it in the OSD cluster first. To create a block device image, execute the 
following::

	rbd create {image-name} --size {megabytes} --dest-pool {pool-name}
	
For example, to create a 1GB image named ``foo`` that stores information in a 
pool named ``swimmingpool``, execute the following::

	rbd create foo --size 1024
	rbd create bar	--size 1024 --pool swimmingpool

.. note:: You must create a pool first before you can specify it as a 
   source. See `Storage Pools`_ for details.

Listing Block Device Images
---------------------------
To list block devices in the ``rbd`` pool, execute the following:: 

	rbd ls

To list block devices in a particular pool, execute the following,
but replace ``{poolname}`` with the name of the pool:: 

	rbd ls {poolname}
	
For example::

	rbd ls swimmingpool
	
Retrieving Image Information
----------------------------
To retrieve information from a particular image, execute the following,
but replace ``{image-name}`` with the name for the image:: 

	rbd --image {image-name} info
	
For example::

	rbd --image foo info
	
To retrieve information from an image within a pool, execute the following,
but replace ``{image-name}`` with the name of the image and replace ``{pool-name}``
with the name of the pool:: 

	rbd --image {image-name} -p {pool-name} info

For example:: 

	rbd --image bar -p swimmingpool info	

Resizing a Block Device Image
-----------------------------
RBD images are thin provisioned. They don't actually use any physical storage 
until you begin saving data to them. However, they do have a maximum capacity 
that you set with the ``--size`` option. If you want to increase (or decrease)
the maximum size of a RADOS block device image, execute the following:: 

	rbd resize --image foo --size 2048


Removing a Block Device Image
-----------------------------
To remove a block device, execute the following, but replace ``{image-name}``
with the name of the image you want to remove:: 

	rbd rm {image-name}
	
For example:: 

	rbd rm foo
	
To remove a block device from a pool, execute the following, but replace 
``{image-name}`` with the name of the image to remove and replace 
``{pool-name}`` with the name of the pool:: 

	rbd rm {image-name} -p {pool-name}
	
For example:: 

	rbd rm bar -p swimmingpool


Snapshotting Block Device Images
--------------------------------
One of the advanced features of RADOS block devices is that you can create 
snapshots of the images to retain a history of an image's state. Ceph supports
RBD snapshots from the ``rbd`` command, from a kernel object, from a 
KVM, and from cloud solutions. Once you create snapshots of an image, you 
can rollback to a snapshot, list snapshots, remove snapshots and purge 
the snapshots.

.. important:: Generally, you should stop i/o before snapshotting an image.
   If the image contains a filesystem, the filesystem should be in a
   consistent state before snapshotting too.

 


.. _Storage Pools: ../../config-cluster/pools
.. _RBD – Manage RADOS Block Device (RBD) Images: ../../man/8/rbd/