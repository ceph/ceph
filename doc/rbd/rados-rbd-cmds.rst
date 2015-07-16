=======================
 Block Device Commands
=======================

.. index:: Ceph Block Device; image management

The ``rbd`` command enables you to create, list, introspect and remove block
device images. You can also use it to clone images, create snapshots,
rollback an image to a snapshot, view a snapshot, etc. For details on using
the ``rbd`` command, see `RBD – Manage RADOS Block Device (RBD) Images`_ for
details. 

.. important:: To use Ceph Block Device commands, you must have access to 
   a running Ceph cluster.


Creating a Block Device Image
=============================

Before you can add a block device to a node, you must create an image for it in
the :term:`Ceph Storage Cluster` first. To create a block device image, execute
the  following::

	rbd create {image-spec} --size {M/G/T} 
	
For example, to create a 1GB image named ``bar`` that stores information in a
pool named ``swimmingpool``, execute the following::

	rbd create swimmingpool/bar --size 1G

If you don't specify pool when creating an image, it will be stored in the
default pool ``rbd``. For example, to create a 1GB image named ``foo`` stored in
the default pool ``rbd``, execute the following::

        rbd create foo --size 1G

.. note:: You must create a pool first before you can specify it as a 
   source. See `Storage Pools`_ for details.

Listing Block Device Images
===========================

To list block devices in the ``rbd`` pool, execute the following
(i.e., ``rbd`` is the default pool name):: 

	rbd ls

To list block devices in a particular pool, execute the following,
but replace ``{poolname}`` with the name of the pool:: 

	rbd ls {poolname}
	
For example::

	rbd ls swimmingpool
	
Retrieving Image Information
============================

To retrieve information from a particular image, execute the following,
but replace ``{image-name}`` with the name for the image:: 

	rbd --image {image-name} info
	
For example::

	rbd --image foo info
	
To retrieve information from an image within a pool, execute the following,
but replace ``{image-name}`` with the name of the image and replace ``{image-spec}``
with the name of the pool:: 

	rbd info {image-spec}

For example:: 

	rbd info swimmingpool/foo 

Resizing a Block Device Image
=============================

:term:`Ceph Block Device` images are thin provisioned. They don't actually use
any physical storage  until you begin saving data to them. However, they do have
a maximum capacity  that you set with the ``--size`` option. If you want to
increase (or decrease) the maximum size of a Ceph Block Device image, execute
the following:: 

	rbd resize swimmingpool/foo --size 2048


Removing a Block Device Image
=============================

To remove a block device, execute the following, but replace ``{image-name}``
with the name of the image you want to remove:: 

	rbd rm {image-name}
	
For example:: 

	rbd rm foo
	
To remove a block device from a pool, execute the following, but replace 
``{image-name}`` with ``{image-spec}``

	rbd rm {image-spece} 
	
For example:: 

	rbd rm swimmingpool/bar

<image-spec> is [pool-name]/image-name and <snap-spec> is [pool-name]/image-name@snap-name
or you may specify individual pieces of names with -p/--pool <pool-name>, --image <image-name> and/or --snap <snap-name>.

.. _Storage Pools: ../../rados/operations/pools
.. _RBD – Manage RADOS Block Device (RBD) Images: ../../man/8/rbd/
