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

Create a Block Device Pool
==========================

#. On the admin node, use the ``ceph`` tool to `create a pool`_.

#. On the admin node, use the ``rbd`` tool to initialize the pool for use by RBD::

        rbd pool init <pool-name>

.. note:: The ``rbd`` tool assumes a default pool name of 'rbd' when not
   provided.

Create a Block Device User
==========================

Unless specified, the ``rbd`` command will access the Ceph cluster using the ID
``admin``. This ID allows full administrative access to the cluster. It is
recommended that you utilize a more restricted user wherever possible.

To `create a Ceph user`_, with ``ceph`` specify the ``auth get-or-create``
command, user name, monitor caps, and OSD caps::

        ceph auth get-or-create client.{ID} mon 'profile rbd' osd 'profile {profile name} [pool={pool-name}][, profile ...]'

For example, to create a user ID named ``qemu`` with read-write access to the
pool ``vms`` and read-only access to the pool ``images``, execute the
following::

	ceph auth get-or-create client.qemu mon 'profile rbd' osd 'profile rbd pool=vms, profile rbd-read-only pool=images'

The output from the ``ceph auth get-or-create`` command will be the keyring for
the specified user, which can be written to ``/etc/ceph/ceph.client.{ID}.keyring``.

.. note:: The user ID can be specified when using the ``rbd`` command by
        providing the ``--id {id}`` optional argument.

Creating a Block Device Image
=============================

Before you can add a block device to a node, you must create an image for it in
the :term:`Ceph Storage Cluster` first. To create a block device image, execute
the  following::

	rbd create --size {megabytes} {pool-name}/{image-name}

For example, to create a 1GB image named ``bar`` that stores information in a
pool named ``swimmingpool``, execute the following::

	rbd create --size 1024 swimmingpool/bar

If you don't specify pool when creating an image, it will be stored in the
default pool ``rbd``. For example, to create a 1GB image named ``foo`` stored in
the default pool ``rbd``, execute the following::

	rbd create --size 1024 foo

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

To list deferred delete block devices in the ``rbd`` pool, execute the 
following:: 

        rbd trash ls

To list deferred delete block devices in a particular pool, execute the 
following, but replace ``{poolname}`` with the name of the pool:: 

        rbd trash ls {poolname}

For example::

        rbd trash ls swimmingpool

Retrieving Image Information
============================

To retrieve information from a particular image, execute the following,
but replace ``{image-name}`` with the name for the image:: 

	rbd info {image-name}
	
For example::

	rbd info foo
	
To retrieve information from an image within a pool, execute the following,
but replace ``{image-name}`` with the name of the image and replace ``{pool-name}``
with the name of the pool:: 

	rbd info {pool-name}/{image-name}

For example:: 

	rbd info swimmingpool/bar

Resizing a Block Device Image
=============================

:term:`Ceph Block Device` images are thin provisioned. They don't actually use
any physical storage  until you begin saving data to them. However, they do have
a maximum capacity  that you set with the ``--size`` option. If you want to
increase (or decrease) the maximum size of a Ceph Block Device image, execute
the following:: 

	rbd resize --size 2048 foo (to increase)
	rbd resize --size 2048 foo --allow-shrink (to decrease)


Removing a Block Device Image
=============================

To remove a block device, execute the following, but replace ``{image-name}``
with the name of the image you want to remove:: 

	rbd rm {image-name}

For example:: 

	rbd rm foo
 
To remove a block device from a pool, execute the following, but replace 
``{image-name}`` with the name of the image to remove and replace 
``{pool-name}`` with the name of the pool:: 

	rbd rm {pool-name}/{image-name}

For example:: 

	rbd rm swimmingpool/bar

To defer delete a block device from a pool, execute the following, but 
replace ``{image-name}`` with the name of the image to move and replace 
``{pool-name}`` with the name of the pool:: 

        rbd trash mv {pool-name}/{image-name}

For example:: 

        rbd trash mv swimmingpool/bar

To remove a deferred block device from a pool, execute the following, but 
replace ``{image-id}`` with the id of the image to remove and replace 
``{pool-name}`` with the name of the pool:: 

        rbd trash rm {pool-name}/{image-id}

For example:: 

        rbd trash rm swimmingpool/2bf4474b0dc51

.. note::

  * You can move an image to the trash even it has shapshot(s) or actively 
    in-use by clones, but can not be removed from trash.

  * You can use *--delay* to set the defer time (default is 0), and if its 
    deferment time has not expired, it can not be removed unless you use 
    force.

Restoring a Block Device Image
==============================

To restore a deferred delete block device in the rbd pool, execute the 
following, but replace ``{image-id}`` with the id of the image::

        rbd trash restore {image-d}

For example:: 

        rbd trash restore 2bf4474b0dc51

To restore a deferred delete block device in a particular pool, execute 
the following, but replace ``{image-id}`` with the id of the image and 
replace ``{pool-name}`` with the name of the pool::

        rbd trash restore {pool-name}/{image-id}

For example:: 

        rbd trash restore swimmingpool/2bf4474b0dc51

Also you can use *--image* to rename the iamge when restore it, for 
example::

        rbd trash restore swimmingpool/2bf4474b0dc51 --image new-name


.. _create a pool: ../../rados/operations/pools/#create-a-pool
.. _Storage Pools: ../../rados/operations/pools
.. _RBD – Manage RADOS Block Device (RBD) Images: ../../man/8/rbd/
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user
