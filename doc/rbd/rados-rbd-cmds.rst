=============================
 Basic Block Device Commands
=============================

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

#. Use the ``ceph`` tool to `create a pool`_.

#. Use the ``rbd`` tool to initialize the pool for use by RBD:

   .. prompt:: bash $

      rbd pool init <pool-name>

   .. note:: The ``rbd`` tool assumes a default pool name of 'rbd' if no pool
      name is specified in the command.


Create a Block Device User
==========================

Unless otherwise specified, the ``rbd`` command uses the Ceph user ID ``admin``
to access the Ceph cluster. The ``admin`` Ceph user ID allows full
administrative access to the cluster. We recommend that you acess the Ceph
cluster with a Ceph user ID that has fewer permissions than the ``admin`` Ceph
user ID does. We call this non-``admin`` Ceph user ID a "block device user" or
"Ceph user".

To `create a Ceph user`_, use the ``ceph auth get-or-create`` command to
specify the Ceph user ID name, monitor caps (capabilities), and OSD caps
(capabilities):

.. prompt:: bash $

   ceph auth get-or-create client.{ID} mon 'profile rbd' osd 'profile {profile name} [pool={pool-name}][, profile ...]' mgr 'profile rbd [pool={pool-name}]'

For example: to create a Ceph user ID named ``qemu`` that has read-write access
to the pool ``vms`` and read-only access to the pool ``images``, run the
following command:

.. prompt:: bash $

   ceph auth get-or-create client.qemu mon 'profile rbd' osd 'profile rbd pool=vms, profile rbd-read-only pool=images' mgr 'profile rbd pool=images'

The output from the ``ceph auth get-or-create`` command is the keyring for the
specified Ceph user ID, which can be written to
``/etc/ceph/ceph.client.{ID}.keyring``.

.. note:: Specify the Ceph user ID by providing the ``--id {id} argument when
   using the ``rbd`` command. This argument is optional. 

Creating a Block Device Image
=============================

Before you can add a block device to a node, you must create an image for it in
the :term:`Ceph Storage Cluster`. To create a block device image, run a command of this form: 

.. prompt:: bash $

   rbd create --size {megabytes} {pool-name}/{image-name}

For example, to create a 1GB image named ``bar`` that stores information in a
pool named ``swimmingpool``, run this command:

.. prompt:: bash $

   rbd create --size 1024 swimmingpool/bar

If you don't specify a pool when you create an image, then the image will be
stored in the default pool ``rbd``. For example, if you ran this command, you
would create a 1GB image named ``foo`` that is stored in the default pool
``rbd``:

.. prompt:: bash $

   rbd create --size 1024 foo

.. note:: You must create a pool before you can specify it as a source. See
   `Storage Pools`_ for details.

Listing Block Device Images
===========================

To list block devices in the ``rbd`` pool, run the following command:

.. prompt:: bash $

   rbd ls

.. note:: ``rbd`` is the default pool name, and ``rbd ls`` lists the commands
   in the default pool.

To list block devices in a particular pool, run the following command, but
replace ``{poolname}`` with the name of the pool: 

.. prompt:: bash $

   rbd ls {poolname}
	
For example:

.. prompt:: bash $

   rbd ls swimmingpool

To list "deferred delete" block devices in the ``rbd`` pool, run the
following command:

.. prompt:: bash $

   rbd trash ls

To list "deferred delete" block devices in a particular pool, run the 
following command, but replace ``{poolname}`` with the name of the pool:

.. prompt:: bash $

   rbd trash ls {poolname}

For example:

.. prompt:: bash $

   rbd trash ls swimmingpool

Retrieving Image Information
============================

To retrieve information from a particular image, run the following command, but
replace ``{image-name}`` with the name for the image:

.. prompt:: bash $

   rbd info {image-name}
	
For example:

.. prompt:: bash $

   rbd info foo
	
To retrieve information from an image within a pool, run the following command,
but replace ``{image-name}`` with the name of the image and replace
``{pool-name}`` with the name of the pool:

.. prompt:: bash $

   rbd info {pool-name}/{image-name}

For example:

.. prompt:: bash $

   rbd info swimmingpool/bar

.. note:: Other naming conventions are possible, and might conflict with the
   naming convention described here. For example, ``userid/<uuid>`` is a
   possible name for an RBD image, and such a name might (at the least) be
   confusing. 

Resizing a Block Device Image
=============================

:term:`Ceph Block Device` images are thin provisioned. They don't actually use
any physical storage until you begin saving data to them. However, they do have
a maximum capacity that you set with the ``--size`` option. If you want to
increase (or decrease) the maximum size of a Ceph Block Device image, run one
of the following commands:

Increasing the Size of a Block Device Image
-------------------------------------------

.. prompt:: bash $

   rbd resize --size 2048 foo

Decreasing the Size of a Block Device Image
-------------------------------------------

.. prompt:: bash $

   rbd resize --size 2048 foo --allow-shrink


Removing a Block Device Image
=============================

To remove a block device, run the following command, but replace
``{image-name}`` with the name of the image you want to remove:

.. prompt:: bash $

   rbd rm {image-name}

For example:

.. prompt:: bash $

   rbd rm foo

Removing a Block Device from a Pool
-----------------------------------

To remove a block device from a pool, run the following command but replace
``{image-name}`` with the name of the image to be removed, and replace
``{pool-name}`` with the name of the pool from which the image is to be
removed:

.. prompt:: bash $

   rbd rm {pool-name}/{image-name}

For example:

.. prompt:: bash $

   rbd rm swimmingpool/bar

"Defer Deleting" a Block Device from a Pool
-------------------------------------------

To defer delete a block device from a pool (which entails moving it to the
"trash" and deleting it later), run the following command but replace
``{image-name}`` with the name of the image to be moved to the trash and
replace ``{pool-name}`` with the name of the pool:

.. prompt:: bash $

   rbd trash mv {pool-name}/{image-name}

For example:

.. prompt:: bash $

   rbd trash mv swimmingpool/bar

Removing a Deferred Block Device from a Pool
--------------------------------------------

To remove a deferred block device from a pool, run the following command but
replace ``{image-id}`` with the ID of the image to be removed, and replace
``{pool-name}`` with the name of the pool from which the image is to be
removed:

.. prompt:: bash $

   rbd trash rm {pool-name}/{image-id}

For example:

.. prompt:: bash $
   
   rbd trash rm swimmingpool/2bf4474b0dc51

.. note::

  * You can move an image to the trash even if it has snapshot(s) or is
    actively in use by clones. However, you cannot remove it from the trash
    under those conditions.

  * You can use ``--expires-at`` to set the deferment time (default is
    ``now``). If the deferment time has not yet arrived, you cannot remove the
    image unless you use ``--force``.

Restoring a Block Device Image
==============================

To restore a deferred delete block device in the rbd pool, run the 
following command but replace ``{image-id}`` with the ID of the image:

.. prompt:: bash $

   rbd trash restore {image-id}

For example:

.. prompt:: bash $

   rbd trash restore 2bf4474b0dc51

Restoring a Block Device Image in a Specific Pool
-------------------------------------------------

To restore a deferred delete block device in a particular pool, run the
following command but replace ``{image-id}`` with the ID of the image and
replace ``{pool-name}`` with the name of the pool:

.. prompt:: bash $

  rbd trash restore {pool-name}/{image-id}

For example:

.. prompt:: bash $

   rbd trash restore swimmingpool/2bf4474b0dc51


Renaming an Image While Restoring It
------------------------------------

You can also use ``--image`` to rename the image while restoring it. 

For example:

.. prompt:: bash $
   
   rbd trash restore swimmingpool/2bf4474b0dc51 --image new-name


.. _create a pool: ../../rados/operations/pools/#create-a-pool
.. _Storage Pools: ../../rados/operations/pools
.. _RBD – Manage RADOS Block Device (RBD) Images: ../../man/8/rbd/
.. _create a Ceph user: ../../rados/operations/user-management#add-a-user
