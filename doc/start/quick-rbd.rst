==========================
 Block Device Quick Start
==========================

To use this guide, you must have executed the procedures in the `Storage
Cluster Quick Start`_ guide first. Ensure your :term:`Ceph Storage Cluster` is
in an ``active + clean`` state before working with the :term:`Ceph Block
Device`. 

.. note:: The Ceph Block Device is also known as :term:`RBD` or :term:`RADOS`
   Block Device.


.. ditaa:: 
           /------------------\         /----------------\
           |    Admin Node    |         |   ceph-client  |
           |                  +-------->+ cCCC           |
           |    ceph-deploy   |         |      ceph      |
           \------------------/         \----------------/


You may use a virtual machine for your ``ceph-client`` node, but do not 
execute the following procedures on the same physical node as your Ceph 
Storage Cluster nodes (unless you use a VM). See `FAQ`_ for details.


Install Ceph
============

#. Verify that you have an appropriate version of the Linux kernel. 
   See `OS Recommendations`_ for details. ::
   
	lsb_release -a
	uname -r

#. On the admin node, use ``ceph-deploy`` to install Ceph on your 
   ``ceph-client`` node. ::

	ceph-deploy install ceph-client
	
#. On the admin node, use ``ceph-deploy`` to copy the Ceph configuration file
   and the ``ceph.client.admin.keyring`` to the ``ceph-client``. :: 

	ceph-deploy admin ceph-client

   The ``ceph-deploy`` utility copies the keyring to the ``/etc/ceph`` 
   directory. Ensure that the keyring file has appropriate read permissions 
   (e.g., ``sudo chmod +r /etc/ceph/ceph.client.admin.keyring``).

Create an rbd pool
==================
#. On the admin node, use the ``ceph`` tool to `Create a Pool`_
   (we recommend the name 'rbd').

Configure a Block Device
========================

#. On the ``ceph-client`` node, create a block device image. :: 

	rbd create foo --size 4096 [-m {mon-IP}] [-k /path/to/ceph.client.admin.keyring]

#. On the ``ceph-client`` node, map the image to a block device. :: 

	sudo rbd map foo --name client.admin [-m {mon-IP}] [-k /path/to/ceph.client.admin.keyring]
	
#. Use the block device by creating a file system on the ``ceph-client`` 
   node. :: 

	sudo mkfs.ext4 -m0 /dev/rbd/rbd/foo
	
	This may take a few moments.
	
#. Mount the file system on the ``ceph-client`` node. ::

	sudo mkdir /mnt/ceph-block-device
	sudo mount /dev/rbd/rbd/foo /mnt/ceph-block-device
	cd /mnt/ceph-block-device

#. Optionally configure the block device to be automatically mapped and mounted
   at boot (and unmounted/unmapped at shutdown) - see the `rbdmap manpage`_.


See `block devices`_ for additional details.

.. _Create a Pool: ../../rados/operations/pools#createpool
.. _Storage Cluster Quick Start: ../quick-ceph-deploy
.. _block devices: ../../rbd/rbd
.. _FAQ: http://wiki.ceph.com/How_Can_I_Give_Ceph_a_Try
.. _OS Recommendations: ../os-recommendations
.. _rbdmap manpage: ../../man/8/rbdmap
