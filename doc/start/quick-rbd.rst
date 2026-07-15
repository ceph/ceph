==========================
 Block Device Quick Start
==========================

Ensure your :term:`Ceph Storage Cluster` is in an ``active + clean`` state
before working with the :term:`Ceph Block Device`.

.. note:: The Ceph Block Device is also known as :term:`RBD` or :term:`RADOS`
   Block Device.


.. ditaa::

           /------------------\         /----------------\
           |    Admin Node    |         |   ceph-client  |
           |                  +-------->+ cCCC           |
           |                  |         |      ceph      |
           \------------------/         \----------------/


You may use a virtual machine for your ``ceph-client`` node, but do not
execute the following procedures on the same physical node as your Ceph
Storage Cluster nodes (unless you use a VM).

Create a Block Device Pool
==========================

#. On the admin node, use the ``ceph`` tool to :ref:`create a pool <createpool>`
   (we recommend the name 'rbd').

#. On the admin node, use the ``rbd`` tool to initialize the pool for use by RBD:

   .. prompt:: bash $

      rbd pool init <pool-name>

Configure a Block Device
========================

#. On the ``ceph-client`` node, create a block device image.

   .. prompt:: bash $

      rbd create foo --size 40G [-m <mon-IP>] [-k /path/to/ceph.client.admin.keyring] [-p <pool-name>]

#. On the ``ceph-client`` node, map the image to a block device.

   .. prompt:: bash $

      sudo rbd map foo [-m <mon-IP>] [-k /path/to/ceph.client.admin.keyring] [-p <pool-name>]

#. Use the block device by creating a file system on the ``ceph-client``
   node.

   .. prompt:: bash $

      sudo mkfs.ext4 -m0 /dev/rbd/<pool-name>/foo

   This may take a few moments.

#. Mount the file system on the ``ceph-client`` node.

   .. prompt:: bash $

      sudo mkdir /mnt/ceph-block-device
      sudo mount /dev/rbd/<pool-name>/foo /mnt/ceph-block-device
      cd /mnt/ceph-block-device

#. Optionally configure the block device to be automatically mapped and mounted
   at boot (and unmounted/unmapped at shutdown) - see the :ref:`rbdmap manpage <rbdmap>`.

See :ref:`ceph_block_device` for additional details.

