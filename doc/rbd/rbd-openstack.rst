===================
 RBD and OpenStack
===================

You may use Ceph block device images with OpenStack with QEMU and ``libvirt`` as
the interface. Ceph stripes block device images as objects across the  cluster,
which means that large Ceph block device images have better  performance than a
standalone server!

To use RBD with OpenStack, you must install QEMU, ``libvirt``, and OpenStack
first. We recommend using a separate physical host for your OpenStack
installation. OpenStack recommends a minimum of  8GB of RAM and a quad-core
processor. The following diagram depicts the OpenStack/Ceph technology stack.


.. ditaa::  +---------------------------------------------------+
            |                    OpenStack                      |
            +---------------------------------------------------+            
            |                     libvirt                       |
            +---------------------------------------------------+            
            |                     QEMU/RBD                      |
            +---------------------------------------------------+
            |                      librbd                       |
            +---------------------------------------------------+
            |                     librados                      |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. _Installing OpenStack: ../../install/openstack

.. important:: To use RBD with OpenStack, you must have a running Ceph cluster.

There are two parts of OpenStack integrated with RBD: images and
volumes. In OpenStack, images are templates for VM images, and are
managed by Glance, the OpenStack image service. Volumes are block
devices that can be used to run VMs. These are managed by the
nova-volume (prior to Folsom) or Cinder (post-Folsom).

RBD is integrated into each of these components, so you can store
images in RBD through Glance, and then boot off of copy-on-write
clones of them created by Cinder or nova-volume (since OpenStack
Folsom and Ceph 0.52).

The instructions below detail the setup for both Glance and Nova,
although they do not have to be used together. You could store images
in RBD while running VMs off of local disk, or vice versa.

Create a Pool
=============

By default, RBD uses the ``rbd`` pool. You may use any available pool.
We recommend creating a pool for Nova/Cinder and a pool for Glance.
Ensure your Ceph cluster is running, then create the pools. ::

    ceph osd pool create volumes
    ceph osd pool create images

See `Create a Pool`_ for detail on specifying the number of placement groups
for your pools, and `Placement Groups`_ for details on the number of placement
groups you should set for your pools.

If you have `cephx authentication`_ enabled, create a new user
for Nova/Cinder and Glance::

    ceph auth get-or-create client.volumes mon 'allow r' osd 'allow rwx pool=volumes, allow rx pool=images'
    ceph auth get-or-create client.images mon 'allow r' osd 'allow rwx pool=images'

.. _Create a Pool: ../../cluster-ops/pools#createpool
.. _Placement Groups: ../../cluster-ops/placement-groups
.. _cephx authentication: ../../cluster-ops/authentication

Configure OpenStack ceph clients
================================

The hosts running glance-api, nova-compute, and
nova-volume/cinder-volume act as Ceph clients. Each requires
the ``ceph.conf`` file::

  ssh your-openstack-server sudo tee /etc/ceph/ceph.conf </etc/ceph/ceph.conf

Install Ceph client packages
----------------------------

On the glance-api host, you'll need the python bindings for librbd::

  sudo apt-get install python-ceph

On the nova-volume or cinder host, the client command line tools are
used::

  sudo apt-get install ceph-common

Setup Ceph client authentication
--------------------------------

If you're using cephx authentication, add the keyrings for client.volumes
and client.images to the appropriate hosts and change their ownership::

  ceph auth get-or-create client.images | ssh your-glance-api-server sudo tee /etc/ceph/ceph.client.images.keyring
  ssh your-glance-api-server sudo chown glance:glance /etc/ceph/ceph.client.images.keyring
  ceph auth get-or-create client.volumes | ssh your-volume-server sudo tee /etc/ceph/ceph.client.volumes.keyring
  ssh your-volume-server sudo chown cinder:cinder /etc/ceph/ceph.client.volumes.keyring

Hosts running nova-compute do not need the keyring. Instead, they
store the secret key in libvirt. Create a temporary copy of the secret
key on the hosts running nova-compute::

  ssh your-compute-host client.volumes.key <`ceph auth get-key client.volumes`

Then, on the compute hosts, add the secret key to libvirt and remove
the temporary copy of the key::

  cat > secret.xml <<EOF
  <secret ephemeral='no' private='no'>
    <usage type='ceph'>
      <name>client.volumes secret</name>
    </usage>
  </secret>
  EOF
  sudo virsh secret-define --file secret.xml
  <uuid of secret is output here>
  sudo virsh secret-set-value --secret {uuid of secret} --base64 $(cat client.volumes.key) && rm client.volumes.key secret.xml

Save the uuid of the secret for configuring nova-compute later.

Finally, on each host running cinder-volume or nova-volume, add
``CEPH_ARGS="--id volumes"`` to the init script or other place that
starts it.

Configure OpenStack to use RBD
==============================

Configuring Glance
------------------
Glance can use multiple backends to store images. To use RBD by
default, edit ``/etc/glance/glance-api.conf`` and add::

    default_store=rbd
    rbd_store_user=images
    rbd_store_pool=images

If you're using Folsom and want to enable copy-on-write cloning of
images into volumes, also add::

    show_image_direct_url=True

Note that this exposes the backend location via Glance's api, so the
endpoint with this option enabled should not be publicly accessible.

Configuring Cinder/nova-volume
------------------------------
OpenStack requires a driver to interact with RADOS block devices. You must also
specify the pool name for the block device. On your OpenStack host, navigate to
the ``/etc/cinder`` directory. Open the ``cinder.conf`` file in a text editor using
sudo privileges and add the following lines to the file::

	volume_driver=cinder.volume.driver.RBDDriver
	rbd_pool=volumes

If you're not using cinder, replace cinder with nova in the previous section.

If you're using `cephx authentication`_, also configure the user and
uuid of the secret you added to libvirt earlier::

    rbd_user=volumes
    rbd_secret_uuid={uuid of secret}

Restart OpenStack
=================

To activate the RBD driver and load the RBD pool name into the configuration,
you must restart OpenStack. Navigate the directory where you installed 
OpenStack, and execute the following:: 

	./rejoin-stack.sh

If you have OpenStack configured as a service, you can also execute:: 

	sudo service nova-volume restart

Once OpenStack is up and running, you should be able to create a volume with 
OpenStack on a Ceph RADOS block device.

Booting from RBD
================

If you're using OpenStack Folsom or later, you can create a volume
from an image using the cinder command line tool::

    cinder create --image-id {id of image} --display-name {name of volume} {size of volume}

Before Ceph 0.52 this will be a full copy of the data, but in 0.52 and
later when Glance and Cinder are both using RBD this is a
copy-on-write clone, so volume creation is very fast.

In the OpenStack dashboard you can then boot from that volume by
launching a new instance, choosing the image that you created the
volume from, and selecting 'boot from volume' and the volume you
created.
