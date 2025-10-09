=============================
 Block Devices and OpenStack
=============================

.. index:: Ceph Block Device; OpenStack

You can attach Ceph Block Device images to OpenStack instances through ``libvirt``,
which configures the QEMU interface to ``librbd``. Ceph stripes block volumes
across multiple OSDs within the cluster, which means that large volumes can
realize better performance than local drives on a standalone server!

To use Ceph Block Devices with OpenStack, you must install QEMU, ``libvirt``,
and OpenStack first. We recommend using a separate physical node for your
OpenStack installation. OpenStack recommends a minimum of 8GB of RAM and a
quad-core processor. The following diagram depicts the OpenStack/Ceph
technology stack.


.. ditaa::

            +---------------------------------------------------+
            |                    OpenStack                      |
            +---------------------------------------------------+
            |                     libvirt                       |
            +------------------------+--------------------------+
                                     |
                                     | configures
                                     v
            +---------------------------------------------------+
            |                       QEMU                        |
            +---------------------------------------------------+
            |                      librbd                       |
            +---------------------------------------------------+
            |                     librados                      |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. important:: To use Ceph Block Devices with OpenStack, you must have
   access to a running Ceph Storage Cluster.

Three parts of OpenStack integrate with Ceph's block devices:

- **Images**: OpenStack Glance manages images for VMs. Images are immutable.
  OpenStack treats images as binary blobs and downloads them accordingly.

- **Volumes**: Volumes are block devices. OpenStack uses volumes to boot VMs,
  or to attach volumes to running VMs. OpenStack manages volumes using
  Cinder services.

- **Guest Disks**: Guest disks are guest operating system disks. By default,
  when you boot a virtual machine, its disk appears as a file on the file system
  of the hypervisor (usually under ``/var/lib/nova/instances/<uuid>/``). Prior
  to OpenStack Havana, the only way to boot a VM in Ceph was to use the
  boot-from-volume functionality of Cinder. However, now it is possible to boot
  every virtual machine inside Ceph directly without using Cinder, which is
  advantageous because it allows you to perform maintenance operations easily
  with the live-migration process. Additionally, if your hypervisor dies it is
  also convenient to trigger ``nova evacuate`` and reinstate the virtual machine
  elsewhere almost seamlessly. In doing so,
  :ref:`exclusive locks <rbd-exclusive-locks>` prevent multiple
  compute nodes from concurrently accessing the guest disk.


You can use OpenStack Glance to store images as Ceph Block Devices, and you
can use Cinder to boot a VM using a copy-on-write clone of an image.

The instructions below detail the setup for Glance, Cinder and Nova, although
they do not have to be used together. You may store images in Ceph block devices
while running VMs using a local disk, or vice versa.

.. important:: Using QCOW2 for hosting a virtual machine disk is NOT recommended.
   If you want to boot virtual machines in Ceph (ephemeral backend or boot
   from volume), please use the ``raw`` image format within Glance.

.. index:: pools; OpenStack

Create a Pool
=============

By default, Ceph block devices live within the ``rbd`` pool. You may use any
suitable pool by specifying it explicitly. We recommend creating a pool for
Cinder and a pool for Glance. Ensure your Ceph cluster is running, then create the pools. ::

    ceph osd pool create volumes
    ceph osd pool create images
    ceph osd pool create backups
    ceph osd pool create vms

See `Create a Pool`_ for detail on specifying the number of placement groups for
your pools, and `Placement Groups`_ for details on the number of placement
groups you should set for your pools.

Newly created pools must be initialized prior to use. Use the ``rbd`` tool
to initialize the pools::

        rbd pool init volumes
        rbd pool init images
        rbd pool init backups
        rbd pool init vms

.. _Create a Pool: ../../rados/operations/pools#createpool
.. _Placement Groups: ../../rados/operations/placement-groups


Configure OpenStack Ceph Clients
================================

The nodes running ``glance-api``, ``cinder-volume``, ``nova-compute`` and
``cinder-backup`` act as Ceph clients. Each requires the ``ceph.conf`` file::

  ssh {your-openstack-server} sudo tee /etc/ceph/ceph.conf </etc/ceph/ceph.conf


Install Ceph client packages
----------------------------

On the ``glance-api`` node, you will need the Python bindings for ``librbd``::

  sudo apt-get install python-rbd
  sudo yum install python-rbd

On the ``nova-compute``, ``cinder-backup`` and on the ``cinder-volume`` node,
use both the Python bindings and the client command line tools::

  sudo apt-get install ceph-common
  sudo yum install ceph-common


Setup Ceph Client Authentication
--------------------------------

If you have `cephx authentication`_ enabled, create a new user for Nova/Cinder
and Glance. Execute the following::

    ceph auth get-or-create client.glance mon 'profile rbd' osd 'profile rbd pool=images' mgr 'profile rbd pool=images'
    ceph auth get-or-create client.cinder mon 'profile rbd' osd 'profile rbd pool=volumes, profile rbd pool=vms, profile rbd-read-only pool=images' mgr 'profile rbd pool=volumes, profile rbd pool=vms'
    ceph auth get-or-create client.cinder-backup mon 'profile rbd' osd 'profile rbd pool=backups' mgr 'profile rbd pool=backups'

Add the keyrings for ``client.cinder``, ``client.glance``, and
``client.cinder-backup`` to the appropriate nodes and change their ownership::

  ceph auth get-or-create client.glance | ssh {your-glance-api-server} sudo tee /etc/ceph/ceph.client.glance.keyring
  ssh {your-glance-api-server} sudo chown glance:glance /etc/ceph/ceph.client.glance.keyring
  ceph auth get-or-create client.cinder | ssh {your-volume-server} sudo tee /etc/ceph/ceph.client.cinder.keyring
  ssh {your-cinder-volume-server} sudo chown cinder:cinder /etc/ceph/ceph.client.cinder.keyring
  ceph auth get-or-create client.cinder-backup | ssh {your-cinder-backup-server} sudo tee /etc/ceph/ceph.client.cinder-backup.keyring
  ssh {your-cinder-backup-server} sudo chown cinder:cinder /etc/ceph/ceph.client.cinder-backup.keyring

Nodes running ``nova-compute`` need the keyring file for the ``nova-compute``
process::

  ceph auth get-or-create client.cinder | ssh {your-nova-compute-server} sudo tee /etc/ceph/ceph.client.cinder.keyring

They also need to store the secret key of the ``client.cinder`` user in
``libvirt``. The libvirt process needs it to access the cluster while attaching
a block device from Cinder.

Create a temporary copy of the secret key on the nodes running
``nova-compute``::

  ceph auth get-key client.cinder | ssh {your-compute-node} tee client.cinder.key

Then, on the compute nodes, add the secret key to ``libvirt`` and remove the
temporary copy of the key::

  uuidgen
  457eb676-33da-42ec-9a8c-9293d545c337

  cat > secret.xml <<EOF
  <secret ephemeral='no' private='no'>
    <uuid>457eb676-33da-42ec-9a8c-9293d545c337</uuid>
    <usage type='ceph'>
      <name>client.cinder secret</name>
    </usage>
  </secret>
  EOF
  sudo virsh secret-define --file secret.xml
  Secret 457eb676-33da-42ec-9a8c-9293d545c337 created
  sudo virsh secret-set-value --secret 457eb676-33da-42ec-9a8c-9293d545c337 --base64 $(cat client.cinder.key) && rm client.cinder.key secret.xml

Save the uuid of the secret for configuring ``nova-compute`` later.

.. important:: You don't necessarily need the UUID on all the compute nodes.
   However from a platform consistency perspective, it's better to keep the
   same UUID.

.. _cephx authentication: ../../rados/configuration/auth-config-ref/#enabling-disabling-cephx


Configure OpenStack to use Ceph
===============================

Configuring Glance
------------------

Glance can use multiple back ends to store images. To use Ceph block devices by
default, configure Glance like the following.


Kilo and after
~~~~~~~~~~~~~~

Edit ``/etc/glance/glance-api.conf`` and add under the ``[glance_store]`` section::

    [glance_store]
    stores = rbd
    default_store = rbd
    rbd_store_pool = images
    rbd_store_user = glance
    rbd_store_ceph_conf = /etc/ceph/ceph.conf
    rbd_store_chunk_size = 8

For more information about the configuration options available in Glance please refer to the OpenStack Configuration Reference: http://docs.openstack.org/.

Enable copy-on-write cloning of images
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that this exposes the back end location via Glance's API, so the endpoint
with this option enabled should not be publicly accessible.

Any OpenStack version except Mitaka
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to enable copy-on-write cloning of images, also add under the ``[DEFAULT]`` section::

    show_image_direct_url = True

Disable cache management (any OpenStack version)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Disable the Glance cache management to avoid images getting cached under ``/var/lib/glance/image-cache/``,
assuming your configuration file has ``flavor = keystone+cachemanagement``::

    [paste_deploy]
    flavor = keystone

Image properties
~~~~~~~~~~~~~~~~

We recommend to use the following properties for your images:

- ``hw_scsi_model=virtio-scsi``: add the virtio-scsi controller and get better performance and support for discard operation
- ``hw_disk_bus=scsi``: connect every cinder block devices to that controller
- ``hw_qemu_guest_agent=yes``: enable the QEMU guest agent
- ``os_require_quiesce=yes``: send fs-freeze/thaw calls through the QEMU guest agent


Configuring Cinder
------------------

OpenStack requires a driver to interact with Ceph block devices. You must also
specify the pool name for the block device. On your OpenStack node, edit
``/etc/cinder/cinder.conf`` by adding::

    [DEFAULT]
    ...
    enabled_backends = ceph
    glance_api_version = 2
    ...
    [ceph]
    volume_driver = cinder.volume.drivers.rbd.RBDDriver
    volume_backend_name = ceph
    rbd_pool = volumes
    rbd_ceph_conf = /etc/ceph/ceph.conf
    rbd_flatten_volume_from_snapshot = false
    rbd_max_clone_depth = 5
    rbd_store_chunk_size = 4
    rados_connect_timeout = -1

If you are using `cephx authentication`_, also configure the user and uuid of
the secret you added to ``libvirt`` as documented earlier::

    [ceph]
    ...
    rbd_user = cinder
    rbd_secret_uuid = 457eb676-33da-42ec-9a8c-9293d545c337

Note that if you are configuring multiple cinder back ends,
``glance_api_version = 2`` must be in the ``[DEFAULT]`` section.


Configuring Cinder Backup
-------------------------

OpenStack Cinder Backup requires a specific daemon so don't forget to install it.
On your Cinder Backup node, edit ``/etc/cinder/cinder.conf`` and add::

    backup_driver = cinder.backup.drivers.ceph
    backup_ceph_conf = /etc/ceph/ceph.conf
    backup_ceph_user = cinder-backup
    backup_ceph_chunk_size = 134217728
    backup_ceph_pool = backups
    backup_ceph_stripe_unit = 0
    backup_ceph_stripe_count = 0
    restore_discard_excess_bytes = true


Configuring Nova to attach Ceph RBD block device
------------------------------------------------

In order to attach Cinder devices (either normal block or by issuing a boot
from volume), you must tell Nova (and libvirt) which user and UUID to refer to
when attaching the device. libvirt will refer to this user when connecting and
authenticating with the Ceph cluster. ::

    [libvirt]
    ...
    rbd_user = cinder
    rbd_secret_uuid = 457eb676-33da-42ec-9a8c-9293d545c337

These two flags are also used by the Nova ephemeral back end.


Configuring Nova
----------------

In order to boot virtual machines directly from Ceph volumes, you must
configure the ephemeral backend for Nova.

It is recommended to enable the RBD cache in your Ceph configuration file; this
has been enabled by default since the Giant release. Moreover, enabling the
client admin socket allows the collection of metrics and can be invaluable
for troubleshooting.

This socket can be accessed on the hypervisor (Nova compute) node::

    ceph daemon /var/run/ceph/ceph-client.cinder.19195.32310016.asok help

To enable RBD cache and admin sockets, ensure that on each hypervisor's
``ceph.conf`` contains::

    [client]
        rbd cache = true
        rbd cache writethrough until flush = true
        admin socket = /var/run/ceph/guests/$cluster-$type.$id.$pid.$cctid.asok
        log file = /var/log/qemu/qemu-guest-$pid.log
        rbd concurrent management ops = 20

Configure permissions for these directories::

    mkdir -p /var/run/ceph/guests/ /var/log/qemu/
    chown qemu:libvirtd /var/run/ceph/guests /var/log/qemu/

Note that user ``qemu`` and group ``libvirtd`` can vary depending on your system.
The provided example works for RedHat based systems.

.. tip:: If your virtual machine is already running you can simply restart it to enable the admin socket


Restart OpenStack
=================

To activate the Ceph block device driver and load the block device pool name
into the configuration, you must restart the related OpenStack services.
For Debian based systems execute these commands on the appropriate nodes::

    sudo glance-control api restart
    sudo service nova-compute restart
    sudo service cinder-volume restart
    sudo service cinder-backup restart

For Red Hat based systems execute::

    sudo service openstack-glance-api restart
    sudo service openstack-nova-compute restart
    sudo service openstack-cinder-volume restart
    sudo service openstack-cinder-backup restart

Once OpenStack is up and running, you should be able to create a volume
and boot from it.


Booting from a Block Device
===========================

You can create a volume from an image using the Cinder command line tool::

    cinder create --image-id {id of image} --display-name {name of volume} {size of volume}

You can use `qemu-img`_ to convert from one format to another. For example::

    qemu-img convert -f {source-format} -O {output-format} {source-filename} {output-filename}
    qemu-img convert -f qcow2 -O raw precise-cloudimg.img precise-cloudimg.raw

When Glance and Cinder are both using Ceph block devices, the image is a
copy-on-write clone, so new volumes are created quickly. In the OpenStack
dashboard, you can boot from that volume by performing the following steps:

#. Launch a new instance.
#. Choose the image associated to the copy-on-write clone.
#. Select 'boot from volume'.
#. Select the volume you created.

.. _qemu-img: ../qemu-rbd/#running-qemu-with-rbd
