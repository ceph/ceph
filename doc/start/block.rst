=====================
 Starting to use RBD
=====================

Introduction
============

`RBD` is the block device component of Ceph. It provides a block
device interface to a Linux machine, while striping the data across
multiple `RADOS` objects for improved performance. For more
information, see :ref:`rbd`.


Installation
============

To use `RBD`, you need to install a Ceph cluster. Follow the
instructions in :doc:`/ops/install/index`. Continue with these
instructions once you have a healthy cluster running.


Setup
=====

The default `pool` used by `RBD` is called ``rbd``. It is created for
you as part of the installation. If you wish to use multiple pools,
for example for access control, see :ref:`create-new-pool`.

First, we need a ``client`` key that is authorized to access the right
pool. Follow the instructions in :ref:`add-new-key`. Let's set the
``id`` of the key to be ``bar``. You could set up one key per machine
using `RBD`, or let them share a single key; your call. Make sure the
keyring containing the new key is available on the machine.

Then, authorize the key to access the new pool. Follow the
instructions in :ref:`auth-pool`.


Usage
=====

`RBD` can be accessed in two ways:

- as a block device on a Linux machine
- via the ``rbd`` network storage driver in Qemu/KVM


.. rubric:: Example: As a block device

Using the ``client.bar`` key you set up earlier, we can create an RBD
image called ``tengigs``::

	rbd --name=client.bar create --size=10240 tengigs

And then make that visible as a block device::

	touch secretfile
	chmod go= secretfile
	cauthtool --name=bar --print-key /etc/ceph/client.bar.keyring >secretfile
	rbd map tengigs --user bar --secret secretfile

.. todo:: the secretfile part is really clumsy

For more information, see :doc:`rbd </man/8/rbd>`\(8).


.. rubric:: Example: As a Qemu/KVM storage driver via Libvirt

You'll need ``kvm`` v0.15, and ``libvirt`` v0.8.7 or newer.

Create the RBD image as above, and then refer to it in the ``libvirt``
virtual machine configuration::

    <disk type='network' device='disk'>
      <source protocol='rbd' name='rbd/tengigs'>
          <host name='10.0.0.101' port='6789'/>
          <host name='10.0.0.102' port='6789'/>
          <host name='10.0.0.103' port='6789'/>
      </source>
      <target dev='vda' bus='virtio'/>
    </disk

.. todo:: use secret keys

.. todo:: ceph.conf usage for mon addresses

.. todo:: pending libvirt xml schema changes
