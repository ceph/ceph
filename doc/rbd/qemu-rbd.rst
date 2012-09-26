==============
 QEMU and RBD
==============

Ceph integrates with the QEMU virtual machine. For details on QEMU, see 
`QEMU Open Source Processor Emulator`_. For QEMU documentation, see
`QEMU Manual`_. 

.. important:: To use RBD with QEMU, you must have a running Ceph cluster.
   
Installing QEMU on Ubuntu 12.04 Precise
---------------------------------------
QEMU packages are incorporated into the Ubuntu 12.04 precise distribution. To 
install QEMU on precise, execute the following:: 

	sudo apt-get install qemu

Installing QEMU on Earlier Versions of Ubuntu
---------------------------------------------
For Ubuntu distributions 11.10 oneiric and earlier, you must install 
the 0.15 version of QEMU or later. To build QEMU from source, use the
following procedure::

	cd {your-development-directory}
	git clone git://git.qemu.org/qemu.git
	cd qemu
	./configure --enable-rbd
	make; make install

Creating RBD Images with QEMU
-----------------------------
You can create an RBD image from QEMU. You must specify ``rbd``, 
the pool name, and the name of the image you wish to create. You must also
specify the size of the image. ::

	qemu-img create -f rbd rbd:{pool-name}/{image-name} {size}

For example::

	qemu-img create -f rbd rbd:data/foo 10G

Resizing RBD Images with QEMU
-----------------------------
You can resize an RBD image from QEMU. You must specify ``rbd``, 
the pool name, and the name of the image you wish to resize. You must also
specify the size of the image. ::

	qemu-img resize -f rbd rbd:{pool-name}/{image-name} {size}

For example::

	qemu-img resize -f rbd rbd:data/foo 10G


Retrieving RBD Image Information with QEMU
------------------------------------------
You can retrieve RBD image information from QEMU. You must 
specify ``rbd``, the pool name, and the name of the image. ::

	qemu-img info -f rbd rbd:{pool-name}/{image-name}

For example::

	qemu-img info -f rbd rbd:data/foo


Running QEMU with RBD
---------------------

QEMU can pass a block device from the host on to a guest, but since
QEMU 0.15, there's no need to map an RBD image as a block device on
the host. Instead, QEMU can access an RBD image as a virtual block
device directly via ``librbd``. This performs better because it avoids
an additional context switch, and can take advantage of `RBD caching`_.

You can use ``qemu-img`` to convert existing virtual machine images to RBD.
For example, if you have a qcow2 image, you could run::

    qemu-img convert -f qcow2 -O rbd debian_squeeze.qcow2 rbd:data/squeeze

To run a virtual machine booting from that image, you could run::

    qemu -m 1024 -drive format=raw,file=rbd:data/squeeze

Using `RBD caching`_ can significantly improve performance.
Since QEMU 1.2, QEMU's cache options control RBD caching::

    qemu -m 1024 -drive format=rbd,file=rbd:data/squeeze,cache=writeback

If you have an older version of QEMU, you can set the RBD cache
configuration (like any Ceph configuration option) as part of the
'file' parameter::

    qemu -m 1024 -drive format=raw,file=rbd:data/squeeze:rbd_cache=true

.. _RBD caching: ../../config-cluster/rbd-config-ref/#rbd-cache-config-settings


Enabling discard/TRIM
---------------------

Since Ceph version 0.46 and QEMU version 1.1, RBD supports the discard
operation. This means that a guest can send TRIM requests to let RBD
reclaim unused space. This can be enabled in the guest by mounting
ext4 or xfs with the ``discard`` option.

For this to be available to the guest, it must be explicitly enabled
for the block device. To do this, you must specify a
discard_granularity associated with the drive::

    qemu -m 1024 -drive format=raw,file=rbd:data/squeeze,id=drive1,if=none \
         -device driver=ide-hd,drive=drive1,discard_granularity=512

Note that this uses the IDE driver. The virtio driver does not
support discard.


.. _QEMU Open Source Processor Emulator: http://wiki.qemu.org/Main_Page
.. _QEMU Manual: http://wiki.qemu.org/Manual
