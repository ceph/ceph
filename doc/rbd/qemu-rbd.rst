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

   
.. _QEMU Open Source Processor Emulator: http://wiki.qemu.org/Main_Page
.. _QEMU Manual: http://wiki.qemu.org/Manual
