=================================
 Using ``libvirt`` with Ceph RBD
=================================

The ``libvirt`` library creates a virtual machine abstraction layer between 
hypervisor interfaces and the software applications that use them. With 
``libvirt``, developers and system administrators can focus on a common 
management framework, common API, and common shell interface (i.e., ``virsh``)
to many different hypervisors, including: 

- QEMU/KVM
- XEN
- LXC
- VirtualBox
- etc.

Ceph RADOS block devices support QEMU/KVM, which means you can use RADOS
block devices with software that interfaces with ``libvirt``. For example, 
OpenStack's integration to Ceph uses ``libvirt`` to interact with QEMU/KVM, 
and QEMU/KVM interacts with RADOS block devices via ``librbd``.

See `libvirt Virtualization API`_ for details.

Installing ``libvirt`` on Ubuntu 12.04 Precise
----------------------------------------------

``libvirt`` packages are incorporated into the Ubuntu 12.04 precise 
distribution. To install ``libvirt`` on precise, execute the following:: 

	sudo apt-get update && sudo apt-get install libvirt-bin


Installing ``libvirt`` on Earlier Versions of Ubuntu
----------------------------------------------------

For Ubuntu distributions 11.10 oneiric and earlier, you must build 
``libvirt`` from source. Clone the ``libvirt`` repository, and use
`AutoGen`_ to generate the build. Then execute ``make`` and
``make install`` to complete the installation. For example::

	git clone git://libvirt.org/libvirt.git
	cd libvirt
	./autogen.sh
	make
	sudo make install 

See `libvirt Installation`_ for details.

.. _AutoGen: http://www.gnu.org/software/autogen/
.. _libvirt Installation: http://www.libvirt.org/compiling.html
.. _libvirt Virtualization API: http://www.libvirt.org