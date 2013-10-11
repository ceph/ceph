====================
 Installing libvirt
====================


Prerequisites
=============

- `Install`_ and `configure`_ a Ceph Storage Cluster
- `Install and configure`_ QEMU/KVM


Installing ``libvirt`` on Ubuntu 12.04 Precise
==============================================

``libvirt`` packages are incorporated into the Ubuntu 12.04 precise 
distribution. To install ``libvirt`` on precise, execute the following:: 

	sudo apt-get update && sudo apt-get install libvirt-bin


Installing ``libvirt`` on Earlier Versions of Ubuntu
====================================================

For Ubuntu distributions 11.10 oneiric and earlier, you must build  ``libvirt``
from source. Clone the ``libvirt`` repository, and use `AutoGen`_ to generate
the build. Then, execute ``make`` and ``make install`` to complete the
installation. For example::

	git clone git://libvirt.org/libvirt.git
	cd libvirt
	./autogen.sh
	make
	sudo make install 

See `libvirt Installation`_ for details.


.. _libvirt Installation: http://www.libvirt.org/compiling.html
.. _AutoGen: http://www.gnu.org/software/autogen/
.. _Install: ../index
.. _configure: ../../rados/configuration
.. _Install and configure: ../../rbd/qemu-rbd
