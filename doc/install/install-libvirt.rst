=================
 Install libvirt
=================

To use ``libvirt`` with Ceph, you must have a running Ceph Storage Cluster, and
you must have installed and configured `QEMU`_.


Debian Packages
===============

``libvirt`` packages are incorporated into Ubuntu 12.04 Precise Pangolin and
later versions of Ubuntu. To install ``libvirt`` on these distributions,
execute the following:: 

	sudo apt-get update && sudo apt-get install libvirt-bin


RPM Packages
============

To use ``libvirt`` with a Ceph Storage Cluster, you must  have a running Ceph
Storage Cluster and you must also install a version of QEMU with ``rbd`` format
support.  See `QEMU`_ for details.


``libvirt`` packages are incorporated into the recent CentOS/RHEL distributions. 
To install ``libvirt``, execute the following:: 

	sudo yum install libvirt


Build ``libvirt``
=================

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
.. _QEMU: ../install-qemu
