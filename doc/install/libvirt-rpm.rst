====================
 Installing libvirt
====================

To use ``libvirt`` with a Ceph Storage Cluster, you must 
have a running Ceph Storage Cluster. You must also install QEMU. 
See `Installing QEMU`_ for details. 


``libvirt`` packages are incorporated into the recent CentOS/RHEL distributions. 
To install ``libvirt``, execute the following:: 

	sudo yum install libvirt

See `libvirt Installation`_ for details.


.. _libvirt Installation: http://www.libvirt.org/compiling.html
.. _Installing QEMU: ../qemu-rpm