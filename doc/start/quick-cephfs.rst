====================
 CephFS Quick Start
====================

To use this guide, you must have executed the procedures in the `5-minute
Quick Start`_ guide first. Execute this quick start on the client machine.


Kernel Driver
=============

Mount Ceph FS as a kernel driver. :: 

	sudo mkdir /mnt/mycephfs
	sudo mount -t ceph {ip-address-of-monitor}:6789:/ /mnt/mycephfs


.. note:: Mount the CephFS filesystem on the client machine,
   not the cluster machine. See `FAQ`_ for details.


Filesystem in User Space (FUSE)
===============================

Mount Ceph FS as with FUSE. Replace {username} with your username. ::

	sudo mkdir /home/{username}/cephfs
	sudo ceph-fuse -m {ip-address-of-monitor}:6789 /home/{username}/cephfs


Additional Information
======================

See `CephFS`_ for additional information. CephFS is not quite as stable
as the block device and the object storage gateway. Contact `Inktank`_ for 
details on running CephFS in a production environment.

.. _5-minute Quick Start: ../quick-start
.. _CephFS: ../../cephfs/
.. _Inktank: http://inktank.com
.. _FAQ: ../../faq#try-ceph
