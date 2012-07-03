==============================
 RBD Kernel Object Operations
==============================

.. important:: To use kernel object operations, you must have a running Ceph cluster.

Load the Ceph RBD Module
------------------------

To map an RBD image to a kernel object, first load the Ceph RBD module:: 

	modprobe rbd

Get a List of RBD Images
------------------------

To mount an RBD image, first return a list of the images. ::

	rbd list

Map a Block Device with ``rbd``
-------------------------------

Use ``rbd`` to map an image name to a kernel object. You must specify the 
image name, the pool name, and the client name. If you use ``cephx`` 
authentication, you must also specify a secret. ::

	sudo rbd map {image-name} --pool {pool-name} --name {client-name} --secret {client-secret}	

For example:: 

 sudo rbd map foo --pool rbd --name client.admin
 
If you use ``cephx`` authentication, you must also specify a secret. ::

	echo "10.20.30.40  name=admin,secret=/path/to/secret rbd foo" | sudo tee /sys/bus/rbd/add 

Map a Block Device with ``add``
-------------------------------

To map an RBD image to a kernel object directly, enter the IP address of
the monitor, the user name, and the RBD image name as follows:: 

	echo "{mon-ip-address}  name={user-name} rbd {image-name}" | sudo tee /sys/bus/rbd/add
	
For example:: 

	echo "10.20.30.40  name=admin rbd foo" | sudo tee /sys/bus/rbd/add	
	
If you use ``cephx`` authentication, you must also specify a secret. ::

	echo "10.20.30.40  name=admin,secret=/path/to/secret rbd foo" | sudo tee /sys/bus/rbd/add

A kernel block device resides under the ``/sys/bus/rbd/devices`` directory and
provides the following functions: 

+------------------+------------------------------------------------------------+
| Function         | Description                                                |
+==================+============================================================+
| ``client_id``    | Returns the client ID of the given device ID.              |
+------------------+------------------------------------------------------------+
| ``create_snap``  | Creates a snap from a snap name and a device ID.           |
+------------------+------------------------------------------------------------+
| ``current_snap`` | Returns the most recent snap for the given device ID.      |
+------------------+------------------------------------------------------------+
| ``major``        |                                                            |
+------------------+------------------------------------------------------------+
| ``name``         | Returns the RBD image name of the device ID.               |
+------------------+------------------------------------------------------------+
| ``pool``         | Returns the pool source of the device ID.                  |
+------------------+------------------------------------------------------------+
| ``refresh``      | Refreshes the given device with the SDs.                   |
+------------------+------------------------------------------------------------+
| ``size``         | Returns the size of the device.                            |
+------------------+------------------------------------------------------------+
| ``uevent``       |                                                            |
+------------------+------------------------------------------------------------+


Show Mapped Block Devices
-------------------------

To show RBD images mapped to kernel block devices with the ``rbd`` command, 
specify the ``showmapped`` option. ::

	sudo rbd showmapped

Images are mounted as devices sequentially starting from ``0``. To list all 
devices mapped to kernel objects, execute the following:: 

	ls /sys/bus/rbd/devices	

Unmapping a Block Device
------------------------	

To unmap an RBD image with the ``rbd`` command, specify the ``rm`` option 
and the device name (i.e., by convention the same as the RBD image name). :: 

	sudo rbd unmap /dev/rbd/{poolname}/{imagename}
	
For example::

	sudo rbd unmap /dev/rbd/rbd/foo

To unmap an RBD image from a kernel object, specify its index and use ``tee`` 
to call ``remove`` as follows, but replace ``{device-number}`` with the number 
of the device you want to remove:: 

	echo {device-number} | sudo tee /sys/bus/rbd/remove
	
