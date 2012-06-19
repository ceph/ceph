==============================
 RBD Kernel Object Operations
==============================

.. important:: To use kernel object operations, you must have a running Ceph cluster.

Add a Block Device
------------------
To add an RBD image as a kernel object, first load the Ceph RBD module:: 

	modprobe rbd

Map the RBD image to the kernel object with ``add``, specifying the IP address 
of the monitor, the user name, and the RBD image name as follows:: 

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


List Block Devices
------------------
Images are mounted as devices sequentially starting from ``0``. To list the 
devices mounted, execute the following:: 

	ls /sys/bus/rbd/devices	


Removing a Block Device
-----------------------	
To remove an RBD image, specify its index and use ``tee`` to call ``remove`` as
follows, but replace ``{device-number}`` with the number of the device you want
to remove:: 

	echo {device-number} | sudo tee /sys/bus/rbd/remove	


Creating a Snapshot
-------------------
To create a snapshot of a device, you must specify the device number. ::

	echo sn1 | sudo tee /sys/bus/rbd/devices/0{device-number}/create_snap
	
