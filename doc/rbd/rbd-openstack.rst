===================
 RBD and OpenStack
===================
You may utilize RBD with OpenStack. To use RBD with OpenStack, you must install
QEMU, ``libvirt``, and OpenStack first. We recommend using a separate physical
host for your OpenStack installation. OpenStack recommends a minimum of 
8GB of RAM and a quad-core processor. If you have not already installed
OpenStack, install it now. See `Installing OpenStack`_ for details.

.. _Installing OpenStack: ../../install/openstack

.. important:: To use RBD with OpenStack, you must have a running Ceph cluster.
.. tip: To get started quickly, turn off cephx authentication. 

Create a Pool
-------------
By default, RBD uses the ``data`` pool. You may use any available RBD pool. 
We recommend creating a pool for Nova. Ensure your Ceph cluster is running, 
then create a pool. ::

	sudo rados mkpool nova

Install Ceph Common on the OpenStack Host
-----------------------------------------
OpenStack operates as a Ceph client. You must install Ceph common on the 
OpenStack host, and copy your Ceph cluster's ``ceph.conf`` file to the 
``/etc/ceph`` directory. If you have installed Ceph on the host, Ceph common
is already included. :: 

	sudo apt-get install ceph-common
	cd /etc/ceph
	ssh your-openstack-server sudo tee /etc/ceph/ceph.conf <ceph.conf

Add the RBD Driver and the Pool Name to ``nova.conf``
-----------------------------------------------------
OpenStack requires a driver to interact with RADOS block devices. You must also
specify the pool name for the block device. On your OpenStack host, navigate to
the ``/etc/conf`` directory. Open the ``nova.conf`` file in a text editor using
sudo privileges and add the following lines to the file::

	volume_driver=nova.volume.driver.RBDDriver
	rbd_pool=nova

Restart OpenStack	
-----------------
To activate the RBD driver and load the RBD pool name into the configuration,
you must restart OpenStack. Navigate the directory where you installed 
OpenStack, and execute the following:: 

	./rejoin-stack.sh

If you have OpenStack configured as a service, you can also execute:: 

	sudo service nova-volume restart

Once OpenStack is up and running, you should be able to create a volume with 
OpenStack on a Ceph RADOS block device.