=============================
 Deploying with ``mkcephfs``
=============================

Copy Configuration File to All Hosts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ceph's ``mkcephfs`` deployment script does not copy the configuration file you
created from the Administration host to the OSD Cluster hosts. Copy the
configuration file you created (*i.e.,* ``mycluster.conf`` in the example below)
from the Administration host to ``etc/ceph/ceph.conf`` on each OSD Cluster host
if you are using ``mkcephfs`` to deploy Ceph.

::

	ssh myserver01 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver02 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver03 sudo tee /etc/ceph/ceph.conf <mycluster.conf


Create the Default Directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``mkcephfs`` deployment script does not create the default server directories. 
Create server directories for each instance of a Ceph daemon. The ``host`` 
variables in the ``ceph.conf`` file determine which host runs each instance of 
a Ceph daemon. Using the exemplary ``ceph.conf`` file, you would perform 
the following:

On ``myserver01``::

	sudo mkdir srv/osd.0
	sudo mkdir srv/mon.a

On ``myserver02``::

	sudo mkdir srv/osd.1
	sudo mkdir srv/mon.b

On ``myserver03``::

	sudo mkdir srv/osd.2
	sudo mkdir srv/mon.c
	sudo mkdir srv/mds.a

Run ``mkcephfs``
~~~~~~~~~~~~~~~~
Once you have copied your Ceph Configuration to the OSD Cluster hosts
and created the default directories, you may deploy Ceph with the 
``mkcephfs`` script.

.. note::  ``mkcephfs`` is a quick bootstrapping tool. It does not handle more 
           complex operations, such as upgrades.

For production environments, deploy Ceph using Chef cookbooks. To run 
``mkcephfs``, execute the following:: 

   cd /etc/ceph
   sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring
	
The script adds an admin key to the ``ceph.keyring``, which is analogous to a 
root password. See `Authentication`_ when running with ``cephx`` enabled.


.. _Authentication: authentication