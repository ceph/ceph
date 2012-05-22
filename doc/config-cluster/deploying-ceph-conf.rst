==================================
 Deploying the Ceph Configuration
==================================
Ceph's ``mkcephfs`` deployment script does not copy the configuration file you
created from the Administration host to the OSD Cluster hosts. Copy the
configuration file you created (*i.e.,* ``mycluster.conf`` in the example below)
from the Administration host to ``etc/ceph/ceph.conf`` on each OSD Cluster host
if you are using ``mkcephfs`` to deploy Ceph.

::

	ssh myserver01 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver02 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver03 sudo tee /etc/ceph/ceph.conf <mycluster.conf

The current deployment script does not create the default server directories. Create
server directories for each instance of a Ceph daemon. Using the exemplary 
``ceph.conf`` file, you would perform the following:

On ``myserver01``::

	mkdir srv/osd.0
	mkdir srv/mon.a

On ``myserver02``::

	mkdir srv/osd.1
	mkdir srv/mon.b

On ``myserver03``::

	mkdir srv/osd.2
	mkdir srv/mon.c

On ``myserver04``::

	mkdir srv/osd.3

.. important:: The ``host`` variable determines which host runs each instance of a Ceph daemon.
