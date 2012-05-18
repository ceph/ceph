====================
 Starting a Cluster
====================
To start your Ceph cluster, execute the ``ceph`` with the ``start`` command. 
The usage may differ based upon your Linux distribution. For example, for most
newer Debian/Ubuntu distributions, you may use the following syntax:: 

	sudo service ceph start [options] [start|restart] [daemonType|daemonID]

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph [options] [start|restart] [daemonType|daemonID]
	
The following examples illustrates a typical use case::

	sudo service ceph -a start	
	sudo /etc/init.d/ceph -a start

Once you execute with ``-a``, Ceph should begin operating. You may also specify
a particular daemon instance to constrain the command to a single instance. For
example:: 

	sudo /etc/init.d/ceph start osd.0