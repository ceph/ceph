=====================
 Operating a Cluster
=====================

The ``ceph`` service provides functionality to **start**, **restart**, and 
**stop** your Ceph cluster. Each time you execute ``ceph`` processes, you
must specify at least one option and one command. You may also specify a daemon 
type or a daemon instance. For most newer Debian/Ubuntu distributions, you may 
use the following syntax:: 

	sudo service ceph [options] [commands] [daemons]

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph [options] [commands] [daemons]

The ``ceph`` service options include:

+-----------------+----------+-------------------------------------------------+
| Option          | Shortcut | Description                                     |
+=================+==========+=================================================+
| ``--verbose``   |  ``-v``  | Use verbose logging.                            |
+-----------------+----------+-------------------------------------------------+
| ``--valgrind``  | ``N/A``  | (Dev and QA only) Use `Valgrind`_ debugging.    |
+-----------------+----------+-------------------------------------------------+
| ``--allhosts``  |  ``-a``  | Execute on all hosts in ``ceph.conf.``          |
|                 |          | Otherwise, it only executes on ``localhost``.   |
+-----------------+----------+-------------------------------------------------+
| ``--restart``   | ``N/A``  | Automatically restart daemon if it core dumps.  |
+-----------------+----------+-------------------------------------------------+
| ``--norestart`` | ``N/A``  | Don't restart a daemon if it core dumps.        |
+-----------------+----------+-------------------------------------------------+
| ``--conf``      |  ``-c``  | Use an alternate configuration file.            |
+-----------------+----------+-------------------------------------------------+

The ``ceph`` service commands include:

+------------------+------------------------------------------------------------+
| Command          | Description                                                |
+==================+============================================================+
|    ``start``     | Start the daemon(s).                                       |
+------------------+------------------------------------------------------------+
|    ``stop``      | Stop the daemon(s).                                        |
+------------------+------------------------------------------------------------+
|  ``forcestop``   | Force the daemon(s) to stop. Same as ``kill -9``           |
+------------------+------------------------------------------------------------+
|   ``killall``    | Kill all daemons of a particular type.                     | 
+------------------+------------------------------------------------------------+
|  ``cleanlogs``   | Cleans out the log directory.                              |
+------------------+------------------------------------------------------------+
| ``cleanalllogs`` | Cleans out **everything** in the log directory.            |
+------------------+------------------------------------------------------------+

For subsystem operations, the ``ceph`` service can target specific daemon types by
adding a particular daemon type for the ``[daemons]`` option. Daemon types include: 

- ``mon``
- ``osd``
- ``mds``

The ``ceph`` service's ``[daemons]`` setting may also target a specific instance:: 

	sudo /etc/init.d/ceph -a start osd.0

Where ``osd.0`` is the first OSD in the cluster.


Starting a Cluster
==================

To start your Ceph cluster, execute ``ceph`` with the ``start`` command. 
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


Stopping a Cluster
==================

To stop your Ceph cluster, execute ``ceph`` with the ``stop`` command. 
The usage may differ based upon your Linux distribution. For example, for most
newer Debian/Ubuntu distributions, you may use the following syntax:: 

	sudo service ceph [options] stop [daemonType|daemonID]

For example:: 

	sudo service ceph -a stop	

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph -a stop
	
Ceph should shut down the operating processes.


.. _Valgrind: http://www.valgrind.org/