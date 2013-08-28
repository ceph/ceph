=====================
 Operating a Cluster
=====================

.. index:: Upstart; operating a cluster

Running Ceph with Upstart
=========================

When deploying Ceph Cuttlefish and beyond with ``ceph-deploy``,  you may start
and stop Ceph daemons on a :term:`Ceph Node` using the event-based `Upstart`_. 
Upstart does not require you to define daemon instances in the Ceph configuration
file (although, they are still required for ``sysvinit`` should you choose to 
use it).

To list the Ceph Upstart jobs and instances on a node, execute:: 

	sudo initctl list | grep ceph

See `initctl`_ for additional details.

Starting all Daemons
--------------------

To start all daemons on a Ceph Node (irrespective of type), execute the
following:: 

	sudo start ceph-all
	

Stopping all Daemons	
--------------------

To stop all daemons on a Ceph Node (irrespective of type), execute the
following:: 

	sudo stop ceph-all
	

Starting all Daemons by Type
----------------------------

To start all daemons of a particular type on a Ceph Node, execute one of the
following:: 

	sudo start ceph-osd-all
	sudo start ceph-mon-all
	sudo start ceph-mds-all


Stopping all Daemons by Type
----------------------------

To stop all daemons of a particular type on a Ceph Node, execute one of the
following::

	sudo stop ceph-osd-all
	sudo stop ceph-mon-all
	sudo stop ceph-mds-all


Starting a Daemon
-----------------

To start a specific daemon instance on a Ceph Node, execute one of the
following:: 

	sudo start ceph-osd id={id}
	sudo start ceph-mon id={hostname}
	sudo start ceph-mds id={hostname}

For example:: 

	sudo start ceph-osd id=1
	sudo start ceph-mon id=ceph-server
	sudo start ceph-mds id=ceph-server


Stopping a Daemon
-----------------

To stop a specific daemon instance on a Ceph Node, execute one of the
following:: 

	sudo stop ceph-osd id={id}
	sudo stop ceph-mon id={hostname}
	sudo stop ceph-mds id={hostname}

For example:: 

	sudo stop ceph-osd id=1
	sudo start ceph-mon id=ceph-server
	sudo start ceph-mds id=ceph-server



.. index:: Ceph service; sysvinit; operating a cluster


Running Ceph as a Service
=========================

When you deploy Ceph Argonaut or Bobtail with ``mkcephfs``, use the 
service or traditional sysvinit.

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
| ``--allhosts``  |  ``-a``  | Execute on all nodes in ``ceph.conf.``          |
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

The ``ceph`` service's ``[daemons]`` setting may also target a specific instance.

To start a Ceph daemon on the local :term:`Ceph Node`, use the following syntax::

	sudo /etc/init.d/ceph start osd.0

To start a Ceph daemon on another node, use the following syntax:: 

	sudo /etc/init.d/ceph -a start osd.0

Where ``osd.0`` is the first OSD in the cluster.


Starting a Cluster
------------------

To start your Ceph cluster, execute ``ceph`` with the ``start`` command. 
The usage may differ based upon your Linux distribution. For example, for most
newer Debian/Ubuntu distributions, you may use the following syntax:: 

	sudo service ceph [options] [start|restart] [daemonType|daemonID]

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph [options] [start|restart] [daemonType|daemonID]
	
The following examples illustrates a typical use case::

	sudo service ceph -a start	
	sudo /etc/init.d/ceph -a start

Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should begin
operating. You may also specify a particular daemon instance to constrain the
command to a single instance. To start a Ceph daemon on the local Ceph Node, 
use the following syntax::

	sudo /etc/init.d/ceph start osd.0

To start a Ceph daemon on another node, use the following syntax:: 

	sudo /etc/init.d/ceph -a start osd.0


Stopping a Cluster
------------------

To stop your Ceph cluster, execute ``ceph`` with the ``stop`` command. 
The usage may differ based upon your Linux distribution. For example, for most
newer Debian/Ubuntu distributions, you may use the following syntax:: 

	sudo service ceph [options] stop [daemonType|daemonID]

For example:: 

	sudo service ceph -a stop	

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph -a stop
	
Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should shut
down. You may also specify a particular daemon instance to constrain the
command to a single instance. To stop a Ceph daemon on the local Ceph Node, 
use the following syntax::

	sudo /etc/init.d/ceph stop osd.0

To stop a Ceph daemon on another node, use the following syntax:: 

	sudo /etc/init.d/ceph -a stop osd.0




.. _Valgrind: http://www.valgrind.org/
.. _Upstart: http://upstart.ubuntu.com/index.html
.. _initctl: http://manpages.ubuntu.com/manpages/raring/en/man8/initctl.8.html