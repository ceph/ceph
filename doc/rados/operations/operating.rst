=====================
 Operating a Cluster
=====================

.. index:: Upstart; operating a cluster

Running Ceph with Upstart
=========================

When deploying Ceph Cuttlefish and beyond with ``ceph-deploy`` on Ubuntu, you 
may start and stop Ceph daemons on a :term:`Ceph Node` using the event-based 
`Upstart`_.  Upstart does not require you to define daemon instances in the 
Ceph configuration file.

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


Running Ceph
============

Each time you to **start**, **restart**, and  **stop** Ceph daemons (or your
entire cluster) you must specify at least one option and one command. You may
also specify a daemon type or a daemon instance. ::

	{commandline} [options] [commands] [daemons]


The ``ceph`` options include:

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

The ``ceph`` commands include:

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

For subsystem operations, the ``ceph`` service can target specific daemon types
by adding a particular daemon type for the ``[daemons]`` option. Daemon types
include: 

- ``mon``
- ``osd``
- ``mds``



Running Ceph with sysvinit
--------------------------

Using traditional ``sysvinit`` is the recommended way to run  Ceph with CentOS,
Red Hat, Fedora, Debian and SLES distributions. You may also use it for older
distributions of Ubuntu.


Starting all Daemons
~~~~~~~~~~~~~~~~~~~~

To start your Ceph cluster, execute ``ceph`` with the ``start`` command. 
Use the following syntax:: 

	sudo /etc/init.d/ceph [options] [start|restart] [daemonType|daemonID]
	
The following examples illustrates a typical use case::

	sudo /etc/init.d/ceph -a start

Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should begin
operating.


Stopping all Daemons	
~~~~~~~~~~~~~~~~~~~~

To stop your Ceph cluster, execute ``ceph`` with the ``stop`` command. 
Use the following syntax:: 

	sudo /etc/init.d/ceph [options] stop [daemonType|daemonID]
	
The following examples illustrates a typical use case::

	sudo /etc/init.d/ceph -a stop

Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should stop
operating.


Starting all Daemons by Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start all Ceph daemons of a particular type on the local Ceph Node, use the
following syntax::

	sudo /etc/init.d/ceph start {daemon-type}
	sudo /etc/init.d/ceph start osd

To start all Ceph daemons of a particular type on another node, use the
following syntax:: 

	sudo /etc/init.d/ceph -a start {daemon-type}
	sudo /etc/init.d/ceph -a start osd


Stopping all Daemons by Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To stop all Ceph daemons of a particular type on the local Ceph Node, use the
following syntax::

	sudo /etc/init.d/ceph stop {daemon-type}
	sudo /etc/init.d/ceph stop osd

To stop all Ceph daemons of a particular type on another node, use the
following syntax:: 

	sudo /etc/init.d/ceph -a stop {daemon-type}
	sudo /etc/init.d/ceph -a stop osd


Starting a Daemon
~~~~~~~~~~~~~~~~~

To start a Ceph daemon on the local Ceph Node, use the following syntax::

	sudo /etc/init.d/ceph start {daemon-type}.{instance}
	sudo /etc/init.d/ceph start osd.0

To start a Ceph daemon on another node, use the following syntax:: 

	sudo /etc/init.d/ceph -a start {daemon-type}.{instance}
	sudo /etc/init.d/ceph -a start osd.0


Stopping a Daemon
~~~~~~~~~~~~~~~~~

To stop a Ceph daemon on the local Ceph Node, use the following syntax::

	sudo /etc/init.d/ceph stop {daemon-type}.{instance}
	sudo /etc/init.d/ceph stop osd.0

To stop a Ceph daemon on another node, use the following syntax:: 

	sudo /etc/init.d/ceph -a stop {daemon-type}.{instance}
	sudo /etc/init.d/ceph -a stop osd.0


Running Ceph as a Service
-------------------------

When you deploy Ceph Argonaut or Bobtail with ``ceph-deploy``, you may operate
Ceph as a service (you may also use sysvinit).


Starting all Daemons
~~~~~~~~~~~~~~~~~~~~

To start your Ceph cluster, execute ``ceph`` with the ``start`` command. 
Use the following syntax:: 

	sudo service ceph [options] [start|restart] [daemonType|daemonID]
	
The following examples illustrates a typical use case::

	sudo service ceph -a start	

Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should begin
operating. 


Stopping all Daemons	
~~~~~~~~~~~~~~~~~~~~

To stop your Ceph cluster, execute ``ceph`` with the ``stop`` command. 
Use the following syntax:: 

	sudo service ceph [options] stop [daemonType|daemonID]

For example:: 

	sudo service ceph -a stop
	
Once you execute with ``-a`` (i.e., execute on all nodes), Ceph should shut
down.


Starting all Daemons by Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start all Ceph daemons of a particular type on the local Ceph Node, use the
following syntax::

	sudo service ceph start {daemon-type}
	sudo service ceph start osd

To start all Ceph daemons of a particular type on all nodes, use the following
syntax:: 

	sudo service ceph -a start {daemon-type}
	sudo service ceph -a start osd


Stopping all Daemons by Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To stop all Ceph daemons of a particular type on the local Ceph Node, use the
following syntax::

	sudo service ceph stop {daemon-type}
	sudo service ceph stop osd

To stop all Ceph daemons of a particular type on all nodes, use the following
syntax:: 

	sudo service ceph -a stop {daemon-type}
	sudo service ceph -a stop osd


Starting a Daemon
~~~~~~~~~~~~~~~~~

To start a Ceph daemon on the local Ceph Node,  use the following syntax::

	sudo service ceph start {daemon-type}.{instance}
	sudo service ceph start osd.0

To start a Ceph daemon on another node, use the following syntax:: 

	sudo service ceph -a start {daemon-type}.{instance}
	sudo service ceph -a start osd.0


Stopping a Daemon
~~~~~~~~~~~~~~~~~

To stop a Ceph daemon on the local Ceph Node, use the following syntax::

	sudo service ceph stop {daemon-type}.{instance}
	sudo service ceph stop osd.0

To stop a Ceph daemon on another node, use the following syntax:: 

	sudo service ceph -a stop {daemon-type}.{instance}
	sudo service ceph -a stop osd.0




.. _Valgrind: http://www.valgrind.org/
.. _Upstart: http://upstart.ubuntu.com/index.html
.. _initctl: http://manpages.ubuntu.com/manpages/raring/en/man8/initctl.8.html
