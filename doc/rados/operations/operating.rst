=====================
 Operating a Cluster
=====================

.. index:: systemd; operating a cluster


Running Ceph with systemd
==========================

For all distributions that support systemd (CentOS 7, Fedora, Debian
Jessie 8 and later, SUSE), ceph daemons are now managed using native
systemd files instead of the legacy sysvinit scripts.  For example::

        sudo systemctl start ceph.target       # start all daemons
        sudo systemctl status ceph-osd@12      # check status of osd.12

To list the Ceph systemd units on a node, execute::

        sudo systemctl status ceph\*.service ceph\*.target

Starting all Daemons
--------------------

To start all daemons on a Ceph Node (irrespective of type), execute the
following::

	sudo systemctl start ceph.target


Stopping all Daemons
--------------------

To stop all daemons on a Ceph Node (irrespective of type), execute the
following::

        sudo systemctl stop ceph\*.service ceph\*.target


Starting all Daemons by Type
----------------------------

To start all daemons of a particular type on a Ceph Node, execute one of the
following::

        sudo systemctl start ceph-osd.target
        sudo systemctl start ceph-mon.target
        sudo systemctl start ceph-mds.target


Stopping all Daemons by Type
----------------------------

To stop all daemons of a particular type on a Ceph Node, execute one of the
following::

        sudo systemctl stop ceph-mon\*.service ceph-mon.target
        sudo systemctl stop ceph-osd\*.service ceph-osd.target
        sudo systemctl stop ceph-mds\*.service ceph-mds.target


Starting a Daemon
-----------------

To start a specific daemon instance on a Ceph Node, execute one of the
following::

	sudo systemctl start ceph-osd@{id}
	sudo systemctl start ceph-mon@{hostname}
	sudo systemctl start ceph-mds@{hostname}

For example::

	sudo systemctl start ceph-osd@1
	sudo systemctl start ceph-mon@ceph-server
	sudo systemctl start ceph-mds@ceph-server


Stopping a Daemon
-----------------

To stop a specific daemon instance on a Ceph Node, execute one of the
following::

	sudo systemctl stop ceph-osd@{id}
	sudo systemctl stop ceph-mon@{hostname}
	sudo systemctl stop ceph-mds@{hostname}

For example::

	sudo systemctl stop ceph-osd@1
	sudo systemctl stop ceph-mon@ceph-server
	sudo systemctl stop ceph-mds@ceph-server


.. index:: Ceph service; Upstart; operating a cluster



Running Ceph with Upstart
=========================

When deploying Ceph with ``ceph-deploy`` on Ubuntu Trusty, you may start and
stop Ceph daemons on a :term:`Ceph Node` using the event-based `Upstart`_.
Upstart does not require you to define daemon instances in the Ceph
configuration file.

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



.. _Valgrind: http://www.valgrind.org/
.. _Upstart: http://upstart.ubuntu.com/index.html
.. _initctl: http://manpages.ubuntu.com/manpages/raring/en/man8/initctl.8.html
