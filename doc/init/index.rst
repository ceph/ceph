=====================
 Operating a Cluster
=====================

The ``ceph`` process provides functionality to **start**, **restart**, and 
**stop** your Ceph cluster. Each time you execute ``ceph``, you must specify at 
least one option and one command. You may also specify a daemon type or a daemon
instance. For most newer Debian/Ubuntu distributions, you may use the following 
syntax:: 

	sudo service ceph [options] [commands] [daemons]

For older distributions, you may wish to use the ``/etc/init.d/ceph`` path:: 

	sudo /etc/init.d/ceph [options] [commands] [daemons]

The ``ceph`` options include:

+-----------------+----------+-------------------------------------------------+
| Option          | Shortcut | Description                                     |
+=================+==========+=================================================+
| ``--verbose``   |  ``-v``  | Use verbose logging.                            |
+-----------------+----------+-------------------------------------------------+
| ``--valgrind``  | ``N/A``  | (Developers only) Use `Valgrind`_ debugging.    |
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

The ``ceph`` commands include:

+------------------+------------------------------------------------------------+
| Command          | Description                                                |
+==================+============================================================+
|   ``start``      | Start the daemon(s).                                       |
+------------------+------------------------------------------------------------+
|   ``stop``       | Stop the daemon(s).                                        |
+------------------+------------------------------------------------------------+
|  ``forcestop``   | Force the daemon(s) to stop. Same as ``kill -9``           |
+------------------+------------------------------------------------------------+
| ``killall``      | Kill all daemons of a particular type.                     | 
+------------------+------------------------------------------------------------+
| ``cleanlogs``    | Cleans out the log directory.                              |
+------------------+------------------------------------------------------------+
| ``cleanalllogs`` | Cleans out **everything** in the log directory.            |
+------------------+------------------------------------------------------------+

The ``ceph`` daemons include the daemon types: 

- ``mon``
- ``osd``
- ``mds``

The ``ceph`` daemons may also specify a specific instance:: 

	sudo /etc/init.d/ceph -a start osd.0
	
Where ``osd.0`` is the first OSD in the cluster.

.. _Valgrind: http://www.valgrind.org/


.. toctree:: 
   :hidden:

   start-cluster
   Check Cluster Health <check-cluster-health>
   stop-cluster

See `Operations`_ for more detailed information.

.. _Operations: ../ops/index.html
