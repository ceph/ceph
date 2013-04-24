==================
 Memory Profiling
==================

Ceph OSD and metadata server daemons can generate heap profiles using
``tcmalloc``. To generate heap profiles, ensure you have ``google-perftools``
installed::

	sudo apt-get google-perftools

The profiler dumps output to your ``log file`` directory (i.e.,
``/var/log/ceph``). See `Logging and Debugging`_ for details.
To view the profiler logs with Google's performance tools, execute the
following:: 

	google-pprof -gv {log-path/filename}

Refer to `Google Heap Profiler`_ for additional details.

Once you have the heap profiler installed, start your cluster and begin using
the heap profiler. You may enable or disable the heap profiler at runtime, or
ensure that it runs continously. For the following commandline usage, replace
``{daemon-type}`` with ``osd`` or ``mds``, and replace ``daemon-id`` with the
OSD number or metadata server letter.


Starting the Profiler
---------------------

To start the heap profiler, execute the following:: 

	ceph {daemon-type} tell {daemon-id} heap start_profiler

For example:: 

	ceph osd tell 1 heap start_profiler


Printing Stats
--------------

To print out statistics, execute the following:: 

	ceph {daemon-type} tell {daemon-id} heap stats

For example:: 

	ceph osd tell 0 heap stats

.. note:: Printing stats does not require the profiler to be running and does
   not dump the heap allocation information to a file.


Dumping Heap Information
------------------------

To dump heap information, execute the following:: 

	ceph {daemon-type} tell {daemon-id} heap dump

For example:: 

	ceph mds tell a heap dump

.. note:: Dumping heap information only works when the profiler is running.


Releasing Memory
----------------

To release memory that ``tcmalloc`` has allocated but which is not being used by
the Ceph daemon itself, execute the following:: 

	ceph {daemon-type} tell {daemon-id} heap release

For example:: 

	ceph osd tell 2 heap release


Stopping the Profiler
---------------------

To stop the heap profiler, execute the following:: 

	ceph {daemon-type} tell {daemon-id} heap stop_profiler

For example:: 

	ceph {daemon-type} tell {daemon-id} heap stop_profiler

.. _Logging and Debugging: ../log-and-debug
.. _Google Heap Profiler: http://google-perftools.googlecode.com/svn/trunk/doc/heapprofile.html



