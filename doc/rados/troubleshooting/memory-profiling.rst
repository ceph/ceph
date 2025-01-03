==================
 Memory Profiling
==================

Ceph Monitor, OSD, and MDS can report ``TCMalloc`` heap profiles. Install
``google-perftools`` if you want to generate these. Your OS distribution might
package this under a different name (for example, ``gperftools``), and your OS
distribution might use a different package manager. Run a command similar to
this one to install ``google-perftools``: 

.. prompt:: bash 

    sudo apt-get install google-perftools

The profiler dumps output to your ``log file`` directory (``/var/log/ceph``).
See `Logging and Debugging`_ for details.

To view the profiler logs with Google's performance tools, run the following
command:

.. prompt:: bash

    google-pprof --text {path-to-daemon}  {log-path/filename}

For example::

    $ ceph tell osd.0 heap start_profiler
    $ ceph tell osd.0 heap dump
    osd.0 tcmalloc heap stats:------------------------------------------------
    MALLOC:        2632288 (    2.5 MiB) Bytes in use by application
    MALLOC: +       499712 (    0.5 MiB) Bytes in page heap freelist
    MALLOC: +       543800 (    0.5 MiB) Bytes in central cache freelist
    MALLOC: +       327680 (    0.3 MiB) Bytes in transfer cache freelist
    MALLOC: +      1239400 (    1.2 MiB) Bytes in thread cache freelists
    MALLOC: +      1142936 (    1.1 MiB) Bytes in malloc metadata
    MALLOC:   ------------
    MALLOC: =      6385816 (    6.1 MiB) Actual memory used (physical + swap)
    MALLOC: +            0 (    0.0 MiB) Bytes released to OS (aka unmapped)
    MALLOC:   ------------
    MALLOC: =      6385816 (    6.1 MiB) Virtual address space used
    MALLOC:
    MALLOC:            231              Spans in use
    MALLOC:             56              Thread heaps in use
    MALLOC:           8192              Tcmalloc page size
    ------------------------------------------------
    Call ReleaseFreeMemory() to release freelist memory to the OS (via madvise()).
    Bytes released to the OS take up virtual address space but no physical memory.
    $ google-pprof --text \
                   /usr/bin/ceph-osd  \
                   /var/log/ceph/ceph-osd.0.profile.0001.heap
     Total: 3.7 MB
     1.9  51.1%  51.1%      1.9  51.1% ceph::log::Log::create_entry
     1.8  47.3%  98.4%      1.8  47.3% std::string::_Rep::_S_create
     0.0   0.4%  98.9%      0.0   0.6% SimpleMessenger::add_accept_pipe
     0.0   0.4%  99.2%      0.0   0.6% decode_message
     ...

Performing another heap dump on the same daemon creates another file. It is
convenient to compare the new file to a file created by a previous heap dump to
show what has grown in the interval. For example::

    $ google-pprof --text --base out/osd.0.profile.0001.heap \
          ceph-osd out/osd.0.profile.0003.heap
     Total: 0.2 MB
     0.1  50.3%  50.3%      0.1  50.3% ceph::log::Log::create_entry
     0.1  46.6%  96.8%      0.1  46.6% std::string::_Rep::_S_create
     0.0   0.9%  97.7%      0.0  26.1% ReplicatedPG::do_op
     0.0   0.8%  98.5%      0.0   0.8% __gnu_cxx::new_allocator::allocate

See `Google Heap Profiler`_ for additional details.

After you have installed the heap profiler, start your cluster and begin using
the heap profiler. You can enable or disable the heap profiler at runtime, or
ensure that it runs continuously. When running commands based on the examples
that follow, do the following:

#. replace ``{daemon-type}`` with ``mon``, ``osd`` or ``mds`` 
#. replace ``{daemon-id}`` with the OSD number or the MON ID or the MDS ID 


Starting the Profiler
---------------------

To start the heap profiler, run a command of the following form: 

.. prompt:: bash

   ceph tell {daemon-type}.{daemon-id} heap start_profiler

For example:

.. prompt:: bash

   ceph tell osd.1 heap start_profiler

Alternatively, if the ``CEPH_HEAP_PROFILER_INIT=true`` variable is found in the
environment, the profile will be started when the daemon starts running.

Printing Stats
--------------

To print out statistics, run a command of the following form:

.. prompt:: bash

   ceph  tell {daemon-type}.{daemon-id} heap stats

For example:

.. prompt:: bash

   ceph tell osd.0 heap stats

.. note:: The reporting of stats with this command does not require the
   profiler to be running and does not dump the heap allocation information to
   a file.


Dumping Heap Information
------------------------

To dump heap information, run a command of the following form:

.. prompt:: bash

   ceph tell {daemon-type}.{daemon-id} heap dump

For example:

.. prompt:: bash

   ceph tell mds.a heap dump

.. note:: Dumping heap information works only when the profiler is running.


Releasing Memory
----------------

To release memory that ``tcmalloc`` has allocated but which is not being used
by the Ceph daemon itself, run a command of the following form:

.. prompt:: bash

   ceph tell {daemon-type}{daemon-id} heap release

For example:

.. prompt:: bash

    ceph tell osd.2 heap release


Stopping the Profiler
---------------------

To stop the heap profiler, run a command of the following form:

.. prompt:: bash

   ceph tell {daemon-type}.{daemon-id} heap stop_profiler

For example:

.. prompt:: bash

   ceph tell osd.0 heap stop_profiler

.. _Logging and Debugging: ../log-and-debug
.. _Google Heap Profiler: http://goog-perftools.sourceforge.net/doc/heap_profiler.html

Alternative Methods of  Memory Profiling
----------------------------------------

Running Massif heap profiler with Valgrind
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Massif heap profiler tool can be used with Valgrind to measure how much
heap memory is used. This method is well-suited to troubleshooting RadosGW.

See the `Massif documentation
<https://valgrind.org/docs/manual/ms-manual.html>`_ for more information.

Install Valgrind from the package manager for your distribution then start the
Ceph daemon you want to troubleshoot:

.. prompt:: bash

   sudo -u ceph valgrind --max-threads=1024 --tool=massif /usr/bin/radosgw -f --cluster ceph --name NAME --setuser ceph --setgroup ceph

When this command has completed its run, a file with a name of the form
``massif.out.<pid>`` will be saved in your current working directory. To run
the command above, the user who runs it must have write permissions in the
current directory.

Run the ``ms_print`` command to get a graph and statistics from the collected
data in the ``massif.out.<pid>`` file:

.. prompt:: bash

   ms_print massif.out.12345

The output of this command is helpful when submitting a bug report.
