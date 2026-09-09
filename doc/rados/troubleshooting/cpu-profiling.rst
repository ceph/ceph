===============
 CPU Profiling
===============

Use ``perf``, the Linux profiler, to see where a Ceph daemon spends its CPU
time. ``perf`` works with the release packages: no special build of Ceph is
needed. Install the debug symbols for the daemon that you want to profile so
that ``perf`` can resolve function names. The symbols are in the ``-dbg``
packages on Debian-based distributions (for example ``ceph-osd-dbg``) and in
the ``-debuginfo`` packages on RPM-based distributions.

Run ``perf`` on the host that runs the daemon. All of the commands below
require root privileges.


Finding the process
===================

Find the process ID (PID) of the daemon. The following command lists every
OSD process on the host together with its command line, which includes the
OSD ID:

.. prompt:: bash #

   pgrep -a ceph-osd


Watching a daemon live
======================

Run the following command to see a continuously updated list of the functions
in which the daemon spends the most time:

.. prompt:: bash #

   perf top -p {pid}


Recording a profile
===================

Run the following command to record 60 seconds of samples, including call
graphs:

.. prompt:: bash #

   perf record -p {pid} -F 99 --call-graph dwarf -- sleep 60

Run the following command to view the recording. The ``caller`` view shows
what each top function calls; use ``--call-graph callee`` to see instead who
calls each top function:

.. prompt:: bash #

   perf report --call-graph caller

.. note:: A ``perf record`` with ``--call-graph dwarf`` copies part of the
   stack on every sample and can produce large files. Keep the sampling
   frequency low (``-F 99`` in the example above) and the duration short on a
   busy daemon.

See :doc:`/dev/perf` for building flame graphs from a recording and for
compiling Ceph with frame pointers, which allows the cheaper ``--call-graph
fp`` mode.
