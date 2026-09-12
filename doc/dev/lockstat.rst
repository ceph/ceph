
==========
 LockStat
==========

LockStat is a performance analysis tool for Ceph that collects statistics about 
mutex and shared mutex wait times. It provides insights into lock contention 
by recording wait counts, durations, and wait-time distributions (histograms) 
for different lock modes (Write, Read, Try Write, Try Read).

Enabling LockStat
=================

LockStat is compiled into Ceph when the `WITH_CEPH_LOCKSTAT` CMake option is 
enabled (it is ON by default). 

At runtime, LockStat must be globally enabled using the `ENABLE_LOCKSTAT` 
environment variable. This is required for non-debug builds to avoid the 
overhead of checking for lock stats on every lock acquisition when not in use.

To enable LockStat, start the Ceph daemon with the environment variable set:

.. code-block:: bash

   ENABLE_LOCKSTAT=true ceph-osd -i 0

In debug builds (`NDEBUG` not defined), LockStat is enabled by default.

Using Ceph Daemon Commands
==========================

Once a daemon is running with LockStat enabled, you can control the profiling 
at runtime using `ceph daemon` commands.

*   **Start Profiling**:
    Starts recording wait times. You can optionally specify a threshold (in 
    microseconds) to ignore very short wait times.
    
    .. code-block:: bash

       ceph daemon osd.0 lockstat start [threshold_usec]

*   **Stop Profiling**:
    Stops recording wait times. Existing statistics are preserved.
    
    .. code-block:: bash

       ceph daemon osd.0 lockstat stop

*   **Reset Statistics**:
    Clears all currently recorded statistics and restarts profiling from zero.
    
    .. code-block:: bash

       ceph daemon osd.0 lockstat reset

*   **Dump Statistics (JSON)**:
    Dumps the current statistics in a raw JSON format.
    
    .. code-block:: bash

       ceph daemon osd.0 lockstat dump

Viewing Statistics with lockstat.py
===================================

The `src/tools/lockstat.py` script provides a human-readable table of the 
collected lock statistics. It can also be used to send the start/stop/reset 
commands to a daemon.

Running `lockstat.py`
---------------------

The script requires the daemon name (e.g., `osd.0`, `mon.a`) as the first 
argument, followed by a subcommand.

.. code-block:: bash

   # View current stats (default subcommand is 'dump')
   ./src/tools/lockstat.py osd.0

   # View stats with detailed wait-time histograms
   ./src/tools/lockstat.py osd.0 dump --detail

   # Start profiling with a 10us threshold
   ./src/tools/lockstat.py osd.0 start --threshold 10

   # Stop profiling
   ./src/tools/lockstat.py osd.0 stop

   # Reset statistics
   ./src/tools/lockstat.py osd.0 reset

Output Columns
--------------

The `dump` output contains several columns to help identify contended locks:

*   **idx[lock type]**: The internal index of the lock followed by its type 
    ('M' for Mutex, 'R' for RWLock/SharedMutex).
*   **wait_usec[W/R]**: Total time spent waiting for Write (W) or Read (R) 
    access, in microseconds.
*   **wait_count[W/R]**: Total number of times a thread had to wait for 
    access in the respective mode.
*   **usec/wait[W/R]**: Average wait time per acquisition for that mode.
*   **max_wait**: The maximum single wait time recorded for this lock, in 
    microseconds.
*   **busy ratio**: Total wait time for all modes divided by the total 
    elapsed time since profiling started. Values close to 1.0 or higher 
    (due to multiple threads waiting) indicate high contention.
*   **(r + w)/w**: Ratio of (Read + Write) acquisitions to Write 
    acquisitions. Useful for analyzing the shared vs. exclusive usage of 
    shared mutexes.
*   **num_instances**: The number of lock instances sharing this name/identity.
*   **name**: The name of the lock, usually including the source file and 
    line number where it was defined.

Histograms (`--detail`)
-----------------------

When the `--detail` flag is used, `lockstat.py` displays histograms for each 
lock mode that has a non-zero wait count.

The histogram bins are binary-sized, providing a distribution of wait times 
from very short (nanoseconds) to long (seconds). Each bin column is labeled with 
its upper bound (e.g., `<1us`, `<4ms`). Additional rows for `[READ]`, 
`[TRY_WRITE]`, or `[TRY_READ]` are displayed aligned under the main entry 
if those modes recorded any waits.
