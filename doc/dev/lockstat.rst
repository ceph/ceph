==========
 LockStat
==========

LockStat is a performance analysis tool for Ceph that collects statistics about
mutex and shared mutex wait times. It provides insights into lock contention
by recording wait counts, durations, and wait-time distributions (histograms)
for different lock modes (Write, Read, Try Write, Try Read).

Enabling LockStat
=================

Build-time
----------

LockStat is compiled into Ceph when the ``WITH_CEPH_LOCKSTAT`` CMake option is
enabled. The default is ``ON`` for GCC 11+ and Clang 18+; otherwise it defaults
to ``OFF``. The daemon image or binary you run must have been built with
``WITH_CEPH_LOCKSTAT`` enabled or the admin-socket commands below will be
unavailable.

Runtime
-------

At runtime, LockStat must be globally enabled using the ``ENABLE_LOCKSTAT``
environment variable. The value must be exactly ``true``. This is required for
non-debug builds (``NDEBUG`` defined) so that release daemons do not pay the
cost of checking for lock stats on every lock acquisition when LockStat is
unused.

To enable LockStat, start the Ceph daemon with the environment variable set:

.. code-block:: bash

   ENABLE_LOCKSTAT=true ceph-osd -i 0

In debug builds (``NDEBUG`` not defined), LockStat is enabled by default and
``ENABLE_LOCKSTAT`` is not required.

Enabling with cephadm
---------------------

For daemons deployed by cephadm, set ``ENABLE_LOCKSTAT`` in the service spec
via ``extra_container_args``. Those arguments are passed to the container
runtime (``podman``/``docker``), so use ``-e`` / ``--env`` as you would on a
manual ``podman run`` line. See :ref:`cephadm-extra-container-args`.

Example OSD service spec:

.. code-block:: yaml

   service_type: osd
   service_id: default_drive_group
   placement:
     host_pattern: '*'
   extra_container_args:
     - "-e"
     - "ENABLE_LOCKSTAT=true"

The same ``extra_container_args`` block can be added to other service specs
(``mon``, ``mgr``, ``mds``, ``rgw``, and so on). Apply the updated spec with
``ceph orch apply -i <spec.yaml>``. When ``extra_container_args`` change,
cephadm redeploys the affected daemons so the new environment takes effect.

After the daemons are running with LockStat enabled, profiling still must be
started explicitly (see below). ``ENABLE_LOCKSTAT`` only unlocks the feature;
it does not begin recording wait times by itself.

Using Ceph Daemon Commands
==========================

Once a daemon is running with LockStat enabled, you can control profiling
at runtime using ``ceph daemon`` commands.

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

Viewing Statistics with ceph-lockstat
=====================================

The ``src/tools/ceph-lockstat`` script provides a human-readable table of the
collected lock statistics. It can also be used to send the start/stop/reset
commands to a daemon.

Running ``ceph-lockstat``
-------------------------

The script requires the daemon name (e.g., ``osd.0``, ``mon.a``) as the first
argument, followed by a subcommand. For cephadm clusters it can resolve short
names via ``ceph orch ps`` and looks for admin sockets under ``/var/run/ceph``
(including ``/var/run/ceph/<fsid>/``).

.. code-block:: bash

   # View current stats (default subcommand is 'dump')
   ./src/tools/ceph-lockstat osd.0

   # View stats with detailed wait-time histograms
   ./src/tools/ceph-lockstat osd.0 dump --detail

   # Start profiling with a 10us threshold
   ./src/tools/ceph-lockstat osd.0 start --threshold 10

   # Stop profiling
   ./src/tools/ceph-lockstat osd.0 stop

   # Reset statistics
   ./src/tools/ceph-lockstat osd.0 reset

Output Columns
--------------

The ``dump`` output contains several columns to help identify contended locks:

*   **idx[lock type]**: The internal index of the lock followed by its type
    ('M' for Mutex, 'R' for RWLock/SharedMutex, 'C' for CondVar,  
    'S' for SpinLock, 'N' for NoLock).
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

Histograms (``--detail``)
-------------------------

When the ``--detail`` flag is used, ``ceph-lockstat`` displays histograms for
each lock mode that has a non-zero wait count.

The histogram bins are binary-sized. They match
``LockStatEntry::LockStats`` in ``src/common/lockstat.h``
(``bin_low_bits = 4``, ``num_bins = 25``), which yields ``num_bins + 2``
(27) buckets over wait times in nanoseconds:

*   Bin 0 (underflow): ``[0, 2^bin_low_bits)`` = ``[0, 16ns)``
*   Bins 1..``num_bins``: successive power-of-two ranges
    ``[2^(bin_low_bits+i-1), 2^(bin_low_bits+i))`` for ``i`` in 1..25,
    i.e. ``[16ns, 32ns)``, ``[32ns, 64ns)``, … up through
    ``[2^28 ns, 2^29 ns)`` (about ``[268ms, 537ms)``)
*   Bin ``num_bins + 1`` (overflow): ``[2^(bin_low_bits+num_bins), ∞)`` =
    ``[2^29 ns, ∞)`` (about ``≥ 537ms``)

Each bin column is labeled with its upper bound (e.g., ``<1us``,
``<4ms``). Additional rows for ``[READ]``, ``[TRY_WRITE]``, or
``[TRY_READ]`` are displayed aligned under the main entry if those modes
recorded any waits.
