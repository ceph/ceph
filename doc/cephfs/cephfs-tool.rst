.. _cephfs-tool:

===========
cephfs-tool
===========

``cephfs-tool`` is a standalone C++ utility designed to interact directly with
``libcephfs``. The initial implementation focuses on a ``bench`` command to
measure library performance. This allows developers and administrators to
benchmark the userspace library isolated from FUSE or kernel client overhead.

Key features include:

* Multi-threaded Read/Write throughput benchmarking.
* Configurable block sizes, file counts, and fsync intervals.
* Detailed statistical reporting (Mean, Std Dev, Min/Max) for throughput and IOPS.
* Support for specific CephFS user/group impersonation (UID/GID) via ``ceph_mount_perms_set``.
* Optional JSON output and libcephfs perf counter dumps.
* Optional progress reporting during benchmark phases.
* Optional asynchronous I/O benchmarking with configurable queue depth.
* Optional overrides for selected client configuration settings such as object cache and messenger worker threads.

Building
========

The tool can be built outside of the Ceph source tree:

.. code-block:: bash

    g++ --std=c++20 -D_FILE_OFFSET_BITS=64 -O3 -o cephfs-tool cephfs-tool.cc -lcephfs -lpthread -lboost_program_options

Usage
=====

.. code-block:: bash

    cephfs-tool [general-options] <command> [command-options]

Commands
--------

bench
    Run IO benchmark

Options
=======

General Options
---------------

.. option:: -h, --help

   Produce help message

.. option:: -c, --conf <path>

   Ceph config file path

.. option:: -i, --id <id>

   Client ID (default: ``admin``)

.. option:: -k, --keyring <path>

   Path to keyring file

.. option:: --filesystem, --fs <name>

   CephFS filesystem name to mount

.. option:: --uid <uid>

   User ID to mount as (default: ``-1``)

.. option:: --gid <gid>

   Group ID to mount as (default: ``-1``)

.. option:: --client-oc <0|1>

   Override the ``client_oc`` setting for the benchmark mount

.. option:: --client-oc-size <value>

   Override the ``client_oc_size`` setting for the benchmark mount

.. option:: --msgr-workers <n>

   Override ``ms_async_op_threads`` for the benchmark mount. Valid values are
   ``1`` to ``24``; ``0`` keeps the configured default.

Benchmark Options
-----------------

These options are used with the ``bench`` command.

.. option:: --threads <n>

   Number of threads (default: ``1``)

.. option:: --iterations <n>

   Number of iterations (default: ``1``)

.. option:: --files <n>

   Total number of files (default: ``100``)

.. option:: --size <size>

   File size (e.g. 4MB, 0 for creates only) (default: ``4MB``)

.. option:: --block-size <size>

   IO block size (e.g. 1MB) (default: ``4MB``)

.. option:: --fsync-every <size>

   Call fsync every N bytes (default: ``0``)

.. option:: --prefix <str>

   Filename prefix (default: ``benchmark_``)

.. option:: --dir-prefix <str>

   Directory prefix (default: ``bench_run_``)

.. option:: --root-path <path>

   Root path in CephFS (default: ``/``)

.. option:: --per-thread-mount

   Use separate mount per thread

.. option:: --no-cleanup

   Disable cleanup of files

.. option:: --json <path>

   Write structured benchmark results to a JSON file. Use ``-`` to output to stdout.

.. option:: --duration <seconds>

   Limit each write and read phase to ``N`` seconds. A value of ``0`` means the
   benchmark processes all files.

.. option:: --perf-dump <path>

   Dump libcephfs performance counters to the specified file after the benchmark

.. option:: --progress

   Show live progress, bandwidth, file rate, and ETA during benchmark phases

.. option:: --progress-interval <percent>

   Minimum percentage increment between progress updates (default: ``10``;
   valid range: ``1`` to ``100``)

.. option:: --async-io

   Use asynchronous I/O via ``ceph_ll_nonblocking_readv_writev`` for read and
   write phases

.. option:: --queue-depth <n>

   Maximum number of outstanding async I/Os per worker thread (default:
   ``16``; valid range: ``1`` to ``1024``). This option is only meaningful with
   ``--async-io`` and ``--async-io`` cannot be combined with
   ``--per-thread-mount``.

Examples
========

Benchmark throughput with 8 threads:

.. code-block:: bash

    env CEPH_ARGS="--log-to-stderr=false --log-to-file=false --log-file=/tmp/bench.log" \
    ./cephfs-tool -c ~/ceph.conf -k ~/keyring -i scratch --filesystem scratch \
      bench --root-path=/pdonnell --files 256 --size=$(( 128 * 2 ** 20 )) \
      --threads=8 --iterations 3

Output:

.. code-block:: text

    Benchmark Configuration:
      Threads: 8 | Iterations: 3
      Files: 256 | Size: 134217728
      Filesystem: scratch
      Root: /pdonnell
      Subdirectory: bench_run_d942
      UID: -1
      GID: -1

    --- Iteration 1 of 3 ---
    Starting Write Phase...
      Write: 2761.97 MB/s, 21.5779 files/s (11.864s)
    Starting Read Phase...
      Read:  2684.36 MB/s, 20.9716 files/s (12.207s)
    ...
    *** Final Report ***

    Write Throughput Statistics (3 runs):
      Mean:    2727.06 MB/s
      Std Dev: 26.2954 MB/s
      Min:     2698.51 MB/s
      Max:     2761.97 MB/s

    Read Throughput Statistics (3 runs):
      Mean:    2687.24 MB/s
      Std Dev: 5.68904 MB/s
      Min:     2682.16 MB/s
      Max:     2695.18 MB/s

    File Creates Statistics (3 runs):
      Mean:    21.3051 files/s
      Std Dev: 0.205433 files/s
      Min:     21.0821 files/s
      Max:     21.5779 files/s

    File Reads (Opens) Statistics (3 runs):
      Mean:    20.994 files/s
      Std Dev: 0.0444456 files/s
      Min:     20.9544 files/s
      Max:     21.0561 files/s

    Cleaning up...

Benchmark file creation performance (size 0):

.. code-block:: bash

    env CEPH_ARGS="--log-to-stderr=false --log-to-file=false --log-file=/tmp/bench.log" \
    ./cephfs-tool -c ~/ceph.conf -k ~/keyring -i scratch --filesystem scratch \
      bench --root-path=/pdonnell --files=$(( 2 ** 16 )) --size=0 \
      --threads=8 --iterations 3

Output:

.. code-block:: text

    Benchmark Configuration:
      Threads: 8 | Iterations: 3
      Files: 65536 | Size: 0
    ...
    *** Final Report ***

    File Creates Statistics (3 runs):
      Mean:    4001.86 files/s
      Std Dev: 125.337 files/s
      Min:     3863.7 files/s
      Max:     4167.1 files/s

    File Reads (Opens) Statistics (3 runs):
      Mean:    14382.3 files/s
      Std Dev: 556.594 files/s
      Min:     13636.3 files/s
      Max:     14972.8 files/s

    Cleaning up...
