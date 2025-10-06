=======
crimson
=======

Crimson is the code name of ``crimson-osd``, which is the next generation ``ceph-osd``.
It is designed to deliver enhanced performance on fast network and storage devices by leveraging modern technologies such as DPDK and SPDK.

Crimson is intended to be a drop-in replacement for the classic Object Storage Daemon (OSD),
aiming to allow seamless migration from existing ``ceph-osd`` deployments (through backfilling).

The second phase of the project introduces :ref:`seastore`, a complete redesign of the object storage backend built around Crimson's native architecture.
Seastore is optimized for high-performance storage devices like NVMe and may not be suitable for traditional HDDs.
Crimson will continue to support BlueStore ensuring compatibility with HDDs and slower SSDs.

See `ceph.io/en/news/crimson <https://ceph.io/en/news/crimson/>`_

.. highlight:: console

Building Crimson
================

Crimson is not enabled by default. Enable it at build time by running::

  $ WITH_CRIMSON=true ./install-deps.sh
  $ ./do_cmake.sh -DWITH_CRIMSON=ON

Please note, `ASan`_ is enabled by default if Crimson is built from a source
cloned using ``git``.

.. _ASan: https://github.com/google/sanitizers/wiki/AddressSanitizer

Deploying Crimson with cephadm
==============================

.. note::
   Cephadm SeaStore support is in `early stages <https://tracker.ceph.com/issues/71946>`_.

The Ceph CI/CD pipeline builds containers with ``crimson-osd`` replacing the standard ``ceph-osd``.

Once a branch at commit <sha1> has been built and is available in
Shaman / Quay, you can deploy it using the cephadm instructions outlined
in :ref:`cephadm` with the following adaptations.

The latest `main` branch is built `daily <https://shaman.ceph.com/builds/ceph/main>`_
and the images are available in `quay <https://quay.ceph.io/repository/ceph-ci/ceph?tab=tags>`_
(filter ``crimson-release``).
We recommend using one of the latest available builds, as Crimson evolves rapidly.

Use the ``--image`` flag to specify a Crimson build:

.. prompt:: bash #

   cephadm --image quay.ceph.io/ceph-ci/ceph:<sha1>-crimson-release --allow-mismatched-release bootstrap ...


.. note::
   Crimson builds are available in two variants: ``crimson-debug`` and ``crimson-release``.
   For testing purposes the `release` variant should be used.
   The `debug` variant is intended primarily for development.

You'll likely need to include the ``--allow-mismatched-release`` flag to use a non-release branch.

Crimson CPU allocation
======================

.. note::

   #. Allocation options **cannot** be changed after deployment.
   #. `vstart.sh`_ sets these options using the ``--crimson-smp`` flag.

The ``crimson_cpu_num`` parameter defines the number of CPUs used to serve Seastar reactors.
Each reactor is expected to run on a dedicated CPU core.

This parameter **does not have a default value**.
Users must configure it at the OSD level based on system resources and cluster requirements **before** deploying the OSDs.

We recommend setting a value for ``crimson_cpu_num`` that is less than the host's
number of CPU cores (``nproc``) divided by the **number of OSDs on that host**.

For example, for deploying a node with eight CPU cores per OSD:

.. prompt:: bash #

   ceph config set osd crimson_cpu_num 8

Note that ``crimson_cpu_num`` does **not** pin threads to specific CPU cores.
To explicitly assign CPU cores to Crimson OSDs, use the ``crimson_cpu_set`` parameter.
This enables CPU pinning, which *may* improve performance.
However, using this option requires manually setting the CPU set for each OSD,
and is generally less recommended due to its complexity.

Running Crimson
===============

.. note::
   Crimson is in a tech preview stage and is **not suitable for production use**.

After starting your cluster, prior to deploying OSDs, you'll need to configure the
`Crimson CPU allocation`_ and enable Crimson to
direct the default pools to be created as Crimson pools.  You can proceed by running the following after you have a running cluster:

.. note::
   `vstart.sh`_ enables crimson automatically when `--crimson` is used.

.. prompt:: bash #

   ceph config set global 'enable_experimental_unrecoverable_data_corrupting_features' crimson
   ceph osd set-allow-crimson --yes-i-really-mean-it
   ceph config set mon osd_pool_default_crimson true

The first command enables the ``crimson`` experimental feature.  

The second enables the ``allow_crimson`` OSDMap flag.  The monitor will
not allow ``crimson-osd`` to boot without that flag.

The last causes pools to be created by default with the ``crimson`` flag.
Crimson pools are restricted to operations supported by Crimson.
``Crimson-osd`` won't instantiate PGs from non-Crimson pools.

Object Store Backends
=====================

``crimson-osd`` supports two categories of object store backends: **native** and **non-native**.

Native Backends
---------------

Native backends perform I/O operations using the **Seastar reactor**. These are tightly integrated with the Seastar framework and follow its design principles:

.. describe:: seastore

   SeaStore is the primary native object store for Crimson OSD. It is built with the Seastar framework and adheres to its asynchronous, shard-based architecture.

.. describe:: cyanstore

   CyanStore is inspired by ``memstore`` from the classic OSD, offering a lightweight, in-memory object store model.
   CyanStore **does not store data** and should be used only for measuring OSD overhead, without the cost of actually storing data.

Non-Native Backends
-------------------

Non-native backends operate through a **thread pool proxy**, which interfaces with object stores running in **alien threads**—worker threads not managed by Seastar.
These backends allow Crimson to interact with legacy or external object store implementations:

.. describe:: bluestore

   The default object store used by the classic ``ceph-osd``. It provides robust, production-grade storage capabilities.

   The ``crimson_bluestore_num_threads`` option needs to be set according to the cpu set available.
   This defines the number of threads dedicated to serving the BlueStore ObjectStore on each OSD.

   If ``crimson_cpu_num`` is used from `Crimson CPU allocation`_,
   The counterpart ``crimson_bluestore_cpu_set`` should also be used accordingly to
   allow the two sets to be mutually exclusive.

.. describe:: memstore

   An in-memory object store backend, primarily used for testing and development purposes.

vstart.sh
=========

The following options can be used with ``vstart.sh``.

``--crimson``
    Start ``crimson-osd`` instead of ``ceph-osd``.

``--nodaemon``
    Do not daemonize the service.

``--redirect-output``
    Redirect the ``stdout`` and ``stderr`` to ``out/$type.$num.stdout``.

``--osd-args``
    Pass extra command line options to ``crimson-osd`` or ``ceph-osd``.
    This is useful for passing Seastar options to ``crimson-osd``. For
    example, one can supply ``--osd-args "--memory 2G"`` to set the amount of
    memory to use. Please refer to the output of::

      crimson-osd --help-seastar

    for additional Seastar-specific command line options.

``--crimson-smp``
    The number of cores to use for each OSD.
    If BlueStore is used, the balance of available cores
    (as determined by `nproc`) will be assigned to the object store.

``--bluestore``
    Use the alienized BlueStore as the object store backend. This is the default (see above section on the `object store backends`_ for more details)

``--cyanstore``
    Use CyanStore as the object store backend.

``--memstore``
    Use the alienized MemStore as the object store backend.

``--seastore``
    Use SeaStore as the back end object store.

``--seastore-devs``
    Specify the block device used by SeaStore.

``--seastore-secondary-devs``
    Optional.  SeaStore supports multiple devices.  Enable this feature by
    passing the block device to this option.

``--seastore-secondary-devs-type``
    Optional.  Specify the type of secondary devices.  When the secondary
    device is slower than main device passed to ``--seastore-devs``, the cold
    data in faster device will be evicted to the slower devices over time.
    Valid types include ``HDD``, ``SSD``(default), ``ZNS``, and ``RANDOM_BLOCK_SSD``
    Note secondary devices should not be faster than the main device.

To start a cluster with a single Crimson node, run::

  $  MGR=1 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh \
    --without-dashboard --bluestore --crimson \
    --redirect-output

Another SeaStore example::

  $  MGR=1 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x \
    --without-dashboard --seastore \
    --crimson --redirect-output \
    --seastore-devs /dev/sda \
    --seastore-secondary-devs /dev/sdb \
    --seastore-secondary-devs-type HDD

Stop this ``vstart`` cluster by running::

  $ ../src/stop.sh --crimson

daemonize
---------

Unlike ``ceph-osd``, ``crimson-osd`` does not daemonize itself even if the
``daemonize`` option is enabled. In order to read this option, ``crimson-osd``
needs to ready its config sharded service, but this sharded service lives
in the Seastar reactor. If we fork a child process and exit the parent after
starting the Seastar engine, that will leave us with a single thread which is
a replica of the thread that called `fork()`_. Tackling this problem in Crimson
would unnecessarily complicate the code.

Since supported GNU/Linux distributions use ``systemd``, which is able to
daemonize processes, there is no need to daemonize ourselves. 
Those using sysvinit can use ``start-stop-daemon`` to daemonize ``crimson-osd``.
If this is does not work out, a helper utility may be devised.

.. _fork(): http://pubs.opengroup.org/onlinepubs/9699919799/functions/fork.html

logging
-------

``Crimson-osd`` currently uses the logging utility offered by Seastar. See
``src/common/dout.h`` for the mapping between Ceph logging levels to
the severity levels in Seastar. For instance, messages sent to ``derr``
will be issued using ``logger::error()``, and the messages with a debug level
greater than ``20`` will be issued using ``logger::trace()``.

+---------+---------+
| ceph    | seastar |
+---------+---------+
| < 0     | error   |
+---------+---------+
|   0     | warn    |
+---------+---------+
| [1, 6)  | info    |
+---------+---------+
| [6, 20] | debug   |
+---------+---------+
| >  20   | trace   |
+---------+---------+

Note that ``crimson-osd``
does not send log messages directly to a specified ``log_file``. It writes
the logging messages to stdout and/or syslog. This behavior can be
changed using ``--log-to-stdout`` and ``--log-to-syslog`` command line
options. By default, ``log-to-stdout`` is enabled, and ``--log-to-syslog`` is disabled.
Metrics and Tracing
===================

Crimson offers three ways to report stats and metrics.

PG stats reported to mgr
------------------------

Crimson collects the per-pg, per-pool, and per-osd stats in a `MPGStats`
message which is sent to the Ceph Managers. Manager modules can query
them using the `MgrModule.get()` method.

Asock command
-------------

An admin socket command is offered for dumping metrics::

  $ ceph tell osd.0 dump_metrics
  $ ceph tell osd.0 dump_metrics reactor_utilization

Here `reactor_utilization` is an optional string allowing us to filter
the dumped metrics by prefix.

Prometheus text protocol
------------------------

The listening port and address can be configured using the command line options of
`--prometheus_port`
see `Prometheus`_ for more details.

.. _Prometheus: https://github.com/scylladb/seastar/blob/master/doc/prometheus.md

Profiling Crimson
=================

Fio
---

``crimson-store-nbd`` exposes configurable ``FuturizedStore`` internals as an
NBD server for use with ``fio``.

In order to use ``fio`` to test ``crimson-store-nbd``, perform the below steps.

#. You will need to install ``libnbd``, and compile it into ``fio``

   .. prompt:: bash $

      apt-get install libnbd-dev
      git clone git://git.kernel.dk/fio.git
      cd fio
      ./configure --enable-libnbd
      make

#. Build ``crimson-store-nbd``

   .. prompt:: bash $

      cd build
      ninja crimson-store-nbd

#. Run the ``crimson-store-nbd`` server with a block device. Specify
   the path to the raw device, for example ``/dev/nvme1n1``, in place of the created
   file for testing with a block device.

   .. prompt:: bash $

      export disk_img=/tmp/disk.img
      export unix_socket=/tmp/store_nbd_socket.sock
      rm -f $disk_img $unix_socket
      truncate -s 512M $disk_img
      ./bin/crimson-store-nbd \
        --device-path $disk_img \
        --smp 1 \
        --mkfs true \
        --type transaction_manager \
        --uds-path ${unix_socket} &

   Below are descriptions of these command line arguments:

   ``--smp``
     The number of CPU cores to use (Symmetric MultiProcessor)

   ``--mkfs``
     Initialize the device first.

   ``--type``
     The back end to use. If ``transaction_manager`` is specified, SeaStore's
     ``TransactionManager`` and ``BlockSegmentManager`` are used to emulate a
     block device. Otherwise, this option is used to choose a backend of
     ``FuturizedStore``, where the whole "device" is divided into multiple
     fixed-size objects whose size is specified by ``--object-size``. So, if
     you are only interested in testing the lower-level implementation of
     SeaStore like logical address translation layer and garbage collection
     without the object store semantics, ``transaction_manager`` would be a
     better choice.

#. Create a ``fio`` job file named ``nbd.fio``

   .. code:: ini

      [global]
      ioengine=nbd
      uri=nbd+unix:///?socket=${unix_socket}
      rw=randrw
      time_based
      runtime=120
      group_reporting
      iodepth=1
      size=512M

      [job0]
      offset=0

#. Test the Crimson object store, using the custom ``fio`` built just now

   .. prompt:: bash $

      ./fio nbd.fio

CBT
---
We can use `cbt`_ for performance tests::

  $ git checkout main
  $ make crimson-osd
  $ ../src/script/run-cbt.sh --cbt ~/dev/cbt -a /tmp/baseline ../src/test/crimson/cbt/radosbench_4K_read.yaml
  $ git checkout yet-another-pr
  $ make crimson-osd
  $ ../src/script/run-cbt.sh --cbt ~/dev/cbt -a /tmp/yap ../src/test/crimson/cbt/radosbench_4K_read.yaml
  $ ~/dev/cbt/compare.py -b /tmp/baseline -a /tmp/yap -v
  19:48:23 - INFO     - cbt      - prefill/gen8/0: bandwidth: (or (greater) (near 0.05)):: 0.183165/0.186155  => accepted
  19:48:23 - INFO     - cbt      - prefill/gen8/0: iops_avg: (or (greater) (near 0.05)):: 46.0/47.0  => accepted
  19:48:23 - WARNING  - cbt      - prefill/gen8/0: iops_stddev: (or (less) (near 0.05)):: 10.4403/6.65833  => rejected
  19:48:23 - INFO     - cbt      - prefill/gen8/0: latency_avg: (or (less) (near 0.05)):: 0.340868/0.333712  => accepted
  19:48:23 - INFO     - cbt      - prefill/gen8/1: bandwidth: (or (greater) (near 0.05)):: 0.190447/0.177619  => accepted
  19:48:23 - INFO     - cbt      - prefill/gen8/1: iops_avg: (or (greater) (near 0.05)):: 48.0/45.0  => accepted
  19:48:23 - INFO     - cbt      - prefill/gen8/1: iops_stddev: (or (less) (near 0.05)):: 6.1101/9.81495  => accepted
  19:48:23 - INFO     - cbt      - prefill/gen8/1: latency_avg: (or (less) (near 0.05)):: 0.325163/0.350251  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/0: bandwidth: (or (greater) (near 0.05)):: 1.24654/1.22336  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/0: iops_avg: (or (greater) (near 0.05)):: 319.0/313.0  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/0: iops_stddev: (or (less) (near 0.05)):: 0.0/0.0  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/0: latency_avg: (or (less) (near 0.05)):: 0.0497733/0.0509029  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/1: bandwidth: (or (greater) (near 0.05)):: 1.22717/1.11372  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/1: iops_avg: (or (greater) (near 0.05)):: 314.0/285.0  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/1: iops_stddev: (or (less) (near 0.05)):: 0.0/0.0  => accepted
  19:48:23 - INFO     - cbt      - seq/gen8/1: latency_avg: (or (less) (near 0.05)):: 0.0508262/0.0557337  => accepted
  19:48:23 - WARNING  - cbt      - 1 tests failed out of 16

Here we compile and run the same test against two branches: ``main`` and ``yet-another-pr``.
We then compare the results. Along with every test case, a set of rules is defined to check for
performance regressions when comparing the sets of test results. If a possible regression is found, the rule and
corresponding test results are highlighted.

.. _cbt: https://github.com/ceph/cbt

Hacking Crimson
===============


Seastar Documents
-----------------

See `Seastar Tutorial <https://github.com/scylladb/seastar/blob/master/doc/tutorial.md>`_ .
Or build a browsable version and start an HTTP server::

  $ cd seastar
  $ ./configure.py --mode debug
  $ ninja -C build/debug docs
  $ python3 -m http.server -d build/debug/doc/html

You might want to install ``pandoc`` and other dependencies beforehand.

Debugging Crimson
=================

Debugging with GDB
------------------

The `tips`_ for debugging Scylla also apply to Crimson.

.. _tips: https://github.com/scylladb/scylla/blob/master/docs/dev/debugging.md#tips-and-tricks

Human-readable backtraces with addr2line
----------------------------------------

When a Seastar application crashes, it leaves us with a backtrace of addresses, like::

  Segmentation fault.
  Backtrace:
    0x00000000108254aa
    0x00000000107f74b9
    0x00000000105366cc
    0x000000001053682c
    0x00000000105d2c2e
    0x0000000010629b96
    0x0000000010629c31
    0x00002a02ebd8272f
    0x00000000105d93ee
    0x00000000103eff59
    0x000000000d9c1d0a
    /lib/x86_64-linux-gnu/libc.so.6+0x000000000002409a
    0x000000000d833ac9
  Segmentation fault

The ``seastar-addr2line`` utility provided by Seastar can be used to map these
addresses to functions. The script expects input on ``stdin``,
so we need to copy and paste the above addresses, then send EOF by inputting
``control-D`` in the terminal.  One might  use ``echo`` or ``cat`` instead::

  $ ../src/seastar/scripts/seastar-addr2line -e bin/crimson-osd

    0x00000000108254aa
    0x00000000107f74b9
    0x00000000105366cc
    0x000000001053682c
    0x00000000105d2c2e
    0x0000000010629b96
    0x0000000010629c31
    0x00002a02ebd8272f
    0x00000000105d93ee
    0x00000000103eff59
    0x000000000d9c1d0a
    0x00000000108254aa
  [Backtrace #0]
  seastar::backtrace_buffer::append_backtrace() at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:1136
  seastar::print_with_backtrace(seastar::backtrace_buffer&) at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:1157
  seastar::print_with_backtrace(char const*) at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:1164
  seastar::sigsegv_action() at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:5119
  seastar::install_oneshot_signal_handler<11, &seastar::sigsegv_action>()::{lambda(int, siginfo_t*, void*)#1}::operator()(int, siginfo_t*, void*) const at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:5105
  seastar::install_oneshot_signal_handler<11, &seastar::sigsegv_action>()::{lambda(int, siginfo_t*, void*)#1}::_FUN(int, siginfo_t*, void*) at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:5101
  ?? ??:0
  seastar::smp::configure(boost::program_options::variables_map, seastar::reactor_config) at /home/kefu/dev/ceph/build/../src/seastar/src/core/reactor.cc:5418
  seastar::app_template::run_deprecated(int, char**, std::function<void ()>&&) at /home/kefu/dev/ceph/build/../src/seastar/src/core/app-template.cc:173 (discriminator 5)
  main at /home/kefu/dev/ceph/build/../src/crimson/osd/main.cc:131 (discriminator 1)

Note that ``seastar-addr2line`` is able to extract addresses from
its input, so you can also paste the log messages as below::

  2020-07-22T11:37:04.500 INFO:teuthology.orchestra.run.smithi061.stderr:Backtrace:
  2020-07-22T11:37:04.500 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e78dbc
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e7f0
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e8b8
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e985
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  /lib64/libpthread.so.0+0x0000000000012dbf

Unlike the classic ``ceph-osd``, Crimson does not print a human-readable backtrace when it
handles fatal signals like `SIGSEGV` or `SIGABRT`. It is also more complicated
with a stripped binary. So instead of planting a signal handler for
those signals into Crimson, we can use `script/ceph-debug-docker.sh` to map
addresses in the backtrace::

  # assuming you are under the source tree of ceph
  $ ./src/script/ceph-debug-docker.sh  --flavor crimson master:27e237c137c330ebb82627166927b7681b20d0aa centos:8
  ....
  [root@3deb50a8ad51 ~]# wget -q https://raw.githubusercontent.com/scylladb/seastar/master/scripts/seastar-addr2line
  [root@3deb50a8ad51 ~]# dnf install -q -y file
  [root@3deb50a8ad51 ~]# python3 seastar-addr2line -e /usr/bin/crimson-osd
  # paste the backtrace here

Code Walkthroughs
=================

* `Ceph Code Walkthroughs: Crimson <https://www.youtube.com/watch?v=rtkrHk6grsg>`_

* `Ceph Code Walkthroughs: SeaStore <https://www.youtube.com/watch?v=0rr5oWDE2Ck>`_
