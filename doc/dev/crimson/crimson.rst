=======
crimson
=======

Crimson is the code name of crimson-osd, which is the next generation ceph-osd.
It targets fast networking devices, fast storage devices by leveraging state of
the art technologies like DPDK and SPDK, for better performance. And it will
keep the support of HDDs and low-end SSDs via BlueStore. Crimson will try to
be backward compatible with classic OSD.

.. highlight:: console

Building Crimson
================

Crimson is not enabled by default. To enable it::

  $ WITH_SEASTAR=true ./install-deps.sh
  $ mkdir build && cd build
  $ cmake -DWITH_SEASTAR=ON ..

Please note, `ASan`_ is enabled by default if crimson is built from a source
cloned using git.

.. _ASan: https://github.com/google/sanitizers/wiki/AddressSanitizer

Installing Crimson with ready-to-use images
===========================================

An alternative to building Crimson from source is to use container images built
by Ceph CI/CD and deploy them with one of the orchestrators: ``cephadm`` or ``Rook``.
In this chapter documents the ``cephadm`` way.

NOTE: We know that this procedure is suboptimal, but it has passed internal
external quality assurance.::


  $ curl -L https://raw.githubusercontent.com/ceph/ceph-ci/wip-bharat-crimson/src/cephadm/cephadm -o cephadm
  $ cp cephadm /usr/sbin
  $ vi /usr/sbin/cephadm

In the file change ``DEFAULT_IMAGE = 'quay.ceph.io/ceph-ci/ceph:master'``
to ``DEFAULT_IMAGE = 'quay.ceph.io/ceph-ci/ceph:<sha1>-crimson`` where ``<sha1>``
is the commit ID built by the Ceph CI/CD. You may use
https://shaman.ceph.com/builds/ceph/ to monitor branches built by Ceph's Jenkins
and to also discover those IDs.

An example::

  DEFAULT_IMAGE = 'quay.ceph.io/ceph-ci/ceph:1647216bf4ebac6bcf5ad7739e02b38569736cfd-crimson

When the edition is finished::

  chmod 777 cephadm
  podman pull quay.ceph.io/ceph-ci/ceph:<sha1>-crimson
  cephadm bootstrap --mon-ip 10.1.172.208 --allow-fqdn-hostname
  # Set "PermitRootLogin yes" for other nodes you want to use
  echo 'PermitRootLogin yes' >>  /etc/ssh/sshd_config
  systemctl restart sshd

  ssh-copy-id -f -i /etc/ceph/ceph.pub root@<nodename>
  cephadm shell
  ceph orch host add <nodename>
  ceph orch apply osd --all-available-devices

Running Crimson
===============

As you might expect, crimson is not featurewise on par with its predecessor yet.

object store backend
--------------------

At the moment, ``crimson-osd`` offers both native and alienized object store
backends. The native object store backends perform IO using seastar reactor.
They are:

.. describe:: cyanstore

   CyanStore is modeled after memstore in classic OSD.

.. describe:: seastore

   Seastore is still under active development.

While the alienized object store backends are backed by a thread pool, which
is a proxy of the alien store adaptor running in SeaStar. The proxy issues
requests to object stores running in alien threads, i.e., worker threads not
managed by the Seastar framework. They are:

.. describe:: memstore

   The memory backed object store

.. describe:: bluestore

   The object store used by classic OSD by default.

daemonize
---------

Unlike ``ceph-osd``, ``crimson-osd`` does not daemonize itself even if the
``daemonize`` option is enabled. Because, to read this option, ``crimson-osd``
needs to ready its config sharded service, but this sharded service lives
in the seastar reactor. If we fork a child process and exit the parent after
starting the Seastar engine, that will leave us with a single thread which is
the replica of the thread calls `fork()`_. This would unnecessarily complicate
the code, if we would have tackled this problem in crimson.

Since a lot of GNU/Linux distros are using systemd nowadays, which is able to
daemonize the application, there is no need to daemonize by ourselves. For
those who are using sysvinit, they can use ``start-stop-daemon`` for daemonizing
``crimson-osd``. If this is not acceptable, we can whip up a helper utility
to do the trick.


.. _fork(): http://pubs.opengroup.org/onlinepubs/9699919799/functions/fork.html

logging
-------

Currently, ``crimson-osd`` uses the logging utility offered by Seastar. see
``src/common/dout.h`` for the mapping between different logging levels to
the severity levels in Seastar. For instance, the messages sent to ``derr``
will be printed using ``logger::error()``, and the messages with debug level
over ``20`` will be printed using ``logger::trace()``.

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

Please note, ``crimson-osd``
does not send the logging message to specified ``log_file``. It writes
the logging messages to stdout and/or syslog. Again, this behavior can be
changed using ``--log-to-stdout`` and ``--log-to-syslog`` command line
options. By default, ``log-to-stdout`` is enabled, and the latter disabled.


vstart.sh
---------

The following options can be used with ``vstart.sh``.

``--crimson``
    start ``crimson-osd`` instead of ``ceph-osd``

``--nodaemon``
    do not daemonize the service

``--redirect-output``
    redirect the stdout and stderr of service to ``out/$type.$num.stdout``.

``--osd-args``
    pass extra command line options to crimson-osd or ceph-osd. It's quite
    useful for passing Seastar options to crimson-osd. For instance, you could
    use ``--osd-args "--memory 2G"`` to set the memory to use. Please refer
    the output of::

      crimson-osd --help-seastar

    for more Seastar specific command line options.

``--cyanstore``
    use the CyanStore as the object store backend.

``--bluestore``
    use the alienized BlueStore as the object store backend. This is the default
    setting, if not specified otherwise.

``--memstore``
    use the alienized MemStore as the object store backend.

So, a typical command to start a single-crimson-node cluster is::

  $  MGR=1 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x \
    --without-dashboard --cyanstore \
    --crimson --redirect-output \
    --osd-args "--memory 4G"

Where we assign 4 GiB memory, a single thread running on core-0 to crimson-osd.

You could stop the vstart cluster using::

  $ ../src/stop.sh --crimson

Metrics and Tracing
===================

Crimson offers three ways to report the stats and metrics:

pg stats reported to mgr
------------------------

Crimson collects the per-pg, per-pool, and per-osd stats in a `MPGStats`
message, and send it over to mgr, so that the mgr modules can query
them using the `MgrModule.get()` method.

asock command
-------------

an asock command is offered for dumping the metrics::

  $ ceph tell osd.0 dump_metrics
  $ ceph tell osd.0 dump_metrics reactor_utilization

Where `reactor_utilization` is an optional string allowing us to filter
the dumped metrics by prefix.

Prometheus text protocol
------------------------

the listening port and address can be configured using the command line options of
`--prometheus_port`
see `Prometheus`_ for more details.

.. _Prometheus: https://github.com/scylladb/seastar/blob/master/doc/prometheus.md

Profiling Crimson
=================

fio
---

``crimson-store-nbd`` exposes configurable ``FuturizedStore`` internals as an
NBD server for use with fio.

To use fio to test ``crimson-store-nbd``,

#. You will need to install ``libnbd``, and compile fio like

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

#. Run the ``crimson-store-nbd`` server with a block device. Please specify
   the path to the raw device, like ``/dev/nvme1n1`` in place of the created
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

   in which,

   ``--smp``
     how many CPU cores are used

   ``--mkfs``
     initialize the device first

   ``--type``
     which backend to use. If ``transaction_manager`` is specified, SeaStore's
     ``TransactionManager`` and ``BlockSegmentManager`` are used to emulate a
     block device. Otherwise, this option is used to choose a backend of
     ``FuturizedStore``, where the whole "device" is divided into multiple
     fixed-size objects whose size is specified by ``--object-size``. So, if
     you are only interested in testing the lower-level implementation of
     SeaStore like logical address translation layer and garbage collection
     without the object store semantics, ``transaction_manager`` would be a
     better choice.

#. Create an fio job file named ``nbd.fio``

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

#. Test the crimson object store using the fio compiled just now

   .. prompt:: bash $

      ./fio nbd.fio

CBT
---
We can use `cbt`_ for performing perf tests::

  $ git checkout master
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

Where we compile and run the same test against two branches. One is ``master``, another is ``yet-another-pr`` branch.
And then we compare the test results. Along with every test case, a set of rules is defined to check if we have
performance regressions when comparing two set of test results. If a possible regression is found, the rule and
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

.. _tips: https://github.com/scylladb/scylla/blob/master/docs/guides/debugging.md#tips-and-tricks

Human-readable backtraces with addr2line
----------------------------------------

When a seastar application crashes, it leaves us with a serial of addresses, like::

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

``seastar-addr2line`` offered by Seastar can be used to decipher these
addresses. After running the script, it will be waiting for input from stdin,
so we need to copy and paste the above addresses, then send the EOF by inputting
``control-D`` in the terminal::

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

Please note, ``seastar-addr2line`` is able to extract the addresses from
the input, so you can also paste the log messages like::

  2020-07-22T11:37:04.500 INFO:teuthology.orchestra.run.smithi061.stderr:Backtrace:
  2020-07-22T11:37:04.500 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e78dbc
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e7f0
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e8b8
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  0x0000000000e3e985
  2020-07-22T11:37:04.501 INFO:teuthology.orchestra.run.smithi061.stderr:  /lib64/libpthread.so.0+0x0000000000012dbf

Unlike classic OSD, crimson does not print a human-readable backtrace when it
handles fatal signals like `SIGSEGV` or `SIGABRT`. And it is more complicated
when it comes to a stripped binary. So before planting a signal handler for
those signals in crimson, we could to use `script/ceph-debug-docker.sh` to parse
the addresses in the backtrace::

  # assuming you are under the source tree of ceph
  $ ./src/script/ceph-debug-docker.sh  --flavor crimson master:27e237c137c330ebb82627166927b7681b20d0aa centos:8
  ....
  [root@3deb50a8ad51 ~]# wget -q https://raw.githubusercontent.com/scylladb/seastar/master/scripts/seastar-addr2line
  [root@3deb50a8ad51 ~]# dnf install -q -y file
  [root@3deb50a8ad51 ~]# python3 seastar-addr2line -e /usr/bin/crimson-osd
  # paste the backtrace here
