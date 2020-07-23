=======
crimson
=======

Crimson is the code name of crimson-osd, which is the next generation ceph-osd.
It targets fast networking devices, fast storage devices by leveraging state of
the art technologies like DPDK and SPDK, for better performance. And it will
keep the support of HDDs and low-end SSDs via BlueStore. Crismon will try to
be backward compatible with classic OSD.


Running Crimson
===============

As you might expect, crimson is not featurewise on par with its predecessor yet.

daemonize
---------

Unlike ``ceph-osd``, ``crimson-osd`` does daemonize itself even if the
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
| [1, 5)  | info    |
+---------+---------+
| [5, 20] | debug   |
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

To facilitate the development of crimson, following options would be handy when
using ``vstart.sh``,

``--crimson``
    start ``crimson-osd`` instead of ``ceph-osd``

``--nodaemon``
    do not daemonize the service

``--redirect-output``
    redirect the stdout and stderr of service to ``out/$type.$num.stdout``.

``--osd-args``
    pass extra command line options to crimson-osd or ceph-osd. It's quite
    useful for passing Seastar options to crimson-osd.

So, a typical command to start a single-crimson-node cluster is::

  MGR=1 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x --without-dashboard --memstore \
    --crimson --nodaemon --redirect-output \
    --osd-args "--memory 4G"

Where we assign 4 GiB memory, a single thread running on core-0 to crimson-osd.
Please refer ``crimson-osd --help-seastar`` for more Seastar specific command
line options.

You could stop the vstart cluster using::

  ../src/stop.sh --crimson


CBT Based Testing
=================

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


Debugging Crimson
=================


debugging tips
--------------

When a seastar application crashes, it leaves us a serial of addresses, like::

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
