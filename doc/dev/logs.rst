============
 Debug logs
============

The main debugging tool for Ceph is the dout and derr logging functions.
Collectively, these are referred to as "dout logging."

Dout has several log faculties, which can be set at various log
levels using the configuration management system. So it is possible to enable
debugging just for the messenger, by setting debug_ms to 10, for example.

The dout macro avoids even generating log messages which are not going to be
used, by enclosing them in an "if" statement. What this means is that if you
have the debug level set at 0, and you run this code::

	dout(20) << "myfoo() = " << myfoo() << dendl;


myfoo() will not be called here.

Unfortunately, the performance of debug logging is relatively low. This is
because there is a single, process-wide mutex which every debug output
statement takes, and every debug output statement leads to a write() system
call or a call to syslog(). There is also a computational overhead to using C++
streams to consider. So you will need to be parsimonious in your logging to get
the best performance.

Sometimes, enabling logging can hide race conditions and other bugs by changing
the timing of events. Keep this in mind when debugging.

OSD PG log prefix
=================

Most OSD log lines about a placement group start with a prefix that shows
the full state of the PG, for example::

  osd.3 pg_epoch: 844 pg[6.cs0( v 844'13576 (822'3500,844'13576] local-lis/les=817/818 n=13576 ec=817/817 lis/c=817/817 les/c/f=818/818/0 sis=817) [3,0,10]p3(0) r=0 lpr=817 crt=844'13576 lcod 844'13575 mlcod 844'13575 active+clean]

When ``debug_osd`` is 20, or when its memory level is higher than its log
level (for example the default ``1/5``), this full prefix is printed on every
line. Otherwise (for example ``debug_osd = 10``) the full prefix is printed
only when it differs from the last full prefix printed for that PG (and at
least once every 1000 lines of that PG, or every 60 seconds, whichever comes
first), and the other lines carry a compact prefix::

  osd.3 pg_epoch: 844 pg[6.cs0( v 844'13576) p3(0) r=0 active+clean]

The compact prefix keeps the PG id, ``last_update``, the primary (erasure
coded pools only), the role and the PG state. The complete state for any
line is the most recent full prefix for the same PG earlier in the log;
``src/script/expand_pg_log_prefix.py`` rewrites a log with the full prefix
on every line.

A crash calls ``Log::dump_recent()``, which appends a replay of recently
buffered log entries, in their original order, after a "--- begin dump of
recent events ---" marker. Compact lines in that dump were logged before
whatever full prefix appears earlier in the surrounding log, so their
complete state is the most recent full prefix for the same PG *inside the
dump*, not before it; ``expand_pg_log_prefix.py`` handles this by
forgetting all full prefixes it has seen when it reaches that marker.

The lean/full choice is based only on ``debug_osd``'s file log and gather
levels. A sink with its own threshold, such as ``err_to_stderr`` or
``err_to_syslog`` (which only pass level ``-1``, i.e. ``derr``), can still
receive a compact prefix on that sink even though the full state for that
line went only to the file log, because ``gen_prefix()`` has no way to
know which sink, or level, a given line is headed for. The full state for
any such line is still in the file log.

``gen_prefix()`` runs when a dout statement starts, but the line is only
submitted to the log at ``dendl``. If the body of a PG dout statement logs
something else for the same PG after that PG's state has changed, the
inner line can be submitted first, with a compact prefix that already
matches the new state, before the outer line carrying the full prefix for
the old state is submitted. In the log, the compact line then appears
*before* the full prefix it belongs to instead of after it. This is rare
in practice and no known hot path does it.

Performance counters
====================

Ceph daemons use performance counters to track key statistics like number of
inodes pinned. Performance counters are essentially sets of integers and floats
which can be set, incremented, and read using the PerfCounters API.

A PerfCounters object is usually associated with a single subsystem.  It
contains multiple counters. This object is thread-safe because it is protected
by an internal mutex. You can create multiple PerfCounters objects.

Currently, three types of performance counters are supported: u64 counters,
float counters, and long-run floating-point average counters. These are created
by PerfCountersBuilder::add_u64, PerfCountersBuilder::add_fl, and
PerfCountersBuilder::add_fl_avg, respectively. u64 and float counters simply
provide a single value which can be updated, incremented, and read atomically.
floating-pointer average counters provide two values: the current total, and
the number of times the total has been changed. This is intended to provide a
long-run average value.

Performance counter information can be read in JSON format from the
administrative socket (admin_sock). This is implemented as a UNIX domain
socket. The Ceph performance counter plugin for collectd shows an example of how
to access this information. Another example can be found in the unit tests for
the administrative sockets.
