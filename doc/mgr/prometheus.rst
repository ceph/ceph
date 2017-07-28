Prometheus plugin
=================

Provides a Prometheus exporter to pass on Ceph performance counters
from the collection point in ceph-mgr.  Ceph-mgr receives MMgrReport
messages from all MgrClient processes (mons and OSDs, for instance)
with performance counter schema data and actual counter data, and keeps
a circular buffer of the last N samples.  This plugin creates an HTTP
endpoint (like all Prometheus exporters) and retrieves the latest sample
of every counter when polled (or "scraped" in Prometheus terminology).
The HTTP path and query parameters are ignored; all extant counters
for all reporting entities are returned in text exposition format.
(See the Prometheus `documentation <https://prometheus.io/docs/instrumenting/exposition_formats/#text-format-details>`_.)

Enabling
--------

The *prometheus* module is enabled with::

  ceph mgr module enable prometheus

Configuration
-------------

By default the module will accept HTTP requests on port ``9283`` on all
IPv4 and IPv6 addresses on the host.  The port and listen address are both
configurable with ``ceph config-key set``, with keys
``mgr/prometheus/server_addr`` and ``mgr/prometheus/server_port``.
This port is registered with Prometheus's `registry <https://github.com/prometheus/prometheus/wiki/Default-port-allocations>`_.

Notes
-----

Counters and gauges are exported; currently histograms and long-running 
averages are not.  It's possible that Ceph's 2-D histograms could be 
reduced to two separate 1-D histograms, and that long-running averages
could be exported as Prometheus' Summary type.

The names of the stats are exactly as Ceph names them, with
illegal characters ``.`` and ``-`` translated to ``_``.  There is one
label applied, ``daemon``, and its value is the daemon.id for the
daemon in question (e.g. ``{daemon=mon.hosta}`` or ``{daemon=osd.11}``).

Timestamps, as with many Prometheus exporters, are established by
the server's scrape time (Prometheus expects that it is polling the
actual counter process synchronously).  It is possible to supply a
timestamp along with the stat report, but the Prometheus team strongly
advises against this.  This means that timestamps will be delayed by
an unpredictable amount; it's not clear if this will be problematic,
but it's worth knowing about.
