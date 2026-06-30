:orphan:

.. _radostrace:

=====================================================
 radostrace -- eBPF-based RADOS client tracing tool
=====================================================

.. program:: radostrace

Synopsis
========

| **radostrace** [ -p *pid* ] [ -t *seconds* ] [ -d ]


Description
===========

**radostrace** is an eBPF-based tracing tool that provides real-time
visibility into RADOS client operations. It attaches uprobes to the
``Objecter::_send_op`` and ``Objecter::_finish_op`` functions in
librados, and prints one line per completed RADOS operation with its
end-to-end latency, object name, pool, placement group, acting set,
operation types and extent information.

It can trace any running librados-based client -- QEMU/RBD, RGW,
cephfs-mirror, ``rados bench``, etc. -- without code changes,
configuration changes or client restarts. Struct member offsets and
function addresses are extracted at runtime from DWARF debug
information, so it works with any Ceph build as long as debug info is
available (e.g. via the debuginfo/dbgsym packages). The eBPF programs
are verified by the kernel before execution, which makes the tool safe
to use in production. When radostrace is not running there is no
performance impact on the traced processes.

**radostrace** requires root privileges and Linux kernel 5.8 or later
(for BPF ring buffer support).

Options
=======

.. option:: -p pid, --pid pid

   Attach uprobes only to the process with the given process ID. By
   default radostrace traces all processes using the discovered Ceph
   libraries.

.. option:: -t seconds, --timeout seconds

   Run for the specified number of seconds and then exit. By default
   radostrace runs until interrupted.

.. option:: -d, --debug

   Enable debug output for library discovery, DWARF parsing and uprobe
   attachment. BPF-side debug messages can be viewed with
   ``cat /sys/kernel/debug/tracing/trace_pipe``.

.. option:: -h, --help

   Display usage information.

Output
======

radostrace outputs one line per completed RADOS operation::

     pid  client     tid  pool   pg           acting          WR    size  latency     object[ops]
   12345   12345       1     1   1b           [0,1,2]          W    4096     1234     myobject [write][0, 4096]

============  ================================================================
Column        Description
============  ================================================================
pid           Process ID of the client
client        RADOS client ID (global_id from MonClient)
tid           Transaction ID (unique per client)
pool          Pool ID
pg            PG ID
acting        List of OSDs in the acting set for this PG
WR            ``W`` for write operations, ``R`` for read operations
size          Size of the I/O operation in bytes
latency       End-to-end latency in microseconds
object[ops]   Object name followed by operation list and extent info
============  ================================================================

For operations with extents (read/write), the offset and length are
shown in brackets: ``[offset, length]``. RADOS class method invocations
are shown as ``call(class.method)``.

Examples
========

Trace all librados clients on the host::

        sudo radostrace

Trace the I/O of a single QEMU virtual machine::

        pgrep -f "qemu.*myvm"
        12345
        sudo radostrace -p 12345

Trace a radosgw process for one minute::

        sudo radostrace -p $(pgrep radosgw) -t 60

Availability
============

**radostrace** is part of Ceph, a massively scalable, open-source,
distributed storage system. It requires Linux kernel 5.8 or later.
Please refer to the Ceph documentation at https://docs.ceph.com for
more information.

See also
========

:doc:`ceph <ceph>`\(8),
:doc:`rados <rados>`\(8)
