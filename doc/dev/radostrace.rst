==========
RadosTrace
==========

RadosTrace is an eBPF-based tracing tool that provides real-time visibility into
RADOS client operations. It captures latency, object names, target OSDs, and
other operation details for every request flowing through librados. It can be 
used to trace any librados based Ceph clients, including VMs, Cinder/Glance, radosgw,
cephfs-mirror, etc.

Why eBPF for Ceph?
==================

Traditional tracing approaches in Ceph (LTTng, Blkin, Jaeger) require either
recompilation with tracing enabled or explicit instrumentation in the code.
eBPF (Extended Berkeley Packet Filter) brings a fundamentally different approach
with several advantages:

**Zero overhead when disabled**
  Unlike compile-time tracing options, eBPF programs are attached dynamically
  at runtime. When radostrace is not running, there is absolutely no performance
  impact on Ceph processes.

**Dynamic tracing**
  Attach to any running Ceph client process instantly without restarting
  services, changing configuration files, or redeploying. Simply point
  radostrace at a live process and immediately see its RADOS operations.
  When done, detach with no lasting impact on the traced application.

**Production-safe**
  eBPF programs are verified by the Linux kernel before execution, guaranteeing
  they cannot crash the system, access invalid memory, or cause infinite loops.
  This makes eBPF safe to use in production environments.

**No code modification required**
  RadosTrace uses uprobes to attach to existing functions in Ceph libraries.
  There's no need to add static tracepoints to enable tracing.

**Foundation for future tools**
  The initial RadosTrace implementation establishes the eBPF infrastructure in Ceph. The same 
  approach can be extended to trace OSD operations, BlueStore internals, MDS 
  operations/caps and more.

What RadosTrace Captures
========================

RadosTrace instruments the ``Objecter::_send_op`` and ``Objecter::_finish_op``
functions in librados to capture:

- **Latency**: End-to-end time from operation submission to completion
- **Object name**: The RADOS object being accessed
- **Pool and PG**: Pool ID and placement group (m_seed)
- **Target OSDs**: Which OSDs handled the request
- **Operation type**: Read/Write flag and specific OSD operations
- **Extent information**: Offset and length for read/write operations
- **Class methods**: For RADOS class calls, the class and method names

Requirements
============

Kernel and System
-----------------

- Linux kernel 5.8 or later (for BPF ring buffer support)

Building RadosTrace
===================

RadosTrace is built as part of Ceph when the ``WITH_EBPF_TRACE`` option is
enabled (it is ON by default):

.. code-block:: bash

   cd ceph
   ./do_cmake.sh -DWITH_EBPF_TRACE=ON
   cd build
   ninja radostrace

The resulting binary is located at ``build/bin/radostrace``.

To explicitly disable radostrace::

   ./do_cmake.sh -DWITH_EBPF_TRACE=OFF

Usage
=====

Basic Usage
-----------

Run radostrace with root privileges to trace all processes using librados:

.. code-block:: bash

   sudo radostrace

The tool will automatically discover the Ceph libraries and attach uprobes to the relevant functions.

Command Line Options
--------------------

``-p, --pid <pid>``
  Trace only the specified process. This is useful when multiple applications
  are using librados and you want to focus on one:

  .. code-block:: bash

     sudo radostrace -p 12345

``-t, --timeout <seconds>``
  Run for a specified duration and then exit:

  .. code-block:: bash

     sudo radostrace -t 60

``-d, --debug``
  Enable debug output. This prints detailed information about library discovery,
  DWARF parsing, and uprobe attachment. BPF-side debug messages can be viewed
  with:

  .. code-block:: bash

     sudo radostrace -d &
     sudo cat /sys/kernel/debug/tracing/trace_pipe

``-h, --help``
  Display usage information.

Output Format
=============

RadosTrace outputs one line per completed RADOS operation:

.. code-block:: text

     pid  client     tid  pool   pg           acting          WR    size  latency     object[ops]
   12345   12345       1     1   1b           [0,1,2]          W    4096     1234     myobject [write][0, 4096]

Column Descriptions
-------------------

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

Operation Types
---------------

The operation list shows the specific RADOS operations performed:

- ``read``, ``write``, ``writefull``: Basic I/O operations
- ``stat``: Object stat
- ``call(class.method)``: RADOS class method invocation
- ``create``, ``delete``: Object lifecycle operations
- Many more (see ``src/include/rados.h`` for the full list)

For operations with extents (read/write), the offset and length are shown
in brackets: ``[offset, length]``.

Examples
========

Tracing a QEMU Virtual Machine
------------------------------

QEMU uses librbd to access RBD images. To trace a specific VM's I/O operations:

.. code-block:: bash

   # Find the QEMU process ID for your VM
   pgrep -f "qemu.*myvm"
   # Example output: 12345

   # Trace only that VM's RADOS operations
   sudo radostrace -p 12345

You'll see the RBD data objects being accessed::

     pid  client     tid  pool   pg           acting          WR    size  latency     object[ops]
   12345   12345       1     2   f2           [1,4,7]          W    4096      892     rbd_data.1a2b3c.0000000000000001 [write][0, 4096]
   12345   12345       2     2   c1           [2,5,8]          R    4096      456     rbd_data.1a2b3c.0000000000000002 [read][0, 4096]

This is useful for diagnosing VM I/O latency issues, understanding access patterns,
and correlating guest workloads with RADOS operations.

Tracing a RADOS Gateway (RGW)
-----------------------------

To trace low-level RADOS operations performed by a radosgw process:

.. code-block:: bash

   # Find the radosgw process
   pgrep radosgw
   # Example output: 54321

   # Trace the RGW's RADOS operations
   sudo radostrace -p 54321

This helps identify slow RADOS operations, and understand how RGW access RADOS-level objects.

Tracing a vstart Development Cluster
------------------------------------

For development with a vstart cluster, trace RBD bench operations:

.. code-block:: bash

   # Start a vstart cluster (from build directory)

   # Create a pool and RBD image

   # Terminal 1: Start radostrace (it auto-discovers libraries from build/lib)
   sudo ./bin/radostrace

   # Terminal 2: Run rbd bench to generate I/O
   ./bin/rbd bench --io-type write --io-size 4K --io-total 10M <pool>/<img>

You'll see the benchmark operations in real-time::

     pid  client     tid  pool   pg           acting          WR    size  latency     object[ops]
   98765   98765       1     1   1a           [0,1,2]          W    4096     1523     rbd_data.1234.0000000000000000 [write][0, 4096]
   98765   98765       2     1   2b           [1,2,0]          W    4096     1102     rbd_data.1234.0000000000000001 [write][0, 4096]

Output Analysis
===============

For analyzing RadosTrace output to identify problematic OSDs and correlate them
with their physical hosts, refer to the `analysis tool documentation
<https://github.com/taodd/cephtrace/blob/main/doc/analyze-radostrace.md>`_.

Architecture
============

RadosTrace consists of three main components:

BPF Program (radostrace.bpf.c)
------------------------------

The eBPF program runs in kernel context and is attached to two uprobes:

1. ``uprobe_send_op``: Attached to ``Objecter::_send_op``, captures operation
   details when a request is sent to an OSD.

2. ``uprobe_finish_op``: Attached to ``Objecter::_finish_op``, captures the
   completion timestamp and calculates latency.

Data is passed between probes using a BPF hash map keyed by (client_id, tid),
and completed operations are sent to userspace via a BPF ring buffer.

DWARF Parser (dwarf_parser.cc)
------------------------------

Since librados is compiled code, the exact memory locations of function
parameters and struct members vary by build. The DWARF parser reads debug
information from the Ceph libraries to determine:

- Function entry points for uprobe attachment
- Register/stack locations of function parameters
- Struct member offsets for data extraction

This allows radostrace to work with any Ceph build without hardcoded offsets.

Userspace Loader (radostrace.cc)
--------------------------------

The userspace component:
1. Discovers Ceph library paths
2. Parses DWARF information to locate probe points
3. Loads the BPF program and sets struct offset constants
4. Attaches uprobes to the target functions
5. Polls the ring buffer and formats output

