
Application best practices for distributed file systems
=======================================================

CephFS is POSIX compatible, and therefore should work with any existing
applications that expect a POSIX file system.  However, because it is a
network file system (unlike e.g. XFS) and it is highly consistent (unlike
e.g. NFS), there are some consequences that application authors may
benefit from knowing about.

The following sections describe some areas where distributed file systems
may have noticeably different performance behaviors compared with
local file systems.


ls -l
-----

When you run "ls -l", the ``ls`` program
is first doing a directory listing, and then calling ``stat`` on every
file in the directory.

This is usually far in excess of what an application really needs, and
it can be slow for large directories.  If you don't really need all
this metadata for each file, then use a plain ``ls``.

ls/stat on files being extended
-------------------------------

If another client is currently extending files in the listed directory,
then an ``ls -l`` may take an exceptionally long time to complete, as
the lister must wait for the writer to flush data in order to do a valid
read of the every file's size.  So unless you *really* need to know the
exact size of every file in the directory, just don't do it!

This would also apply to any application code that was directly
issuing ``stat`` system calls on files being appended from
another node.

Very large directories
----------------------

Do you really need that 10,000,000 file directory?  While directory
fragmentation enables CephFS to handle it, it is always going to be
less efficient than splitting your files into more modest-sized directories.

Even standard userspace tools can become quite slow when operating on very
large directories. For example, the default behavior of ``ls``
is to give an alphabetically ordered result, but ``readdir`` system
calls do not give an ordered result (this is true in general, not just
with CephFS).  So when you ``ls`` on a million file directory, it is
loading a list of a million names into memory, sorting the list, then writing
it out to the display.

Hard links
----------

Hard links have an intrinsic cost in terms of the internal housekeeping
that a file system has to do to keep two references to the same data.  In
CephFS there is a particular performance cost, because with normal files
the inode is embedded in the directory (i.e. there is no extra fetch of
the inode after looking up the path).

Metadata-heavy workloads and MDS performance
--------------------------------------------

The MDS acts as a cache for the metadata stored in RADOS.  Metadata
performance is very different for workloads whose metadata fits within
that cache (configured using :confval:`mds_cache_memory_limit`; see
:doc:`cache-configuration`).

If your workload has more files than fit in your cache, then make sure you
test it appropriately: don't test your system with a small number of files and
then expect equivalent performance when you move to a much larger number of
files.

Benchmark findings
~~~~~~~~~~~~~~~~~~

Benchmarking with the SPECstorage Solution 2020 SWBUILD workload (a
metadata-intensive software-build simulation) shows that MDS throughput and
latency are governed primarily by two resources: the metadata cache size and
CPU core allocation.

**Cache size sets capacity.**  Configurations of 128 GiB or larger sustain
roughly 13--15 thousand metadata operations per second (kops/s) with average
latency below 30 ms, even when clients request up to 36 kops/s.  At 64 GiB,
performance remains stable up to about 15 kops/s but degrades sharply at higher
load (latency reaching hundreds of milliseconds).  Below 64 GiB, cache eviction
thrashing causes an abrupt performance cliff: achieved throughput drops and
latency rises from milliseconds to seconds once the working set exceeds the
cache.

**CPU cores set scalability.**  With a fixed 64 GiB cache, scaling from one to
four dedicated MDS cores roughly doubles sustainable throughput (from about 9
kops/s to about 13 kops/s) and flattens the latency curve under pressure.
A single core is adequate only for light metadata loads (below about 9 kops/s).

**Combined ceiling.**  Even with 256 GiB cache and four cores, throughput
plateaus around 14--15 kops/s for this benchmark, indicating limits from
other cluster components (CPU, network, or backing storage) in addition to MDS
resources.

Recommendations
~~~~~~~~~~~~~~~

For metadata-heavy production workloads, start with **128 GiB cache and four
dedicated MDS cores**.  This combination provides a practical balance of
throughput (10--15 kops/s), low latency, and resource efficiency for
workloads similar to the SWBUILD benchmark.

Use the following guidelines as a starting point, then tune for your specific
metadata working set:

* **Cache sizing**

  * Below 5 kops/s: 32 GiB may suffice.
  * 5--15 kops/s: 128 GiB (recommended baseline).
  * Above 15 kops/s or bursty workloads: 192--256 GiB or more.

  Maintain 15--20% headroom above your observed working set.  Monitor
  ``mds.cache.memory_usage`` and ``mds.cache.hit_ratio`` to detect when the
  cache is undersized before clients see latency spikes.

* **CPU allocation**

  * One core: light-duty only (below about 9 kops/s).
  * Two cores: acceptable up to about 12 kops/s.
  * Four cores: production standard for workloads of 10 kops/s or more.

  Pin the MDS to dedicated cores (via Ceph orchestrator placement or
  ``taskset``) and isolate it from other services on the host.

* **Deployment**

  * Validate tuning with traces from your actual application, not synthetic
    benchmarks alone.
  * Deploy active/standby MDS pairs with **identical** cache and CPU resources
    on each host (see :ref:`mds-standby`).
  * For extreme scale, consider :ref:`multiple active MDS ranks <cephfs-multimds>`
    with directory pinning rather than relying on unbounded growth of a single
    active MDS.

* **Monitoring**

  Watch ``mds.cache.hit_ratio``, ``mds.cache.memory_usage``, ``mds.cpu.util``,
  and ``mds.latency.*``.  Investigate when cache utilization consistently
  exceeds 80%, average latency exceeds 10 ms, or achieved throughput falls
  below 95% of what clients request.

Caveats and trade-offs
~~~~~~~~~~~~~~~~~~~~~~

**Larger caches improve steady-state performance but affect failover.**
A bigger ``mds_cache_memory_limit`` means more metadata state to warm during
MDS recovery.  After a failover, the replacement MDS must replay its journal
and rebuild its cache before reaching full performance; a larger cache
generally implies a longer recovery interval and more work during client
reconnect.  :ref:`Standby-replay <mds-standby-replay>` reduces failover time
by keeping a standby daemon synchronized with the active MDS journal, but it
consumes additional CPU and memory on the standby host.

**Undersizing is worse than modest over-provisioning.**  Both insufficient
cache and insufficient CPU produce the same signature: a sudden drop in
achieved operation rate accompanied by a steep latency increase.  The resulting
client-visible slowdowns are typically more costly than allocating modest
extra RAM and CPU upfront.

**Benchmark results are workload-specific.**  The SWBUILD numbers above
reflect a particular mix of ``stat``, ``readdir``, ``create``, ``unlink``,
and related operations.  Your application's metadata access pattern, directory
layout, and client count may yield different absolute limits.  Use these
results as a reproducible baseline for capacity planning, not as guaranteed
production ceilings.

Do you need a file system?
--------------------------

Remember that Ceph also includes an object storage interface.  If your
application needs to store huge flat collections of files where you just
read and write whole files at once, then you might well be better off
using the :ref:`Object Gateway <object-gateway>`
