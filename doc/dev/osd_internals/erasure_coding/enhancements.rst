===========================
Erasure coding enhancements
===========================

Objectives
==========

Our objective is to improve the performance of erasure coding, in particular
for small random accesses to make it more viable to use erasure coding pools
for storing block and file data.

We are looking to reduce the number of OSD read and write accesses per client
I/O (sometimes referred to as I/O amplification), reduce the amount of network
traffic between OSDs (network bandwidth) and reduce I/O latency (time to
complete read and write I/O operations). We expect the changes will also
provide modest reductions to CPU overheads.

While the changes are focused on enhancing small random accesses, some
enhancements will provide modest benefits for larger I/O accesses and for
object storage.

The following sections give a brief description of the improvements we are
looking to make. Please see the later design sections for more details

Current Read Implementation
---------------------------

For reference this is how erasure code reads currently work

.. ditaa::

 RADOS Client
                            * Current code reads all data chunks
      ^                     * Discards unneeded data
      |                     * Returns requested data to client
 +----+----+
 | Discard |                If data cannot be read then the coding parity
 |unneeded |                chunks are read as well and are used to reconstruct
 |  data   |                the data
 +---------+
    ^^^^
    ||||
    ||||
    ||||
    |||+----------------------------------------------+
    ||+-------------------------------------+         |
    |+----------------------------+         |         |
    |                             |         |         |
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
 Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
   OSD

Note: All the diagrams illustrate a K=4 + M=2 configuration, however the
concepts and techniques can be used for all K+M configurations.

Partial Reads
-------------

If only a small amount of data is being read it is not necessary to read the
whole stripe, for small I/Os ideally only a single OSD needs to be involved in
reading the data. See also larger chunk size below.

.. ditaa::

 RADOS Client
                            * Optimize by only reading required chunks
      ^                     * For large chunk sizes and sub-chunk reads only
      |                     read a sub-chunk
 +----+----+
 | Return  |                If data cannot be read then extra data and coding
 |  data   |                parity chunks are read as well and are used to
 |         |                reconstruct the data
 +---------+
     ^
     |
     |
     |
     |
     |
     +----------------------------+
                                  |
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
 Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
   OSD

Pull Request https://github.com/ceph/ceph/pull/55196 is implementing most of
this optimization, however it still issues full chunk reads.

Current Overwrite Implementation
--------------------------------

For reference here is how erasure code overwrites currently work

.. ditaa::

 RADOS Client
      |                     * Read all data chunks
      |                     * Merges new data
 +----v-----+               * Encodes new coding parities
 | Read old |               * Writes data and coding parities
 |Merge new |
 |  Encode  |-------------------------------------------------------------+
 |  Write   |---------------------------------------------------+         |
 +----------+                                                   |         |
    ^|^|^|^|                                                    |         |
    |||||||+-------------------------------------------+        |         |
    ||||||+-------------------------------------------+|        |         |
    |||||+-----------------------------------+        ||        |         |
    ||||+-----------------------------------+|        ||        |         |
    |||+---------------------------+        ||        ||        |         |
    ||+---------------------------+|        ||        ||        |         |
    |v                            |v        |v        |v        v         v
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
 Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
   OSD

Partial Overwrites
------------------

Ideally we aim to be able to perform updates to erasure coded stripes by only
updating a subset of the shards (those with modified data or coding
parities). Avoiding performing unnecessary data updates on the other shards is
easy, avoiding performing any metadata updates on the other shards is much
harder (see design section on metadata updates).

.. ditaa::

 RADOS Client
      |                     * Only read chunks that are not being overwritten
      |                     * Merge new data
 +----v-----+               * Encode new coding parities
 | Read old |               * Only write modified data and parity shards
 |Merge new |
 |  Encode  |-------------------------------------------------------------+
 |  Write   |---------------------------------------------------+         |
 +----------+                                                   |         |
    ^  |^ ^                                                     |         |
    |  || |                                                     |         |
    |  || +-------------------------------------------+         |         |
    |  ||                                             |         |         |
    |  |+-----------------------------------+         |         |         |
    |  +---------------------------+        |         |         |         |
    |                              |        |         |         |         |
    |                              v        |         |         v         v
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
 Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
   OSD

This diagram is overly simplistic, only showing the data flows. The simplest
implementation of this optimization retains a metadata write to every
OSD. With more effort it is possible to reduce the number of metadata updates
as well, see design below for more details.

Parity-delta-write
------------------

A common technique used by block storage controllers implementing RAID5 and
RAID6 is to implement what is sometimes called a parity delta write. When a
small part of the stripe is being overwritten it is possible to perform the
update by reading the old data, XORing this with the new data to create a
delta and then read each coding parity, apply the delta and write the new
parity. The advantage of this technique is that it can involve a lot less I/O,
especially for K+M encodings with larger values of K. The technique is not
specific to M=1 and M=2, it can be applied with any number of coding parities.

.. ditaa::

                        Parity delta writes
                        * Read old data and XOR with new data to create a delta
 RADOS Client           * Read old encoding parities apply the delta and write
    |                     the new encoding parities
    |                   
    |                   For K+M erasure codings where K is larger and M is small
    |  +-----+    +-----+  this is much more efficient
    +->| XOR |-+->| GF  |---------------------------------------------------+
  +-+->|     | |  |     |<------------------------------------------------+ |
  | |  +-----+ |  +-----+                                                 | |
  | |          |                                                          | |
  | |          |  +-----+                                                 | |
  | |          +->| XOR |-----------------------------------------+       | |
  | |             |     |<--------------------------------------+ |       | |
  | |             +-----+                                       | |       | |
  | |                                                           | |       | |
  | |                                                           | |       | |
  | +-------------------------------+                           | |       | |
  +-------------------------------+ |                           | |       | |
                                  | |                           | |       | |
                                  | v                           | v       | v
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
  Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
    OSD

Direct Read I/O
---------------

We want clients to submit small I/Os directly to the OSD that stores the data
rather than directing all I/O requests to the Primary OSD and have it issue
requests to the secondary OSDs. By eliminating an intermediate hop this
reduces network bandwidth and improves I/O latency

.. ditaa::

         RADOS Client
               ^
               |
          +----+----+     Client sends small read requests directly to OSD
          | Return  |     avoiding extra network hop via Primary
          |  data   |
          |         |
          +---------+
               ^
               |
               |
               |
               |
               |
               |
               |
  .-----.   .-----.   .-----.   .-----.   .-----.   .-----.
 (       ) (       ) (       ) (       ) (       ) (       )
 |`-----'| |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       | |       | |       | |       | |       | |       |
 |       | |       | |       | |       | |       | |       |
 (       ) (       ) (       ) (       ) (       ) (       )
  `-----'   `-----'   `-----'   `-----'   `-----'   `-----'
  Primary    OSD 2     OSD 3     OSD 4     OSD P     OSD Q
    OSD


.. ditaa::

               RADOS Client
               ^         ^
               |         |
          +----+----+ +--+------+  Client breaks larger read
          | Return  | | Return  |  requests into separate
          |  data   | |  data   |  requests to multiple OSDs
          |         | |         |  
          +---------+ +---------+  Note client loses atomicity
               ^         ^         guarantees if this optimization
               |         |         is used as an update could occur
               |         |         between the two reads
               |         |
               |         |
               |         |
               |         |
               |         |
  .-----.   .-----.   .-----.   .-----.   .-----.   .-----.
 (       ) (       ) (       ) (       ) (       ) (       )
 |`-----'| |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       | |       | |       | |       | |       | |       |
 |       | |       | |       | |       | |       | |       |
 (       ) (       ) (       ) (       ) (       ) (       )
  `-----'   `-----'   `-----'   `-----'   `-----'   `-----'
  Primary    OSD 2     OSD 3     OSD 4     OSD P     OSD Q
    OSD

Distributed processing of writes
--------------------------------

The existing erasure code implementation processes write I/Os on the primary
OSD, issuing both reads and writes to other OSDs to fetch and update data for
other shards. This is perhaps the simplest implementation, but it uses a lot
of network bandwidth. With parity-delta-writes it is possible to distribute
the processing across OSDs to reduce network bandwidth.

.. ditaa::

               Performing the coding parity delta updates on the coding parity
               OSD instead of the primary OSD reduces network bandwidth
 RADOS Client
    |          Note: A naive implementation will increase latency by serializing
    |          the data and coding parity reads, for best performance these
    |          reads need to happen in parallel
    |  +-----+                                                          +-----+
    +->| XOR |-+------------------------------------------------------->| GF  |
  +-+->|     | |                                                        |     |
  | |  +-----+ |                                                        +----++
  | |          |                                              +-----+     ^ |
  | |          +--------------------------------------------->| XOR |     | |
  | |                                                         |     |     | |
  | |                                                         +---+-+     | |
  | +-------------------------------+                           ^ |       | |
  +-------------------------------+ |                           | |       | |
                                  | |                           | |       | |
                                  | |                           | |       | |
                                  | |                           | |       | |
                                  | |                           | |       | |
                                  | v                           | v       | v
  .-----.                       .-----.   .-----.   .-----.   .-----.   .-----.
 (       )                     (       ) (       ) (       ) (       ) (       )
 |`-----'|                     |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       |                     |       | |       | |       | |       | |       |
 |       |                     |       | |       | |       | |       | |       |
 (       )                     (       ) (       ) (       ) (       ) (       )
  `-----'                       `-----'   `-----'   `-----'   `-----'   `-----'
  Primary                        OSD 2     OSD 3     OSD 4     OSD P     OSD Q
    OSD

Direct Write I/O
----------------

.. ditaa::

             RADOS Client
                  |
                  |  Similarly Clients could direct small write I/Os
                  |  to the OSD that needs updating
                  |
                  |  +-----+                        +-----+
                  +->| XOR |-+--------------------->| GF  |
            +-----+->|     | |                      |     | 
            |     |  +-----+ |                      +----++
            |     |          |            +-----+     ^ |
            |     |          +----------->| XOR |     | |
            |     |                       |     |     | |
            |     |                       +---+-+     | |
            |     |                         ^ |       | |
            |     |                         | |       | |
            |     |                         | |       | |
            |     |                         | |       | |
            |     |                         | |       | |
            |     |                         | |       | |
            |     v                         | v       | v
  .-----.   .-----.   .-----.   .-----.   .-----.   .-----.
 (       ) (       ) (       ) (       ) (       ) (       )
 |`-----'| |`-----'| |`-----'| |`-----'| |`-----'| |`-----'|
 |       | |       | |       | |       | |       | |       |
 |       | |       | |       | |       | |       | |       |
 (       ) (       ) (       ) (       ) (       ) (       )
  `-----'   `-----'   `-----'   `-----'   `-----'   `-----'
  Primary    OSD 2     OSD 3     OSD 4     OSD P     OSD Q
    OSD

This diagram is overly simplistic, only showing the data flows - direct writes
are much harder to implement and will need control messages to the Primary to
ensure writes to the same stripe are ordered correctly

Larger chunk size
-----------------

The default chunk size is 4K, this is too small and means that small reads
have to be split up and processed by many OSDs. It is more efficient if small
I/Os can be serviced by a single OSD. Choosing a larger chunk size such as 64K
or 256K and implementing partial reads and writes will fix this issue, but has
the disadvantage that small sized RADOS objects get rounded up in size to a
whole stripe of capacity.

We would like the code to automatically choose what chunk size to use to
optimize for both capacity and performance. Small objects should use a small
chunk size like 4K, larger objects should use a larger chunk size.

Code currently rounds up I/O sizes to multiples of the chunk size, which isn't
an issue with a small chunk size. With a larger chunk size and partial
reads/writes we should round up to the page size rather than the chunk size.

Design
======

We will describe the changes we aim to make in three sections, the first
section looks at the existing test tools for erasure coding and discusses the
improvements we believe will be necessary to get good test coverage for the
changes.

The second section covers changes to the read and write I/O path.

The third section discusses the changes to metadata to avoid the need to
update metadata on all shards for each metadata update. While it is possible
to implement many of the I/O path changes without reducing the number of
metadata updates, there are bigger performance benefits if the number of
metadata updates can be reduced as well.

Test tools
----------

A survey of the existing test tools shows that there is insufficient coverage
of erasure coding to be able to just make changes to the code and expect the
existing CI pipelines to get sufficient coverage. Therefore one of the first
steps will be to improve the test tools to be able to get better test
coverage.

Teuthology is the main test tool used to get test coverage and it relies
heavily on the following tests for generating I/O:

1. **rados** task - qa/tasks/rados.py. This uses ceph_test_rados
   (src/test/osd/TestRados.cc) which can generate a wide mixture of different
   rados operations. There is limited support for read and write I/Os,
   typically using offset 0 although there is a chunked read command used by a
   couple of tests.

2. **radosbench** task - qa/tasks/radosbench.py. This uses the **rados bench**
   (src/tools/rados/rados.cc and src/common/obj_bencher.cc). Can be used to
   generate sequential and random I/O workloads, offset starts at 0 for
   sequential I/O. I/O size can be set but is constant for whole test.

3. **rbd_fio** task - qa/tasks/fio.py. This uses **fio** to generate
   read/write I/O to an rbd image volume

4. **cbt** task - qa/tasks/cbt.py. This uses the Ceph benchmark tool **cbt**
   to run fio or radosbench to benchmark the performance of a cluster.

5. **rbd bench**. Some of the standalone tests use rbd bench
   (src/tools/rbd/action/Bench.cc) to generate small amounts of I/O
   workload. It is also used by the **rbd_pwl_cache_recovery** task.

It is hard to use these tools to get good coverage of I/Os to non-zero (and
non-stripe aligned) offsets, or to generate a wide variety of offsets and
lengths of I/O requests including all the boundary cases for chunks and
stripes. There is scope to improve either rados, radosbench or rbd bench to
generate much more interesting I/O patterns for testing erasure coding.

For the optimizations described above it is essential that we have good tools
for checking the consistency of either selected objects or all objects in an
erasure coded pool by checking that the data and coding parities are
coherent. There is a test tool **ceph-erasure-code-tool** which can use the
plugins to encode and decode data provided in a set of files. However there
does not seem to be any scripting in teuthology to perform consistency checks
by using objectstore tool to read data and then using this tool to validate
consistency. We will write some teuthology helpers that use
ceph-objectstore-tool and ceph-erasure-code-tool to perform offline
validation.

We would also like an online way of performing full consistency checks, either
for specific objects or for a whole pool. Inconveniently EC pools do not
support class methods so it's not possible to use this as a way of
implementing a full consistency check. We will investigate putting a flag on a
read request, on the pool or implementing a new request type to perform a full
consistency check on an object and look at making extensions to the rados CLI
to be able to perform these tests. See also the discussion on deep scrub
below.

When there is more than one coding parity and there is an inconsistency
between the data and the coding parities it is useful to try and analyze the
cause of the inconsistency. Because the multiple coding parities are providing
redundancy, there can be multiple ways of reconstructing each chunk and this
can be used to detect the most like cause of the inconsistency. For example
with a 4+2 erasure coding and a dropped write to 1st data OSD, the stripe (all
6 OSDs) will be inconsistent, as will be any selection of 5 OSDs that includes
the 1st data OSD, but data OSDs 2,3 and 4 and the two coding parity OSDs will
be still be consistent. While there are many ways a stripe could get into this
state, a tool could conclude that the most likely cause is a missed update to
OSD 1. Ceph does not have a tool to perform this type of analysis, but it
should be easy to extend ceph-erasure-code-tool.

Teuthology seems to have adequate tools for taking OSDs offline and bringing
them back online again. There are a few tools for injecting read I/O errors
(without taking an OSD offline) but there is scope to improve these
(e.g. ability to specify a particular offset in an object that will fail a
read, more controls over setting and deleting error inject sites).

The general philosophy of teuthology seems to be to randomly inject faults and
simply through brute force get sufficient coverage of all the error
paths. This is a good approach for CI testing, however when EC code paths
become complex and require multiple errors to occur with precise timings to
cause a particular code path to execute it becomes hard to get coverage
without running the tests for a very long time. There are some standalone
tests for EC which do test some of the multiple failure paths, but these tests
perform very limited amounts of I/O and don't inject failures while there are
I/Os in flight so miss some of the interesting scenarios.

To deal with these more complex error paths we propose developing a new type
of thrasher for erasure coding that injects a sequence of errors and makes use
of debug hooks to capture and delay I/O requests at particular points to
ensure an error inject hits a particular timing window. To do this we will
extend the tell osd command to include extra interfaces to inject errors and
capture and stall I/Os at specific points.

Some parts of erasure coding such as the plugins are stand alone bits of code
which can be tested with unit tests. There are already some unit tests and
performance benchmark tools for erasure coding, we will look to extend these
to get further coverage of code that can be run stand alone.

I/O path changes
----------------

Avoid unnecessary reads and writes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The current code reads too much data for read and overwrite I/Os. For
overwrites it will also rewrite unmodified data. This occurs because reads and
overwrites are rounded up to full-stripe operations. This isn’t a problem when
data is mainly being accessed sequentially but is very wasteful for random I/O
operations. The code can be changed to only read/write necessary shards. To
allow the code to efficiently support larger chunk sizes I/Os should be
rounded to page size I/Os instead of chunk sized I/Os.

The first simple set of optimizations eliminates unnecessary reads and
unnecessary writes of data, but retains writes of metadata on all shards. This
avoids breaking the current design which depends on all shards receiving a
metadata update for every transaction. When changes to the metadata handling
are completed (see below) then it will be possible to make further
optimizations to reduce the number of metadata updates for additional savings.

Parity-delta-write
^^^^^^^^^^^^^^^^^^

The current code implements overwrites by performing a full-stripe read,
merging the overwritten data, calculating new coding parities and performing a
full-stripe write. Reading and writing every shard is expensive, there are a
number of optimizations that can be applied to speed this up. For a K+M
configuration where M is small, it is often less work to perform a
parity-delta-write. This is implemented by reading the old data that is about
to be overwritten and XORing it with the new data to create a delta. The
coding parities can then be read, updated to apply the delta and
re-written. With M=2 (RAID6) this can result in just 3 read and 3 writes to
perform an overwrite of less than one chunk.

Note that where a large fraction of the data in the stripe is being updated,
this technique can result in more work than performing a partial overwrite,
however if both update techniques are supported it is fairly easy to calculate
for a given I/O offset and length which is the optimal technique to use.

Write I/Os submitted to the Primary OSD will perform this calculation to
decide whether to use a full-stripe update or a parity-delta-write. Note that
if read failures are encountered while performing a parity-delta-write and it
is necessary to reconstruct data or a coding parity then it will be more
efficient to switch to performing a full-stripe read, merge and write.

Not all erasure codings and erasure coding libraries support the capability of
performing delta updates, however those implemented using XOR and/or GF
arithmetic should. We have checked jerasure and isa-l and confirmed that they
support this feature, although the necessary APIs are not currently exposed by
the plugins. For some erasure codes such as clay and lrc it may be possible to
apply delta updates, but the delta may need to be applied in so many places
that this makes it a worthless optimization. This proposal suggests that
parity-delta-write optimizations are initially implemented only for the most
commonly used erasure codings. Erasure code plugins will provide a new flag
indicating whether they support the new interfaces needed to perform delta
updates.

Direct reads
^^^^^^^^^^^^

Read I/Os are currently directed to the primary OSD which then issues reads to
other shards. To reduce I/O latency and network bandwidth it would be better
if clients could issue direct read requests to the OSD storing the data,
rather than via the primary. There are a few error scenarios where the client
may still need to fallback to submitting reads to the primary, a secondary OSD
will have the option of failing a direct read with -EAGAIN to request the
client retries the request to the primary OSD.

Direct reads will always be for <= one chunk. For reads of more than one chunk
the client can issue direct reads to multiple OSDs, however these will no
longer guaranteed to be atomic because an update (write) may be applied in
between the separate read requests. If a client needs atomicity guarantees
they will need to continue to send the read to the primary.

Direct reads will be failed with EAGAIN where a reconstruct and decode
operation is required to return the data. This means only reads to primary OSD
will need to handle the reconstruct code path. When an OSD is backfilling we
don't want the client to have large quantities of I/O failed with EAGAIN,
therefore we will make the client detect this situation and avoid issuing
direct I/Os to a backfilling OSD.

For backwards compatibility, for client requests that cannot cope with the
reduced guarantees of a direct read, and for scenarios where the direct read
would be to an OSD that is absent or backfilling, reads directed to the
primary OSD will still be supported.

Direct writes
^^^^^^^^^^^^^

Write I/Os are currently directed to the primary OSD which then updates the
other shards. To reduce latency and network bandwidth it would be better if
clients could direct small overwrites requests directly to the OSD storing the
data, rather than via the primary. For larger write I/Os and for error
scenarios and abnormal cases clients will continue to submit write I/Os to the
primary OSD.

Direct writes will always be for <= one chunk and will use the
parity-delta-write technique to perform the update. For medium sized writes a
client may issue direct writes to multiple OSDs, but such updates will no
longer be guaranteed to be atomic. If a client requires atomicity for a larger
write they will need to continue to send it to the primary.

For backwards compatibility, and for scenarios where the direct write would be
to an OSD that is absent, writes directed to the primary OSD will still be
supported.

I/O serialization, recovery/backfill and other error scenarios
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Direct writes look fairly simple until you start considering all the abnormal
scenarios. The current implementation of processing all writes on the Primary
OSD means that there is one central point of control for the stripe that can
manage things like the ordering of multiple inflight I/Os to the same stripe,
ensuring that recovery/backfill for an object has been completed before it is
accessed and assigning the object version number and modification time.

With direct I/Os these become distributed problems. Our approach is to send a
control path message to the Primary OSD and let it continue to be the central
point of control. The Primary OSD will issue a reply when the OSD can start
the direct write and will be informed with another message when the I/O has
completed. See section below on metadata updates for more details.

Stripe cache
^^^^^^^^^^^^

Erasure code pools maintain a stripe cache which stores shard data while
updates are in progress. This is required to allow writes and reads to the
same stripe to be processed in parallel. For small sequential write workloads
and for extreme hot spots (e.g. where the same block is repeatedly re-written
for some kind of crude checkpointing mechanism) there would be a benefit in
keeping the stripe cache slightly longer than the duration of the I/O. In
particularly the coding parities are typically read and written for every
update to a stripe. There is obviously a balancing act to achieve between
keeping the cache long enough that it reduces the overheads for future I/Os
versus the memory overheads of storing this data. A small (MiB as opposed to
GiB sized cache) should be sufficient for most workloads. The stripe cache can
also help reduce latency for direct write I/Os by allowing prefetch I/Os to
read old data and coding parities ready for later parts of the write operation
without requiring more complex interlocks.

The stripe cache is less important when the default chunk size is small
(e.g. 4K), because even with small write I/O requests there will not be many
sequential updates to fill a stripe. With a larger chunk size (e.g. 64K) the
benefits of a good stripe cache become more significant because the stripe
size will be 100’s KiB to small number of MiB’s and hence it becomes much more
likely that a sequential workload will issue many I/Os to the same stripe.

Automatically choose chunk size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default chunk size of 4K is good for small objects because the data and
coding parities are rounded up to whole chunks and because if an object has
less than one data stripe of data then the capacity overheads for the coding
parities are higher (e.g. a 4K object in a 10+2 erasure coded pool has 4K of
data and 8K of coding parity, so there is a 200% overhead). However the
optimizations above all provide much bigger savings if the typical random
access I/O only reads or writes a single shard. This means that so long as
objects are big enough that a larger chunk size such as 64K would be better.

Whilst the user can try and predict what their typically object size will be
and choose an appropriate chunk size, it would be better if the code could
automatically select a small chunk size for small objects and a larger chunk
size for larger objects. There will always be scenarios where an object grows
(or is truncated) and the chosen chunk size becomes inappropriate, however
reading and re-writing the object with a new chunk size when this happens
won’t have that much performance impact. This also means that the chunk size
can be deduced from the object size in object_info_t which is read before the
objects data is read/modified. Clients already provide a hint as to the object
size when creating the object so this could be used to select a chunk size to
reduce the likelihood of having to re-stripe an object

The thought is to support a new chunk size of auto/variable to enable this
feature, it will only be applicable for newly created pools, there will be no
way to migrate an existing pool.

Deep scrub support
^^^^^^^^^^^^^^^^^^

EC Pools with overwrite do not check CRCs because it is too costly to update
the CRC for the object on every overwrite, instead the code relies on
Bluestore to maintain and check CRCs. When an EC pool is operating with
overwrite disabled a CRC is kept for each shard, because it is possible to
update CRCs as the object is appended to just by calculating a CRC for the new
data being appended and then doing a simple (quick) calculation to combine the
old and new CRC together.

In dev/osd_internals/erasure_coding/proposals.rst it discusses the possibility
of keeping CRCs at a finer granularity (for example per chunk), storing these
either as an xattr or an omap (omap is more suitable as large objects could
end up with a lot of CRC metadata) and updating these CRCs when data is
overwritten (the update would need to perform a read-modify-write at the same
granularity as the CRC). These finer granularity CRCs can then easily be
combined to produce a CRC for the whole shard or even the whole erasure coded
object.

This proposal suggests going in the opposite direction - EC overwrite pools
have survived without CRCs and relied on Bluestore up until now, so why is
this feature needed? The current code doesn’t check CRCs if overwrite is
enabled, but sadly still calculates and updates a CRC in the hinfo xattr, even
if performing overwrites which mean that the calculated value will be
garbage. This means we pay all the overheads of calculating the CRC and get no
benefits.

The code can easily be fixed so that CRCs are calculated and maintained when
objects are written sequentially, but as soon as the first overwrite to an
object occurs the hinfo xattr will be discarded and CRCs will no longer be
calculated or checked. This will improve performance when objects are
overwritten, and will improve data integrity in cases where they are not.

While the thought is to abandon EC storing CRCs in objects being overwritten,
there is an improvement that can be made to deep scrub. Currently deep scrub
of an EC with overwrite pool just checks that every shard can read the object,
there is no checking to verify that the copies on the shards are consistent. A
full consistency check would require large data transfers between the shards
so that the coding parities could be recalculated and compared with the stored
versions, in most cases this would be unacceptably slow. However for many
erasure codes (including the default ones used by Ceph) if the contents of a
chunk are XOR’d together to produce a longitudinal summary value, then an
encoding of the longitudinal summary values of each data shard should produce
the same longitudinal summary values as are stored by the coding parity
shards. This comparison is less expensive than the CRC checks performed by
replication pools. There is a risk that by XORing the contents of a chunk
together that a set of corruptions cancel each other out, but this level of
check is better than no check and will be very successful at detecting a
dropped write which will be the most common type of corruption.

Metadata changes
----------------

What metadata do we need to consider?

1. object_info_t. Every Ceph object has some metadata stored in the
   object_info_t data structure. Some of these fields (e.g. object length) are
   not updated frequently and we can simply avoid performing partial writes
   optimizations when these fields need updating. The more problematic fields
   are the version numbers and the last modification time which are updated on
   every write. Version numbers of objects are compared to version numbers in
   PG log entries for peering/recovery and with version numbers on other
   shards for backfill. Version numbers and modification times can be read by
   clients.

2. PG log entries. The PG log is used to track inflight transactions and to
   allow incomplete transactions to be rolled forward/backwards after an
   outage/network glitch. The PG log is also used to detect and resolve
   duplicate requests (e.g. resent due to network glitch) from
   clients. Peering currently assumes that every shard has a copy of the log
   and that this is updated for every transaction.

3. PG stats entries and other PG metadata. There is other PG metadata (PG
   stats is the simplest example) that gets updated on every
   transaction. Currently all OSDs retain a cached and a persistent copy of
   this metadata.

How many copies of metadata are required?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The current implementation keeps K+M replicated copies of metadata, one copy
on each shard. The minimum number of copies that need to be kept to support up
to M failures is M+1. In theory metadata could be erasure encoded, however
given that it is small it is probably not worth the effort. One advantage of
keeping K+M replicated copies of the metadata is that any fully in sync shard
can read the local copy of metadata, avoiding the need for inter-OSD messages
and asynchronous code paths. Specifically this means that any OSD not
performing backfill can become the primary and can access metadata such as
object_info_t locally.

M+1 arbitrarily distributed copies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A partial write to one data shard will always involve updates to the data
shard and all M coding parity shards, therefore for optimal performance it
would be ideal if the same M+1 shards are updated to track the associated
metadata update. This means that for small random writes that a different M+1
shards would get updated for each write. The drawback of this approach is that
you might need to read K shards to find the most up to date version of the
metadata.

In this design no shard will have an up to date copy of the metadata for every
object. This means that whatever shard is picked to be the acting primary that
it may not have all the metadata available locally and may need to send
messages to other OSDs to read it. This would add significant extra complexity
to the PG code and cause divergence between Erasure coded pools and Replicated
pools. For these reasons we discount this design option.

M+1 copies on known shards
^^^^^^^^^^^^^^^^^^^^^^^^^^

The next best performance can be achieved by always applying metadata updates
to the same M+1 shards, for example choosing the 1st data shard and all M
coding parity shards. Coding parity shards will get updated by every partial
write so this will result in zero or one extra shard being updated. With this
approach only 1 shard needs to be read to find the most up to date version of
the metadata.

We can restrict the acting primary to be one of the M+1 shards, which means
that once any incomplete updates in the log have been resolved that the
primary will have an up to date local copy of all the metadata, this means
that much more of the PG code can be kept unchanged.

Partial Writes and the PG log
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Peering currently assumes that every shard has a copy of the log, however
because of inflight updates and small term absences it is possible that some
shards are missing some of the log entries. The job of peering is to combine
the logs from the set of present shards to form a definitive log of
transactions that have been committed by all the shards. Any discrepancies
between a shards log and the definitive log are then resolved, typically by
rolling backwards transactions (using information held in the log entry) so
that all the shards are in a consistent state.

To support partial writes the log entry needs to be modified to include the
set of shards that are being updated. Peering needs to be modified to consider
a log entry as missing from a shard only if a copy of the log entry on another
shard indicates that this shard was meant to be updated.

The logs are not infinite in size, and old log entries where it is known that
the update has been successfully committed on all affected shards are
trimmed. Log entries are first condensed to a pg_log_dup_t entry which can no
longer assist in rollback of a transaction but can still be used to detect
duplicated client requests, and then later completely discarded. Log trimming
is performed at the same time as adding a new log entry, typically when a
future write updates the log. With partial writes log trimming will only occur
on shards that receive updates, which means that some shards may have stale
log entries that should have been discarded.

TBD: I think the code can already cope with discrepancies in log trimming
between the shards. Clearly an in flight trim operation may not have completed
on every shard so small discrepancies can be dealt with, but I think an absent
OSD can cause larger discrepancies. I believe that this is resolved during
Peering, with each OSD keeping a record of what the oldest log entry should be
and this gets shared between OSDs so that they can work out stale log entries
that were trimmed in absentia. Hopefully this means that only sending log
trimming updates to shards that are creating new log entries will work without
code changes.

Backfill
^^^^^^^^

Backfill is used to correct inconsistencies between OSDs that occur when an
OSD is absent for a longer period of time and the PG log entries have been
trimmed. Backfill works by comparing object versions between shards. If some
shards have out of date versions of an object then a reconstruct is performed
by the backfill process to update the shard. If the version numbers on objects
are not updated on all shards then this will break the backfill process and
cause a huge amount of unnecessary reconstruct work. This is unacceptable, in
particular for the scenario where an OSD is just absent for maintenance for a
relatively short time with noout set. The requirement is to be able to
minimize the amount of reconstruct work needed to complete a backfill.

In dev/osd_internals/erasure_coding/proposals.rst it discusses the idea of
each shard storing a vector of version numbers that records the most recent
update that the pair <this shard, other shard> both should have participated
in. By collecting this information from at least M shards it is possible to
work out what the expected minimum version number should be for an object on a
shard and hence deduce whether a backfill is required to update the
object. The drawback of this approach is that backfill will need to scan M
shards to collect this information, compared with the current implementation
that only scans the primary and shard(s) being backfilled.

With the additional constraint that a known M+1 shards will always be updated
and that the (acting) primary will be one of these shards, it will be possible
to determine whether a backfill is required just by examining the vector on
the primary and the object version on the shard being backfilled. If the
backfill target is one of the M+1 shards the existing version number
comparison is sufficient, if it is another shard then the version in the
vector on the primary needs to be compared with the version on the backfill
target. This means that backfill does not have to scan any more shards than it
currently does, however the scan of the primary does need to read the vector
and if there are multiple backfill targets then it may need to store multiple
entries of the vector per object increasing memory usage during the backfill.

There is only a requirement to keep the vector on the M+1 shards, and the
vector only needs K-1 entires because we only need to track version number
differences between any of the M+1 shards (which should have the same version)
and each of the K-1 shards (which can have a stale version number). This will
slightly reduce the amount of extra metadata required. The vector of version
numbers could be stored in the object_info_t structure or stored as a separate
attribute.

Our preference is to store the vector in the object_info_t structure because
typically both are accessed together, and because this makes it easier to
cache both in the same object cache. We will keep metadata and memory
overheads low by only storing the vector when it is needed.

Care is required to ensure that existing clusters can be upgraded. The absence
of the vector of version numbers implies that an object has never had a
partial update and therefore all shards are expected to have the same version
number for the object and the existing backfill algorithm can be used.

Code references
"""""""""""""""

PrimaryLogPG::scan_range - this function creates a map of objects and their
version numbers, on the primary it tries to get this information from the
object cache, otherwise it reads to OI attribute. This will need changes to
deal with the vectors. To conserve memory it will need to be provided with the
set of backfill targets so it can select which part of the vector to keep.

PrimaryLogPG::recover_backfill - this function call scan_range for the local
(primary) and sends MOSDPGScan to the backfill targets to get them to perform
the same scan. Once it has collected all the version numbers it compares the
primary and backfill targets to work out which objects need to be
recovered. This will also need changes to deal with the vectors when comparing
version numbers.

PGBackend::run_recovery_op - recovers a single object. For an EC pool this
involves reconstructing the data for the shards that need backfilling (read
other shards and use decode to recover). This code shouldn't need any changes.

Version number and last modification time for clients
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Clients can read the object version number and set expectations about what the
minimum version number is when making updates. Clients can also read the last
modification time. There are use cases where it is important that these values
can be read and give consistent results, but there is also a large number of
scenarios where this information is not required.

If the object version number is only being updated on a known M+1 shards for
partial writes, then where this information is required it will need to
involve a metadata access to one of those shards. We have arranged for the
primary to be one of the M+1 shards so I/Os submitted to the primary will
always have access to the up to date information.

Direct write I/Os need to update the M+1 shards, so it is not difficult to
also return this information to the client when completing the I/O.

Direct read I/Os are the problem case, these will only access the local shard
and will not necessarily have access to the latest version and modification
time. For simplicity we will require clients that require this information to
send requests to the primary rather than using the direct I/O
optimization. Where a client does not need this information they can use the
direct I/O optimizations.

The direct read I/O optimization will still return a (potentially stale)
object version number. This may still be of use to clients to help understand
the ordering of I/Os to a chunk.

Direct Write with Metadata updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here's the full picture of what a direct write performing a parity-delta-write
looks like with all the control messages:

.. ditaa::

                   RADOS Client
 
                        |  ^
                        |  |
                        1  28
 +-----+                |  |
 |     |<------27-------+--+
 |     |                |  |
 |     |  +-------------|->|
 |     |  |             |  |
 |     |<-|----2--------+  |<--------------------------------------------+
 | Seq |  |             |  |<----------------------------+               |
 |     |  |    +----3---+  |                             |               |
 |     |  |    |        +--|-----------------------5-----|---+           |
 |     |  |    |        +--|-------4---------+           |   |           |
 |     +--|-10-|------->|  |                 |           |   |           |
 |     |  |    |    +---+  |                 |           |   |           |
 |     |  |    |    |   |  |                 |           |   |           |
 |     |  |    |    v   |  |                 |           |   |           |
 +----++  |    |  +---+ |  |                 |           |   |           |
   ^  |   |    |  |XOR+-|--|----------15-----|-----------|---|-----+     |
   |  |   |    |  |13 +-|--|-------14--------|-----+     |   |     |     |
   |  |   |    |  +---+ |  |                 |     |     |   |     |     |
   |  |   |    |    ^   |  |                 |     v     |   |     v     |
   |  |   |    |    |   |  |                 |  +------+ |   |  +------+ |
   6  11  |    |    |   |  |                 |  | XOR  | |   |  |  GF  | |
   |  |   |    |    |   |  |                 |  | 18   | |   |  |  21  | |
   |  |   |    |    12  16 |                 |  +----+-+ |   |  +----+-+ |
   |  |   |    |    |   |  |                 |    ^  |   |   |    ^  |   |
   |  |   |    |    |   |  |                 |    |  |   |   |    |  |   |
   |  |   |    |    |   |  |                 |    17 19  |   |    20 22  |
   |  |   |    |    |   |  |                 |    |  |   |   |    |  |   |
   |  |   |    |    |   v  |                 |    |  v   |   |    |  v   |
   |  |   |    |  +-+----+ |                 |  +-+----+ |   |  +-+----+ |
   |  |   |    +->|Extent| |                 +->|Extent| |   +->|Extent| |
   |  |   23      |Cache | 24                   |Cache | 25     |Cache | 26
   |  |   |       +----+-+ |                    +----+-+ |      +----+-+ |
   |  |   |         ^  |   |                      ^  |   |        ^  |   |
   |  |   |         |  |   |                      |  |   |        |  |   |
   |  +---+         7  +---+                      8  +---+        9  +---+
   |  |             |  |                          |  |            |  |
   |  v             |  v                          |  v            |  v
  .-----.          .-----.   .-----.   .-----.   .-----.         .-----.
 (       )        (       ) (       ) (       ) (       )       (       )
 |`-----'|        |`-----'| |`-----'| |`-----'| |`-----'|       |`-----'|
 |       |        |       | |       | |       | |       |       |       |
 |       |        |       | |       | |       | |       |       |       |
 (       )        (       ) (       ) (       ) (       )       (       )
  `-----'          `-----'   `-----'   `-----'   `-----'         `-----'
 Primary            OSD 2     OSD 3     OSD 4     OSD P           OSD Q
   OSD
 
 * Xattr            * No Xattr                    * Xattr         * Xattr
 * OI               * Stale OI                    * OI            * OI
 * PG log           * Partial PG log              * PG log        * PG log
 * PG stats         * No PG stats                 * PG stats      * PG stats

Note: Only the primary OSD and parity coding OSDs (the M+1 shards) have Xattr,
up to date object info, PG log and PG stats. Only one of these OSDs is
permitted to become the (acting) primary. The other data OSDs 2,3 and 4 (the
K-1 shards) do not have Xattrs or PG stats, may have state object info and
only have PG log entries for their own updates. OSDs 2,3 and 4 may have stale
OI with an old version number. The other OSDs have the latest OI and a vector
with the expected version numbers for OSDs 2,3 and 4.

1. Data message with Write I/O from client (MOSDOp)
2. Control message to Primary with Xattr (new msg MOSDEcSubOpSequence)

Note: the primary needs to be told about any xattr update so it can update its
copy, but the main purpose of this message is to allow the primary to sequence
the write I/O. The reply message at step 10 is what allows the write to start
and provides the PG stats and new object info including the new version
number. If necessary the primary can delay this to ensure that
recovery/backfill of the object is completed first and deal with overlapping
writes. Data may be read (prefetched) before the reply, but obviously no
transactions can start.

3. Prefetch request to local extent cache
4. Control message to P to prefetch to extent cache (new msg
   MOSDEcSubOpPrefetch equivalent of MOSDEcSubOpRead)
5. Control message to Q to prefetch to extent cache (new msg
   MOSDEcSubOpPrefetch equivalent of MOSDEcSubOpRead)
6. Primary reads object info
7. Prefetch old data
8. Prefetch old P
9. Prefetch old Q

Note: The objective of these prefetches is to get the old data, P and Q reads
started as quickly as possible to reduce the latency of the whole I/O. There
may be error scenarios where the extent cache is not able to retain this and
it will need to be re-read. This includes the rare/pathological scenarios
where there is a mixture of writes sent to the primary and writes sent
directly to the data OSD for the same object.

10. Control message to data OSD with new object info + PG stats (new msg
    MOSDEcSubOpSequenceReply)
11. Transaction to update object info + PG log + PG stats
12. Fetch old data (hopefully cached)

Note: For best performance we want to pipeline writes to the same stripe. The
primary assigns the version number to each write and consequently defines the
order in which writes should be processed. It is important that the data shard
and the coding parity shards apply overlapping writes in the same order. The
primary knows what set of writes are in flight so can detect this situation
and indicate in its reply message at step 10 that an update must wait until an
earlier update has been applied. This information needs to be forwarded to the
coding parities (steps 14 and 15) so they can also ensure updates are applied
in the same order.

13. XOR new and old data to create delta
14. Data message to P with delta + Xattr + object info + PG log + PG stats
    (new msg MOSDEcSubOpDelta equivalent of MOSDEcSubOpWrite)
15. Data message to Q with delta + Xattr + object info + PG log + PG stats
    (new msg MOSDEcSubOpDelta equivalent of MOSDEcSubOpWrite)
16. Transaction to update data + object info + PG log
17. Fetch old P (hopefully cached)
18. XOR delta and old P to create new P
19. Transaction to update P + Xattr + object info + PG log + PG stats
20. Fetch old Q (hopefully cached)
21. XOR delta and old Q to create new Q
22. Transaction to update Q + Xattr + object info + PG log + PG stats
23. Control message to data OSD for commit (new msg MOSDEcSubOpDeltaReply
    equivalent of MOSDEcSubOpWriteReply)
24. Local commit notification
25. Control message to data OSD for commit (new msg MOSDEcSubOpDeltaReply
    equivalent of MOSDEcSubOpWriteReply)
26. Control message to data OSD for commit (new msg MOSDEcSubOpDeltaReply
    equivalent of MOSDEcSubOpWriteReply)
27. Control message to Primary to signal end of write (variant of new msg
    MOSDEcSubOpSequence)
28. Control message reply to client (MOSDOpReply)

Upgrade and backwards compatibility
-----------------------------------

A few of the optimizations can be made just by changing code on the primary
OSD with no backwards compatibility concerns regarding clients or the other
OSDs. These optimizations will be enabled as soon as the primary OSD upgrades
and will replace the existing code paths.

The remainder of the changes will be new I/O code paths that will exist
alongside the existing code paths.

Similar to EC Overwrites many of the changes will need to ensure that all OSDs
are running new code and that the EC plugins support new interfaces required
for parity-delta-writes. A new pool level flag will be required to enforce
this. It will be possible to enable this flag (and hence enable the new
performance optimizations) after upgrading an existing cluster. Once set it
will not be possible to add down level OSDs to the pool. It will not be
possible to turn this flag off other than by deleting the pool. Downgrade is
not supported because:

1. It is not trivial to quiesce all I/O to a pool to ensure that none of the
   new I/O code paths are in use when the flag is cleared.

2. The PG log format for new I/Os will not be understood by down level
   OSDs. It would be necessary to ensure the log has been trimmed of all new
   format entries before clearing the flag to ensure that down level OSDs will
   be able to interpret the log.

3. Additional xattr data will be stored by the new I/O code paths and used by
   backfill. Down level code will not understand how to backfill a pool that
   has been running the new I/O paths and will get confused by the
   inconsistent object version numbers. While it is theoretically possible to
   disable partial updates and then scan and update all the metadata to return
   the pool to a state where a downgrade is possible, we have no intention of
   writing this code.

The direct I/O changes will additionally require clients to be running new
code. These will require that the pool has the new flag set and that a new
client is used. Old clients can use pools with the new flag set, just without
the direct I/O optimization.

Not under consideration
-----------------------

There is a list of enhancements discussed in
doc/dev/osd_internals/erasure_coding/proposals.rst, the following are not
under consideration:

1. RADOS Client Acknowledgement Generation optimization

When updating K+M shards in an erasure coded pool, in theory you don’t have to
wait for all the updates to complete before completing the update to the
client, because so long as K updates have completed any viable subset of
shards should be able to roll forward the update.

For partial writes where only M+1 shards are updated this optimization does
not apply as all M+1 updates need to complete before the update is completed
to the client.

This optimization would require changes to the peering code to work out
whether partially completed updates need to be rolled forwards or
backwards. To roll an update forwards it would be simplest to mark the object
as missing and use the recovery path to reconstruct and push the update to
OSDs that are behind.

2. Avoid sending read request to local OSD via Messenger

The EC backend code has an optimization for writes to the local OSD which
avoids sending a message and reply via messenger. The equivalent optimization
could be made for reads as well, although a bit more care is required because
the read is synchronous and will block the thread waiting for the I/O to
complete.

Pull request https://github.com/ceph/ceph/pull/57237 is making this
optimization

Stories
=======

This is our high level breakdown of the work. Our intention is to deliver this
work as a series of PRs. The stories are roughly in the order we plan to
develop. Each story is at least one PR, where possible they will be broken up
further. The earlier stories can be implemented as stand alone pieces of work
and will not introduce upgrade/backwards compatibility issues. The later
stories will start breaking backwards compatibility, here we plan to add a new
flag to the pool to enable these new features. Initially this will be an
experimental flag while the later stories are developed.

Test tools - enhanced I/O generator for testing erasure coding
--------------------------------------------------------------

* Extend rados bench to be able to generate more interesting patterns of I/O
  for erasure coding, in particular reading and writing at different offsets
  and for different lengths and making sure we get good coverage of boundary
  conditions such as the sub-chunk size, chunk size and stripe size
* Improve data integrity checking by using a seed to generate data patterns
  and remembering which seed is used for each block that is written so that
  data can later be validated

Test tools - offline consistency checking tool
----------------------------------------------

* Test tools for performing offline consistency checks combining use of
  objectstore_tool with ceph-erasure-code-tool
* Enhance some of the teuthology standalone erasure code checks to use this
  tool

Test tools - online consistency checking tool
---------------------------------------------

* New CLI to be able to perform online consistency checking for an object or a
  range of objects that reads all the data and coding parity shards and
  re-encodes the data to validate the coding parities

Switch for JErasure to ISA-L
----------------------------

The JErasure library has not been updated since 2014, the ISA-L library is
maintained and exploits newer instructions sets (e.g. AVX512, AVX2) which
provides faster encoding/decoding

* Change defaults to ISA-L in upstream ceph
* Benchmark Jerasure and ISA-L
* Refactor Ceph isa_encode region_xor() to use AVX when M=1
* Documentation updates
* Present results at performance weekly

Sub Stripe Reads
----------------

Ceph currently reads an integer number of stripes and discards unneeded
data. In particular for small random reads it will be more efficient to just
read the required data

* Help finish Pull Request https://github.com/ceph/ceph/pull/55196 if not
  already complete
* Further changes to issue sub-chunk reads rather than full-chunk reads

Simple Optimizations to Overwrite
---------------------------------

Ceph overwrites currently read an integer number of stripes, merge the new
data and write an integer number of stripes. This story makes simple
improvements by making the same optimizations as for sub stripe reads and for
small (sub-chunk) updates reducing the amount of data being read/written to
each shard.

* Only read chunks that are not being fully overwritten (code currently reads
  whole stripe and then merges new data)
* Perform sub-chunk reads for sub-chunk updates
* Perform sub-chunk writes for sub-chunk updates

Eliminate unnecessary chunk writes but keep metadata transactions
-----------------------------------------------------------------

This story avoids re-writing data that has not been modified. A transaction is
still applied to every OSD to update object metadata, the PG log and PG stats.

* Continue to create transactions for all chunks but without the new write data
* Add sub-chunk writes to transactions where data is being modified

Avoid zero padding objects to a full stripe
-------------------------------------------

Objects are rounded up to an integer number of stripes by adding zero
padding. These buffers of zeros are then sent in messages to other OSDs and
written to the OS consuming storage. This story make optimizations to remove
the need for this padding

* Modifications to reconstruct reads to avoid reading zero-padding at the end
  of an object - just fill the read buffer with zeros instead
* Avoid transfers/writes of buffers of zero padding. Still send transactions
  to all shards and create the object, just don't populate it with zeros
* Modifications to encode/decode functions to avoid having to pass in buffers
  of zeros when objects are padded

Erasure coding plugin changes to support distributed partial writes
-------------------------------------------------------------------

This is preparatory work for future stories, it adds new APIs to the erasure
code plugins.

* Add a new interface to create a delta by XORing old and new data together
  and implement this for the ISA-L and JErasure plugins
* Add a new interface to apply a delta to one coding parity by using XOR/GF
  and implement this for the ISA-L and JErasure plugins
* Add a new interface which reports which erasure codes support this feature
  (ISA-L and JErasure will support it, others will not)

Erasure coding interface to allow RADOS clients to direct I/Os to OSD storing the data
--------------------------------------------------------------------------------------

This is preparatory work for future stories, its adds a new API for clients

* New interface to convert the pair (pg, offset) to {OSD, remaining chunk
  length}

We do not want clients to have to dynamically link to the erasure code plugins
so this code will need to be part of librados. However this interface needs to
understand how erasure codes distribute data and coding chunks to be able to
perform this translation.

We will only support ISA-L and JErasure plugins where there is a trivial
striping of data chunks to OSDs.

Changes to object_info_t
------------------------

This is preparatory work for future stories.

This adds the vector of version numbers to object_info_t which will be used
for partial updates. For replicated pools and for erasure coded objects that
are not overwritten we will avoid storing extra data in object_info_t.

Changes to PGLog and Peering to support updating a subset of OSDs
-----------------------------------------------------------------

This is preparatory work for future stories.

* Modify the PG log entry to store a record of which OSDs are being updated
* Modify peering to use this extra data to work out OSDs that are missing
  updates

Change to selection of (acting) primary
---------------------------------------

This is preparatory work for future stories.

Constrain the choice of primary to be the first data OSD or one of the erasure
coding parities. If none of these OSDs are available and up to date then the
pool must be offline.

Implement parity-delta-write with all computation on the primary
----------------------------------------------------------------

* Calculate whether its more efficient for an update to perform a full stripe
  overwrite or a parity-delta-write
* Implement new code paths to perform the parity-delta-write
* Test tool enhancements. We want to make sure that both parity-delta-write
  and full-stripe write are tested. We will add a new conf file option with a
  choice of 'parity-delta', 'full-stripe', 'mixture for testing' or
  'automatic' and update teuthology test cases to predominately use a mixture.

Upgrades and backwards compatibility
------------------------------------

* Add a new feature flag for erasure coded pools
* All OSDs must be running new code to enable the flag on the pool
* Clients may only issue direct I/Os if the flag is set
* OSDs running old code may not join a pool with the flag set
* Its not possible to turn the feature flag off (other than by deleting the
  pool)

Changes to Backfill to use the vector in object_info_t
------------------------------------------------------

This is preparatory work for future stories.

* Modify the backfill process to use the vector of version numbers in
  object_info_t so that when partial updates occur we do not backfill OSDs
  which did not participate in the partial update.
* When there is a single backfill target extract the appropriate version
  number from the vector (no additional storage required)
* When there are multiple backfill targets extract the subset of the vector
  required by the backfill targets and select the appropriate entry when
  comparing version numbers in PrimaryLogPG::recover_backfill

Test tools - offline metadata validation tool
---------------------------------------------

* Test tools for performing offline consistency checking of metadata, in
  particular checking the vector of version numbers in object_info_t matches
  the versions on each OSD, but also for validating PG log entries

Eliminate transactions on OSDs not updating data chunks
-------------------------------------------------------

Peering, log recovery and backfill can now all cope with partial updates using
the vector of version numbers in object_info_t.

* Modify the overwrite I/O path to not bother with metadata only transactions
  (except to the Primary OSD)
* Modify the update of the version numbers in object_info_t to use the vector
  and only update entries that are receiving a transaction
* Modify the generation of the PG log entry to record which OSDs are being
  updated

Direct reads to OSDs (single chunk only)
----------------------------------------

* Modify OSDClient to route single chunk read I/Os to the OSD storing the data
* Modify OSD to accept reads from non-primary OSD (expand existing changes for
  replicated pools to work with EC pools as well)
* If necessary fail the read with EAGAIN if the OSD is unable to process the
  read directly
* Modify OSDClient to retry read by submitting to Primary OSD if read is
  failed with EAGAIN
* Test tool enhancements. We want to make sure that both direct reads and
  reads to the primary are tested. We will add a new conf file option with a
  choice of 'prefer direct', 'primary only' or 'mixture for testing' and
  update teuthology test cases to predominately use a mixture.

The changes will be made to the OSDC part of the RADOS client so will be
applicable to rbd, rgw and cephfs.

We will not make changes to other code that has its own version of RADOS
client code such as krbd, although this could be done in the future.

Direct reads to OSDs (multiple chunks)
--------------------------------------

* Add a new OSDC flag NONATOMIC which allows OSDC to split a read into
  multiple requests
* Modify OSDC to split reads spanning multiple chunks into separate requests
  to each OSD if the NONATOMIC flag is set
* Modifications to OSDC to coalesce results (if any sub read fails the whole
  read needs to fail)
* Changes to librbd client to set NONATOMIC flag for reads
* Changes to cephfs client to set NONATOMIC flag for reads

We are only changing a very limited set of clients, focusing on those that
issue smaller reads and are latency sensitive. Future work could look at
extending the set of clients (including krbd).

Implement distributed parity-delta-write
----------------------------------------

* Implement new message MOSDEcSubOpDelta and MOSDEcSubOpDeltaReply
* Change primary to calculate delta and send MOSDEcSubOpDelta message to
  coding parity OSDs
* Modify coding parity OSDs to apply the delta and send MOSDEcSubOpDeltaReply
  message

Note: This change will increase latency because the coding parity reads start
after the old data read. Future work will fix this.

Test tools - EC error injection thrasher
----------------------------------------

* Implement a new type of thrasher that specifically injects faults to stress
  erasure coded pools
* Take one or multiple (up to M) OSDs down, more focus on taking different
  subsets of OSDs down to drive all the different EC recovery paths than
  stressing out peering/recovery/backfill (the existing OSD thrasher excels at
  this)
* Inject read I/O failures to force reconstructs using decode for single and
  multiple failures
* Inject delays using osd tell type interface to make it easier to test OSD
  down at all the interesting stages of EC I/Os
* Inject delays using osd tell type interface to slow down an OSD transaction
  or message to expose the less common completion orders for parallel work

Implement prefetch message MOSDEcSubOpPrefetch and modify extent cache
----------------------------------------------------------------------

* Implement new message MOSDEcSubOpPrefetch
* Change primary to issue this message to the coding parity OSDs before
  starting read of old data
* Change the extent cache so that each OSD caches its own data rather than
  caching everything on the primary
* Change coding parity OSDs to handle this message and read the old coding
  parity into the extent cache
* Changes to extent cache to retain the prefetched old parity until the
  MOSDEcSubOpDelta message is received, and to discard this on error paths
  (e.g. new OSDMap)

Implement sequencing message MOSDEcSubOpSequence
------------------------------------------------

* Implement new message MODSEcSubOpSequence and MOSDEcSubOpSequenceReply
* Modify primary code to create these messages and route them locally to
  itself in preparation for direct writes

Direct writes to OSD (single chunk only)
----------------------------------------

* Modify OSDC to route single chunk write I/Os to the OSD storing the data
* Changes to issue MOSDEcSubOpSequence and MOSDEcSubOpSequenceReply between
  data OSD and primary OSD

Direct writes to OSD (multiple chunks)
--------------------------------------

* Modifications to OSDC to split multiple chunk writes into separate requests
  if NONATOMIC flag is set
* Further changes to coalescing completions (in particular reporting version
  number correctly)
* Changes to librbd client to set NONATOMIC flag for reads
* Changes to cephfs client to set NONATOMIC flag for reads

We are only changing a very limited set of clients, focusing on those that
issue smaller writes and are latency sensitive. Future work could look at
extending the set of clients.

Deep scrub / CRC
----------------

* Disable CRC generation in the EC code for overwrites, delete hinfo Xattr
  when first overwrite occurs
* For objects in pool with new feature flag set that have not been overwritten
  check CRC, even if pool overwrite flag is set. The presence/absence of hinfo
  can be used to determine if the object has been overwritten
* For deep scrub requests XOR the contents of the shard to create a
  longitudinal check (8 bytes wide?)
* Return the longitudinal check in the scrub reply message, have the primary
  encode the set of longitudinal replies to check for inconsistencies

Variable chunk size erasure coding
----------------------------------

* Implement new pool option for automatic/variable chunk size
* When object size is small use a small chunk size (4K) when the pool is using
  the new option
* When object size is large use a large chunk size (64K or 256K?)
* Convert the chunk size by reading and re-writing the whole object when a
  small object grows (append)
* Convert the chunk size by reading and re-writing the whole object when a
  large object shrinks (truncate)
* Use the object size hint to avoid creating small objects and then almost
  immediately converting them to a larger chunk size

CLAY Erasure Codes
------------------

In theory CLAY erasure codes should be good for K+M erasure codes with larger
values of M, in particular when these erasure codes are used with multiple
OSDs in the same failure domain (e.g. an 8+6 erasure code with 5 servers each
with 4 OSDs). We would like to improve the test coverage for CLAY and perform
some more benchmarking to collect data to help substantiate when people should
consider using CLAY.

* Benchmark CLAY erasure codes - in particular the number of I/O required for
  backfills when multiple OSDs fail
* Enhance test cases to validate the implementation
* See also https://bugzilla.redhat.com/show_bug.cgi?id=2004256
