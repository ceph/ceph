Bounded bucket-index maintenance
================================

``bucket_check_index_page`` scans one bucket-index shard in bounded calls.
``bucket_rebuild_index_commit`` publishes its accumulated statistics only if the
same object generation still exists. Both are internal RGW CLS methods. The
public builders and ``rgw_cls_index_check`` are declared in
``cls/rgw/cls_rgw_client.h`` and require C++20. Their wire implementation uses Datum;
the scan, accounting, concurrency, and storage rules remain native RGW policy.

Progress and limits
-------------------

Start with a default-constructed ``rgw_cls_index_check``. Each request contains its
opaque cursor and positive row and byte limits. The server clamps these to 1,000
stored rows and 4 MiB of encoded key/value records per call. A record costs its key
and value lengths plus two 32-bit length prefixes. The limit is checked before
decoding a stored row. The OSD may fetch additional encoded rows under its own OMAP
limits; this protocol does not bound all backing-store allocation. The control
request and encoded bucket header each have a separate 4 MiB limit.

The scan visits plain keys below the reserved namespaces, plain keys above them,
then instance keys. One call visits one region. Empty regions can produce a valid
page with no statistics and an advanced phase. OLH records and other namespaces do
not contribute. Native entry eligibility, category accounting, rounding, and unsigned
counter arithmetic match the full-call check operation.

The first reply captures the object's native identity and user version. Every
subsequent page and final write verifies both. Treat the cursor as opaque and keep
it with its accumulator; a marker alone cannot resume this operation. A changed,
removed, or recreated shard invalidates the scan. This is a per-object generation
fence, with no bucket-wide snapshot or atomic multi-shard publication.

Apply each successful reply with ``check.consume(page)``. It validates decoding,
generation, phase, and forward progress, constructs candidate totals, then advances
both totals and cursor together. Decode errors and allocation exceptions preserve
the prior state. Retain the same state and request after a failed read; never apply
a successful reply twice. Abandoning the scan before publication writes no statistics.

.. list-table:: Selected statuses
   :header-rows: 1

   * - Status
     - Meaning and action
   * - ``-ECANCELED``
     - Generation changed; discard the accumulator and begin a fresh scan.
   * - ``-E2BIG``
     - A control request, header, or first row exceeds its limit. A smaller row
       budget cannot repair an oversized stored record.
   * - ``-EINVAL``
     - Invalid limits, cursor, phase, or operation composition.
   * - ``-EIO``
     - Malformed stored data or a reply that fails progress validation.
   * - ``-EOPNOTSUPP``
     - The installed CLS module predates this protocol.

Publication and compatibility
-----------------------------

Issue each new method as the sole operation in its RADOS request. The server rejects
preceding suboperations counted by the native backend; this check cannot detect
every preceding operation or any following operation. Callers must enforce the
sole-operation contract in both directions.
Use the existing full-call methods for compound operations.

After ``check.complete()``, the rebuild builder submits the cursor and accumulated
category totals. The server verifies generation and the native resharding guard
before writing the header. As with the historical rebuild, the resulting header
preserves ``tag_timeout``, ``ver``, and ``syncstopped``; other metadata starts at its
default value. Failed writes publish no partial header. A definite failed operation
can retry its unchanged request. If a transport failure leaves the final write's
outcome uncertain, a newly submitted request may see a changed generation after a
successful write; start a fresh check to resolve that uncertainty.

RGW's bucket-index service reads all shards before starting guarded publications.
An error or cancellation can still leave earlier shard publications committed.
On an older module, the service falls back to the existing full-call check or
rebuild. That fallback retains its historical unbounded scan and concurrency rules.
An older client can continue to use the original methods on a new module. The
standalone example below reports unsupported paging and leaves fallback selection
to its caller.

Runnable client
---------------

Build the optional example in an existing Ceph build configured with RGW:

.. code-block:: console

   cmake --build build --parallel 8 --target ceph_cls_rgw_maintenance_example
   build/bin/ceph_cls_rgw_maintenance_example ceph.conf .rgw.buckets.index SHARD_OBJECT

The default command checks one existing shard. Add ``--rebuild`` to publish its
calculated statistics. The example uses ``client.admin`` and its normal pool
permissions. This loop keeps its cursor and totals together:

.. literalinclude:: ../../../src/cls/rgw/examples/index_maintenance.cc
   :language: cpp
   :start-after: // begin-page-loop
   :end-before: // end-page-loop

:download:`Download the complete client <../../../src/cls/rgw/examples/index_maintenance.cc>`.
