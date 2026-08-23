.. _radosgw_s3_rdma:

=============
S3 over RDMA
=============

.. versionadded:: Umbrella

Ceph Object Gateway can serve S3 object data over RDMA using the NVIDIA
cuObject library. The S3 control plane (authentication, headers,
metadata) stays on HTTP, while object data moves out of band: it is
DMA-written straight into memory that the S3 client registered with its
RDMA NIC, including GPU memory on clients using GPUDirect Storage.

A client using the cuObject client library (``libcuobjclient``, for
example via NooBaa's ``s3perf.js --rdma`` or an AWS SDK middleware)
registers a memory window and sends its RDMA descriptor with each
request in the ``x-amz-rdma-token`` header. The descriptor is opaque:
it names the client's memory window, remote key and Dynamically
Connected (DC) target, so any server holding it (and the matching
``dc_key``) can push data into that window without a pre-established
connection.

Two data paths are available for GET:

Staged (gateway) mode
  The gateway runs its own ``cuObjServer``. Object data is read from
  RADOS as usual, accumulated in a pre-registered gateway buffer, and
  RDMA-written to the client in one transfer at the end of the request.
  This is the base mode; it requires the gateway host to have an
  RDMA-capable NIC and the ``cuobjserver`` library, and it is also used
  for PUT (the upload must flow through the gateway's checksum,
  compression and encryption filters). Build with
  ``-DWITH_RADOSGW_CUOBJ=ON`` and enable with ``rgw_cuobj_enabled``.

OSD passthrough mode
  With ``rgw_cuobj_osd_passthrough`` enabled, the gateway forwards the
  client's descriptor to the OSDs instead: each stripe read carries an
  *advisory delivery descriptor* (an optional field on the RADOS
  request message holding the opaque token, the stripe's offset within
  the requested range, and a lease). An OSD that can push builds an
  op-aware placement plan and RDMA-writes the reply data directly into
  the client's memory window, returning only byte counts; an OSD that
  cannot — not built with cuObject, disabled, lease expired, or a
  retransmitted request — simply returns the data inline as a normal
  read, which the gateway treats as the signal to restart the GET in a
  fallback mode. Degradation is therefore always plain, correct,
  in-band data; there is no protocol error to handle.

  Object data never touches the gateway, removing the OSD-to-gateway
  network hop and the gateway staging buffer, and letting transfer
  bandwidth scale with the number of OSDs. This matches the cuObject
  architecture's "gateway instructs data nodes" reference flow. In this
  mode the gateway itself needs neither the cuObject library nor an
  RDMA NIC; the OSDs do (build them with ``-DWITH_OSD_CUOBJ=ON`` and
  set ``osd_cuobj_enabled``).

  Because the descriptor rides on the request message rather than in a
  special operation, any read shape can use it: plain reads take a
  linear placement, sparse reads scatter per extent (the extent map
  stays inline), erasure-coded *primary* reads work unchanged (the
  reply is reconstructed logical data), and erasure-coded *direct*
  reads — the client split-read path — have each shard OSD scatter its
  ~16K chunks to their logical positions in the client window, so the
  shards' concurrent writes interleave into the client's buffer and
  client-side reassembly disappears.

Fallback behavior
=================

Modes degrade transparently, per request:

#. **Passthrough** is attempted when the token is present,
   ``rgw_cuobj_osd_passthrough`` is enabled and the request is
   eligible (see below). If any stripe comes back inline — an OSD
   that predates the feature, was built without it, has it disabled,
   refused an expired lease or a retransmitted op — the gateway
   restarts the whole GET in the next mode down. The restart is
   invisible to the client: no HTTP bytes have been sent, and
   rewriting any client memory ranges that were already delivered is
   harmless. When stripe operations already reached the OSDs, the
   gateway first waits ``rgw_cuobj_fence_wait_ms`` so that a write
   still queued in a failed OSD's NIC cannot land after the fallback
   rewrites the same ranges (see Correctness below).
#. **Staged** mode is used when the gateway has a working
   ``cuObjServer``.
#. Otherwise the response carries the data in the **HTTP body** with
   ``x-amz-rdma-reply: 501``, which the cuObject protocol defines as
   the "fall back to HTTP" signal.

Passthrough eligibility
=======================

A GET uses passthrough only when the gateway would not need to touch
the data:

* the object is not compressed and not encrypted (no server-side
  transform may run);
* no Lua data script or Arrow Flight filter is attached;
* the object is not a Swift DLO/SLO user manifest;
* D3N datacache is not enabled; and
* the requested range fits within the client's registered window.

Replicated and erasure-coded pools are both supported (EC data is
served by the primary as reconstructed logical data; the shard-direct
interleave additionally serves librados split reads).

Multipart objects and range requests are fully supported; every stripe
lands at its logical offset within the requested range.

Configuration
=============

Gateway (staged mode and protocol handling):

* ``rgw_cuobj_enabled``, ``rgw_cuobj_rdma_ip``, ``rgw_cuobj_rdma_port``,
  ``rgw_cuobj_buffer_size``, ``rgw_cuobj_buffer_count``,
  ``rgw_cuobj_num_dcis`` — the staged-mode ``cuObjServer``.
* ``rgw_cuobj_osd_passthrough`` — enable OSD-direct delivery for GET.
* ``rgw_cuobj_crc64nvme`` — ask the OSDs to CRC64-NVME each stripe as
  it is RDMA-written; the gateway combines the per-stripe values in
  logical order and, for whole-object GETs, verifies the result
  against the object's stored full-object ``crc64nvme`` checksum
  before responding. This is end-to-end integrity across client
  memory, the fabric and the storage node — corruption anywhere on
  that path fails the GET instead of reaching the application. On by
  default; per-stripe checksums are computed with carry-less-multiply
  accelerated tables.
* ``rgw_cuobj_lease_ms`` — lease carried on each stripe operation; an
  OSD refuses to *start* an RDMA write later than this after receiving
  the op and returns the data inline instead.
* ``rgw_cuobj_fence_wait_ms`` — how long a passthrough attempt that
  already reached the OSDs waits before restarting in a fallback mode;
  size it to cover the RDMA transport's retry budget (roughly two
  seconds at the cuObject defaults).

OSD (passthrough execution):

* ``osd_cuobj_enabled`` — instantiate the OSD's cuObject endpoint.
* ``osd_cuobj_rdma_ip`` — RDMA interface address; defaults to the
  OSD's public address. **Must** be set explicitly when the RDMA NIC
  is not the public-network interface.
* ``osd_cuobj_rdma_port`` — local ``rdma_cm`` binding; ``0`` (the
  default) lets the library choose. Clients never connect to this
  port.
* ``osd_cuobj_buffer_size`` / ``osd_cuobj_buffer_count`` — the
  pre-registered staging pool. The buffer size must cover the largest
  stripe read (``rgw_get_obj_max_req_size``, default 4 MiB); requests
  that cannot be served from the pool fall back to slower one-shot
  registrations.
* ``osd_cuobj_num_dcis`` — DC initiators; must be at least the number
  of OSD op worker threads.
* ``osd_cuobj_dc_key`` — must match the cuObject client library's DC
  key cluster-wide (default ``0xffeeddcc``, the library default).

Deployment notes
================

* OSD nodes need a ConnectX-5 or newer (or RoCE-capable) NIC,
  ``rdma-core``, and the proprietary ``cuobjserver`` library from
  NVIDIA. No GPU or CUDA toolkit is needed on OSD or gateway hosts;
  only the *client* needs CUDA for GPU-memory targets.
* The in-flight window per GET is bounded by
  ``rgw_get_obj_window_size`` (default 16 MiB), which throttles how
  much RDMA traffic the OSDs aim at one client NIC at a time.
* The descriptor grants write access to the client's registered
  window until the client deregisters it. Three mechanisms keep stale
  writes out of a reused buffer (the lease/interlock scheme): the
  gateway drains outstanding stripe operations before any response —
  and because the OSD-side push completes before the op reply is
  sent, a drained reply *is* the interlock for every OSD still in
  contact; OSDs deliver retransmitted requests inline (RADOS re-sends
  reads after peering changes) so a stripe is never double-pushed;
  and for OSDs that vanish mid-request, the lease bounds how long
  after receipt a write may still start, so the gateway's fence wait
  (``rgw_cuobj_fence_wait_ms``, applied before any fallback rewrite)
  outlasts lease-plus-transport-drain and the window is quiescent
  before it is written again. The lease is measured against the
  wall clock, so it is best-effort fencing across clock steps —
  size it with slack rather than treating it as a hard barrier. The
  ``cuobj status`` OSD admin-socket command exposes plan and
  in-flight-write counters for observing the interlock.
* Planned follow-ups: moving the OSD-side push off the op worker
  thread (submissions are already batched asynchronously, but the
  reply path still waits for the batch); shard-space sparse reads on
  erasure-coded direct reads (currently delivered inline); and
  eliminating the OSD's staging copy by registering the BlueStore
  hugepage read-buffer pool with the RDMA NIC.

Integrity
=========

Because the gateway never touches passthrough data, verification moves
to where the data actually is: each OSD checksums (CRC64-NVME) the
exact bytes it pushed, after they crossed the fabric, and the gateway
folds the per-stripe values with the same combining math S3 uses for
multipart full-object checksums. Whole-object GETs of objects that
carry a stored full-object ``crc64nvme`` checksum (the AWS
``x-amz-checksum-crc64nvme`` type) are verified before any response
bytes are committed. Erasure-coded *direct* (split) reads and sparse
reads currently omit per-stripe checksums — their interleaved layouts
do not concatenation-combine — and simply skip verification.

Accounting
==========

Bytes moved over RDMA appear in the beast access log, the ops log and
the usage log (attributed to bytes sent for GET, bytes received for
PUT), even though they do not traverse the HTTP socket.
