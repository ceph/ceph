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
  client's descriptor to the OSDs instead: for each stripe of the
  object, the gateway issues a ``READ_RDMA`` RADOS operation carrying
  the token and the stripe's offset within the requested range. Each
  OSD reads its stripe locally and RDMA-writes it directly into the
  client's memory window; only byte counts travel back to the gateway,
  which sends the HTTP response once every stripe has been delivered.
  Object data never touches the gateway, removing the OSD-to-gateway
  network hop and the gateway staging buffer, and letting transfer
  bandwidth scale with the number of OSDs. This matches the cuObject
  architecture's "gateway instructs data nodes" reference flow. In this
  mode the gateway itself needs neither the cuObject library nor an
  RDMA NIC; the OSDs do (build them with ``-DWITH_OSD_CUOBJ=ON`` and
  set ``osd_cuobj_enabled``).

Fallback behavior
=================

Modes degrade transparently, per request:

#. **Passthrough** is attempted when the token is present,
   ``rgw_cuobj_osd_passthrough`` is enabled and the request is
   eligible (see below). If any stripe is refused with
   ``EOPNOTSUPP`` — an OSD that predates the feature, was built
   without it, has it disabled, or serves an erasure-coded pool — the
   gateway restarts the whole GET in the next mode down. The restart
   is invisible to the client: no HTTP bytes have been sent, and
   rewriting any client memory ranges that were already delivered is
   harmless.
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
* D3N datacache is not enabled;
* the requested range fits within the client's registered window; and
* the object lives in replicated pools (erasure-coded pools fall back).

Multipart objects and range requests are fully supported; every stripe
lands at its logical offset within the requested range.

Configuration
=============

Gateway (staged mode and protocol handling):

* ``rgw_cuobj_enabled``, ``rgw_cuobj_rdma_ip``, ``rgw_cuobj_rdma_port``,
  ``rgw_cuobj_buffer_size``, ``rgw_cuobj_buffer_count``,
  ``rgw_cuobj_num_dcis`` — the staged-mode ``cuObjServer``.
* ``rgw_cuobj_osd_passthrough`` — enable OSD-direct delivery for GET.

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
  window until the client deregisters it. Two mechanisms keep stale
  writes out of a reused buffer: the gateway drains outstanding
  stripe operations before sending any response, and OSDs refuse
  re-sent ``READ_RDMA`` operations (RADOS re-transmits reads after
  peering changes) so a stripe cannot be double-executed — the
  request falls back to the single-writer staged path instead. One
  residual hazard remains and is inherent to one-sided RDMA: a write
  already stalled inside a failed OSD's NIC retry queue can complete
  after the fallback response, so clients should deregister windows
  they consider dead rather than reuse them across error recovery
  (the cuObject client library mints a fresh token per operation).
* Erasure-coded pools, and moving the OSD-side RDMA write off the op
  worker thread, are planned follow-ups; see the tracker.

Accounting
==========

Bytes moved over RDMA appear in the beast access log, the ops log and
the usage log (attributed to bytes sent for GET, bytes received for
PUT), even though they do not traverse the HTTP socket.
