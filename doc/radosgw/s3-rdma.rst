.. _radosgw_s3_rdma:

============
S3 over RDMA
============

.. versionadded:: Umbrella

S3 over RDMA keeps the S3 control plane on HTTP and moves object data
out of band. Authentication, headers and metadata travel in the HTTP
request and response. The object bytes go directly into memory that the
client registered with its network adapter, and that memory can be GPU
memory.

.. contents::
   :local:
   :depth: 2

Overview
========

Clients
-------

The gateway serves three kinds of client. They differ in how they
describe the memory that receives the data.

cuObject clients
  A client that uses NVIDIA's cuObject client library
  (``libcuobjclient``) registers a memory window. It sends a descriptor
  for the window in the ``x-amz-rdma-token`` header of an ordinary S3
  request. The descriptor names a Dynamically Connected (DC) target,
  which exists only on NVIDIA ConnectX adapters. Any server that holds
  the descriptor and the shared DC key can write into the window
  without a connection to the client. NooBaa's ``s3perf.js --rdma`` and
  AWS SDK middleware for cuObject are examples.

libfabric clients
  A client that uses libfabric registers a window with a libfabric
  provider. It sends a libfabric token in ``x-amz-rdma-token``. The
  token names the wire protocol, the client's endpoint and the memory
  key. Any server process whose provider speaks the same wire protocol
  can write into the window without a connection to the client. The
  provider selects the
  wire: ``tcp``, ``shm``, RDMA reliable connections, AWS SRD or Ultra
  Ethernet.

Reliable Connection clients
  AMD's hipObject library is an example of a client that uses Reliable
  Connection (RC) transport. Such a client pairs a queue pair with the
  gateway through the ``hipobj-rc-v2`` control protocol on
  ``/.hipobj-rc/``. A queue pair
  is the RDMA endpoint of one connection. RC exists on every RDMA
  adapter. Only the paired gateway can write to the client, so the
  gateway relays the data.

Data paths
----------

Object data takes one of two paths, whatever the client's transport.

OSD-direct
  The OSDs write each stripe straight into a window. For cuObject and
  libfabric clients, the window is the client's own memory, and the
  object data never passes through the gateway. This is OSD passthrough
  mode, and its bandwidth grows with the number of OSDs. The gateway
  needs no RDMA adapter and no transport library for it. For RC
  clients, the window is a gateway session buffer. The gateway writes
  the buffer to the client over the paired queue pair while the
  stripes arrive. This is OSD-direct relay. ``rgw_rdma_osd_direct``
  turns on both, and it is on by default.

Gateway-staged
  The gateway reads the object from RADOS into a registered buffer and
  writes it to the client over the client's transport. For cuObject
  clients, the gateway's cuObject server writes the buffer in one
  transfer. For RC clients, the gateway writes the session buffer while
  the stripes arrive, which is staged relay. Libfabric clients have no
  gateway-staged mode yet, so they fall back to HTTP. A gateway-staged
  PUT takes the object into a registered buffer, and stores it through
  the gateway's filters, such as compression and encryption.

The gateway uses OSD-direct when it can, and gateway-staged otherwise.
An RC PUT is the exception: it is always gateway-staged. A body-less
cuObject or libfabric PUT is OSD-direct when ``rgw_rdma_osd_direct`` is
on, a transport in ``oob_client_transports`` serves the token, and none
of the cases in `OSD-direct PUT`_ applies, such as a filter that needs
the bytes. A cuObject PUT is gateway-staged only when it cannot be
OSD-direct, for example because the gateway must encrypt or compress
the object. When an OSD cannot pull a stripe of an OSD-direct PUT, the
gateway answers ``501``, and does not stage the PUT instead.

OSD-direct PUT
--------------

With ``rgw_rdma_osd_direct``, a PUT can leave the object in client memory.
The client registers the object's bytes as a window that peers can
read. It then sends a PUT with ``x-amz-rdma-token`` and an empty body.
The window's size is the object's size. Both token types work: a
libfabric token whose window allows remote reads, and a cuObject
descriptor.

The gateway writes the object as stripes of ``rgw_obj_stripe_size``
that carry no data. Each stripe write carries a delivery descriptor
with the pull flag. The primary OSD of the stripe RDMA-reads the stripe
out of the window into a staging buffer, and then runs the write as
usual. In an erasure-coded pool, the primary encodes the stripe and
sends each shard to its OSD. In a replicated pool, it sends the stripe
to the replicas. The object's bytes never pass through the gateway.

Each pulled stripe counts against the throttle window of the gateway's
writer, ``rgw_put_obj_min_window_size``, as if its bytes passed through
the gateway. A stripe larger than the window counts as one whole
window, so at most one such pull runs at a time.

Each OSD reports the CRC-64/NVME of the bytes that it pulled. If the
client chose CRC-32C, each OSD also reports the CRC-32C of those bytes.
A client chooses CRC-32C with ``x-amz-checksum-crc32c`` or with
``x-amz-checksum-algorithm: CRC32C``. The gateway folds each set of
values in object order, with the math of `Integrity`_. It stores the
checksum of the client's algorithm as the object's full-object checksum.
If the client chose no algorithm, the gateway stores ``crc64nvme``. If
the client sends the checksum, the two values must match. Otherwise
the PUT fails with ``400 BadDigest``, and nothing is stored. A later
passthrough GET of the whole object compares the stored checksum with
what the OSDs deliver.

The object's head holds no data. The gateway writes the head last, in
one operation with the checksum and the ETag, so a reader sees the old
object or the new one, never part of the new one.

The gateway has no MD5 of the object. The ETag has the form of a
multipart upload's ETag: the MD5 of the stripes' CRC-64/NVME values in
big-endian order, then ``-`` and the number of stripes. The suffix tells
S3 clients that the ETag is not the MD5 of the object. The same bytes in
the same stripes always get the same ETag.

Multisite sync does not check these objects with
``rgw_sync_obj_etag_verify``. The fetching zone skips the check for an
object stored in one piece whose ETag has the multipart form.

A successful PUT answers ``200`` with ``x-amz-rdma-reply: 200``,
``x-amz-rdma-bytes-transferred`` and the stored checksum in
``x-amz-checksum-crc64nvme`` or ``x-amz-checksum-crc32c``. The answer
also carries the folded values of the OSDs in
``x-amz-rdma-checksum-crc64nvme``. If the client chose CRC32C, the answer
also carries ``x-amz-rdma-checksum-crc32c``.

A body-less PUT whose token names a window of size 0 stores an empty
object. The gateway stores no other body-less PUT with a token as an
empty object. A token that names no window fails the PUT with ``400
InvalidArgument`` on every gateway, whatever the token's type. Its
address and size fields must each be 1 to 16 hex digits, with no ``0x``
prefix.

The gateway then checks the window's size against ``rgw_max_put_size``
(``400 EntityTooLarge``), and against the bucket and user quotas (``403
QuotaExceeded``). It makes these checks before any byte moves, and
before it can decline the PUT. So a PUT that fails them gets the error,
not a ``501``. A retry with the body fails the same way.

If the gateway cannot serve a body-less PUT out of band, it answers
``501`` with ``x-amz-rdma-reply: 501``, and the client sends the body
instead. Unless the token is a cuObject descriptor that a
gateway-staged PUT serves, the gateway declines these PUTs. A
gateway-staged PUT needs the gateway's cuObject server, and a frontend
thread that has a cuObject channel.

* PUTs that arrive while ``rgw_rdma_osd_direct`` is off, or whose token
  no transport in ``oob_client_transports`` serves.
* Appends, parts of multipart uploads, and PUTs with ``Content-MD5``.
* Objects that the placement compresses or that the gateway encrypts,
  torrents, and PUTs that a Lua data filter reads.
* PUTs with a client checksum other than ``CRC64NVME`` or ``CRC32C``.
* PUTs on a gateway with a nonzero ``rados_osd_op_timeout``.
* PUTs behind the ``d4n`` SAL filter, which must see the bytes to cache
  them, and PUTs to a store other than RADOS. Other filters, such as
  ``rgw_filter`` ``base``, pass the pulled write on.

The gateway also declines a PUT with a stripe that an OSD could not
pull, whatever the token's type, and does not stage it instead. The
gateway removes the stripes that it wrote, and the old object stays.

A body-less PUT archives the current version of the object for Swift
versioning, and reserves a bucket notification, only when its object
is out of the client's memory: after the OSDs pulled every stripe, or
after the gateway's staged read of the window completed. Nothing of the
new object is visible then. A PUT that is declined, or that fails
before then, thus leaves neither behind, and the client sends it again.

Only S3 requests take a token. A Swift PUT with ``x-amz-rdma-token``
and a body stores the body. One without a body fails with ``501 Not
Implemented``, and stores nothing. Ignoring the token would store an
empty object.

Every OSD executor can pull. The libfabric executor asks its provider
for RMA reads, and pulls into a buffer of the OSD's arena (see
`Registered memory`_). A provider without RMA reads still starts, for
delivery only. The cuObject executor reads with
``handlePutObject``, as the gateway's staged PUT does.

The OSD checks every pull of a write request before it starts any of
them. The pulls of one request are all or nothing. After one fails, no
later pull starts, and no write of the request runs. With
``osd_oob_transfer_threads`` at 0, a pull holds the object's lock, the
PG's lock and the op thread until it completes. That takes at most the
pool's lease plus drain. With transfer threads, a transfer thread runs
the pull, and the write holds no lock (see `Pulled writes on transfer
threads`_).

An OSD that cannot pull fails the write with ``EOPNOTSUPP``. That
happens in these cases:

* The OSD was built without a transport.
* None of its transports can pull with the token.
* Its client message throttle (``osd_client_message_size_cap``) has no
  room for the payload now.
* Its transfer threads refuse the pull, over one of their caps or
  because the OSD stops. The OSD does not pull on the op thread instead.
* Its transport refuses the pull before any read goes out. Examples are
  a payload larger than a staging buffer, a full arena, a busy channel,
  or too little time left to start the read.
* The write waited in its placement group past the pool's lease, so the
  OSD starts no pull (see below).

When a read of the window went out and then failed, or was cut off, the
OSD fails the write with ``EIO``. The OSD reports no other error of its
transport to the client.

A pulled write that would fail whatever its payload fails before the
OSD pulls anything, and its descriptor is declined. The OSD checks the
snap context, the size limit ``osd_max_object_size``, the appends of an
erasure-coded pool without overwrites, and a full pool, as the write
does. It fails the write with the error that the write gets inline,
such as ``EFBIG`` past the size limit. One error is different. A write
that does not append at the end of an object in an erasure-coded pool
without overwrites fails with ``EINVAL``, not with the ``EOPNOTSUPP`` of
the same write inline. ``EOPNOTSUPP`` would tell the client to send the
payload inline, and the write would fail again. The OSD checks no op
after one whose failure the request allows (``FAILOK``). When an op of
the write fails, the PG log keeps the error, as for the same write
inline: a resend gets it as a duplicate, and the reply waits until the
entry is committed.

The pulled payloads of one request count against that throttle
together, as the data of one inline message does. The OSD reserves them
once, after it checks every pull and before the first one starts. As
the messenger does, it admits a request larger than the throttle's
limit while the throttle is at or under its limit. The request gives
the bytes back when it completes. The pulled and inline bytes of one
request together must not exceed ``osd_max_write_size``. The OSD fails
a larger request with ``EMSGSIZE`` before it pulls anything.

The OSD's op scheduler, such as mClock, also charges a request for its
pulled payloads, as it does for inline data. The OSD reads their lengths
when the request arrives, before it queues the request.

In the client, the RADOS client counts a pulled write's length against
``objecter_inflight_op_bytes``, as it counts the data of an inline
write.

An OSD of an earlier release fails the write with ``EINVAL``, because
the write has no data. Below an Umbrella ``require_osd_release``, the
RADOS client does not send the pull descriptor. Every OSD then fails
the write with ``EINVAL``, and the result is marked declined. In all
these cases nothing is written.

When a pulled write fails, its reply marks the descriptor of an op
declined only when nothing of that op's pull reached the transport. The
reply leaves the result flags of an op at 0 when its pull started. That
holds whether the write fails in its pull or after it.

The OSD can answer a resent pulled write as a duplicate, when an
earlier attempt already did the write. The reply then reports what the
earlier attempt pulled: the byte count and the checksums. For this, the
OSD keeps a 30-byte record of each pull as the return data of its op.
The PG log keeps that record with the write.

The PG log keeps the record whether or not the request asks for return
data with ``CEPH_OSD_FLAG_RETURNVEC``. The RADOS client sets that flag
on a request that pulls only when its other ops return no data, as the
gateway's stripe writes do. A request with a ``CALL`` or a read, for
example, does not get the flag. For such a request the PG log keeps
only the records of the pulls, and no data that its other ops returned.
The reply to a duplicate of it carries no return data, as the reply to
the write did not. The record must fit in
``osd_max_write_op_reply_len``, 64 bytes by default. Without a record,
the reply to a duplicate reports no bytes and no checksums. It does not
mark the descriptor declined, because the earlier attempt may have read
the window, and the client fences the window. The gateway declines the
PUT. The same holds for a duplicate of a write that failed after its
pull.

The window must stay registered and unchanged until the reply arrives.
A token for a readable window lets any holder read that memory. When
the PUT is complete, re-key the window.

An OSD counts the lease and the drain of a pull from when the write
arrived, as it does for the pushes of a read. It starts no pull after
the pool's lease, and every read of the window that it started lands or
is cut off by the lease plus the drain. A stripe write that waited
longer than the lease in its placement group therefore pulls nothing,
and fails with ``EOPNOTSUPP``, its descriptor declined. A laggy or
degraded PG, a scrub, the object's lock or another pulled write of the
object can cause such a wait. The gateway then declines the PUT, and
the client sends the body. A librados caller that still waits can send
the data inline. The gateway answers a PUT only after every
stripe write completes, so the client keeps its window until then. A
librados caller that gives up a pulled write before its reply, as when
the operation timed out, may reuse the memory once the lease, the drain
and ``osd_oob_cutoff_late_tolerance`` have passed since the last attempt
of the write reached its OSD, as for a read (see
``rdma_delivery_drain``). It can close or re-key the window instead.

A gateway with a nonzero ``rados_osd_op_timeout`` declines OSD-direct
PUTs with ``501``. A stripe write could time out while it waits, and the
client could then reuse its window while the OSD can still pull it.

An OSD can still read the window after the reply, when the RADOS client
resent a stripe write to another OSD. The pull of the earlier attempt
runs until it ends: at most the pool's lease plus drain after that
attempt reached its OSD. That OSD writes nothing from what it reads. A
re-key before then only makes that read fail. See `Pulled writes on
transfer threads`_.

Transports
----------

A transport moves object data between two processes out of band. Ceph
uses transports in two roles:

Client transports
  The transports that move object data to and from clients.
  ``oob_client_transports`` lists them, and so decides which tokens the
  cluster accepts.

Cluster transport
  The transport that moves object data between Ceph daemons. It carries
  the shard reads that an erasure-coded primary gathers, and the stripes
  that the OSDs write into the gateway's relay buffers.
  ``oob_cluster_transport`` names it.

Three transports exist:

libfabric
  Any libfabric provider, for example ``verbs_ofi_rxm``, ``uet`` or
  ``tcp``. It writes into the windows that libfabric tokens name. It can
  be a client transport and the cluster transport.

cuObject (``cuobj``)
  Writes into the windows that DC descriptors name, with NVIDIA's
  ``cuobjserver`` library. It needs a ConnectX adapter on each host that
  runs it. It can be a client transport and the cluster transport.

Reliable Connection (``rc``)
  The gateway's ``hipobj-rc-v2`` server. It is a client transport, and
  only the gateway runs it.

An OSD runs the libfabric and cuObject transports through executors. An
executor writes the bytes of a read into the window that a token names,
and pulls the payload of a write out of such a window.

Every out-of-band read is advisory. When a daemon cannot deliver read
data out of band, it sends the same bytes in band, and the request
still completes with correct data. A gateway-staged PUT is not: when
the gateway cannot take the object from the client's memory, as when
no staging buffer holds it, the PUT fails.

Configuring a cluster
=====================

Quick start
-----------

Put the options below in the ``[global]`` section of the cluster's
configuration, so that every OSD and every gateway reads the same
values. A daemon reads them only when it starts.

#. Set ``oob_network`` to the subnets of the interfaces that carry
   out-of-band traffic. See `The out-of-band network`_.
#. Set ``oob_client_transports`` to the transports of your clients: the
   libfabric provider that they use, ``cuobj`` for cuObject clients,
   and ``rc`` for RC clients such as hipObject.
#. Set ``oob_cluster_transport`` to the transport between the daemons:
   one libfabric provider, or ``cuobj``. If you leave it empty, the
   gathers of erasure-coded reads stay on the messenger, and RC GETs
   take staged relay.
#. Give each OSD and each gateway unlimited locked memory:
   ``LimitMEMLOCK=infinity`` in its systemd unit, or ``--ulimit
   memlock=-1:-1`` under cephadm. See `Registered memory`_ and
   `Deploying with cephadm`_.
#. Restart the OSDs and the gateways.

This configuration serves libfabric, cuObject and RC clients on
ConnectX adapters, and moves data between the daemons over libfabric::

  [global]
  oob_network           = 10.90.0.0/24
  oob_client_transports = verbs_ofi_rxm, cuobj, rc
  oob_cluster_transport = verbs_ofi_rxm

``fi_info`` names a layered provider with a semicolon, as in
``verbs;ofi_rxm``. A semicolon starts a comment in a configuration file,
and drops the rest of the line, so write the name with an underscore
instead: ``verbs_ofi_rxm``. Both spellings name the same provider, and
so does ``verbs`` alone.

The gateway then uses OSD-direct transfers for every token of a client
transport. Each transport also needs some host configuration. See
`cuObject`_, `libfabric`_ and `UET`_.

The out-of-band network
-----------------------

``oob_network`` works like ``public_network``. It lists subnets, and
each daemon takes the first address of its host in one of them. That
is the daemon's out-of-band address. The daemon finds the interface
that carries the address, and from the interface what each transport
needs:

* ``verbs_ofi_rxm``, ``cuobj`` and ``rc``: the RDMA device, its port,
  and the index of the RoCE v2 GID that holds the address. The GID
  table names the interface of each GID, so an address on a VLAN above
  a bond works too. If no GID holds the address, as for an IPoIB address
  on InfiniBand, the daemon uses the RDMA device of the interface. The
  port is the interface's ``dev_port`` plus one.

  An InfiniBand bond has no RDMA device of its own. For a bond, the
  daemon uses the device and port of the active slave. When no slave is
  active, it uses the first slave by name that has an RDMA device. It
  also follows a bond of bonds. An Ethernet bond whose address no RoCE
  GID holds gets no RDMA device. The daemon looks only once, when it
  starts, so after a failover it stays on the old port until it
  restarts.

  The cuObject DC target behind an OSD's gather windows uses the same
  device, port and GID, also for an IPv6 address. It needs a GID that
  holds the address. On an IPoIB address, the OSD logs ``no RDMA device
  carries <address>``, opens no gather windows, and peers reply inline.
* ``uet``: the interface itself, which is the provider's domain. The
  provider on a device model such as rocm-ernic names its domain after
  the RDMA device of the interface. If the interface does not open, the
  daemon tries that RDMA device.
* ``tcp`` and other providers: the address, to bind to.

If no RDMA device carries the address, the daemon logs ``no RDMA device
carries <address>``, and starts no transport that needs one.

Each entry of ``oob_network`` must be a subnet with a prefix length, as
in ``public_network``: ``10.90.0.0/24`` or ``fd00::/64``. An entry such
as ``10.90.0.5`` or ``10.90.0.0/255.255.255.0`` is an error. The daemon
logs it, for example ``oob_network entry '10.90.0.5' is not a subnet,
such as 10.0.0.0/24 or fd00::/64``. It then starts no out-of-band
transport, but it does not exit.

If ``oob_network`` is empty, an OSD uses its public address. That works
only when the RDMA adapter carries the public address. A gateway uses
its ``public_addr`` option instead, and only when that option is set.
The gateway does not take an address from ``public_network``.

Without an address, the gateway's cuObject server does not start, and
logs ``no out-of-band address: set oob_network``. Its RC server takes
the first RDMA device, and the first RoCE v2 GID on port 1. Its
libfabric relay windows take the device that the provider chooses.
Either can be the wrong device, so set ``oob_network`` or ``oob_addr``
for every gateway.

Two options change the choice for one daemon:

* ``oob_network_interface`` limits the choice to the interfaces that it
  names. The value can use ``$id``, for example ``uet-o$id``.
* ``oob_addr`` gives the address directly. Set it in the section of the
  daemon, such as ``[osd.3]``.

Choosing transports
-------------------

``oob_client_transports`` lists the transports that the cluster serves
to clients:

* Each OSD starts an executor for the libfabric provider in the list,
  and one for ``cuobj``.
* The gateway starts its cuObject server for ``cuobj``, for
  gateway-staged mode, and its ``hipobj-rc-v2`` server for ``rc``.
* The gateway passes a cuObject descriptor to the OSDs only when the
  list names ``cuobj``. It passes a libfabric token only when the list
  names a libfabric provider. An OSD whose provider speaks another wire
  protocol than the token returns the stripe inline, and the gateway
  falls back. See `Fallback order`_.

``oob_cluster_transport`` names one transport for the transfers between
daemons. Every daemon must run the same one. It turns on two things:

* The primary of an erasure-coded client read lends windows to its peer
  shards, which write their shard data into them. It lends none for a
  fast read, or for a sub-read of less than 64 KiB. See `Gathering
  shard reads out of band`_.
* The gateway exposes its RC session buffers to the OSDs, which write
  their stripes into them. See `GET relay`_.

A libfabric provider works on any adapter that it supports. ``cuobj``
needs a ConnectX adapter on every host that runs it.

An OSD runs one libfabric endpoint. If ``oob_cluster_transport`` and
``oob_client_transports`` name different libfabric providers, the OSD
starts the provider of ``oob_cluster_transport``, and logs a warning
for the other one. A daemon also skips, with a warning, a transport
that it was built without or that fails to start.

Registered memory
-----------------

Each daemon that runs a transport allocates one arena of memory when it
starts. ``oob_memory`` sets the size of the arena, 2 GiB by default, in
whole blocks of 64 MiB. All transports of the daemon share it. A daemon
that runs no transport allocates nothing. An OSD whose transports all
fail to start frees its arena.

Transfers take buffers of the arena at the size that they need, rounded
up to a power of two from 1 MiB to 64 MiB:

* An OSD stages in one buffer each read that it delivers out of band,
  and each payload that it pulls. Each executor registers the whole
  arena once for this.
* The gateway stages each cuObject transfer of gateway-staged mode in
  one buffer.
* The gateway gives each RC session a buffer of the session's size. It
  takes the buffer from the arena at PREPARE. It registers the buffer
  for that session alone, because the client and the OSDs write into
  it. When the session ends, the gateway removes the registration and
  returns the buffer. A PUT's buffer and queue pair have their own
  protection domain. No other session's queue pair can reach the
  buffer, whatever key it holds.

The executor of the cluster transport also reserves 16 gather windows of
8 MiB, two blocks of the arena, when the OSD starts. It reserves them
only when at least one more block stays for staging, that is with an
``oob_memory`` of 192 MiB or more. Otherwise the OSD logs ``oob_memory
has no room for 16 gather windows of 8388608 bytes and for staging``,
and gathers stay inline. If the executor cannot set the windows up, the
two blocks go back to staging. That happens when the libfabric window
pool cannot register them, or when the cuObject DC target fails to
open. See `Gathering shard reads out of band`_.

A transfer larger than 64 MiB, or one that finds no room in the arena,
takes its fallback. An OSD delivers the read inline, and a shard replies
inline. A pull fails, and the gateway declines the PUT with ``501``. A
cuObject GET gets the HTTP body, and a cuObject PUT fails with ``503``.
An RC PREPARE fails with ``413`` for a transfer larger than 64 MiB, and
with ``503 SlowDown`` when the arena is full.

A window that a daemon lends to a peer gets a memory key of its own. A
libfabric window gets a new key when it comes back. The OSD's gather
windows behind a cuObject DC target keep their key. See `Reusing a
window`_.

The arena is far above the usual 8 MiB ``memlock`` limit, and each
registration counts against the daemon's ``RLIMIT_MEMLOCK``. Give the
OSDs and the gateways an unlimited one. On a host, set
``LimitMEMLOCK=infinity`` in the systemd unit. Under cephadm, add
``--ulimit memlock=-1:-1`` to the container arguments (see `Deploying
with cephadm`_). For a vstart cluster, run ``ulimit -l unlimited``.
``CAP_IPC_LOCK`` does not help, because a daemon that switches to the
``ceph`` user (``--setuser ceph``) does not keep it in its effective
set.

Each OSD registers an arena of its own, so a host locks ``oob_memory``
once for each OSD that it runs. On a host with many OSDs, set a smaller
``oob_memory`` in the ``osd`` section.

The arena is not part of ``osd_memory_target``. The OSD maps it outside
the heap, so the cache autotuner sizes BlueStore's caches without it.
An OSD that runs a transport uses up to ``osd_memory_target`` plus
``oob_memory``, and the registration keeps the whole arena in memory.
Plan host memory for both. Lower ``osd_memory_target`` by
``oob_memory``. With cephadm's ``osd_memory_target_autotune``, lower
``mgr/cephadm/autotune_memory_target_ratio`` so that the host keeps
``oob_memory`` free for each OSD.

Deploying with cephadm
----------------------

cephadm runs each daemon in a container. The container image must come
from a build with ``WITH_OOB_OFI``, and with ``WITH_RADOSGW_CUOBJ`` for
cuObject. Both are off by default. The image also needs libfabric 1.20
or later, and the ``cuobjserver`` library for cuObject. See `Build
options`_.

Add what the daemons need with ``extra_container_args`` in the service
specification. See :ref:`cephadm-extra-container-args`.

* An OSD runs in a privileged container that mounts ``/dev`` and
  ``/sys``. It needs only ``--ulimit memlock=-1:-1``.
* A gateway runs in an unprivileged container without the RDMA device
  nodes. When ``oob_client_transports`` names ``rc`` or ``cuobj``, the
  gateway needs ``--ulimit memlock=-1:-1`` and
  ``--device=/dev/infiniband``. A gateway that serves only libfabric
  clients needs neither.

For example:

.. code-block:: yaml

   service_type: rgw
   service_id: s3
   extra_container_args:
     - "--ulimit memlock=-1:-1"
     - "--device=/dev/infiniband"

cephadm splits a plain string at its spaces, so ``--ulimit
memlock=-1:-1`` becomes two arguments. Give an environment variable,
such as ``FI_UNIVERSE_SIZE`` for soft-RoCE, as an ``-e`` argument.

cephadm runs each daemon with ``--setuser ceph``. The gateway's
cuObject server then has the limit that `Limitations`_ describes.

UET works under cephadm when an OSD gets its ``uet`` transport before it
switches users (see `UET`_). Give ``oob_cluster_transport`` or
``oob_client_transports`` as an entrypoint argument in the OSD's service
specification. See :ref:`cephadm-extra-entrypoint-args`. For example:

.. code-block:: yaml

   service_type: osd
   service_id: uet
   # placement and data_devices as usual
   extra_entrypoint_args:
     - "--oob_cluster_transport=uet"

An option on the command line overrides the configuration that the
monitors store, as ``ceph.conf`` does. So
``ceph config set osd oob_cluster_transport ...`` has no effect on
these OSDs. To change the transport, change the service specification
and redeploy the OSDs.

The OSD's privileged container holds ``CAP_NET_RAW``. A gateway with
``uet`` relay windows needs ``--cap-add=NET_RAW`` in its
``extra_container_args``, because podman's default capabilities do not
include it. Each daemon also needs the provider's wrapper in its
container. Mount the wrapper's directory with ``-v``, and point
``FI_PROVIDER_PATH`` at it with ``-e``.

How a read reaches client memory
================================

The delivery descriptor
-----------------------

A RADOS read can carry a delivery descriptor. The descriptor belongs to
one operation in the request, so each read in a compound request can
carry its own. It holds these fields:

* ``token``: the client's token. The OSD reads only enough of it to
  choose an executor, and the executor reads the rest. See
  `Executors and tokens`_.
* ``base_offset``: the offset in the window where the first byte of the
  operation's data goes.
* ``flags``: requests, such as a CRC-64/NVME of the delivered bytes,
  and ``FLAG_PRIOR_SETTLED``, which only the RADOS client sets, on a
  resend. It clears that flag in a caller's flags, and librados passes
  on only the ``RDMA_DELIVERY_WANT_*`` bits. See `Retries`_.

librados clients set it with ``ObjectReadOperation::set_rdma_delivery()``.
The RADOS client sends descriptors only when the cluster's
``require_osd_release`` is Umbrella or later. Otherwise every read is
inline, and its result is marked declined, because no descriptor left
the client.

The descriptors travel after the fields of the version 9 ``osd_op``
message, and only when some operation carries one. An OSD that does not
know them ignores them and replies inline. The results travel the same
way, after the fields of the version 8 ``osd_op_reply``.

An OSD that delivers the data out of band writes it into the window and
reports the byte count in the operation's result. An OSD that cannot or
will not deliver it replies with the data inline, as if the request had
no descriptor. An OSD replies inline in these cases:

* No running executor serves the token.
* The pool's ``rdma_delivery_lease`` expired before the transfer
  started, or too little of the pool's ``rdma_delivery_drain`` is left
  to finish it. See `Fencing a window before reuse`_.
* The request is a retransmission, and the RADOS client does not vouch
  that every earlier attempt settled. RADOS resends reads after peering
  changes, and an inline reply makes sure that a stripe is never
  written twice. See `Retries`_.
* The OSD's PG read lease (``readable_until``) lapsed. See
  `Placement plans`_.
* The OSD's transfer threads refused or shed the transfer, over one of
  their caps or because the OSD stopped. See `Transfer threads`_.
* The descriptor has flag bits that the OSD does not know.

With an inline reply, the OSD also says whether it started a transfer
for the operation at all. When it did not, as in the cases above, or
when its executor sent nothing (no staging buffer, a budget already
spent, no address for the client yet, an endpoint busy adding a
client), the result is marked declined:
nothing of the operation reached the window. When the OSD delivered the
data over a transport whose writes are delivery-complete, the result is
marked landed: every byte was in the window before the reply. The RADOS
client marks a result resent when it sent the operation more than once,
since an earlier attempt may have started a transfer that this result
knows nothing of, unless every earlier attempt settled. librados
reports these as ``rdma_delivery_result::DECLINED``,
``rdma_delivery_result::LANDED`` and ``rdma_delivery_result::RESENT``.

An OSD of an older release marks results neither declined nor landed.
The caller then fences, because it cannot tell such a reply from a push
that was cut off. An OSD of this release that was built without any
transport marks every result that carries a descriptor declined. The
caller then does not need to fence its window. Such an OSD fails pulled
writes with ``EOPNOTSUPP``. The RADOS client reports declined by itself
in three cases: for descriptors that it withheld, for the descriptors
of an operation that it failed before it sent it, and for the
descriptor of an operation that a split read did not send. A split read
sends the descriptors of plain reads only, so a sparse read that it
splits reads declined, and its data comes back inline.

Retries
-------

An attempt is settled when its reply marked every operation that
carries a descriptor declined or landed. No write of a settled attempt can land
after its reply. An attempt that went out without its descriptors, below
an Umbrella ``require_osd_release``, is settled too.

Reads bounce routinely. A replica whose PG read lease lapsed answers a
balanced read with ``-EAGAIN``. So does a replica whose read failed
with ``-EIO``, and the shard of an erasure-coded shard-direct read
whose read failed with ``-EIO``. The RADOS client then sends the read
to the primary, which reads another copy or reconstructs the data from
the other shards. An OSD that answers a request with an error before it
executes the reads marks every operation that carries a descriptor
declined.

When every earlier attempt of an operation settled, the RADOS client
sets ``FLAG_PRIOR_SETTLED`` on the descriptors of the resend. The OSD
then delivers the resend out of band, as it would a first attempt, and
the result is not marked resent. An attempt that got no reply, as when
the RADOS client resent it after a map change or a session reset, makes
every later attempt a plain resend. An OSD that does not know the flag
delivers the resend inline.

A split read that ends in ``-EAGAIN``, because a sub-read bounced or
the sub-replies mixed inline and out-of-band data, is retried at the
primary. The retry is a first attempt when every sub-read settled, and
a resend otherwise.

A split read that completes reports one result for each operation,
folded from its sub-reads. The result is declined when every sub-read
declined. It is landed when some bytes moved and every sub-read landed
or declined. A replica's share that lies wholly past the end of the
object declines, for example. The result is resent when any sub-read
was resent. A split read that fails with an error other than
``-EAGAIN`` reports its result the same way, but never as landed.

Executors and tokens
--------------------

An OSD starts the executors of the transports in
``oob_client_transports`` and ``oob_cluster_transport``: a libfabric
executor, a cuObject executor, or both. See `Choosing transports`_. It
chooses an executor for each token by the shape of
the token. A token whose third field is ``ofi`` and a version, such as
``ofi1``, goes to the libfabric executor. The executor serves it only
when the token names the wire protocol of the OSD's own endpoint. A
token whose third field is hex goes to the cuObject executor. A
libfabric token of another wire or of an unknown version, and any token
that no running executor serves, is delivered inline.

Placement plans
---------------

The OSD turns each read into a placement plan. A plan maps ranges of the
reply to offsets in the window. The shape of the plan follows the read:

* A plain read has one range.
* A sparse read has one range per extent. The extent map stays inline.
* An erasure-coded primary read returns reconstructed logical data, so
  it has one range.
* An erasure-coded shard-direct read has one range per chunk. Each
  shard OSD writes its chunks to their logical positions in the window.
  The writes of the shards interleave in the client's buffer, so the
  client does not reassemble anything.

The OSD copies the reply into a registered staging buffer, writes each
range, and waits until every write completes. Only then does it send
the reply. Over a transport whose writes are delivery-complete, a reply
therefore means that the bytes are in the window. cuObject's DC
transport is reliable: a write completes only after the client's NIC
acknowledged it, as a write over libfabric's verbs provider does. So the
cuObject executor's results are marked landed too. Over a libfabric
provider without delivery-complete writes, they are never marked landed.
The gateway then waits for the fence before it answers such a GET, and
declines passthrough for a while (see `Fencing a window before reuse`_).
The OSD waits at most until the deadline that
`Fencing a window before reuse`_ describes. It cuts off the writes that
are still in flight then, and delivers the read inline.

With ``osd_oob_direct_read`` set to ``cached`` or ``nocache``, the OSD
reads the data of a read that the libfabric executor delivers straight
into a buffer of its arena (``oob_memory``), and the executor writes it
from there with no copy. BlueStore copies only what it has in its cache
and what is compressed. With ``cached``, it keeps a copy of what it read
in its cache. With ``nocache``, it does not, so a cold read is not
copied at all. An erasure-coded primary that gathers over
``oob_cluster_transport`` reads its own shard the same way, and copies
the shards that its peers push into its gather windows into the buffer
once. An erasure-coded PG reads this way only with the optimized EC
code.

Before an OSD starts a transfer, it reads its PG read lease
(``readable_until``) again. The readability test at dispatch does not
cover a read that stalled after dispatch. An OSD whose lease lapsed,
such as a primary that lost contact with its peers, replies inline and
marks the result declined, instead of writing into a window that a new
acting set can already serve. No push of the request starts after that
lease or the pool's ``rdma_delivery_lease`` ends. In a request with
several descriptor-bearing reads, a later read whose push cannot start
in time is delivered inline and declined.

Transfer threads
----------------

With ``osd_oob_transfer_threads`` at 0, the default, an OSD runs each
transfer on the op worker thread that serves the request. That thread
holds the PG's lock until the transfer ends, so other operations on the
PG wait. With a value above 0, that many transfer threads run the OSD's
pushes and pulls instead. These are the pushes of reads into client
windows, the pushes of erasure-coded shards into gather windows, and
the pulls of OSD-direct PUTs. See `Gathering shard reads out of band`_
and `Pulled writes on transfer threads`_.

Under the PG's lock, the op thread checks the read, plans its push, and
fixes the reply's versions, result and flags. It then hands the push to
a transfer thread, releases the object's lock and the request's unit of
``osd_client_message_cap``, and goes on. The reply still leaves only
after every write completes or is cut off. It never leaves before the
op thread releases the object's lock.

Replies to descriptor-bearing reads therefore leave a PG in the order
that their pushes complete. A later write to the same object can be
acknowledged before an earlier read's reply. That read still returns
the data and the version from before the write.

A push or a pull waits in a queue until a transfer thread is free. A
queued push starts nothing once the pool's lease ends, or once the PG
read lease that the OSD read at handoff ends. A queued pull starts
nothing once the pool's lease ends. ``osd_oob_transfer_queue_max`` and
``osd_oob_transfer_queue_bytes`` cap the transfers that wait for or run
on the transfer threads, and the OSD refuses a transfer over either cap.
In each case, the OSD delivers the read of a push inline and marks it
declined. A pull fails its write with ``EOPNOTSUPP`` when it waited too
long, or when the OSD refused it.

When ``osd_fast_shutdown`` is off, a stopping OSD refuses each new push
or pull for its transfer threads. Once its op threads stop, it sheds the
transfers still queued. A shed transfer starts nothing. The OSD delivers
the read of a shed push inline, marks it declined, and still sends the
reply. It then waits for the running transfers, each push until its
writes land or are cut off, and each pull until it ends. It stops its
messengers only after that.

Some transfers stay on the op thread, also with transfer threads.
cuObject transfers, pulls included, stay there unless
``osd_oob_cuobj_transfer_threads`` is set. Each thread that runs them
needs a DC initiator of its own (see `cuObject`_). The pulls of a
request that runs in a coroutine also stay on the op thread (see
`Pulled writes on transfer threads`_). The caps do not count the
transfers that stay on the op threads.

Pulled writes on transfer threads
---------------------------------

With ``osd_oob_transfer_threads`` above 0, a transfer thread runs the
pulls of an OSD-direct PUT (see `OSD-direct PUT`_). Under the PG's lock,
the op thread checks every pull of the write request, and reserves the
payloads in the client message throttle. It then hands one job with all
the pulls to the transfer threads. The write parks: it waits for the
job, and the op thread releases the object's lock and goes on.

When the pulls end, the transfer thread queues a kick for the PG. The
kick goes ahead of the client ops in the OSD's queue. Under the PG's
lock, it queues the write again, with the ops that wait behind it. The
write runs again from the start. It takes the pulled payloads, and goes
on as a write with inline data does. If a pull failed, the write fails
with that pull's error instead.

While the write waits, the PG keeps a hold on its object. The hold
keeps only the waiting ops, and no lock, so a scrub or a backfill can
still work on the object. Every later client op on the object or its
snapshots waits in the hold, behind the write. That includes reads, and
other pulled writes, which start no pull meanwhile. Ops on other objects
of the PG go on.

Writes to one object therefore keep their order. The PG checks the
holds before any other list that an op can wait in, such as the wait
for a scrub. The kick queues the write first, and the ops behind it
after it, in one step. So a scrub or a recovery cannot let a later op
pass the write. Only the flush ops of a cache tier, and PG ops such as
an object listing, pass a hold.

Nothing answers the write while its pulls run, not even with an error.
A client that is blocklisted meanwhile gets ``EBLOCKLISTED`` only after
the pulls end. The pulls end by the pool's lease plus drain after the
write arrived. Unlike a read, a waiting write keeps its unit of
``osd_client_message_cap`` and its reservation in the client message
throttle until it completes.

A request with a ``CALL`` on an erasure-coded pool with
``allow_ec_optimizations`` runs in a coroutine. Its pulls run on its op
thread, and it does not park. A pull on the op thread holds the object's
lock, the PG's lock and the op thread until it ends.

When the PG's interval changes and the OSD stays its primary, the PG
queues each waiting write again, with the ops behind it. A write whose
pulls still run then waits for the same pulls again. It does not pull
again, and it does not reserve the throttle again. When the OSD is no
longer the primary, or when it stops, the PG drops the waiting writes
and their ops. It sends them no reply.

When a write runs again, the PG can drop it as it drops any op: after a
split, a session reset or a backoff. A pull of a dropped write that has
not started never starts. A pull that already started runs until it
ends, and the OSD discards its payload. The client sends a dropped
write again.

The OSD keeps a list, by request, of the pulls that run on its transfer
threads. A later attempt of a write can reach the OSD while a pull of an
earlier attempt still runs. Examples are a resend after a split, a
session reset or an interval change. The later attempt then waits for
that pull in the hold of its object, in whichever PG it reaches. It
pulls nothing, and nothing answers it, until that pull ends. It then
pulls the window itself, or the OSD answers it as a duplicate when the
write is done.

A resend to another OSD, such as a new primary, does not wait. That OSD
can pull the window and answer while the earlier pull still reads it.
The OSD of the earlier attempt discards what it reads.

Gateway behavior
================

Passthrough eligibility
-----------------------

A GET uses passthrough only when the gateway does not need to touch the
data:

* The object is not compressed and not encrypted.
* No Lua data script or Arrow Flight filter applies.
* The object is not a Swift DLO or SLO manifest.
* The D3N data cache is off.
* The requested range fits in the client's registered window.
* The gateway's ``rados_osd_op_timeout`` is 0, the default. A stripe
  read that times out has no reply that the fence can follow, and an
  attempt of it that is still on its way to an OSD could write the
  client's window after the fence. The gateway logs this once, at
  level 1, and serves such GETs in the next mode. It declines
  OSD-direct PUTs for the same reason (see `OSD-direct PUT`_), and
  stages the GETs of RC clients instead of having the OSDs relay them
  (see `GET relay`_).

Multipart objects and range requests are supported. Every stripe lands
at its logical offset in the requested range. Replicated and
erasure-coded pools are supported. See `Erasure-coded pools`_.

Fallback order
--------------

Only a GetObject request, an HTTP ``GET``, can take an out-of-band
mode. The gateway serves a SelectObjectContent request over HTTP
whatever token it carries, because Select runs the data through its
query engine. So does a GET on the website endpoint of an object that
has ``x-amz-website-redirect-location``, which the gateway answers with
its ``301``. So does the ErrorDocument that the website endpoint serves
for a missing or forbidden key. The page goes out over HTTP with the
status of the error, such as ``404``, and with ``x-amz-rdma-reply:
501``. A client then never takes the page for the object.

The gateway tries these modes in order, for each request:

#. Passthrough, when the request has a token of a transport in
   ``oob_client_transports``, ``rgw_rdma_osd_direct`` is on, and the
   request is eligible (see `Passthrough eligibility`_). If any stripe
   comes back inline, the gateway restarts the whole GET in the next
   mode. The client does not see the restart, because the gateway has
   not sent HTTP bytes yet. When stripe operations were already sent, the gateway first
   waits for the fence described in
   `Fencing a window before reuse`_, unless no write of the request can
   land any more: every stripe came back declined or landed, and none
   was resent. One OSD that does not deliver out of band then costs a
   re-read over HTTP, not the fence. A stripe read that fails fails the
   GET instead, and the gateway sends the error behind the same fence.
#. Gateway-staged mode, when the token is a cuObject descriptor and the
   gateway runs its cuObject server (``cuobj`` in
   ``oob_client_transports``). The whole response must fit the client's
   window, the size in the token, as for passthrough. The gateway's arena
   must also have room for a buffer that holds the whole response, at
   most 64 MiB. See `Registered memory`_.

   The gateway first puts every byte of the response in the window
   with its RDMA write. Only then does it send the status line,
   ``x-amz-rdma-reply`` and ``x-amz-rdma-bytes-transferred``. If the
   write fails or is short, the client gets a plain S3 error
   (``500 InternalError``) with no ``x-amz-rdma-*`` headers. The client
   must not use the window's contents then.
#. The HTTP body, with ``x-amz-rdma-reply: 501``. The cuObject protocol
   defines this value as the signal to fall back to HTTP.

Some stores and filters do not pass the token on, such as
``rgw_filter`` ``d4n`` or an older SAL store. They send the data to the
gateway instead. The gateway then serves that same read in the next
mode, and does not read the object again. If such a store also sent
stripe operations with the token, their writes could land over the
data. The gateway then fails the GET behind the fence. A gateway-staged
response becomes a ``500`` error, but a response over HTTP has already
gone out by then.

When the data went out of band, the response carries
``x-amz-rdma-reply`` and ``x-amz-rdma-bytes-transferred``. The reply
has the status of the response itself: ``206`` for a range request, and
``200`` otherwise. A ``HEAD`` moves no data. It ignores the token, and
its response carries ``Content-Length`` and no ``x-amz-rdma-*`` header.

The gateway limits how much data the OSDs aim at one client at a time
with ``rgw_get_obj_window_size`` (default 16 MiB).

Anonymous requests
------------------

A token names the endpoint that the OSDs, or the gateway's cuObject
server, connect to, and the memory that they write into or read from.
Only the object's ACL and the bucket policy guard a request with a
token. So an anonymous reader of a public bucket could make the OSDs
connect to any endpoint that its token names. By default the gateway
therefore ignores the token of an anonymous request
(``rgw_rdma_token_allow_anonymous`` is false):

* An anonymous GET gets the body over HTTP, with ``x-amz-rdma-reply:
  501``.
* An anonymous PUT with a body stores the body.
* An anonymous PUT without a body that carries a token is declined with
  ``501`` and ``x-amz-rdma-reply: 501``, as a PUT that the gateway
  cannot serve out of band is. Ignoring the token would store an empty
  object. Nothing is stored, and the client sends the body instead.

Set ``rgw_rdma_token_allow_anonymous`` to true to let anonymous requests
move data out of band. The gateway reads the option for each request.

This does not limit what an authenticated client can do. Its token can
still name any endpoint that the OSDs can reach, also one that is not
its own, and the OSDs will connect to it and write into or read from it.
Keep the out-of-band network apart from other networks, and limit who
can reach it. The gateway and the OSDs do not yet check the endpoint of
a token against a list of allowed networks.

Fencing a window before reuse
-----------------------------

A token gives write access to the window until the client deregisters
the window. Three rules keep a stale write out of a window that is in
use again:

* The gateway drains every outstanding stripe operation before it
  responds. An OSD finishes its writes before it sends the operation's
  reply. A drained reply is therefore the interlock for every OSD that
  is still in contact.
* An OSD delivers a retransmitted request inline, unless every earlier
  attempt of it settled, so that none of them can still write the
  window. See `Retries`_.
* Two pool options bound every transfer, and the OSD enforces both.
  An OSD takes on a push, a transfer that writes into a window, only
  within ``rdma_delivery_lease`` of receiving the operation. Every
  write of it lands, or is cut off, within ``rdma_delivery_drain`` after
  the lease, also a write that the executor posts after the lease, as
  when it waited for a new peer. This covers an OSD that disappears
  during the request, and an operation that the gateway's RADOS client
  sent again. A pull keeps to the same bounds, and only reads the
  window (see `OSD-direct PUT`_).

  The gateway waits for a fence, 12 s, before a fallback rewrites the
  window, and before it answers the client with an error. It counts the
  fence from when the last stripe operation completed. The fence is the
  largest lease plus drain that any OSD allows, 6 s plus 4 s, and the
  largest ``osd_oob_cutoff_late_tolerance`` that an OSD takes, 2 s, on
  top. A libfabric cut-off can end that much later than the drain, and
  no reply follows the cut-off of a resent stripe's earlier attempt, or
  of a stripe whose read got no reply. The gateway does not read the
  tolerance, because each OSD reads it from its own configuration, and
  no reply says the value that an OSD used. An OSD rejects a value above
  2 s. The gateway does not use the pool's own values. The pool's read tier
  can serve the read, and an OSD whose OSDMap is newer than the
  gateway's can hold larger values.

  The gateway skips the wait when every stripe came back declined, so
  no transfer started, or landed, so every write completed before the
  reply, and no stripe was resent. A stripe that was resent, one an OSD
  started a transfer for and then returned inline (a push cut off), one
  from an older OSD, and one without a result, as when its read timed
  out, keep the wait. A stripe counts as resent only when an earlier
  attempt of it may still write: a resend whose earlier attempts all
  settled is not marked resent, and neither is a split read that the
  primary redoes after every sub-read settled. See `Retries`_. The same
  rule holds before a success: before the response, and before a
  gateway-staged write into the window. An OSD whose transport does not
  promise delivery-complete writes, as a libfabric provider without
  ``FI_DELIVERY_COMPLETE``, never says landed, and its reply can
  come before the bytes that it reports placed. So over such a transport
  an out-of-band GET waits for the fence before its response. Over a
  transport that promises delivery-complete writes, a GET that
  delivered everything waits only when a stripe was resent, because the
  stripe's earlier attempt may still write. The window is then quiet
  before it is written again, and before the client hears back.

  Rather than make every GET wait, the gateway declines passthrough for
  10 minutes after an OSD placed a stripe without saying that it
  landed. It logs that at level 1. It then serves GETs with tokens of
  that kind, libfabric tokens or cuObject descriptors, staged or over
  HTTP with ``x-amz-rdma-reply: 501``. When the 10 minutes have passed,
  one GET tries passthrough again, and the others keep declining it
  until that GET is done, or for a minute at most. If a stripe of that
  GET does not land either, it waits for the fence, and the gateway
  declines passthrough for 10 more minutes. If its stripes land,
  passthrough goes on.

How an OSD cuts off a write depends on the transport:

* The libfabric executor cancels only the late transfer's writes, and
  keeps its endpoint, when the provider promises that a cancelled write
  is discarded: no packet of it is sent once the cancel returns. The
  UET provider says so through an endpoint option. Other transfers on
  the OSD, to other clients and for gathers, go on. Without that
  promise, or when a cancel fails, the executor closes its endpoint and
  opens a new one. A libfabric provider discards the operations of an
  endpoint that closes. That cut-off also fails the other writes in
  flight on the endpoint, and those reads are delivered inline. ``ofi
  status`` counts both kinds: ``plans_cut_off`` and ``cutoffs``.
* cuObject cannot cancel a posted write. A write that the client stops
  acknowledging keeps retrying for the retry budget of the DC transport.
  The executor sets the ACK timeout to about 67 ms (IB exponent 14), and
  the retry count to 7. IB lets a NIC run that timer at up to four times
  its nominal value, so the budget is about 2.15 s. The budget counts
  from the last packet that the client acknowledged.

  So the cuObject executor stops waiting that long before the deadline,
  and posts no write after that point. It does not start a transfer
  when less than that is left, and ``cuobj status`` counts those reads
  in ``budget_refused``. A write that still makes progress at that
  point restarts the count with each acknowledged packet. It can land
  after the deadline, by as long as it keeps making progress.

  The writes of a transfer that stopped waiting stay posted on its
  thread's cuObject channel until they complete or fail. A write posted
  behind them would wait for them, and could land after its own
  transfer's drain. So until then the thread starts no transfer: it
  delivers its reads inline, with nothing staged, and ``cuobj status``
  counts them in ``channel_busy``. The first read on the thread that
  finds the earlier writes done gives the earlier transfer's staging
  buffer back. On an op worker thread, the buffer stays held until then,
  also when no further read comes. A transfer thread also makes this
  check each time it has nothing to run for 100 ms.

  A write whose submission fails may have posted part of itself, and
  nothing reports a completion for that part. The executor keeps that
  transfer's staging buffer until the DC retry budget has run out on the
  part. The budget counts from when the writes ahead of the part on the
  channel were done. The thread goes on starting transfers meanwhile. A transfer
  posted behind a part that is still retrying times out and holds the
  channel, as above.
* A provider over kernel sockets cannot cut off a write. Such providers
  are ``tcp``, a provider layered on it such as ``tcp_ofi_rxm``,
  ``sockets`` and ``udp``. Closing a socket does not discard the bytes
  that the kernel already queued on it, and the kernel delivers them
  later. The OSD logs a warning at startup. Use these providers for
  tests only.
* ``shm`` does not say whether closing an endpoint discards its writes.
  The OSD logs a warning at startup for it too. Use it for tests only.

The libfabric executor stops waiting for its writes early enough to cut
them off within the drain: by what a cut-off is expected to take, plus a
scheduling slack. The first guess of the cost is 100 ms. Each clean
cut-off then moves the estimate toward twice what it took plus 20 ms:
at once when that is more, and a quarter of the way when it is less.
The estimate never exceeds 2 s. Closing and reopening an endpoint takes
well under 1 ms on ``tcp`` and on the UET reference provider, so the
estimate falls to about 20 ms over a dozen cut-offs. The slack covers a
thread that the scheduler did not run in time. It starts at 50 ms, and
each cut-off moves it the same way toward twice how late the cut-off
started, plus 5 ms. It never drops below 10 ms. Any thread of the
executor that polls its endpoint also cuts off every transfer whose
time has come, so that one idle thread does not delay a cut-off;
``cutoffs_on_behalf`` counts those. A transfer whose budget is no
longer than the cost and the slack together is delivered inline, and
``budget_refused`` counts it. A transfer that has too little budget
left when it is about to start is also delivered inline, before it
sends anything, and ``late_starts`` counts it.
Starting it would only cut it off at once, and every other write in
flight with it.

A cut-off can still end after the budget it protects, usually because
it started late on a loaded host. Whoever waits for the OSD's reply is
not affected: the reply follows the cut-off. That covers a client that
receives a response, and the gateway's fallback, which waits for the
fence after every operation has completed. Only a client that gives a
request up, and counts the fence from when it sent the request, needs
to allow for it. So a cut-off that ends late by no more than
``osd_oob_cutoff_late_tolerance`` (1 s by default), and that itself
took no longer, is counted in ``cutoffs_late``, and the OSD raises the
``OOB_CUTOFF_LATE`` health warning, with the worst lateness, for an hour
after the latest one. Delivery goes on. 1 s is a third of the default drain, and
thousands of times what a cut-off costs, so a cut-off that late is a
scheduling delay, not a transport that fails to discard.

A cut-off can also end later than the tolerance although the close or
cancel itself was quick. It then started late because the OSD's threads
did not run: the process was stopped (``SIGSTOP``), paused for a long
time, swapped out, or starved, or its virtual machine did not run. The
OSD says so when nothing polled its endpoint for about as long. The
endpoint is clean once the close or cancel returns: nothing of the
writes it cut off is sent any more. So the OSD logs the cut-off to the
cluster log as an error, with its lateness, raises the
``OOB_CUTOFF_PAST_TOLERANCE`` health error for an hour, and goes on
delivering. Stopping the executor would not undo a write that already
landed, and it would turn every long pause into an outage until the OSD
restarts. Set ``osd_oob_cutoff_policy`` to ``disable`` to stop the
libfabric executor instead. The OSD reads that option only when it
starts. ``ceph config set`` stores a change, and a running OSD keeps
its old value until it restarts. ``ceph tell osd.N config set`` and
``injectargs`` refuse the change.

Who can such a late write reach? Say the OSD received the operation at
``t``. Its writes must land by ``t + lease + drain``, and one landed
``L`` later than that:

* A client that received a response for the request is not affected.
  The OSD replies after the cut-off, the gateway responds after the
  OSD's reply, and the client reuses its window after the response.
* The gateway's fallback is not affected. It waits for the fence, the
  largest lease plus drain and the tolerance, after every operation
  completed, so past the OSD's reply, which follows the cut-off.
* A fallback after the Objecter resent the operation, whose first
  attempt went to the paused OSD, is covered when ``L`` is shorter than
  the tolerance plus the time from ``t`` to when the resent operation
  completed. The
  Objecter resends only once the paused OSD is marked down, after
  ``osd_heartbeat_grace`` (20 s by default) of silence. A pause shorter
  than that does not lead to a resend at all. And over a software
  provider, such as the UET reference provider, nothing moves while the
  OSD's threads do not run, since it sends only while polled. Only a
  longer pause, over a transport that moves data without the OSD's
  threads, such as rocm-ernic's emulated UET engine or a NIC, can let a
  write land after this fence.
* A client that gave a request up, and reused its window lease plus
  drain plus the tolerance after the request's last operation reached
  its OSD, can see its window written.
  The health error tells an operator that this was possible.

A cut-off can fail. The provider may fail to close the endpoint, so its
writes go on. Or the close or cancel itself may take longer than the
tolerance, as when a provider drains its writes instead of discarding
them, so that every cut-off would be late. In these cases writes may
land in a client's window after the fence, and may keep doing so. With
``osd_oob_cutoff_policy`` set to ``disable`` or ``abort``, a quick
cut-off that ends later than the tolerance fails too, although its
endpoint is clean.

When a cut-off fails, the OSD stops its libfabric executor. It delivers
inline every read that the executor would have written, lends none of
its gather windows, logs the cause to the cluster log, and raises the
``OOB_DELIVERY_UNSAFE`` health error. With ``osd_oob_cutoff_policy`` set
to ``abort``, the OSD exits instead. That ends the writes of a software
provider, and a device drops the queue pair's. Restart the OSD once the
transport is fixed.

``OOB_DELIVERY_DOWN`` means that a cut-off could not reopen the
endpoint. No write is at risk then, but the OSD stops its libfabric
executor as above, and logs the cause to the cluster log. It does not
exit, even with ``osd_oob_cutoff_policy`` set to ``abort``. Restart the
OSD once the transport is fixed.

The lease and the drain bound the OSD side only. They are not a bound
on how long a client keeps a window registered. The OSD measures the age
of a request on its monotonic clock, from when the request arrived. So a
step of the wall clock does not move the lease or the drain.

Reusing a window
----------------

The fence covers writes that an OSD cut off. It does not cover a
duplicate of a write that already completed. A provider that retransmits
without connection state at the target, as UET's RUDI mode does, can
deliver a copy of a packet after the write it belongs to has completed,
and after the response that reported the bytes placed. The target keeps
no record of the write, so it places the copy wherever the copy's key
points. Copies can arrive until the network's longest packet lifetime
has passed. That is short on a healthy fabric, but it grows with every
queue a packet waits in. A soak test with 1.5 s of delay on a client's
link saw copies land 300 ms after their GET completed.

So a window owner must not reuse a window's memory while a copy of an
earlier write can still arrive under the window's key. A copy can land
whenever the key is valid, also while the window sits idle between
requests, and while its owner writes new data into it. Re-keying just
before the next request is therefore too late. The owner must do one
of these:

* Retire the key as soon as the request ends, when the response has
  arrived, and before the memory is read for another use, written, or
  lent again: deregister the memory region and register it again,
  under a new key. A write that carries the old key then fails instead
  of landing. This is the usual RDMA practice, and it is cheap on a
  software provider. Make sure that the provider does not hand the old
  key out again soon. The UET provider can also change a region's key
  in place, with ``fi_control(&mr->fid, FI_UET_MR_REKEY, &key)``: the
  old key is dead when the call returns, the region keeps its
  registration, and over a device it is one command instead of
  unpinning and pinning the memory again. The data of the request itself can still be
  read before the key is retired: a copy carries the same bytes to the
  same place.
* Leave the memory untouched, and issue no new token for it, until the
  network's longest packet lifetime has passed since the response.

A request can also end without a response: the connection to the
gateway resets, the gateway exits, or the client times out. The OSDs
may then still write into the window, as `Fencing a window before
reuse`_ describes: until the pool's ``rdma_delivery_lease`` plus its
``rdma_delivery_drain`` after the last operation of the request reached
its OSD, plus the OSDs' ``osd_oob_cutoff_late_tolerance`` for a cut-off
that ends late. Copies of those writes can follow until the packet
lifetime after that. The gateway sends the stripe operations of a GET
over the whole GET, and does not see that the client is gone before it
answers. So its last operation can reach an OSD long after the client
gave up, and a gateway-staged GET still writes the window itself after
its read. An S3 client cannot count this bound, and must retire the key
at once. Only a librados caller, which sends the operations itself, can
count it instead, and leave the window untouched until then (see
``ObjectReadOperation::set_rdma_delivery()``). A soak test that killed
the gateway saw OSD writes land in clients' windows 50 to 350 ms
afterwards. That is within the contract, but a client that reuses the
window at once sees its new data overwritten.

The gateway's fence waits 12 s, the largest lease plus drain and the
largest late cut-off tolerance, whatever the pool. Set the client's
request timeout above the fence plus the time that the request takes.
A GET whose OSD writes can still land gets its fallback or its error
only after that fence. A client that times out
first gets no answer, and cannot tell when the gateway sent the GET's
last stripe operation. It must retire the key.

With UET there is a third way. Writes into a region that its owner did
not mark ``IDEMPOTENT_SAFE`` use RUD instead of RUDI. RUD keeps state
for each initiator at the target, and places no packet twice. A window
owner that sets ``FI_UET_RUDI=0`` in its environment registers its
windows that way, and every OSD then writes them with RUD. It costs
throughput, which falls apart once the round trip grows past the
retransmit timeout (``FI_UET_TX_TIMEOUT``, 200 ms by default): every
packet is sent again before its acknowledgment can arrive. On a test
cluster with 300 ms of delay on one OSD's port, transfers through it
kept missing their deadlines: 94 cut-offs in three minutes, and, before
late writes could be cut off one at a time, about 90% of GETs from
every client fell back to HTTP.

Ceph does the first for the windows it lends itself, as soon as the
operation that used a window ends, also when a gather or a relay
failed. An OSD's libfabric gather windows get a new key when the gather
releases them, in place where the provider can. When a window is
registered again instead, the key that leaves service is not used for
another window for 10 s. An endpoint also asks a provider with a fixed
table of regions, such as UET's, for a table of 16384 regions. A gather
window returned while a ``FI_THREAD_DOMAIN`` provider adds a client gets
its new key from a later gather instead, and is not lent before (see
`libfabric`_).

The gateway closes the libfabric relay window of an RC session when the
session ends, which retires the window's key. The registration cache of
the verbs provider keeps the key of a closed region valid. So a daemon
sets ``FI_MR_CACHE_MONITOR=disabled`` in its own environment before it
opens a libfabric endpoint. If the provider keeps the key of a closed
region all the same, the gateway finds this at startup and logs it.
Every relay buffer whose window an OSD got then stays out of use for
10 s after its session. A buffer of a staged GET, whose window no OSD
got, goes back at once.

A libfabric window whose gather or relay may still have a write in
flight stays out of use for its quarantine all the same, and for 10 s
at least. Its old key keeps a late write out only if the provider drops
what carries it. A gather window whose re-key fails also stays out of
use for 10 s.

Only the OSD's gather windows behind a cuObject DC target keep their
key. The gateway registers each relay buffer behind its DC target for
one session, so each session's buffer has a key of its own. The gateway
removes that registration when the session ends. A buffer that OSD
writes may still reach stays registered until its quarantine passes,
and only then goes back to the arena. Until then, the registration
keeps the buffer's key from naming other memory.

Erasure-coded pools
===================

Primary reads and shard-direct reads
------------------------------------

An erasure-coded pool serves a passthrough GET by one of two read
paths. The pool and the gateway's read policy select the path.

Primary reads
  This is the default. The primary reconstructs the logical data and
  writes one contiguous range for each stripe. A plain erasure-coded
  pool works without changes on the client.

Shard-direct reads
  Each shard OSD writes the chunks that it holds to their logical
  positions in the window. Two settings enable this, and both are off
  by default:

  * ``allow_ec_optimizations`` on the pool, which sets the pool's
    ``split_reads`` flag. Replicated pools always have that flag, but
    a client splits a replicated read only when
    ``osd_min_split_replica_read_size`` is set. It is 0 by default,
    which turns that off.
  * ``rados_replica_read_policy = balance`` on the gateway. The pool
    flag only permits split reads. A read is split only when the client
    asks for a balanced read.

  With only the pool flag, reads go to the primary. A shard that cannot
  read its chunks bounces the read to the primary. See
  `Shard read errors`_.

The interleave happens only when a gateway stripe spans several
erasure-coded stripes. When ``rgw_obj_stripe_size`` equals the pool's
``stripe_width``, each shard holds one contiguous range of the request.
The plan then has one write, as for a primary read.

Gathering shard reads out of band
---------------------------------

The primary of an erasure-coded read normally collects the shards it
needs from its peers in the sub-read replies, over the messenger. When
``oob_cluster_transport`` names a transport, the peers write their shard
data into the primary's memory over it instead. This needs a pool with
``allow_ec_optimizations``. The primary then decodes as before, and its
client delivery writes the logical data into the client's window.
Object data then crosses the cluster network out of band on both hops.
The first hop is from the shards to the primary, and the second is from
the primary to the client.

The gather covers every read that the primary decodes for a client,
whether or not the client's read carries a delivery descriptor. Reads
for partial writes and recovery reads do not gather. A partial write
needs the very shards that it reads, so it cannot decode around a shard
whose push failed its checksum. Fast reads do not gather either: the
reads of a pool with ``fast_read`` set, and the synchronous reads of
object classes. A fast read completes once enough shards reply, and a
window still lent to a slower shard then stays out of use for its
quarantine.

The primary lends one registered window to each peer shard that it
reads 64 KiB or more from, and puts the window's token in the sub-read.
The shard reads as usual, writes all the data it read into the window,
and replies with only the extents and the crc32c of what it wrote. The
primary copies the shard buffers out of the window, checks the copy
against that checksum, and returns the window to its pool. The check
costs a crc32c of the shard data on each side, and does not cover data
that was wrong at rest. The primary reads its own shard without a
window.

A copy that does not match - a write that completed short, or landed
somewhere else, or a late write into the window - counts as a failed
push. The primary logs the window and its key, counts it in
``gather_crc_mismatch``, and quarantines the window. It then reads that
shard again, once, inline, and every later sub-read of the gather comes
back inline too. Only if that read fails too does the primary decode
from the remaining shards. A push whose extents run past the end of its
window, and pushed extents with no window to find them in, fail the
same way, without the count.

The primary records these failures as transport faults, not media
errors, so it never repairs a shard for one. See `Shard read errors`_.

The gather is advisory too. A shard that cannot write replies inline.
A shard follows the same bounds as for client delivery, counted from
when the sub-read arrived. A window whose data the primary did not use
stays out of use for the pool's ``rdma_delivery_lease`` plus its
``rdma_delivery_drain``. This covers a shard that replied inline with
data it read, since a write it attempted may still land, and a read
that was cancelled or restarted. A shard that read nothing - every
object of its sub-read failed, a media error say - attempted no write,
and its window comes back at once, so one object a shard cannot read,
read over and over, cannot use up the windows. So does the window of a
shard that says in its reply that its executor sent nothing into it:
the push was refused before any write went out, because the shard's
endpoint was adding a client, no staging buffer was free, or the lease
had run out, say. The libfabric executor also gives every released
window a new key, after which no write meant
for the last gather can land where the provider drops writes with a
retired key. A window whose data the primary used is lent again at once,
unless its new key fails. One whose data it did not use keeps its
quarantine, and stays out of use for 10 s at least once it has a new
key. A cuObject window gets no new key. See `Reusing a window`_.

With ``osd_oob_transfer_threads`` above 0, a shard runs its push on a
transfer thread, and sends its sub-read reply from there. Its op thread
reads and checks the sub-read under the PG's lock, hands the push off,
and goes on. The reply still leaves only after the push completes or is
cut off. So the primary reads a window only after the shard's reply. A
push that waits in the queue past the pool's lease, or that the shard
refuses or sheds, sends nothing. The shard then replies inline and says
that it sent nothing, as when its executor refuses a push.

A shard's sub-read replies then leave its PG in the order that their
pushes complete. A reply that leaves after an interval change carries
the old epoch, and the primary drops it. The primary still completes
the client reads of a PG in the order that it started them. So while a
shard's push is slow, later client reads of that PG wait for it. The
shard's PG goes on serving writes and other sub-reads meanwhile.

The executor of ``oob_cluster_transport`` lends the windows. It reserves
16 windows of 8 MiB in the OSD's arena when it starts. A shard read
larger than a window, or one that finds no free window, is returned
inline. So is a shard read of less than 64 KiB, which costs less over
the messenger than as a push, and uses up the few windows.

The libfabric executor registers each window on its endpoint, and
starts a progress thread that places the data pushed into the windows.
The cuObject executor puts the windows behind a DC target on the device,
port and GID that carry the OSD's out-of-band address. It registers
them for remote writes only, so no one that holds a window's token and
the DC key can read a window back. A peer can write into a window only
over the window's transport, so every OSD must run the same cluster
transport.

A cuObject gather window keeps its token. The DC target registers the
windows once, and each window has the same token every time the
primary lends it. So a late write with the token of an earlier gather
does not fail: it lands in the window. Two things keep such a write out
of a read:

* The quarantine. A window whose data the primary did not use stays out
  of use for the pool's ``rdma_delivery_lease`` plus its
  ``rdma_delivery_drain``, from the time that the primary returns it. A
  shard counts the same time from the time that it receives the
  sub-read: it starts its writes within the lease, and they end within
  the drain after that. So the quarantine keeps out the late writes of a
  shard that received the sub-read before the primary returned the
  window. A sub-read can also reach its shard after that time. For
  example, an interval change cancels the read while the sub-read is on
  its way, and the shard does not have the new map yet. The writes of
  that shard can land after the quarantine, in a window that a later
  gather uses.
* The checksum. The primary checks what it copies out of a window
  against the crc32c that the shard sent. A write that lands while a
  later gather uses the window makes that gather read the shard again,
  inline, and counts in ``gather_crc_mismatch``.

Only the checksum catches a write that lands after the quarantine. The
libfabric executor also gives a window a new key when it comes back, so
a write with the old key fails where the provider drops writes with a
retired key. See `Reusing a window`_.

The token travels in a new trailing field of the sub-read message, and
the pushed extents, their crc32c, and whether the shard sent nothing,
in new trailing fields of the reply. An OSD of an older release ignores
the token and replies inline.

Shard read errors
-----------------

A shard-direct read reads one shard and has nothing to rebuild from.
When the shard's store fails the read with a media error (``-EIO``),
the shard bounces the read with ``-EAGAIN``. It logs ``<pgid>
shard-direct read of <object> failed: <error>, bounced to primary
osd.<N> to reconstruct`` to the cluster log as an error, and counts the
bounce in the OSD performance counter ``ec_direct_read_redirect_eio``.
The RADOS client then sends the read to the primary as a plain read,
and the primary decodes around the bad shard.

The bounced sub-read starts no transfer. When every other sub-read of
the split landed or declined, the retry to the primary is a first
attempt, and costs the gateway nothing. Over a libfabric provider
without delivery-complete writes, no sub-read lands.
A bounce in a read over several shards then makes the retry a resend,
which the primary delivers inline. The gateway then waits for its fence
and falls back. See `Retries`_ and `Fencing a window before reuse`_.

When a shard's store fails a client read that the primary of a pool
with ``allow_ec_optimizations`` decodes, the primary also repairs the
shard, unless ``osd_ec_repair_on_read`` is off. It marks the object
missing on that shard and recovers it at once. It logs ``<pgid> shard
<shard> failed to read <object> v <version> with a media error;
marking it missing to repair it from the other shards`` to the cluster
log as a warning, and counts the shard in the OSD performance counter
``ec_read_repair``. The PG goes through recovery with ``repair`` set,
and the pushes count as repairs on the OSDs that take them, toward
``OSD_TOO_MANY_REPAIRS``. The read completes with the
decoded data either way.

The primary repairs only in an active and clean PG with no scrub
running and no write holding the object, and only while k+1 shards
that the read did not find damaged hold the object. A pool with m=1
therefore never repairs on read. There, every split GET that reads the
shard pays the cost of its bounce until the shard is repaired. A shard
left alone is found by the next read of the object, or by a deep scrub.

A gather window fault is not a media error. The shard is healthy and
the fabric lost its push, so the primary records the fault as
``-EBADMSG``, and never repairs the shard for it. When the decode needs
the shard, the primary reads it again once, inline. It reads around the
shard only when that read fails too. Both kinds of failure appear in
the cluster-log warning ``Error(s) ignored for <object> (shard errors
...) enough copies available``: ``-5`` for a media error, ``-74`` for a
gather fault that the decode did without.

With ``osd_read_ec_check_for_errors`` on (off by default), the read
fails with ``-EIO`` instead, whatever the cause, and nothing is
repaired. A gather fault whose shard the primary read again
successfully does not fail the read.

Reliable Connection clients (hipobj-rc-v2)
==========================================

Control protocol
----------------

An RC transfer needs a queue pair on each side, paired before data
moves. The client and the gateway exchange the pairing parameters in
three signed HTTP requests:

``POST /.hipobj-rc/prepare``
  The client sends its RC token (queue pair number and GID), a packet
  sequence number (PSN), a cookie, the operation, the object and the
  byte range. The range can start at any offset in the object, and only
  its size is limited. The gateway authorizes the request as the S3
  operation that it stands for. It creates a session, and answers with
  the session ID and its own queue pair number and PSN.

  A client on the gateway's host can send the all-zero token instead,
  and the gateway then pairs with its own GID. A token that names a GID
  of the gateway's host does the same: the gateway's own GID, another
  GID of a port of one of the host's RDMA devices, the RoCE GID of an
  address of the host's interfaces, or a loopback address. The gateway
  takes such a token only from a client on its host, and answers
  ``400`` otherwise. It reads the GIDs and addresses of the host when it
  starts, and again every 5 seconds.

  A client is on the gateway's host when its HTTP connection comes from
  a loopback address, 127.0.0.0/8 or ``::1``. The gateway reads the
  connection's peer address, not ``rgw_remote_addr_param``. A connection
  from another address of the host's interfaces does not count, because
  a proxy on the host connects from such an address for clients on
  other hosts. So does a container network that translates the address
  of a client to an address of the node. For example, the haproxy of
  cephadm's ingress service connects to the gateway at the host's
  address, and the clients behind it never count as local, also when it
  runs on the gateway's host. A proxy that connects to the gateway from
  a loopback address, for example one with ``127.0.0.1`` as its backend,
  makes every client behind it count as local. With such a proxy, set
  ``rgw_rdma_rc_trust_loopback`` to false. No client then counts as
  local, and no client can send the all-zero token or a token that
  names a GID of the gateway's host.

``POST /.hipobj-rc/ready``
  The client sends its queue pair number and the address and remote key
  of its memory region. The gateway pairs its queue pair with the
  client's, runs the transfer, and answers when the transfer is
  complete. The answer carries the byte count, the ETag, the version ID
  and a CRC-64/NVME of the delivered bytes.

``POST /.hipobj-rc/cancel``
  The client ends a session that it no longer needs. When a READY of
  the session is still transferring, the gateway stops the transfer: it
  moves the session's queue pair to the error state, and the transfer
  ends at its next step. That READY answers ``409
  RdmaSessionCancelled``, also when CANCEL comes while the READY pairs
  its queue pair, or while it returns the session for another READY
  after ``RdmaPeerBusy``. A PUT stores nothing, unless the gateway had
  read the whole buffer already. CANCEL answers once the transfer has
  stopped. If it has not stopped after 5 s, or by the session's deadline
  if that comes first, for example while a read from RADOS is slow,
  CANCEL answers ``409 RdmaTransferInProgress``, and the session ends
  when the transfer does. Only one CANCEL of a session waits. Another
  CANCEL of the session that comes meanwhile answers ``409
  RdmaTransferInProgress`` at once. A client can send CANCEL again
  later. Once the session has ended, CANCEL succeeds.

Every ``x-amz-rdma-*`` request header must be signed. With SigV4 it
must be in the ``SignedHeaders`` list; SigV2 signs every ``x-amz-*``
header. The gateway refuses a request with an unsigned protocol header
with ``403``. A device on the network path can change such a header
without breaking the signature. The gateway also refuses anonymous
requests. A session belongs to the identity that prepared it: the user,
subuser or role session. READY and CANCEL from another identity fail,
and leave the session alone. A CANCEL of a session that has already
ended succeeds. The per-user limits count the sessions of an account
together, those of all its users, subusers and role sessions. Outside
an account, they count those of a user and its subusers together.

When ``oob_client_transports`` does not name ``rc``, or the RC service
is not up, the control routes answer ``501`` with
``x-amz-rdma-protocol-status: unsupported``. The service starts after
the frontends, and stays down if its setup fails. That answer tells a
hipObject client to use plain HTTP.

GET relay
---------

For a GET, the session buffer sits between the OSDs and the client.
The object reaches the buffer in one of two ways:

OSD-direct relay
  The gateway exposes its session buffers to the OSDs. Each stripe read
  carries a delivery descriptor for the session buffer, and the OSDs
  write their stripes into it, as in passthrough mode. The gateway does
  not copy the object. This needs ``rgw_rdma_osd_direct`` and an
  ``oob_cluster_transport``. With a libfabric provider, the gateway
  exposes the buffers over libfabric, on any adapter that the provider
  supports, when the provider has delivery-complete writes. With
  ``cuobj``, it exposes them as a cuObject DC target,
  which needs an mlx5 adapter on the gateway, and only when
  ``rgw_rdma_rc_dc_relay`` is on. See `Relay buffers behind a DC
  target`_.

Staged relay
  This is gateway-staged mode for RC clients. The gateway reads the
  object from RADOS as usual and copies it into the session buffer. The
  gateway uses this path when OSD-direct relay is not available, and
  for objects whose data it must transform, such as compressed objects.
  It is also the fallback when an OSD returns a stripe inline.

In both cases the gateway sends the bytes to the client while the read
is still in progress. Each time the start of the buffer is complete up
to a new point, the gateway writes the new bytes to the client. A
stripe that an OSD relayed counts as complete when the OSD's result
says that it landed. Over a libfabric provider without delivery-complete
writes, the OSD can reply before its bytes are in the buffer, and its
result does not say landed. The gateway would then have to wait until
the reads are done and the fence has passed, 12 s, before it
could write those bytes to the client. So over such a provider it uses
the staged relay instead, and logs ``does not promise delivery-complete
writes`` when it starts. The OSDs' cuObject writes into a DC target say
landed. When a relayed stripe does not say landed all the same, as from
an OSD of an older release, the gateway writes its bytes to the client
only after the reads are done and the fence has passed. That wait does
not count against ``rgw_rdma_rc_exec_timeout_ms`` for the writes to the
client that follow it. See `Fencing a window before reuse`_. The
last write carries the session cookie as its immediate value. RC
delivers writes in order, so the completion of the last write tells the
client that all earlier writes are in its memory.

The client arms the receive for the last write before it sends READY.
A last write that finds no receive fails after six RNR retries, each a
wait of the client's ``min_rnr_timer``. A write to a queue pair that
does not answer at all fails too. If no earlier write of the transfer
reached the client, READY then answers ``409 RdmaPeerBusy``, and the
session stays prepared. When the OSDs wrote into its relay window, the
window then gets a new key first, so that a late duplicate of an OSD
write of the first attempt cannot land in the next one. Where the window
cannot get a new key, as with a provider that caches registrations, the
gateway ends the session and answers ``500`` instead. The gateway does
the same on a device that does not take a queue pair from RESET back to
INIT, such as an Intel E810 under irdma. It logs that at startup. A transfer that
the gateway staged gave no OSD the window, and its window keeps its key.
If earlier writes reached the client, the
gateway ends the session and answers ``500``, and the client prepares a
new session.

A GET of more than 64 KiB always writes to the client before its last
write, so an unarmed receive ends it with ``500``. Before a client
sends READY again after a ``409``, it arms the receive. It also resets
its queue pair and brings it back to RTS with the PSNs from PREPARE, or
pairs a new one. The gateway starts its writes again from the PSN that
PREPARE gave. A queue pair that kept its state can drop them as
duplicates.

A relay can fail after the OSDs received delivery descriptors. An OSD
can then still take on a write into the session buffer until the pool's
``rdma_delivery_lease`` expires. That write can land until the pool's
``rdma_delivery_drain`` runs out after the lease. When the reads fail,
the gateway keeps the buffer out of use for the fence, 12 s (see
`Fencing a window before reuse`_). It skips that when every stripe
came back declined or landed, and none was resent. Over a libfabric
provider without delivery-complete writes, no stripe comes back landed,
so such a relay is held unless every stripe was
declined. When the reads deliver the whole object, the gateway holds the
buffer by the same rule: a stripe that did not come back landed can
still have bytes on their way, and the earlier attempt of a resent
stripe read can write after every reply. That is so even when the writes
to the client fail afterwards. A session whose buffer is held this way
ends, even when the client's queue pair was only not ready, which
otherwise lets the client send READY again. When a stripe comes back
inline, the gateway waits for the same fence before staged relay copies
the object into the buffer.

The gateway also closes the buffer's libfabric window when the session
ends, which retires the window's key. No OSD write meant for that
session can then land where the provider drops writes with a retired
key. A buffer whose session left a write in flight keeps the
quarantine, and for 10 s at least. Any other buffer is free at once,
unless the provider caches registrations. Such a provider keeps the key
of a closed window, so every relay buffer then stays out of use for
10 s after its session. See `Reusing a window`_.

Relay buffers behind a DC target
--------------------------------

A DC target takes a write from any initiator that holds its DC key. The
OSDs' cuObject library writes with ``cuobj_dc_key``, the key of every
cuObject initiator and target, which every cuObject client holds as
well. Its default is the library's, ``0xffeeddcc``. The descriptor that
an OSD receives names no key of its own. So the gateway cannot give a
relay buffer a protection domain, a memory window or a key that the
OSDs can write through and a cuObject client cannot. A host that holds
the DC key, and learns or guesses a relay buffer's DC target number,
remote key and address, can write into the buffer while the relay runs.
The gateway would then send those bytes to the RC client as object
data, and that client can be another user.

So the gateway does not expose RC relay buffers behind a DC target
unless ``rgw_rdma_rc_dc_relay`` is on, and stages RC GETs instead.
Turn it on only when every host that holds ``cuobj_dc_key`` is
trusted: for example, when the key is a secret that only the OSDs and
the gateways hold, and no cuObject client uses the cluster. Relay
windows over libfabric take no key that clients share: a writer needs
the window's own key, which the gateway retires when the session ends.

PUT
---

For a PUT, the gateway takes a session buffer at PREPARE, arms a
receive on it, and gives the client its address and remote key. The
buffer has a registration of its own, whose addresses start at 0, so
the address is always 0, and the client does not learn where the buffer
is in the gateway's memory. The client writes the whole object with one
RDMA write-with-immediate,
with the session cookie as its immediate value. The gateway then stores
the buffer through the normal PUT path. Bucket default encryption,
compression, notifications and object lock apply as they do to any PUT.

Transports
==========

cuObject
--------

The cuObject descriptor is opaque to Ceph. The gateway and the OSDs
read only its leading address and size fields. The rest names the
client's remote key and DC target. Every writer needs the DC key that
the client library uses, which is ``0xffeeddcc`` by default.

The OSD executor needs ``cuobj`` in ``oob_client_transports`` or in
``oob_cluster_transport``, a build with ``WITH_RADOSGW_CUOBJ``, a ConnectX-5 or
newer adapter, ``rdma-core``, and NVIDIA's proprietary ``cuobjserver``
library. Gateway-staged mode for cuObject clients needs ``cuobj`` in
``oob_client_transports``, and the same build, adapter and library on
the gateway host. No GPU is needed on the OSD, gateway or client hosts.
Only GPU memory targets on the client need CUDA.

Each OSD creates one DC initiator for each op worker thread, and one
for each transfer thread with ``osd_oob_cuobj_transfer_threads``. The
gateway creates one for each frontend thread (``rgw_thread_pool_size``),
and at least 128. Every DC initiator and target uses ``cuobj_dc_key``.
The library chooses its ``rdma_cm`` port, and clients never connect to
it.

The gateway sends the cuObject library's log to standard error. By
default the library logs only its errors. With ``debug_rgw`` at 20 or
higher when the gateway starts, it also logs its info and debug
messages. A gateway that runs in the foreground (``-f``), as the
systemd unit and cephadm run it, keeps these lines. A gateway that
daemonizes closes standard error first, so the library's messages are
lost, errors included. The OSD does not configure the library's log,
and ``debug_osd`` has no effect on it.

The gateway starts its cuObject server after its frontends. Until the
server is up, a request that needs gateway-staged mode moves its data
over HTTP. A GET gets the HTTP body with ``x-amz-rdma-reply: 501``. A
PUT with a body stores that body, and a PUT without one is declined
with ``501``. If the server fails to start, the gateway logs
``cuObj RDMA server init failed``, and such requests always use HTTP.
Passthrough GETs and OSD-direct PUTs with a cuObject descriptor do not
use the gateway's server. The OSDs serve them whether or not it is up.

If the library has no DC initiator for a gateway frontend thread, the
thread logs ``cuObject channel allocation failed`` once. It then serves
its gateway-staged requests over HTTP. A GET gets the HTTP body with
``x-amz-rdma-reply: 501``. A PUT without a body that needs gateway-staged
mode is declined with ``501``, and the client sends the body. A GET that
took its staging buffer on another thread and resumes on this one fails
with ``500``.

Three parts of the host configuration are easy to miss:

``rdma_ucm`` must be loaded
  The cuObject server connects through ``rdma_cm``, so
  ``/dev/infiniband/rdma_cm`` must exist::

    modprobe rdma_ucm

  Without it, the OSD logs ``cuObjServer RDMA session failed to start``
  and disables cuObject delivery. A passing ``ib_send_bw`` run does not
  prove that the module is loaded. perftest uses raw verbs by default
  (``rdma_cm QPs : OFF``), so the fabric can reach line rate while
  cuObject cannot start a session.

Locked memory must be raised
  The cuObject executor and the cuObject server register the daemon's
  arena, ``oob_memory``, 2 GiB by default. That is far above the usual
  8 MiB ``memlock`` limit. Without more, the registration fails. See
  `Registered memory`_.

The RDMA address must belong to the RDMA device
  The daemon's address in ``oob_network`` must be an address that the
  ConnectX adapter carries. When the ConnectX ports are bonded and
  tenant traffic is VLAN-tagged, that is the address on the VLAN above
  the bond. It is usually not the public address, so an empty
  ``oob_network`` does not work there. ``ibv_devinfo`` and the GID
  table under ``/sys/class/infiniband/<device>/ports/1/gids`` show the
  addresses of the device. A RoCE v2 entry whose GID ends in the
  IPv4-mapped form of the address shows the pairing. If no adapter
  carries the address, the daemon logs ``no RDMA device carries
  <address>``.

A client that reads into host memory needs no GPU and no NVIDIA kernel
driver. ``libcufile`` reaches that configuration only with DMABuf
enabled. Otherwise it logs ``nvidia_peermem.ko is not loaded. Disabling
UserSpace RDMA access.``, registers no RDMA devices, and
``cuMemObjGetDescriptor`` fails::

  export CUFILE_DMABUF_ENABLE=true

The client's own RoCE address must also be in ``rdma_dev_addr_list`` in
``cufile.json``, which is empty by default. ``CUFILE_ENV_PATH_JSON``
selects another copy of the file::

  "rdma_dev_addr_list": [ "10.0.9.7" ],

Leave ``rdma_transport_type`` at ``DC_V1``, and keep ``rdma_dc_key``
equal to ``cuobj_dc_key``. The defaults on both sides agree.

libfabric
---------

The libfabric code is the ofi-rma library, in ``src/ofi-rma``. It
depends only on libfabric, so a client can link it to lend windows
without building Ceph. It can also lend windows in GPU memory, through
libfabric's ``FI_HMEM``. Set ``WITH_SYSTEM_OFI_RMA`` to build against an
installed ofi-rma 0.4 instead of the submodule. The build takes no
other minor version.

The owner of a window sends this token::

  <base hex>:<size hex>:ofi1:<wire>:<endpoint name hex>:<memory key hex>[:<auth hex>]

The fields are:

* ``base``: the remote address of the first byte of the window, as the
  owner's provider reads it. That is a virtual address when the
  provider uses ``FI_MR_VIRT_ADDR``, and an offset into the region
  otherwise. A writer writes byte ``i`` of the window at ``base + i``.
* ``size``: the length of the window.
* ``wire``: the wire protocol and its version, from the provider's
  ``fi_ep_attr``, for example ``rxm.1`` for ``verbs_ofi_rxm`` or
  ``xnet.1`` for ``tcp``. Both ends must speak the same wire. The token
  names the protocol, not the provider, so the UET providers of two NIC
  vendors take each other's tokens.
* ``endpoint name``: the bytes that ``fi_getname()`` returns for the
  owner's endpoint, at most 192 bytes. A writer passes them to
  ``fi_av_insert()``.
* ``memory key``: the key of the region behind the window. Providers
  with keys longer than 8 bytes are not supported.
* ``auth``: optional. The authorization key that the region is bound
  to, such as a UET JobID. ofi-rma 0.4 does not use it yet, and an OSD
  delivers a token that has it inline.

The token starts with the same ``addr:size`` fields as a cuObject
descriptor, so the gateway forwards it in passthrough mode without
reading the rest. The owner never adds a writer to its address vector,
so any OSD that holds the token can write into the window.

The executor asks the provider for delivery-complete writes. The
completion of a write then means that the bytes are in the owner's
memory. A provider that cannot promise this still works, but the OSD
logs a warning at startup. The gateway then serves most GETs staged or
over HTTP instead of in passthrough (see `Fencing a window before
reuse`_).

The first write to a peer adds the peer to the provider's address
vector. Some providers take long to do that. A thread of the executor
adds the peer. A write that gives up waiting for it has sent nothing,
and its read is delivered inline. The insert goes on, so a later read
finds the peer ready. The executor asks the provider for a thread-safe
domain. With one, the writes to a new peer wait for its insert, each
only until its deadline, and writes to other peers go on meanwhile.
The OSD logs at startup which case applies: ``concurrent peer
inserts`` or ``peer inserts pause writes``.

A provider that offers only ``FI_THREAD_DOMAIN`` allows no other call
during an insert, and adds one peer at a time. The insert first waits
until the writes in flight are done, so that none of them misses its
cut-off, and until the gather windows lent before it are back (see
below), and then holds the endpoint until the provider returns. The
endpoint is quiet from when the insert starts to wait until it is
done. An op thread or a transfer thread waits at most 10 ms for a quiet
endpoint, also for its own read's client. Once the endpoint has been
quiet for 10 ms, the thread does not wait at all. It then goes on
without the endpoint, so that the operations and transfers queued
behind it do not wait too:

* A read for any client, a new client's first read included, is
  delivered inline, and declined. A push this OSD makes as a shard of
  an erasure-coded read is delivered inline, and its reply says that
  nothing was sent, so the primary lends the window again at once.
  ``insert_busy`` counts both.
* A gather lends no window, and its shards reply inline.
  ``windows_declined_insert`` counts the windows not lent.
* A gather window returned meanwhile gets its new key from a later
  gather, and is not lent before. ``windows_rekeys_put_off`` counts
  it.

A quiet spell shorter than 10 ms, as when ``tcp`` adds a peer while no
write is in flight and no gather window is lent, refuses nothing.
Inserts queued back to back quiet the endpoint one after another.

The progress thread that places data in gather windows cannot poll
during the insert. A shard that pushes into a window then can wait for
the insert, and hold the thread that runs its push. The insert
therefore first waits for the windows lent before it, polling
meanwhile, so that the shards' pushes into them land: until the
gathers return them, which takes about as long as a gather, or until
8 s, the default lease plus drain, after a window was lent. A shard
whose push comes later still, in a pool with a longer lease or drain,
waits for the insert within its budget: a push that runs out of budget
is cut off, and the shard replies inline.

Clients choose their endpoint names, so the executor bounds what they
can make it do. At most 64 first contacts are queued or running at
once; a read for yet another new client is delivered inline at once,
and ``inserts_refused`` counts it. The address vector holds at most 1024
peers. A new one evicts the least recently used peers that no write is
using, never one that is being added. When every peer is in use, a read
for a new client is delivered inline, and ``inserts_refused`` counts it
too. A failed insert is remembered for a second, and reads for that
client are delivered inline meanwhile.

Providers that progress manually place incoming data only while the
application polls them. Each window owner in Ceph polls its endpoint
from a thread. A libfabric client must poll its endpoint while it waits
for the response, or use a provider with automatic progress.

These providers are known to work:

``tcp``
  Needs no special hardware. It binds to the daemon's address in
  ``oob_network``. It cannot cut off a write that misses its deadline,
  so use it for tests only.

``shm``
  Works between processes on one host. It does not say whether closing
  an endpoint discards its writes, so use it for tests only.

``verbs_ofi_rxm``
  RDMA reliable connections on any verbs device, through libfabric's
  ``verbs;ofi_rxm``. The daemon finds the
  device, for example ``mlx5_0`` or ``rxe0``, from its address in
  ``oob_network``. On soft-RoCE the
  provider fails to open with "Unable to create verbs CQ". The rxm
  provider sizes its completion queues from the universe size, and
  soft-RoCE allows 32767 entries per queue. Set
  ``FI_UNIVERSE_SIZE=16`` in the environment of every process.

``uet``
  Ultra Ethernet. See `UET`_.

UET
---

Ultra Ethernet Transport (UET) reaches Ceph through the libfabric
executor. UET's RUDI mode is reliable, unordered and connectionless,
and it is meant for idempotent operations. A window owner keeps no
state for the OSDs that write into it. cuObject's DC transport has the
same property, but UET runs on any Ethernet adapter.

The UEC reference provider (https://github.com/ultraethernet/uet-ref-prov)
is a software UET stack over raw Ethernet sockets. It does not register
itself with libfabric. A libfabric wrapper that registers it as the
provider ``uet`` is needed. Point ``FI_PROVIDER_PATH`` at the directory
that holds the wrapper, in the environment of every daemon and client.

Each OSD needs these settings:

* ``uet`` in ``oob_client_transports``, in ``oob_cluster_transport``,
  or in both.
* An address in ``oob_network`` on the interface that the OSD sends and
  receives on. The OSD uses the interface as the provider's domain, and
  the interface's IPv4 address is the OSD's fabric endpoint. The
  reference provider needs one interface for each process, so give each
  OSD an interface of its own, and set ``oob_network_interface`` to its
  name. The value can use ``$id``, for example ``uet-o$id``.
* The ``CAP_NET_RAW`` capability, for the raw socket that the provider
  opens.

The gateway's relay windows need ``uet`` in ``oob_cluster_transport``,
an address in ``oob_network`` on the gateway's own UET interface, and
``CAP_NET_RAW``.

The packaged systemd units and cephadm start each daemon as root, with
``--setuser ceph``. A daemon normally loses every capability when it
switches to the ``ceph`` user. For ``uet``, the OSD and the gateway
keep ``CAP_NET_RAW``, and no other capability, in their permitted set.
Each raises it into its effective set only while its endpoint opens, on
a short-lived thread of its own. The threads that the provider and
ofi-rma start during the open keep it in their effective set.

An OSD keeps the capability when ``uet`` is the provider of its
libfabric endpoint. That provider is ``oob_cluster_transport`` when it
names a libfabric provider, or else the first libfabric provider in
``oob_client_transports``. For example, with no cluster transport,
``oob_client_transports = tcp, uet`` makes ``tcp`` the provider, and the
OSD keeps nothing. The OSD switches users before it reads the
configuration that the monitors store. So the setting must be in
``ceph.conf``, in ``CEPH_ARGS`` or on the command line.

When the OSD keeps the capability, it logs
``set uid:gid to <uid>:<gid> (ceph:ceph), keeping CAP_NET_RAW for uet``.
Its main thread then shows ``CapPrm: 0000000000002000`` and
``CapEff: 0000000000000000`` in ``/proc/<pid>/status``. When root does
not hold the capability, that line ends in
``without CAP_NET_RAW, which uet needs`` instead. Without the
capability, the endpoint fails to open, and the OSD logs
``libfabric endpoint:`` with ``the process does not hold CAP_NET_RAW``
and a hint.

With ``set_keepcaps`` on (it is off by default), the OSD keeps its whole
permitted set through the switch instead. With ``bluestore_use_ebd`` on,
the default, the OSD's main thread later drops every capability that
the block-device plugins do not need, when it first opens BlueStore's
block device. It keeps ``CAP_NET_RAW`` through that step when ``uet``
is the provider. That step comes after the OSD reads the monitors'
configuration, and it changes only the main thread. The threads that
the main thread starts afterwards, the op threads among them, get the
smaller set. Threads that started between the switch and that step
keep the whole permitted set, such as the messenger worker threads,
which start when the OSD reads the monitors' configuration. With
``bluestore_use_ebd`` off, nothing trims the set.

A gateway keeps ``CAP_NET_RAW`` when ``rgw_rdma_osd_direct`` is on,
``oob_client_transports`` names ``rc``, and ``oob_cluster_transport`` is
``uet``. The gateway switches users when its frontends start, after it
reads the monitors' configuration, so ``ceph config set`` works for it.
It logs ``kept CAP_NET_RAW for the uet relay windows``, or
``WARNING: CAP_NET_RAW, which the uet relay windows need, is not available``.

Every thread that the OSD starts after the switch holds ``CAP_NET_RAW``
in its permitted set, its messenger and op threads included. In the
gateway, only the main thread holds it after the switch, and only until
the RC service has opened its relay windows. The main thread then drops
it, and logs ``rgw_rdma_rc: dropped CAP_NET_RAW``, before the RC service
opens its RDMA device and starts its threads. It drops it as well when
the RC service fails to start. The gateway starts the RC service before
the cuObject server, so the main thread has dropped the capability
before the cuObject server opens its RDMA files and starts its threads.
The server's threads hold no capability, and the main thread keeps the
credentials that opened those files, which the kernel checks on its
later calls into the server. The threads that the provider and ofi-rma
start during the open keep it, and so no other thread of the gateway
holds it. The HTTP worker threads start before the switch, and hold no
capability. Code that runs on a thread that holds the capability can
raise it.

A daemon that runs as another user from the start, without
``--setuser``, needs ``CAP_NET_RAW`` itself, for example in its ambient
set.

An OSD-direct PUT (see `OSD-direct PUT`_) needs RMA reads. The OSD
always asks for them, and the wrapper must offer ``FI_READ`` and
``FI_REMOTE_READ``. Under RUDI, a read is idempotent at
the client, so it goes over RUDI too: each request packet asks for one
packet of data, which the client sends back in its answer, keeping no
state for the OSD. When the wrapper offers ``FI_HMEM``, the client can
lend a GPU buffer, for GETs and PUTs alike. The reference provider
copies each packet with the CPU, so it can do that only for GPU memory
that the CPU can address, such as an APU's.

Give every UET interface the same MTU, and use jumbo frames where the
network allows them. The provider sizes each packet's payload from the
interface MTU: about 8 KiB at an MTU of 9000, against 1 KiB at 1500. UET
travels over UDP, to port 4793 by default.

The wrapper must discard an endpoint's writes when the endpoint closes,
as ``fi_endpoint(3)`` requires. The OSD relies on that to cut off a late
write, unless the wrapper promises that a cancelled write is discarded;
it then cancels the late write alone. A wrapper that drains its writes
on close instead lets a cut-off write land after the deadline. The OSD
measures each cut-off. One whose close or cancel itself takes longer
than ``osd_oob_cutoff_late_tolerance``, as a draining close does, counts
as a failed cut-off, and the OSD stops its libfabric executor. The OSD
also warns at startup when the wrapper says that closing does not
discard. See `Fencing a window before reuse`_.

A wrapper that offers ``FI_THREAD_SAFE`` lets the OSD add a new client
while its writes to other clients go on. One that offers only
``FI_THREAD_DOMAIN`` pauses the OSD's writes for each new client, and
reads, the new client's first among them, are delivered inline
meanwhile. See `libfabric`_.

The reference provider has these limits:

* It is software. A window owner places incoming data only while it
  polls its endpoint.
* It keeps process-wide state, so each process has one interface and
  one endpoint. Processes on one host need separate interfaces and
  addresses.
* The first write to a new peer finds the next hop. Current versions
  ask the kernel over rtnetlink, and wait for ARP at most
  ``UET_NH_WAIT_MS`` (1 s by default). Older versions run ``ip route``,
  ``arp`` and ``ping``, which takes 10 seconds when the peer does not
  answer the ping, as rocm-ernic's emulated UET NIC did not. Reads whose
  deadline passes during the wait are delivered inline.
* A memory key is the index of a region in a table that the provider
  fills round robin, so a key comes back after as many registrations as
  the table holds. The OSD and the gateway ask for 16384 regions. See
  `Reusing a window`_.
* RUDI keeps no state at the target, so duplicates of completed writes
  can arrive late. See `Reusing a window`_.
* It sends writes unencrypted, because the security sublayer is not
  configured.
* It supports IPv4 only.

To run several endpoints on one host, put each UET interface in its own
VRF, the gateway's interface included. The provider finds a peer's next
hop by a route lookup on its own interface. Without VRFs, a peer's
address is a local address of the host, so the lookup finds no next hop
on the wire.

Integrity
=========

The gateway does not touch passthrough data, so it cannot compute a
checksum itself. When ``rgw_rdma_checksum`` is on, the gateway asks each
OSD for checksums of the data that the OSD delivered. The OSD computes
the values from the bytes that it writes. For an erasure-coded primary
read, those are the bytes after reconstruction. The OSD reports one
value for each range of its placement plan. When the plan has one
range, that value also stands for the whole reply. The executor does
not matter.

The OSDs compute CRC-64/NVME, CRC-32C, or both. A client asks for one
with the request header ``x-amz-rdma-checksum-algorithm``, which takes
``CRC64NVME``, the default, or ``CRC32C``. For a GET of the whole object
whose stored full-object checksum is CRC-64/NVME or CRC-32C, the gateway
also asks for that algorithm. Any other value of the header
fails the GET with ``400 InvalidArgument``.

The gateway folds the values in logical order with the same combining
math that S3 uses for multipart full-object checksums. For a
shard-direct read, the RADOS client first folds the ranges of all
shards. The ranges must cover the operation's bytes without gaps. The
fold
works the same way for replicated and erasure-coded pools.

The response reports each folded value in its own header,
``x-amz-rdma-checksum-crc64nvme`` or ``x-amz-rdma-checksum-crc32c``,
rendered as S3 renders ``x-amz-checksum-crc64nvme`` and
``x-amz-checksum-crc32c``. Those S3 headers name the stored checksum of
the whole object. These headers cover exactly the bytes of the
transfer, so a ranged GET gets the checksum of its range.
A client can compare the value with the bytes in its own buffer, which
also covers the final write into its memory. A stripe from an OSD of an
older release comes back without a value. The response then carries no
header for that algorithm.

Some objects have a stored full-object ``crc64nvme`` or ``crc32c``
checksum, the AWS ``x-amz-checksum-crc64nvme`` and
``x-amz-checksum-crc32c`` types. For a whole-object GET of such an
object, the gateway compares the folded value with the stored checksum.
It does so before it sends any response bytes. A mismatch fails the GET
with ``500``, and the gateway logs both values.

A composite checksum is not a full-object checksum. Neither is the
checksum that a release before CRC combining stored for a multipart
upload. The gateway takes an object for a multipart upload when it has
a multipart manifest, or when its ETag has the multipart form. A
lifecycle transition, a multisite sync or a copy keeps that ETag. The
gateway does not ask the OSDs for such a checksum, and does not compare
it.

This comparison covers the storage node, the reconstruction, and the
shard transfers inside the cluster. Ranges with gaps between them do
not fold into a whole-object checksum, so a sparse read with several
extents is not compared.

When the OSDs report no value of the stored checksum's type for every
stripe, the gateway cannot compare. An OSD of an older release, a stripe
without a valid value, or placed ranges that do not cover the stripe
cause this. The gateway then answers the GET, and counts it in its perf
counter ``rdma_checksum_unchecked`` (``ceph daemon <rgw> perf dump``,
section ``rgw``). It also logs a warning at level 1, at most once a
minute, with the number of such GETs since the last warning.

An OSD of an older release does not know the CRC-32C request. It
delivers such a read inline, so the GET falls back to HTTP. It fails
such a pull, so the PUT falls back to the HTTP body. Before you ask for
CRC-32C, make sure that every OSD runs this release.

If ``rgw_rdma_checksum`` is on, RC clients get the same headers in the
READY answer. The READY request asks for an algorithm with
``x-amz-rdma-checksum-algorithm``. For a relayed GET, the gateway uses
the OSDs' values. If a stripe reported no value, the gateway computes
the values from its own buffer. A READY answer of a PUT also reports the object's stored
checksum in ``x-amz-checksum-<algorithm>``.

Configuration reference
=======================

Build options
-------------

``WITH_OOB_OFI``
  The libfabric transport: the OSD's executor, gather windows over
  libfabric, and the gateway's relay windows over libfabric. Needs the
  libfabric headers and library, version 1.20 or later, the first with
  dma-buf registration (``FI_MR_DMABUF``).

``WITH_RADOSGW_CUOBJ``
  The cuObject transport, in the OSD and in the gateway, with NVIDIA's
  ``cuobjserver`` library. It also needs the ``mlx5`` direct verbs at
  build time: ``libmlx5`` and ``infiniband/mlx5dv.h``, from the
  development package of ``rdma-core``. The OSD's DC target for gather
  windows uses them. Without them, the configure step fails.

``WITH_RDMA_RC``
  The gateway's ``hipobj-rc-v2`` server. It is on by default when
  ``WITH_RDMA`` is on. With ``cuobj`` as the cluster transport, the
  gateway's DC target also needs the ``mlx5`` direct verbs library at
  build time.

Pool options
------------

The OSDs enforce both options. The gateway does not read them. It waits
out the largest values that any OSD allows, 10 s in all, and the largest
``osd_oob_cutoff_late_tolerance``, 2 s, so no pool setting must agree
with them. Lowering a pool's values does not shorten
the gateway's wait. Set them with ``ceph osd pool set <pool> <option>
<seconds>``. A value of 0 restores the default.

The lease can be at most 6 seconds and the drain at most 4 seconds.
The monitor rejects larger values, because a transfer can hold an op
thread and the PG's lock while it runs. A transfer on a transfer thread
holds neither (see `Transfer threads`_). The client ops on the object
of a pulled write still wait for its pulls.

``rdma_delivery_lease``
  How long, in seconds, after it receives a stripe operation an OSD can
  still take on a transfer against its delivery descriptor. A transfer
  that would start later is delivered inline. A pull that would start
  later fails its stripe write with ``EOPNOTSUPP``. The default is 5.

``rdma_delivery_drain``
  How long, in seconds, after the lease every write that an OSD started
  lands or is cut off, and every read that it started for a pull. Make
  it longer than a transport needs to cut off its writes. That is about
  2.15 s for cuObject, plus the time that a write still making progress
  at the deadline takes to finish. For libfabric, it is the time an
  endpoint takes to close and reopen. The default is 3.

Common options
--------------

Set these options in the ``global`` section, so that every OSD and
every gateway reads the same values. A daemon reads them only when it
starts.

``oob_network``
  The subnets of the out-of-band network, as a comma-separated list.
  Each daemon takes the first address of its host in these subnets. The
  default is empty. An OSD then uses its public address, and a gateway
  its ``public_addr`` option. See `The out-of-band network`_.

``oob_network_interface``
  Limits the choice of address to these interfaces. The value can use
  ``$id``. The default is empty.

``oob_addr``
  The out-of-band address of one daemon. It overrides ``oob_network``.
  Set it in the section of that daemon. The default is empty.

``oob_client_transports``
  The transports that the cluster serves to clients: a libfabric
  provider, ``cuobj`` and ``rc``. The default is empty, which turns
  out-of-band transfers with clients off. See `Choosing transports`_.

``oob_cluster_transport``
  The transport between daemons: one libfabric provider, or ``cuobj``.
  The default is empty, which keeps gathers on the messenger and relays
  in the gateway. See `Choosing transports`_.

``oob_memory``
  The size of the arena that each daemon allocates for out-of-band
  transfers, in whole blocks of 64 MiB. The default is 2 GiB. It comes
  on top of ``osd_memory_target``. See `Registered memory`_.

``cuobj_dc_key``
  The DC key of every cuObject DC initiator and target in the cluster.
  It must match the key of the cuObject client library, ``rdma_dc_key``
  in ``cufile.json``. Give it in hexadecimal after ``0x``, as
  ``cufile.json`` does, or in decimal. The default is ``0xffeeddcc``,
  the library's default. The cluster refuses any other value: ``ceph
  config set`` rejects it, and a daemon that finds one in its
  configuration file ignores it without a message, and keeps the
  default. ``ceph config show`` reports the key that a daemon uses.

Gateway options
---------------

``rgw_rdma_osd_direct``
  Have the OSDs move object data straight to and from client memory.
  The OSDs write the data of a GET into client windows and into RC relay
  buffers. They pull the object of a body-less PUT out of client memory.
  The default is true. See `Data paths`_ and `OSD-direct PUT`_.

``rgw_rdma_token_allow_anonymous``
  Let anonymous requests use ``x-amz-rdma-token``. The default is false,
  which makes the gateway ignore the token of an anonymous GET or PUT,
  and decline an anonymous PUT without a body that carries one with
  ``501``. See `Anonymous requests`_.

``rgw_rdma_rc_trust_loopback``
  Take an RC request whose HTTP connection comes from a loopback address
  as a request of a client on the gateway's host. Only such a client can
  send the all-zero token or a token that names a GID of the host. The
  default is true. Set it to false when a proxy on the gateway's host
  connects to the gateway from a loopback address. See `Control
  protocol`_.

``rgw_rdma_rc_dc_relay``
  Expose RC relay buffers to the OSDs behind a cuObject DC target, when
  ``oob_cluster_transport`` is ``cuobj``. The default is false, and the
  gateway then stages RC GETs. See `Relay buffers behind a DC target`_.

``rgw_rdma_checksum``
  Compute checksums of out-of-band transfers. The gateway reports them
  in ``x-amz-rdma-checksum-<algorithm>``, and compares whole-object
  passthrough GETs with the stored checksum. An OSD-direct PUT computes
  them whatever this option says. The default is true. See
  `Integrity`_.

RC sessions:

* ``rgw_rdma_rc_max_sessions`` and ``rgw_rdma_rc_max_sessions_per_user``:
  the session limits, 1024 and 64 by default. A PREPARE over a limit,
  or one that finds no room in the arena, gets ``503 SlowDown``.
* ``rgw_rdma_rc_max_bytes_per_user``: the most arena memory that the
  session buffers of one user can hold. A PREPARE that would go over it
  gets ``503 SlowDown``. A user who holds no buffer can always take one,
  also one larger than the limit. The default, 0, is a fair share of
  ``oob_memory``. While one user holds buffers and no other user waits
  for one, that user can hold all of ``oob_memory``. A gateway that
  serves one account, for example as a KV cache for an inference
  service, then uses all of its memory for that account. While several
  users hold buffers, each can hold an equal part of ``oob_memory``. A
  user whose PREPARE found no room in the arena counts as one of them
  for 10 s, so that the users who hold the memory leave room for it as
  their buffers come back. Set a fixed limit for a gateway whose users
  must not wait for each other at all.
* ``rgw_rdma_rc_send_depth``: the writes to the client that can be in
  flight for one session, 64 by default and 1 at least.
* ``rgw_rdma_rc_prepare_timeout_ms`` and ``rgw_rdma_rc_exec_timeout_ms``:
  how long a session waits for READY, and how long a transfer can take,
  100 s and 30 s by default.
* ``rgw_rdma_rc_path_mtu``: the path MTU, in bytes, of the queue pairs
  that the gateway pairs with clients. Both ends of a pairing must use
  the same path MTU, and ``hipobj-rc-v2`` does not carry it. When the
  clients' path MTU is lower than the gateway port's, every transfer
  longer than it fails. So set this option to the clients' path MTU,
  for example 1024 for clients on a 1500-byte Ethernet MTU.

  The value is the RoCE path MTU, the ``active_mtu`` that
  ``ibv_devinfo`` shows, not the interface MTU. The gateway rounds it
  down to 256, 512, 1024, 2048 or 4096, and never goes above the port's
  active MTU. The default, 0, takes the port's active MTU. A value from
  1 to 255 keeps the RC server from starting.

Each session holds one buffer from PREPARE until the session ends. A
transfer larger than 64 MiB is refused with ``413``. The buffer comes
from the arena, and goes back to it when the session ends. Its size is
the transfer size rounded up to a power of two, 1 MiB at least. The
gateway registers the buffer, and zeroes a PUT's, up to 64 MiB of work,
on a worker thread, not on the frontend thread that takes the PREPARE, so
that many PREPAREs at once cannot take the frontend threads from other
requests. The PREPARE waits for it there. A PREPARE on the synchronous
beast frontend (``rgw_beast_enable_async`` off) still does this work on
its frontend thread, because it cannot wait off it.

A buffer that an OSD write can still land in stays out of the arena for
the fence, 12 s (see `Fencing a window before reuse`_).
See `GET relay`_. Such a buffer no longer counts against its user's
session limit, but it counts against ``rgw_rdma_rc_max_bytes_per_user``
until it is back in the arena. Until it goes back, no RC session and no
cuObject staging can use that memory. If the gateway cannot remove the
buffer's registration, it never uses that memory again, and the buffer
no longer counts against its user.

OSD options
-----------

``osd_oob_cutoff_policy``
  What the OSD does when its libfabric executor cannot cut off writes
  in time. With ``warn``, the default, a cut-off that fails stops the
  executor and raises ``OOB_DELIVERY_UNSAFE``. A quick cut-off that
  ends later than the tolerance raises ``OOB_CUTOFF_PAST_TOLERANCE``,
  and delivery goes on. With ``disable``, such a late cut-off also
  stops the executor. With ``abort``, the OSD exits in both cases. See
  `Fencing a window before reuse`_.

``osd_oob_cutoff_late_tolerance``
  How late a cut-off can end, past the budget it protects, and still be
  only counted and warned of (``OOB_CUTOFF_LATE``). A close or cancel
  that itself takes longer fails. The default is 1000 ms, and the largest
  value is 2000 ms. The gateway's fence counts the largest value,
  whatever the OSDs set.

Only the libfabric executor cuts off writes, so on the OSDs these two
options apply to it alone. The cuObject executor stops waiting ahead
of the deadline instead. The OSD reads both options only when it
starts. See `Fencing a window before reuse`_.

``osd_ec_repair_on_read``
  Have the primary repair a shard whose store failed a client read with
  a media error. The default is true. See `Shard read errors`_.

The OSD reads the options of its transfer threads only when it starts.
See `Transfer threads`_.

``osd_oob_transfer_threads``
  The number of transfer threads that run the OSD's pushes and pulls.
  The default is 0, which runs each transfer on its op thread. An OSD
  whose only transport is cuObject, with
  ``osd_oob_cuobj_transfer_threads`` off, starts no transfer threads.
  All its transfers then stay on the op threads, and the two caps below
  do not apply to them.

``osd_oob_transfer_queue_max``
  How many transfers can wait for or run on the transfer threads. The
  OSD refuses a transfer past this cap. It delivers the data of a
  refused push inline, and fails the write of a refused pull. The
  default is 64.

``osd_oob_transfer_queue_bytes``
  How many bytes of data the transfers that wait for or run on the
  transfer threads can hold. The OSD refuses a transfer that takes them
  past this cap. The default is 512 MiB.

``osd_oob_transfer_thread_timeout``
  The heartbeat grace of a transfer thread. A thread that runs one
  transfer for longer marks the OSD unhealthy, but the OSD never kills
  it. A transfer ends within the pool's lease plus drain, which is 10 s
  at most, plus ``osd_oob_cutoff_late_tolerance`` for a late cut-off.
  Pushes and pulls both count that time from receipt. With
  transfer threads, the OSD raises a value that does not cover that to
  1 s past it, and logs a warning. The default is 30 s.

``osd_oob_cuobj_transfer_threads``
  Run cuObject transfers on the transfer threads too. The default is
  false, which keeps them on the op threads. With true, the OSD also
  creates a DC initiator for each transfer thread. See `cuObject`_.

``osd_oob_direct_read``
  How the OSD reads the data of a read that its libfabric executor
  delivers: ``off``, ``cached`` or ``nocache``. With ``off``, the
  default, the OSD reads into the object store's buffers, and copies the
  data into a staging buffer. With ``cached`` or ``nocache``, it reads
  the data straight into a buffer of its arena. With ``nocache``,
  BlueStore keeps no copy of it in its cache. A change takes effect at
  once. See `Placement plans`_.

Operations
==========

Status
------

Each executor reports its counters through the OSD's admin socket. An
OSD registers each command only when that executor started::

  ceph daemon osd.N cuobj status
  ceph daemon osd.N ofi status

Both report plans started, completed and failed, and bytes written.
Both also show ``gather_crc_mismatch``: gathered shard data that did not
match its shard's checksum, whichever executor lent the window. See
`Gathering shard reads out of band`_. ``cuobj status`` also shows the
writes in flight, and these counters:

* ``buffers_leaked``: the staging buffers that plans kept. A plan keeps
  its buffer when it ends with writes outstanding, because it timed out
  or a poll of the channel failed without resetting it. It also keeps
  it when a submission failed.
* ``buffers_reclaimed``: the kept buffers that came back since.
* ``stale_completions``: the completions on a thread's channel that no
  running plan waited for. These are the completions of writes that
  earlier plans left posted, which a later transfer or an idle transfer
  thread took. They also include completions that the library returned
  a second time, which count toward no plan.
* ``channel_busy``: the reads delivered inline, with nothing staged,
  because writes of a plan that timed out, or whose poll failed, were
  still posted on the thread's channel. The buffer of a failed
  submission does not hold the channel.
* ``budget_refused``: the reads delivered inline, with nothing staged,
  because less than the DC retry budget was left.

A kept buffer comes back when a later cuObject read on its thread finds
its writes completed, or when the transport resets the channel. A
transfer thread also checks its channel each time it has nothing to run
for 100 ms. An op worker thread that serves no further cuObject read
keeps the buffer, so
``buffers_leaked`` can stay above ``buffers_reclaimed`` on an idle OSD.

``ofi status`` shows the windows lent and exhausted, ``staging_busy``,
the reads delivered inline because no staging buffer was free, the
provider and how the endpoint cuts off writes (``transport``), and the
endpoint's last error (``last_error``). It also shows:

* ``staging_leaked``: the staging allocations and frees that failed
  with an exception, as when the arena's lists could not allocate. Each
  can have lost a buffer of the arena until the OSD restarts. The count
  covers the whole daemon, which shares one arena.
* ``cutoffs``, the endpoint resets, and ``plans_cut_off``, the late
  transfers cancelled alone; ``cancels_failed``, the cancels that fell
  back to a reset; ``cancel_discards``, whether the executor cancels
  late transfers alone, and ``close_discards``, whether the provider
  promises that closing an endpoint discards its writes (``-1`` when it
  does not say). See `Fencing a window before reuse`_.
* ``cutoffs_failed`` and ``cutoffs_late``, ``max_cutoff_lateness_ms``
  and ``late_tolerance_ms``, ``unsafe`` and ``broken``.
* ``cutoff_slack_ms``, the scheduling slack, and ``cutoffs_on_behalf``.
* ``cutoffs_past_tolerance``, the quick cut-offs that ended later than
  the tolerance, and ``max_poll_gap_ms``, the longest the endpoint went
  without being polled, of a second or more.
* ``cutoff_cost_ms``, the current estimate of a cut-off's cost, and
  ``budget_refused`` and ``late_starts``, the reads delivered inline
  because too little of their budget was left.
* ``peer_timeouts``: reads that gave up, with nothing sent, while a new
  peer was added to the address vector or while they waited for the
  endpoint; with a ``FI_THREAD_DOMAIN`` provider, ``insert_busy``
  counts those that gave up on an insert. ``peers``,
  ``pending_inserts`` and ``inserts_refused``. See `libfabric`_.
* ``insert_busy``: calls of op or transfer threads that gave up on an
  insert of a ``FI_THREAD_DOMAIN`` provider, having waited for it at
  most 10 ms: writes refused with nothing sent, to a known client or to
  a new one not yet added (the read delivered inline and declined, or
  the shard's push delivered inline), gathers' reads of a window ordered
  without the endpoint, and, rarely, a gather window's token or new key
  asked for just as an insert began.
  ``windows_declined_insert``: gather windows not lent meanwhile, one
  for each shard a gather would have lent one to.
  ``windows_rekeys_put_off``: windows returned meanwhile, re-keyed by a
  later gather. See `libfabric`_.
* ``windows_rekeyed``, ``windows_rekeyed_quarantined`` (re-keyed after a
  gather that did not finish cleanly, and quarantined all the same),
  ``windows_rekey_failed`` and ``key_collisions``;
  ``rekeys_in_place`` and ``rekeys_reregistered``, and
  ``rekey_in_place``, whether the provider re-keys in place. See
  `Reusing a window`_.

An OSD that stopped its libfabric executor after a failed cut-off raises
the health error ``OOB_DELIVERY_UNSAFE`` in ``ceph health detail``. One
that stopped it after it lost its endpoint raises the warning
``OOB_DELIVERY_DOWN``. One that cut off writes late raises the warning
``OOB_CUTOFF_LATE`` within the tolerance, and the health error
``OOB_CUTOFF_PAST_TOLERANCE`` beyond it, for an hour. These stay raised
next to the first two, after the executor stopped. ``OOB_CUTOFF_LATE``
is not shown while ``OOB_CUTOFF_PAST_TOLERANCE`` or
``OOB_DELIVERY_UNSAFE`` is. Only the libfabric executor raises these
checks.

Two OSD performance counters count erasure-coded reads that hit a media
error. ``ec_direct_read_redirect_eio`` counts shard-direct reads that a
shard could not read and bounced to the primary.
``ec_read_repair`` counts the shards that a primary marked missing to
repair them after a read decoded around their media error. Each event
also goes to the cluster log. See `Shard read errors`_.

An OSD that started a transport also reports the ``oob_xfer``
performance counters, which ``ceph daemon osd.N perf dump oob_xfer``
shows. See `Transfer threads`_.

* ``queued``: the pushes and pulls that wait for a transfer thread.
* ``running``: the pushes and pulls that run now, on a transfer thread
  or on an op thread.
* ``refused``: the transfers refused over a cap or while the OSD
  stopped.
* ``expired``: the transfers that waited too long to start: past the
  pool's lease from receipt, for a push and for a pull.
* ``shed``: the transfers still queued when the OSD stopped.
* ``queue_lat``: how long a transfer waited for a transfer thread.
* ``push_lat``: how long a push took, on a transfer thread or on an op
  thread.
* ``pull_lat``: how long the pulls of one write request took, on a
  transfer thread or on an op thread.

A refused, expired or shed transfer starts nothing. The data of such a
push goes inline. A refused or expired pull fails its write. When
``expired`` or ``refused`` grows under load, the transfer threads do
not keep up. More reads then go inline, and the gateway declines more
PUTs.

``ceph daemon osd.N dump_oob_transfers`` shows the settings of the
transfer threads and the number of queued transfers. Its
``pinned_bytes`` is the data that the transfer threads hold. The command
also shows each running transfer:

* ``kind``, which is ``push`` or ``pull``, ``age``, ``queued_for``,
  ``post_by_in``, ``pinned_bytes``, and ``inline``, which is true for a
  transfer that runs on its op thread.
* For a read push: ``object``, ``op``, ``bytes`` and ``ranges``.
* For a push of an erasure-coded shard: ``pgid``, the shard's own PG
  instance, such as ``1.0s1``, ``sub_read``, the primary's sub-read
  tid, and ``bytes``.
* For a pull: ``object``, ``ops``, the number of pulls in the write
  request, and ``bytes``, their payloads together.

A read whose op thread handed its push off gets the event
``oob_handed_off`` in ``ceph daemon osd.N dump_historic_ops``. A read or
a shard's sub-read with a push gets ``oob_settled`` when its reply
leaves. ``op_r_latency`` counts such a read from receipt to its reply,
with the push included. A pulled write that waits for its pulls, and
each op that waits behind it, gets the event ``waiting for oob pull``.

``ceph pg <pgid> query`` lists the holds of the PG in
``oob_pull_holds``, one for each object. Each entry has the ``object``,
the ``owner``, the request ID of the waiting write, and the request IDs
of the ``waiters`` behind it. ``age`` is the time since the hold began,
in seconds. ``settled`` is true once the pulls that the hold waits for
ended. ``own_pulls`` is false when the owner waits for the pulls of an
earlier attempt of its request. See `Pulled writes on transfer
threads`_.

Accounting
----------

Bytes that move out of band appear in the beast access log, the ops log
and the usage log. They count as bytes sent for a GET and bytes
received for a PUT, although they do not cross the HTTP socket. The ops
log and the usage log take the direction from the operation, not from
the HTTP method. The RC control requests are all POSTs.

They count whatever becomes of the request. A GET counts the bytes that
the OSDs report for its stripe reads, also when the GET fails or falls
back to another mode, and then the bytes of the mode that answers it. A
gateway-staged write that is short counts what it wrote. An OSD-direct
PUT counts the bytes that the OSDs report they pulled, also when the
gateway declines the PUT afterwards. ``x-amz-rdma-bytes-transferred``
still reports only the bytes of the response itself.

For an RC GET, the gateway counts the bytes that it sends to the
client, in OSD-direct and staged relay alike. It does not count the
bytes that the OSDs write into the relay buffer. A READY that fails
after some writes counts what it sent. When the client sends READY
again after a ``409``, the gateway counts those bytes again. For an RC
PUT, the gateway counts the bytes that the client writes into the
session buffer, also when it does not store them.

Rate limits charge the same bytes. They count against the user's and
the bucket's ``max-read-bytes`` for a GET, and ``max-write-bytes`` for
a PUT. The operation gives the direction, so the bytes of an RC GET
count as reads although the request is a ``POST``. The gateway charges
out-of-band bytes once, when the request ends, and HTTP bytes as they
cross the socket. Either way the request completes, and the debt holds
back later requests in the same direction. See
:ref:`radosgw-rate-limit-management`.

The gateway admits or holds back a request by its HTTP method. Every RC
request is a ``POST``, so the gateway checks an RC GET against the
write limits, ``max-write-ops`` and ``max-write-bytes``. Its bytes still
count against ``max-read-bytes``. The read debt that RC GETs build up
therefore holds back the user's HTTP GETs, but not the next RC GET.

Testing
-------

* ``unittest_ofi_rma`` tests the libfabric token format, writes over
  the ``tcp`` and ``shm`` providers, slow and refused first contacts,
  that slow first contacts on a ``FI_THREAD_DOMAIN`` endpoint hold no
  op thread longer than the insert wait, whether it lends, returns or
  reads a window or writes to a known or a new peer, that their inserts
  let pushes into windows lent before them land first, eviction,
  cut-offs that fail or end late, within and beyond the tolerance,
  writers left idle near their deadlines, late writes cancelled alone
  (over ``tcp``, with a test hook that stands in for a provider whose
  cancel discards), the cut-off cost estimate,
  writes that start too late, concurrent writes and their CPU use,
  that a write with a re-keyed window's old token does not land, and
  the gather window cycle under load, with cut-offs alongside.
* ``unittest_osd_ofi`` tests the OSD's libfabric executor over ``tcp``.
  It covers the lending of gather windows, a pull of a write's payload,
  and cut-offs that fail or take too long. The tests that need ``tcp``
  skip only when libfabric offers no ``tcp`` provider with what ofi-rma
  asks for. Any other failure to open the endpoint fails the test.
* ``unittest_ec_gather`` tests the check of gathered shard data against
  its shard's checksum, and that a reply says when its shard sent
  nothing into the window. It also runs a shard's push through the
  transfer threads with a fake executor: placed, failed, expired,
  refused and shed.
* ``unittest_oob_xfer`` tests the transfer threads: their caps,
  transfers past their leases, and shedding at stop. It also tests
  transfers that run on the submitting thread, the heartbeat of a long
  transfer, and what ``dump_oob_transfers`` shows. It checks the bounds
  of a transfer from a monotonic receipt time, and that no step of the
  wall clock moves the receipt time of a request.
* ``unittest_oob_read_delivery`` tests that the read push gives the same
  replies as a model of it in the test, for every kind of read and
  outcome. It also tests the handoff to the transfer threads, pushes past
  their leases and shed pushes. It checks that the reply leaves only after the
  op thread releases the object.
* ``unittest_oob_pull`` tests the pulled write with a fake executor. It
  covers the reservation in the client message throttle, also for a
  request larger than the limit. It tests that no pull starts before
  every pull is checked, and that the pulls are all or nothing. It
  checks the checksums of the payloads as pulled, and that a second
  run pulls and reserves nothing. It checks that an error reply declines
  only the ops whose pull started nothing. It covers the park and the
  kick, the job of a dropped write, and a resend that waits for an
  earlier job.

  It also checks the PG log's record of a pull, and that the reply to a
  duplicate reports the pull from it. It checks that a pull counts its
  bounds from receipt, and that a write that waited past the lease pulls
  nothing and fails with ``EOPNOTSUPP``.
* ``unittest_oob_placement`` tests placement plans.
* ``unittest_oob_abandoned`` tests how the cuObject executor counts a
  completion against the plan that posted it, and when it gives the
  staging buffer of a plan that timed out back. It checks that a
  completion that the library returns twice counts once, and gives back
  no buffer that a posted write still uses.
* ``unittest_rdma_token`` tests token parsing and transport lists.
* ``unittest_oob_caps`` tests when an OSD or a gateway keeps
  ``CAP_NET_RAW`` across ``--setuser``, and how it raises the capability
  while an endpoint opens. It runs the tests that need
  capabilities in a user namespace, and skips them where the system
  refuses one. Its test of the switch itself needs root.
* ``unittest_oob_gid`` tests how a daemon finds the GID that holds its
  address in the GID tables under sysfs, in the form that the kernel
  prints, and that it skips the entries that are no GID.
* ``unittest_crc64nvme`` tests CRC-64/NVME against the NVM Command Set
  test cases, and checks that its implementations agree.
* ``unittest_rdma_delivery`` tests the wire formats of delivery
  descriptors and results, how the client library marks a result
  resent, and when an attempt counts as settled, so that a resend
  carries ``FLAG_PRIOR_SETTLED``. It follows a push that an OSD cut off
  through the client library into the gateway's decision to fall back
  to HTTP. It also checks that ``osd_op`` and ``osd_op_reply`` carry the
  descriptors and results only as a suffix of their version 9 and
  version 8 encodings. It checks that the client library reports
  withheld descriptors as declined, and that ``ObjectOperation::dup()``
  keeps the descriptor and result vectors aligned with the operations.
  It checks that the scheduler cost of a request counts its pulled
  bytes, and on which requests the client library sets
  ``CEPH_OSD_FLAG_RETURNVEC``.
* ``qa/standalone/erasure-code/test-erasure-eio.sh`` covers shard-direct
  reads whose shard read fails with ``-EIO``, and their bounce to the
  primary.
* ``qa/standalone/osd/osd-oob-async.sh`` runs pushes on transfer threads
  over the libfabric ``tcp`` provider. While one push is held, the other
  operations on its PG do not wait for it, and with 0 threads they do.
  It covers read pushes and erasure-coded shard pushes, an interval
  change, a slow stop, and pushes past the lease or over
  ``osd_oob_transfer_queue_max``. Its librados client is
  ``ceph_test_osd_oob_read``, which needs a build with ``WITH_OOB_OFI``.

  It also runs pulls on transfer threads. While a pull is held, the
  operations on other objects of its PG do not wait, and with 0 threads
  they do. An inline write and a read behind the pulled write keep their
  order. It holds a pull during a deep scrub, a backfill, a repeer, a
  primary change, a blocklist, a session reset and a slow stop. It
  splits the PG before the pull starts, and while it runs.

  It checks that a pulled write with a ``CALL`` on an erasure-coded pool
  with ``allow_ec_optimizations`` pulls on its op thread. It checks that
  the reply to a resend that the OSD answers as a duplicate reports the
  earlier pull. It checks that a pulled write that waited past the lease
  pulls nothing and fails with ``EOPNOTSUPP``, its descriptor declined.
  The ``put`` mode of ``ceph_test_osd_oob_read`` sends the pulled
  writes. With ``--call``, it prints ``crc=declined`` for a write whose
  resend the OSD answered as a duplicate.
* ``qa/standalone/osd/osd-oob-copies.sh`` counts the copies that
  out-of-band transfers make in the OSDs. The OSDs run with
  ``libceph_test_copycount`` preloaded, which counts the bytes that a
  process copies with ``memcpy`` and ``memmove``. Each test moves one
  object out of band over the ``tcp`` provider, and checks what the OSDs
  copied against the budget of its path. A read into the arena
  (``osd_oob_direct_read``) of a replicated pool, or of an erasure-coded
  pool with balanced reads, copies nothing, also for a range. An
  erasure-coded gather copies the gathered shard once. A read with
  ``osd_oob_direct_read`` off copies the whole object, and a pulled write
  copies the whole object on top of what a plain write copies. A test
  that fails prints the places that copied. The ``ceph-test`` package
  ships the library.
* ``unittest_rgw_rdma_fence`` tests when the gateway fences, and how it
  reads each stripe's reply.
* ``unittest_rgw_rdma_get`` tests, against fakes of the store, how the
  gateway picks the out-of-band mode of a GET with ``x-amz-rdma-token``,
  which GETs are eligible, and how a GET falls back when the store
  streams the data. It covers the passthrough hold after a stripe that
  did not land, the stored CRC and the checksum headers of a GET,
  gateway-staged GETs, and what the gateway leaves behind of a body-less
  PUT that it declines.
* ``unittest_rgw_sal_filter_rdma`` tests that a SAL filter passes the
  out-of-band parameters of a read and the pulled writes of an
  OSD-direct PUT on to the store below it, and that the ``d4n`` filter
  declines pulled writes.
* ``unittest_rgw_etag_verifier`` tests that the ETag verifier of
  multisite sync skips an object stored in one piece whose ETag has the
  multipart form, as an OSD-direct PUT stores it, and verifies every
  other ETag.
* ``unittest_rgw_ofi_sigv4`` tests the SigV4 signing of the libfabric
  test clients against the examples of the S3 API reference.
* ``unittest_rgw_rdma_rc_wire`` tests the ``hipobj-rc-v2`` wire
  encoding and, with ``WITH_RDMA_RC``, the session limits, the order of
  session teardowns and the path MTU.
* ``unittest_rgw_rdma_rc_session`` tests the RC session service on the
  host's first verbs device: limits, BUSY and CANCEL, the loopback and
  host-GID checks, and the PUT buffer. Each test skips on a host without
  one.
* ``ceph_test_rgw_ofi_get`` is an S3 client for OSD-direct delivery
  over libfabric. It registers a window, sends a signed GET with the
  token, and compares the window with a local copy of the object::

    ceph_test_rgw_ofi_get <provider> <domain|-> <node|-> <endpoint> \
      <bucket> <key> <access key> <secret> <expected file>

* ``ceph_test_rgw_ofi_put`` is an S3 client for OSD-direct PUTs over
  libfabric. It registers the object as a window that peers can read,
  sends a signed PUT with the token and no body, and reads the object
  back over HTTP. When the gateway declines with ``501``, it sends the
  body instead, as a real client would::

    ceph_test_rgw_ofi_put <provider> <domain|-> <node|-> <endpoint> \
      <bucket> <key> <access key> <secret> <file>

  These environment variables change what the two clients do:

  * ``OFI_S3_CKSUM=CRC32C``: use CRC-32C instead of CRC-64/NVME.
  * ``OFI_S3_RANGE=<first>-<last>``: the get client GETs that byte
    range.
  * ``OFI_S3_ALLOW_FALLBACK=1``: the get client accepts a response over
    HTTP, and checks its body.
  * ``OFI_S3_PUT_CRC=<base64>``: the put client sends that checksum, and
    the gateway must refuse the PUT with ``400`` and store nothing.
  * ``OFI_S3_PUT_WINDOW=write``: the put client registers the window for
    writes only, so that the OSDs cannot read it and the gateway
    declines.

* ``ceph_test_rgw_ofi_soak`` is a soak client for OSD-direct GETs over
  libfabric. Its ``put`` command writes a set of objects over HTTP. Each
  8-byte word of an object names the object and the word's offset, so
  any byte found in a window shows where it came from. Its ``run``
  command then sends GETs for a set time, whole and ranged, from several
  threads, each into a window of its memory. Each window holds a poison
  pattern. After a GET, the bytes that the GET asked for must hold the
  object's data, the rest of the token's range and a guard band around
  it must still hold the poison, and the OSDs' CRC must match. Before a
  GET, the client poisons the bytes of the window's last GET again, and
  checks the range that the GET's checks cover. It checks the whole
  window before a quarantine and at the end of the run, so a stray write
  anywhere in it is found. The window stays out of use for the
  quarantine, and a byte that changes meanwhile is a late write. For a
  GET that failed without a response, a write may land until the fence
  after the GET ended, but not after it. The fence is the lease plus the
  drain plus the late tolerance, and where a thrasher stops OSDs, the
  longest stop too. The client prints each violation, and exits with 3
  when it found any::

    ceph_test_rgw_ofi_soak put <endpoint> <bucket> <access key> <secret> \
      <manifest> [--crc] [--big]
    ceph_test_rgw_ofi_soak run <provider> <domain|-> <node|-> <endpoint> \
      <bucket> <access key> <secret> <manifest> [--duration S] \
      [--threads N] [--windows N] [--fence-ms MS] [...]

* ``qa/workunits/rgw/run-s3-oob.sh`` sends GETs and PUTs with
  ``ceph_test_rgw_ofi_get`` and ``ceph_test_rgw_ofi_put``.
  ``qa/workunits/rgw/run-s3-oob-soak.sh`` puts the soak client's objects,
  and runs it for ``SOAK_DURATION`` seconds. It starts a client that was
  killed again. ``qa/workunits/rgw/run-s3-oob-elbencho.sh`` builds
  ofi-rma and elbencho on its node, and runs elbencho with ``--s3ofi`` as
  a load generator. Several threads PUT and GET objects of three sizes
  out of band, and elbencho checks every byte and every CRC. The OSDs
  must count a pull for each PUT and a push for each GET. The three S3
  workunits take the client's provider from ``oob_client_transports``,
  and the domain from the RDMA device of the client's address.
  ``qa/workunits/rados/test_oob_read.sh`` sends reads
  and pulled writes from librados with ``ceph_test_osd_oob_read``, to
  pools on the OSDs of its own host. It always uses the ``tcp`` provider
  on 127.0.0.1, and skips when the OSDs of its host do not serve ``tcp``
  to clients. The header of each workunit shows how to run it against a
  vstart cluster.
* The teuthology suite ``rgw/s3-oob`` runs the three workunits, the soak
  for five minutes, and s3tests over HTTP, over ``tcp`` and over
  ``verbs_ofi_rxm`` on soft-RoCE, each with and without the cluster
  transport, on a replicated and an erasure-coded data pool, with each
  read policy, and with ``osd_oob_direct_read`` at random. There, every
  transfer of the get and put clients must go out of band, and no GET of
  the soak may fail. ``rgw/s3-oob-thrash`` runs
  s3tests and then the soak workunit while OSDs are thrashed, with
  delivery faults or network faults at random, and kills radosgw and the
  clients during the soak. ``rgw/s3-oob-standalone`` runs
  ``qa/standalone/osd/osd-oob-async.sh`` and ``osd-oob-copies.sh`` with
  ``CEPH_OOB_REQUIRED=1``, so that they fail rather than skip on a build
  without ``WITH_OOB_OFI``. ``rgw/s3-oob-uet-ernic`` runs
  ``run-s3-oob.sh``, for GETs or for PUTs, and the soak over the ``uet``
  provider on five rocm-ernic VMs. It runs with the transport security
  sublayer off or in cluster mode, and over RUDI or RUD.
  ``rgw/s3-oob-verbs-ernic`` runs the same workloads on the same VMs over
  the ``verbs`` provider, through the ionic RDMA driver of the emulated
  NIC, for the clients and between the OSDs. Both ernic suites need the
  ernic machine type and its provisioner, which upstream teuthology does
  not have yet. ``rgw/s3-oob-rnic`` runs the jobs of ``rgw/s3-oob`` over
  ``verbs_ofi_rxm`` on the RDMA NIC of each host. Such a NIC is a
  ConnectX RoCE port, for example, and the suite uses no soft-RoCE.
  Schedule it on a machine type whose
  nodes have an RDMA NIC on the interface of their address, such as
  mako. Each of its jobs also runs the elbencho workunit. ``rgw/s3-oob-uet-host`` runs the workloads of the ernic suites
  on five bare-metal nodes, such as mako nodes. It uses the software
  core of the ``uet`` provider, and needs no UET NIC. It runs over RUDI or RUD,
  with no fault, or with the provider's packet hold, which makes UET
  packets arrive late (``UET_PKT_HOLD_*``). Its workloads are GETs, PUTs,
  the soak and elbencho. These qa tasks support the
  suites:

  * ``rdma_rxe`` adds a soft-RoCE link on the interface of each host's
    address, checks it with ``ibv_devinfo`` and ``fi_info -p verbs``, and
    deletes it at the end. With ``hardware: true`` it uses the RDMA NIC
    of that interface, and fails a host that has none.
  * ``oob_check`` waits until the transport of every OSD is up. At the
    end, it checks that no OSD is unsafe, that ``gather_crc_mismatch``
    and ``cutoffs_failed`` are 0, that no OSD log reports a gather window
    whose data did not match, and that the cluster does not raise
    ``OOB_DELIVERY_UNSAFE``. It saves each OSD's ``ofi status`` and
    ``oob_xfer`` counters in the job's archive.
  * ``rgw_thrash`` kills radosgw and starts it again, and kills the
    clients.
  * ``thrashosds`` with ``chance_sigstop`` stops OSDs with ``SIGSTOP``
    for longer than ``osd_oob_cutoff_late_tolerance``, and ``netem``
    with ``loss``, ``duplicate``, ``reorder`` and ``corrupt`` damages the
    packets between two hosts. Both are off unless a job asks for them.
  * ``ceph`` with ``daemon_env``, and ``rgw`` with ``env``, give the
    daemons environment variables, such as ``FI_UNIVERSE_SIZE`` for
    soft-RoCE.
  * ``ernic`` checks the emulated ionic NIC of each rocm-ernic guest and
    that libfabric is 1.20 or later. It builds the ``uet`` provider over
    libuet_ernic, installs ``libuet-fi.so`` where libfabric finds it, and
    sets ``oob_network`` and the PDS environment of the daemons and the
    workunits. With ``provider: verbs`` it builds nothing, and checks that
    the ``verbs`` provider has an RDM endpoint on each NIC's RDMA device.
  * ``ernic.rgw_endpoint`` writes the gateway's URL into ``url_file`` on
    the nodes of other clients, for the workunits that run there.
  * ``uet_host`` builds the ``uet`` provider over the software core of
    the reference provider on each node, and installs it where libfabric
    finds it. It sets ``oob_network``, the PDS mode, the packet hold and
    the workunits' ``OOB_DOMAIN``. It drops the UET frames that reach the
    kernel's IP stack, so that the kernel does not answer them with ICMP
    errors. It also gives the S3 client tools ``cap_net_raw``.

  ``tox`` in ``qa`` runs the unit tests of these tasks,
  ``qa/tasks/tests/test_oob_tasks.py``, ``test_ernic.py`` and
  ``test_uet_host.py``.

Limitations
===========

* With ``osd_oob_transfer_threads`` at 0, the default, an OSD runs each
  transfer on the op worker thread. It waits for the transfer to end
  before the next operation on that thread. A cuObject transfer always
  runs that way, unless ``osd_oob_cuobj_transfer_threads`` is set. So
  does the pull of a request that runs in a coroutine. See `Transfer
  threads`_.
* While a pulled write waits for its pulls on a transfer thread, every
  client op on its object waits behind it, reads included. See `Pulled
  writes on transfer threads`_.
* An erasure-coded primary completes the client reads of a PG in the
  order that it started them. While a shard's push is slow, later
  client reads of that PG wait for it, also with transfer threads.
* The cuObject executor keeps one channel per thread that runs its
  transfers. After a transfer on it times out, the thread delivers its
  reads inline until the transfer's writes complete or the DC transport
  gives up on them, for up to the DC retry budget.
* Unless ``osd_oob_direct_read`` is on, an OSD copies each read into a
  staging buffer. It does not register the read's own buffers.
* An OSD runs one libfabric endpoint, so ``oob_cluster_transport`` and
  ``oob_client_transports`` can name one libfabric provider between
  them. See `Choosing transports`_.
* Every call into one libfabric endpoint is serialized, except the
  adding and removing of peers on a provider that offers
  ``FI_THREAD_SAFE``.
* Every libfabric endpoint of one provider must use the same address
  family. Mixed IPv4 and IPv6 endpoints are not tested.
* A split read, erasure-coded shard-direct or replicated balanced,
  delivers a sparse read inline.
* Only cuObject clients can PUT with ``x-amz-rdma-token`` in
  gateway-staged mode. The stored object is the descriptor's window,
  also on a PUT that sends a body. The gateway first checks the window's
  size against ``rgw_max_put_size`` and the quotas. It does so even when
  the frontend thread has no cuObject channel, and reads the body over
  HTTP instead. The object must fit one buffer of the gateway's arena,
  64 MiB, and a PUT that finds no room fails with ``503``. A read from
  the client that returns less than the window fails the PUT with
  ``500``, and nothing is stored.
* Libfabric clients have no gateway-staged mode, so a libfabric GET
  that cannot use passthrough gets the HTTP body. A PUT with a body and
  a libfabric token is read over HTTP, and the response has no
  ``x-amz-rdma-reply`` header. A gateway that does not run its cuObject
  server reads a PUT with a body and any token the same way. A gateway
  that runs its cuObject server takes any other token on a PUT with a
  body for a cuObject descriptor, on a frontend thread that has a
  cuObject channel. If that token names no window, or an empty one, the
  PUT fails with ``400``. On a thread without a channel, the body is
  stored as over HTTP.
* The systemd unit and cephadm start the gateway with
  ``--setuser ceph``. The gateway starts its cuObject server after its
  frontends drop privileges, so the threads that the library starts can
  use its ``rdma_cm`` channel. The frontend threads start before the
  drop, and the drop gives each of them credentials of its own. The
  kernel refuses an ``rdma_cm`` call through a file that other
  credentials opened. So such a call that the library makes on a
  frontend thread during a transfer fails with ``EACCES``, but this is
  not tested. Like the OSD's cuObject server, it runs as the ``ceph``
  user, and cannot open device nodes that only root can open, such as
  ``umad``.
* An OSD-direct PUT does not serve appends, multipart parts, compressed
  or encrypted objects. The cuObject pull is compiled against a header
  stub only and is not tested on hardware.
* The gateway does not serve encrypted objects, or objects with a DLO
  or SLO manifest, to RC clients. PREPARE answers ``501`` with the
  unsupported marker, and the client reads the object over HTTP.
* An RC GET can name a ``versionId``. Multipart part uploads and other
  subresources are not available over RC.
* An RC PUT must arrive as one write-with-immediate that carries the
  whole object.
* An RC transfer runs on the request thread.
* Each RC session registers its buffer when PREPARE runs, and removes
  the registration when the session ends. A PUT also zeroes its buffer.
  On an E810 adapter, this costs about 2.5 ms for a 64 MiB PUT session.
  Zeroing takes 1.6 ms, registering 0.74 ms and deregistering 0.21 ms.
  At 4 MiB it costs about 0.13 ms. Soft-RoCE (``rxe``) is similar.
  PREPARE registers its buffer, and zeroes a PUT's, on one of two
  worker threads, and so does the teardown of ended sessions when the
  arena has no room. The request yields meanwhile, and the frontend
  thread serves other requests. On the synchronous beast frontend, this
  work runs on the frontend thread.
* The cuObject executor's gather windows, and cuObject transfers on the
  transfer threads, are not tested on hardware.
* A token can name any endpoint that the OSDs can reach. The gateway
  and the OSDs do not check it against a list of allowed networks. Only
  ``rgw_rdma_token_allow_anonymous`` keeps anonymous requests from using
  tokens. See `Anonymous requests`_.
