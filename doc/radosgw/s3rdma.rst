.. _radosgw-s3rdma:
.. _radosgw-cuobject:

==========================
 S3 RDMA with cuObject
==========================

.. contents::
   :local:

Ceph Object Gateway can use NVIDIA cuObject to transfer S3 object data between
client memory and the RGW daemon over RDMA. S3 requests, authentication,
metadata, and responses still use the HTTP frontend. Source and RPM builds
include cuObject support by default. RDMA transfers are disabled at runtime
until :confval:`rgw_cuobj_enabled` is set to ``true``.

.. warning::

   S3 RDMA with cuObject is an **experimental** feature intended for testing
   and evaluation. Configuration options and behavior may change between
   releases. Runtime activation requires setting :confval:`rgw_cuobj_enabled`
   to ``true`` and configuring an RDMA interface address. Source and RPM builds
   include the support by default.

See `NVIDIA cuObject documentation`_ for the library architecture and
`cuObject client API`_ for client integration.

Architecture
============

S3 RDMA separates HTTP request handling from object-data transfer:

.. code-block:: text

   S3 client                         RGW daemon
   ---------                         ----------
   S3 API            <-- HTTP -->    Frontend and request processing
   Registered memory <-- RDMA -->    Registered host buffer pool
                                                  |
                                         RGW filters and storage
                                                  |
                                             Ceph cluster

For PUT requests, RGW reads the payload from the client's registered memory
into a registered host buffer, then passes it through the normal upload
filters and storage writer. For GET requests, RGW collects the response after
the download filters have run, writes it to the client's registered memory,
and then sends the HTTP success response. The RGW daemon uses host memory for
these buffers; it does not require a GPU.

The RDMA network connects the client to the RGW daemon. RGW continues to use
its normal storage path to access the Ceph cluster. Enabling S3 RDMA does not
change the network protocol between RGW and the OSDs.

Implementation
==============

* The S3 PUT and GET paths use cuObject for object data when the server is
  available and the request contains an RDMA token.
* Multipart upload parts and ranged GET responses can use RDMA independently,
  subject to the per-transfer buffer limit.
* Authentication, authorization, metadata, and request status use the S3 HTTP
  interface. Existing upload and download filters still process object data.
* HEAD requests return metadata over HTTP and do not transfer object data.
  Requests without an RDMA token use the ordinary HTTP data path.

Requirements
------------

The RGW host and client must have a working RDMA network with Dynamic
Connection (DC) support. RGW uses the ``CUOBJ_PROTO_RDMA_DC_V1`` protocol over
InfiniBand or RoCEv2. Use NVIDIA Mellanox ConnectX-5 or newer adapters with
drivers and firmware supported by cuObject. ConnectX-4 is not supported.
The RGW host must be able to reach the client memory advertised in the RDMA
token, including when HTTP requests arrive through a proxy or load balancer.

Install the following components in addition to the normal Ceph dependencies:

* On the RGW host, the cuObject server runtime library ``libcuobjserver``
  (the development headers are not needed). The integration requires
  cuObject server 2.0.0 or later. Use the `cuObject server downloads`_ and
  `cuObject server release notes`_ for installation instructions and supported
  platforms. By default, RGW binds the library's entry points at runtime, so
  a release with an incompatible ABI is rejected or may need an RGW update.
* RDMA drivers, libraries, and development headers, including ``libibverbs``
  and ``librdmacm``, on the RGW host. The client also needs the RDMA runtime.
* On the client, an S3 application integrated with the cuObject client library,
  together with its CUDA Toolkit and GPUDirect Storage dependencies. See the
  `cuObject client release notes`_ for the supported combinations. An ordinary
  S3 client continues to use HTTP for object data.

Example installation flow
~~~~~~~~~~~~~~~~~~~~~~~~~

The following example installs the CUDA, GPUDirect Storage, cuObject, and
RDMA packages on a Rocky Linux 10 x86_64 host from the NVIDIA network
repository. Use the `CUDA Toolkit downloads`_ page to select the repository for
another distribution or architecture, and adjust the ``13-4`` package version
suffix to the CUDA release you deploy. The cuObject client packages are needed
only on hosts that run the S3 client application, and the cuObject server
packages are needed only on the RGW host.

.. prompt:: bash $

   sudo dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel10/x86_64/cuda-rhel10.repo
   sudo dnf clean all
   sudo dnf makecache
   sudo dnf install cuda-13-4 cuda-toolkit-13-4
   sudo dnf install nvidia-gds-13-4
   sudo dnf install libcuobjclient-13-4 libcuobjclient-devel-13-4
   sudo dnf install cuobjserver libcuobjserver
   sudo dnf install -y rdma-core rdma-core-devel libibverbs-utils infiniband-diags perftest iproute

Note
~~~~

Allow the RGW process to register enough host memory for its buffer pool.
Account for the process's locked-memory limit and, for container deployments,
access to the RDMA devices and the required libraries inside the container.
The default pool reserves **1 GiB per RGW daemon**, plus library overhead.

Limitations
-----------

* Each PUT payload or GET response must fit in one registered server buffer.
  The default limit is 8 MiB, including each part of a multipart upload or each
  selected GET range. See :ref:`radosgw-cuobj-buffer-limits`.
* A zero-length RDMA PUT is rejected. Create empty objects with ordinary HTTP
  PUT requests. Empty-object GETs can succeed without an RDMA data transfer.
* A client must keep its memory registration valid until the request has
  completed and follow the cuObject library's error-handling requirements.
* RGW reports transfer failures over HTTP. After choosing RDMA for a request,
  it does not automatically retry that transfer through the HTTP body.

S3 RDMA Environment Setup
=========================

Building
--------

Follow :ref:`build-ceph` to prepare a Ceph source build. The CMake options
``WITH_RADOSGW_CUOBJ`` and ``WITH_RADOSGW_CUOBJ_SHIM`` are enabled by default.
With ``WITH_RADOSGW_CUOBJ_SHIM=ON``, the NVIDIA cuObject SDK (headers and link
library) is **not** needed to build RGW. The ABI shim loads the server library
with ``dlopen()`` when :confval:`rgw_cuobj_enabled` is set, so the same binaries
run with or without it. If the library cannot be loaded (see
:confval:`rgw_cuobj_library`) or initialization fails, RGW logs the reason and
keeps serving ordinary HTTP requests; any S3 request that carries an RDMA
token then fails with HTTP ``500``. For a new build directory, run from the
Ceph source directory:

.. prompt:: bash $

   ./do_cmake.sh
   cmake --build build --target radosgw --parallel

If an existing build directory has ``WITH_RADOSGW_CUOBJ`` cached as ``OFF``,
enable it explicitly:

.. prompt:: bash $

   cmake -S . -B build -DWITH_RADOSGW_CUOBJ=ON
   cmake --build build --target radosgw --parallel

To omit cuObject support from a source build, configure with
``-DWITH_RADOSGW_CUOBJ=OFF``.

To link directly to cuObject instead of using the ABI shim, install the
cuObject server development package (``libcuobjserver-devel`` on EL) and
configure with ``-DWITH_RADOSGW_CUOBJ_SHIM=0``:

.. prompt:: bash $

   cmake -S . -B build -DWITH_RADOSGW_CUOBJ=ON -DWITH_RADOSGW_CUOBJ_SHIM=0
   cmake --build build --target radosgw --parallel

This mode compiles against the SDK headers and links ``libcuobjserver``
directly. The library and its runtime dependencies must be installed for
RGW to start, even when RDMA is disabled. :confval:`rgw_cuobj_library` is
ignored. Runtime activation still requires :confval:`rgw_cuobj_enabled`.
Use ``-DWITH_RADOSGW_CUOBJ_SHIM=1`` to return to the default shim mode. CMake
also accepts ``OFF`` and ``ON`` in place of ``0`` and ``1``.
``WITH_RADOSGW_CUOBJ_SHIM`` applies only when RGW and ``WITH_RADOSGW_CUOBJ``
are enabled.

RPM builds also include cuObject support by default. The spec sets
``WITH_RADOSGW_CUOBJ=ON`` and uses the default ABI shim, adding no extra build
requirement. To omit support, use
``rpmbuild --rebuild --without cuobj ceph-<version>.src.rpm``, or pass
``--rpmbuild-arg=--without=cuobj`` to ``build-with-container.py``.

For a container RPM build that links directly to cuObject, use
``--without-rgw-cuobj-shim``:

.. prompt:: bash $

   ./src/script/build-with-container.py -d rocky10 --without-rgw-cuobj-shim -e source-rpm -e rpm

This option also enables cuObject support, adds NVIDIA's matching RHEL
repository inside the build image, and installs ``libcuobjserver-devel`` and
its dependencies. It sets ``WITH_RADOSGW_CUOBJ_SHIM=OFF`` for source builds
and passes ``--without=cuobj_shim`` to RPM builds. Use ``-e build`` for a
source build.

Automatic SDK installation supports Rocky Linux, AlmaLinux, RHEL, and CentOS
Stream 9 and 10 on x86_64 and aarch64. Use ``-d rocky9`` for EL9, or override
the base image as described in ``ContainerBuild.md`` for AlmaLinux and RHEL.
Fedora is not supported for automatic SDK installation because NVIDIA's
Fedora repository lacks ``libcuobjserver-devel``; this option reports an
error there. Fedora can still use the default shim build.

For a direct-link RPM build outside a container on a supported distribution,
install the SDK dependencies and disable the spec's ``cuobj_shim`` conditional:

.. prompt:: bash $

   env WITH_RADOSGW_CUOBJ_SHIM=OFF ./install-deps.sh
   rpmbuild --rebuild --without cuobj_shim ceph-<version>.src.rpm

Without this opt-in, ``install-deps.sh`` does not add NVIDIA repositories or
install cuObject packages.

The explicit ``source-rpm`` step ensures the SRPM contains the updated spec.
See ``ContainerBuild.md`` for build image options. ``CONFIGURE_ARGS`` is used
for source builds and does not control RPM builds.

The cuObject server library (``libcuobjserver.so.2`` by default) and its
runtime dependencies must be available to the deployed RGW daemon for RDMA.
With ``WITH_RADOSGW_CUOBJ_SHIM=ON``, the library is bound by symbol name at
runtime and supports cuObject server release 2.x; a library missing any
required symbol is rejected at startup. The ``WITH_RDMA`` option controls
Ceph's async messenger; enabling it alone does not enable cuObject support
in RGW.

Running
-------

Set :confval:`rgw_cuobj_enabled` and :confval:`rgw_cuobj_rdma_ip` for each RGW
daemon that will serve RDMA requests. Use an address assigned to that daemon's
RDMA-capable interface. The address has no default. For example, for a daemon
whose Ceph client name is ``client.rgw.8000``:

.. prompt:: bash #

   ceph config set client.rgw.8000 rgw_cuobj_rdma_ip 192.0.2.10
   ceph config set client.rgw.8000 rgw_cuobj_rdma_port 20886
   ceph config set client.rgw.8000 rgw_cuobj_enabled true

Replace the example client name and IP address with those for your deployment.
The cuObject port is separate from the HTTP frontend port. Configure an
appropriate RDMA address and port for each daemon when running multiple RGWs
on one host. Keep the normal :confval:`rgw_frontends` configuration for S3
requests.

Restart the RGW daemon after changing cuObject settings. RGW creates the
cuObject server and registers its buffer pool during startup. These settings
are not applied to an already initialized server.

If initialization fails, including when the cuObject library is not installed
in shim mode, RGW logs ``cuObj RDMA server init failed`` and continues serving
HTTP requests with RDMA disabled. Requests that carry an RDMA token fail with
HTTP ``500``.
Check the library installation, RDMA address, device access, and available
registered memory before restarting.

.. _radosgw-cuobj-buffer-limits:

Memory Sizing and Transfer Limits
---------------------------------

The pool is shared by PUT and GET requests. Each transfer holds one buffer:

* :confval:`rgw_cuobj_buffer_size` limits a single PUT payload, an individual
  multipart-upload part, or a GET response. The default is **8 MiB**. For a
  ranged GET, only the selected range needs to fit.
* :confval:`rgw_cuobj_buffer_count` limits the number of transfers that can hold
  buffers simultaneously. The default is **128**. Increasing the count does
  not increase the size limit.

The registered pool size is ``rgw_cuobj_buffer_size * rgw_cuobj_buffer_count``
per daemon, in addition to other RGW memory and cuObject resources. For example,
64 buffers of 16 MiB still require 1 GiB of registered memory, while allowing
larger transfers with fewer simultaneous buffer users:

.. prompt:: bash #

   ceph config set client.rgw.8000 rgw_cuobj_buffer_size 16M
   ceph config set client.rgw.8000 rgw_cuobj_buffer_count 64

Restart the daemon for the new pool to take effect. Configure positive buffer
sizes and counts within the memory-registration limits of the installed SDK
and devices.

When all suitable buffers are busy, or a transfer exceeds the buffer size,
RGW returns HTTP ``503 ServiceUnavailable``. There is no automatic HTTP retry
after an RDMA transfer has been selected. Reduce concurrency for pool
exhaustion. For larger objects, use smaller multipart-upload parts, ranged
GETs, a larger configured buffer, or an ordinary HTTP transfer. Other S3
request-size and multipart constraints still apply. Retrying an oversized
request without changing its size or the buffer configuration will continue
to fail.

Client Setup and Verification
-----------------------------

Configure a cuObject-aware S3 client with the RGW HTTP endpoint, S3 credentials,
and the client's RDMA interface. The HTTP endpoint and RDMA interface can use
different networks, but the client memory advertised by the token must be
reachable from RGW. Follow the client's instructions for installing and
configuring its cuObject and GPUDirect Storage dependencies.

Check the configured server settings, using the same daemon name as in the
setup commands:

.. prompt:: bash #

   ceph config get client.rgw.8000 rgw_cuobj_enabled
   ceph config get client.rgw.8000 rgw_cuobj_rdma_ip
   ceph config get client.rgw.8000 rgw_cuobj_rdma_port

After restarting the daemon, check its startup log for a successful pool
initialization. With the default settings, the message includes
``initialized with 128 RDMA buffers of 8388608 bytes``. Configuration values
alone do not prove that RDMA initialization succeeded.

Use the client's RDMA mode to verify both transfer directions:

#. Upload a small object, such as 4 KiB, to a test bucket. Confirm a successful
   HTTP response, ``x-amz-rdma-reply: 200``, and
   ``x-amz-rdma-bytes-transferred: 4096``.
#. Download the object into a registered client buffer. Check the same RDMA
   headers and compare the downloaded data with the original. The successful
   HTTP response has no object-data body.
#. Download a range that fits in the configured buffer. Check HTTP
   ``206 Partial Content``, ``x-amz-rdma-reply: 206``, ``Content-Range``, and a
   transferred-byte count equal to the selected range size. Compare the data
   at the beginning of the registered destination buffer with the selected
   object bytes.
#. Issue a HEAD request. Verify that it returns the object's content length
   without an RDMA transfer or RDMA success headers.
#. Read the uploaded object with an ordinary S3 client and compare its data.
   This also verifies interoperability with the normal HTTP data path.

A client that cannot expose these headers should provide equivalent transfer
status through its cuObject integration. Do not use a hand-written token for
these checks: the token must identify memory registered by the client library.

Request and Response Protocol
=============================

A cuObject-aware client registers its source or destination memory and sends
the descriptor in the ``x-amz-rdma-token`` request header. Generate descriptors
with the cuObject client API. They refer to live memory registrations and
transport resources, so they cannot be replaced by an arbitrary string or
reused after the registration has ended. S3 authentication and permissions
still apply to the HTTP request.

The DC v1 descriptor has the following colon-separated layout::

   addr:size:rkey:reserved:qp_num:lid:gid

The first field is the client memory address and the second is its size in
bytes, both encoded as hexadecimal digits without a ``0x`` prefix, sign, or
whitespace. For example, a size field of ``800000`` represents 8 MiB. The
remaining fields describe the memory key and RDMA endpoint; pass the complete
SDK-generated descriptor unchanged.

PUT and GET use the descriptor as follows:

* **PUT or multipart upload part:** The size field specifies the entire
  payload for this request. The client sends an empty HTTP body, with
  ``Content-Length: 0``, and keeps the payload in registered memory for RGW to
  read. A zero descriptor size is rejected; use an ordinary HTTP PUT to create
  an empty object.
* **GET:** The registered destination must have room for the complete object
  or selected range. RGW determines the transfer length from the S3 response,
  and writes the response starting at the descriptor's address. An object
  range offset does not become an offset into the client buffer.
* **HEAD:** RGW returns ordinary HTTP metadata, including the object's content
  length, without transferring data over RDMA or returning RDMA success
  headers.

On successful RDMA PUT or GET, RGW returns these headers:

.. list-table:: RDMA response headers
   :header-rows: 1
   :widths: 45 55

   * - Header
     - Meaning
   * - ``x-amz-rdma-reply``
     - The RDMA transfer succeeded: ``200`` for PUTs and ordinary GETs,
       or ``206`` for ranged GETs whose HTTP status is
       ``206 Partial Content``.
   * - ``x-amz-rdma-bytes-transferred``
     - The number of object-data bytes transferred, expressed in decimal.

A successful RDMA GET has an empty HTTP body and ``Content-Length: 0``. Its
HTTP status and, for a range, ``Content-Range`` still describe the S3 response.
An empty-object GET reports zero transferred bytes and needs no RDMA buffer.
RGW sends success only after the RDMA operation and normal request processing
have completed. Keep the client buffer registered and available throughout
the request, and use the client library's completion and error handling before
reusing or releasing it.

Malformed address fields, and malformed or zero PUT size fields, return HTTP
``400 InvalidArgument`` when checked by RGW. Short transfers return HTTP
``500``. Other SDK, storage, or filter failures also return an error response.
Error responses omit the RDMA success headers; a failed GET may have written
part of the destination buffer, which must not be treated as a successful
object read.

Requests without the token use ordinary HTTP transfers. A GET or PUT that
carries an RDMA token while RDMA is disabled, the cuObject library is not
installed, or initialization failed returns HTTP ``500`` rather than silently
using the HTTP body. Clients that retry using HTTP must remove the token and,
for PUT, send the object data in the HTTP body.

Logs
====

RGW's cuObject messages use the ``rgw_cuobj:`` prefix and the ``debug_rgw``
logging subsystem. Detailed descriptor and buffer messages use level 21.
:confval:`rgw_cuobj_log_level` separately controls cuObject library messages on
standard error, with values ``off``, ``error``, ``info``, and ``debug``. Its
default is ``error``.

For temporary detailed RGW logging on the example daemon:

.. prompt:: bash #

   ceph config set client.rgw.8000 debug_rgw 21/21

Restore the previous logging level after collecting the required output.
Changes to :confval:`rgw_cuobj_log_level` require a daemon restart.

Troubleshooting
===============

.. list-table:: Common S3 RDMA failures
   :header-rows: 1
   :widths: 35 65

   * - Symptom
     - Checks
   * - ``cuObj RDMA server init failed`` at startup
     - Check that ``libcuobjserver`` can be loaded (:confval:`rgw_cuobj_library`),
       the configured RDMA IP, access to RDMA devices, library versions,
       available memory, and memory-registration limits. RGW continues with
       RDMA disabled after this failure, and S3 requests carrying an RDMA
       token return HTTP ``500``.
   * - No ``x-amz-rdma-reply`` header
     - Check that RGW was built with cuObject support, that runtime enablement
       and startup succeeded, and that the client sent a token. HEAD requests
       and error responses also omit this header.
   * - HTTP ``400 InvalidArgument``
     - Check the SDK-generated descriptor, especially the hexadecimal address
       and the nonzero size for a PUT. Create empty objects through HTTP.
   * - HTTP ``503 ServiceUnavailable``
     - Check the transfer size against :confval:`rgw_cuobj_buffer_size`. If it
       fits, reduce concurrent requests or provision more buffers within the
       available memory budget. Increasing the buffer count does not allow
       larger individual transfers.
   * - HTTP ``500`` or a short transfer
     - Check the RGW and cuObject logs, RDMA connectivity, and the lifetime and
       size of the client memory registration. Storage or filter failures can
       also cause an error. Treat a partly filled GET buffer as a failed read.

.. _radosgw-cuobj-config-ref:

Config Reference
================

The following settings can be applied through ``ceph config set`` as shown
above, or in ``ceph.conf`` under the ``[client.rgw.{instance-name}]`` section.
All eight settings take effect when the RGW daemon starts.

:confval:`rgw_cuobj_num_dcis` configures the library's Dynamic Connection
Interfaces at startup; its default is 128. It is separate from the RGW buffer
count.

.. confval:: rgw_cuobj_enabled
.. confval:: rgw_cuobj_library
.. confval:: rgw_cuobj_rdma_ip
.. confval:: rgw_cuobj_rdma_port
.. confval:: rgw_cuobj_buffer_size
.. confval:: rgw_cuobj_buffer_count
.. confval:: rgw_cuobj_num_dcis
.. confval:: rgw_cuobj_log_level

.. _NVIDIA cuObject documentation: https://docs.nvidia.com/gpudirect-storage/cuobject/
.. _CUDA Toolkit downloads: https://developer.nvidia.com/cuda-downloads
.. _cuObject server downloads: https://developer.nvidia.com/cuobjserver-downloads
.. _cuObject server release notes: https://docs.nvidia.com/gpudirect-storage/cuobject/cuobject-server-release-notes/index.html
.. _cuObject client release notes: https://docs.nvidia.com/gpudirect-storage/cuobject/cuobject-client-release-notes/index.html
.. _cuObject client API: https://docs.nvidia.com/gpudirect-storage/cuobject/cuObjClient-api/index.html
