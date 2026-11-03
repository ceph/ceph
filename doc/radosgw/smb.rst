SMB
===

Ceph Object Gateway namespaces can be shared via SMB,
alongside the traditional HTTP access protocols (S3 and Swift).

In particular, the Ceph Object Gateway can now be configured to
provide file-based access when embedded in the SMB server.

The simplest and preferred way of managing SMB clusters and RGW exports
is using ``ceph smb ...`` commands. See :doc:`/mgr/smb` for more details.

librgw
======

The ``librgw`` library provides a loadable interface to
Ceph Object Gateway services, and instantiates a full Ceph Object Gateway
instance on initialization.

Supported Operations
====================

The RGW SMB interface supports most operations on files and
directories, with the following restrictions:

- Links, including symlinks, are not supported.
- Do not support any ACLS other than standard unix user/group ownerships/permissions.

  + Unix user and group ownership and permissions *are* supported.

- Directories may not be moved/renamed.

  + Files may be moved between directories.

- Only full, sequential *write* I/O is supported

  + i.e., write operations are constrained to be **uploads**.
  + Many typical I/O operations such as editing files in place will necessarily fail as they perform non-sequential stores.
  + Some file utilities *apparently* writing sequentially (e.g., some versions of GNU tar) may fail due to infrequent non-sequential stores.
  + When mounting via SMB, sequential application I/O can generally be constrained to be written sequentially to the SMB server via a synchronous mount option (e.g. -osync in Linux).
  + SMB clients which cannot mount synchronously (e.g., MS Windows) will not be able to upload files.

RGW-SMB Frontend
================

The ``rgw-smb`` frontend provides direct SMB protocol access to RGW buckets,
similar to how ``rgw-nfs`` provides NFS access. This runs as a library instance
serving the SMB protocol, distinct from the standard HTTP/S3 daemon instances.

.. note::
   The ``rgw-smb`` frontend is different from the SMB manager module. The SMB
   manager module provides SMB access to CephFS volumes via Samba containers,
   while the ``rgw-smb`` frontend provides direct SMB protocol access to RGW
   object storage. See :doc:`/mgr/smb` for information about the SMB manager module.

Configuration
-------------

The ``rgw-smb`` frontend is configured as a library instance serving the SMB protocol,
rather than using the :confval:`rgw_frontends` configuration option used by HTTP/S3 daemon instances.

.. important::
   **HTTP and SMB/NFS Protocols Can Coexist**

   When RGW is started as a library instance (``rgw-smb`` or ``rgw-nfs``),
   it will **not** start an HTTP listener by default. However, HTTP/S3 and SMB/NFS
   protocols can run together in the same instance. You can enable HTTP/S3
   access alongside SMB/NFS by configuring additional HTTP frontends using the
   protocol-specific :confval:`rgw_smb_frontends` or :confval:`rgw_nfs_frontends`
   configuration option.

Instance and Protocol Configuration:

- **Instance type**: Library (runs as ``librgw`` for SMB protocol)
- **Protocol type**: SMB
- **Config prefix**: ``rgw_smb_`` (underscore format for configuration keys)
- **Service name**: ``rgw-smb`` (hyphen format for service registration)

Options
-------

``rgw_smb_frontends``

:Description: Additional frontends to enable alongside the SMB protocol.
              Syntax is identical to :confval:`rgw_frontends`. This allows running
              both SMB and HTTP/S3 protocols from the same RGW instance.

:Type: String
:Default: rgw-smb

Running Multiple Frontends
---------------------------

The ``rgw-smb`` instance can serve multiple protocols simultaneously. This is
useful when you need to provide both SMB access and HTTP-based S3/Swift API
access from the same RGW instance.

**Example Configuration:**

.. code-block:: ini

   [client.rgw.smb-gateway]
   # Enable SMB frontend (default for rgw-smb daemon type)
   # Also enable HTTP frontend for S3/Swift API access
   rgw_smb_frontends = beast endpoint=0.0.0.0:8080 ssl_endpoint=0.0.0.0:8443 ssl_certificate=/path/to/cert.pem

In this configuration:

- The SMB protocol is enabled by default (instance type is library serving SMB)
- An HTTP/S3 frontend (Beast) is added to provide S3/Swift API access on port 8080 (HTTP) and 8443 (HTTPS)
- Both protocols share the same RGW backend and can access the same buckets and objects

.. note::
   When running multiple frontends, ensure that:

   - Port numbers do not conflict
   - Network security policies allow access to all configured ports
   - SSL certificates are properly configured for HTTPS endpoints
   - Authentication mechanisms are appropriate for each protocol

See :doc:`/mgr/smb` for detailed configuration and deployment information.

