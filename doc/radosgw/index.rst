.. _object-gateway:

=====================
 Ceph Object Gateway
=====================

:term:`Ceph Object Gateway` is an object storage interface built on top of
``librados``. It provides a RESTful gateway between applications and Ceph
Storage Clusters. :term:`Ceph Object Storage` supports two interfaces:

#. **S3-compatible:** Provides object storage functionality with an interface
   that is compatible with a large subset of the Amazon S3 RESTful API.

#. **Swift-compatible:** Provides object storage functionality with an interface
   that is compatible with a large subset of the OpenStack Swift API.

Ceph Object Storage uses the Ceph Object Gateway daemon (``radosgw``), an HTTP
server designed to interact with a Ceph Storage Cluster. The Ceph Object
Gateway provides interfaces that are compatible with both Amazon S3 and
OpenStack Swift, and it has its own user management. Ceph Object Gateway can
use a single Ceph Storage cluster to store data from Ceph File System and from
Ceph Block device clients. The S3 API and the Swift API share a common
namespace, which means that it is possible to write data to a Ceph Storage
Cluster with one API and then retrieve that data with the other API.

.. ditaa::

            +------------------------+ +------------------------+
            |   S3 compatible API    | |  Swift compatible API  |
            +------------------------+-+------------------------+
            |                      radosgw                      |
            +---------------------------------------------------+
            |                      librados                     |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+

.. note:: Ceph Object Storage does **NOT** use the Ceph Metadata Server.


.. toctree::
   :maxdepth: 1

   HTTP Frontends <frontends>
   Multisite Configuration <multisite>
   Pool Placement and Storage Classes <placement>
   Multisite Sync Policy Configuration <multisite-sync-policy>
   Configuring Pools <pools>
   Config Reference <config-ref>
   Admin Guide <admin>
   S3 API <s3>
   Data caching and CDN <rgw-cache.rst>
   Swift API <swift>
   Admin Ops API <adminops>
   Python binding <api>
   Export over NFS <nfs>
   OpenStack Keystone Integration <keystone>
   OpenStack Barbican Integration <barbican>
   HashiCorp Vault Integration <vault>
   KMIP Integration <kmip>
   Open Policy Agent Integration <opa>
   Multi-tenancy <multitenancy>
   Compression <compression>
   LDAP Authentication <ldap-auth>
   Server-Side Encryption <encryption>
   Bucket Policy <bucketpolicy>
   Dynamic bucket index resharding <dynamicresharding>
   Multi factor authentication <mfa>
   Sync Modules <sync-modules>
   Bucket Notifications <notifications>
   Data Layout in RADOS <layout>
   STS <STS>
   STS Lite <STSLite>
   Keycloak <keycloak>
   Session Tags <session-tags>
   Role <role>
   Orphan List and Associated Tooling <orphans>
   OpenID Connect Provider <oidc>
   troubleshooting
   Manpage radosgw <../../man/8/radosgw>
   Manpage radosgw-admin <../../man/8/radosgw-admin>
   QAT Acceleration for Encryption and Compression <qat-accel>
   S3-select <s3select>
   Lua Scripting <lua-scripting>
   D3N Data Cache <d3n_datacache>
   Cloud Transition <cloud-transition>
   Metrics <metrics>

