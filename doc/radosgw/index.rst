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

Configuration
=============

.. toctree::
   :maxdepth: 1

   Cephadm RGW deployment <cephadm-deploy-rgw>
   Config Reference <config-ref>
   HTTP Frontends <frontends>
   Configuring Pools <pools>
   Zone Features <zone-features>
   Multisite Configuration <multisite>
   Compression <compression>


Administration
==============

.. toctree::
   :maxdepth: 1

   Admin Guide <admin>
   User Accounts <account>
   Multi-tenancy <multitenancy>
   Metrics <metrics>
   Lua Scripting <lua-scripting>
   Troubleshooting <troubleshooting>
   Orphan List and Associated Tooling <orphans>
   Manpage radosgw <../../man/8/radosgw>
   Manpage radosgw-admin <../../man/8/radosgw-admin>


Authentication
==============

.. toctree::
   :maxdepth: 1

   OpenStack Keystone Integration <keystone>
   LDAP Authentication <ldap-auth>
   Multi-factor Authentication <mfa>
   Open Policy Agent Integration <opa>


APIs
====

.. toctree::
   :maxdepth: 1

   S3 API <s3>
   Swift API <swift>
   IAM API <iam>
   STS <STS>
   STS Lite <STSLite>
   Admin Ops API <adminops>
   Python Binding <api>


Other
=====

.. toctree::
   :maxdepth: 1

   Dynamic Bucket Index Resharding <dynamicresharding>
   Sync Modules <sync-modules>
   Data Layout in RADOS <layout>
   Data Caching and CDN <rgw-cache>
   D3N Data Cache <d3n_datacache>
   Export over NFS <nfs>
   Full Object Deduplication <s3_objects_dedup>
