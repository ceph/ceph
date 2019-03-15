.. _object-gateway:

=====================
 Ceph Object Gateway
=====================

:term:`Ceph Object Gateway` is an object storage interface built on top of
``librados`` to provide applications with a RESTful gateway to
Ceph Storage Clusters. :term:`Ceph Object Storage` supports two interfaces:

#. **S3-compatible:** Provides object storage functionality with an interface
   that is compatible with a large subset of the Amazon S3 RESTful API.

#. **Swift-compatible:** Provides object storage functionality with an interface
   that is compatible with a large subset of the OpenStack Swift API.

Ceph Object Storage uses the Ceph Object Gateway daemon (``radosgw``), which is
an HTTP server for interacting with a Ceph Storage Cluster. Since it
provides interfaces compatible with OpenStack Swift and Amazon S3, the Ceph
Object Gateway has its own user management. Ceph Object Gateway can store data
in the same Ceph Storage Cluster used to store data from Ceph Filesystem clients
or Ceph Block Device clients. The S3 and Swift APIs share a common namespace, so
you may write data with one API and retrieve it with the other.

.. ditaa::  +------------------------+ +------------------------+
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

   Manual Install w/Civetweb <../../install/install-ceph-gateway>
   HTTP Frontends <frontends>
   Pool Placement and Storage Classes <placement>
   Multisite Configuration <multisite>
   Configuring Pools <pools>
   Config Reference <config-ref>
   Admin Guide <admin>
   S3 API <s3>
   Swift API <swift>
   Admin Ops API <adminops>
   Python binding <api>
   Export over NFS <nfs>
   OpenStack Keystone Integration <keystone>
   OpenStack Barbican Integration <barbican>
   Open Policy Agent Integration <opa>
   Multi-tenancy <multitenancy>
   Compression <compression>
   LDAP Authentication <ldap-auth>
   Server-Side Encryption <encryption>
   Bucket Policy <bucketpolicy>
   Dynamic bucket index resharding <dynamicresharding>
   Multi factor authentication <mfa>
   Sync Modules <sync-modules>
   Data Layout in RADOS <layout>
   STS Lite <STSLite>
   Role <role>
   troubleshooting
   Manpage radosgw <../../man/8/radosgw>
   Manpage radosgw-admin <../../man/8/radosgw-admin>

