=====================
 Ceph Object Storage
=====================

:term:`Ceph Object Storage` is an object storage interface built on top of
``librgw`` and ``librados`` to provide applications with a RESTful gateway to
Ceph Storage Clusters. Ceph Object Storage supports two interfaces:

#. **S3-compatible:** Provides object storage functionality with an interface 
   that is compatible with a large subset of the Amazon S3 RESTful API.

#. **Swift-compatible:** Provides object storage functionality with an interface
   that is compatible with a large subset of the OpenStack Swift API.
   
Ceph Object Storage uses the RADOS Gateway daemon (``radosgw``), which is a
FastCGI module for interacting with ``librgw`` and ``librados``. Since it
provides interfaces compatible with OpenStack Swift and Amazon S3, RADOS Gateway
has its own user management. RADOS Gateway can store data in the same Ceph
Storage Cluster used to store data from Ceph Filesystem clients or Ceph Block
Device clients. The S3 and Swift APIs share a common namespace, so you may write
data with  one API and retrieve it with the other. 

.. ditaa::  +------------------------+ +------------------------+
            |   S3 compatible API    | |  Swift compatible API  |
            +------------------------+-+------------------------+
            |                     radosgw                       |
            +---------------------------------------------------+
            |                     librados                      |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+   

.. note:: Ceph Object Storage does **NOT** use the Ceph Metadata Server.


.. toctree::
	:maxdepth: 1

	Manual Install <manual-install>
	Configuration <config>
	Config Reference <config-ref>
	Purging Temp Data <purge-temp>
	S3 API <s3>
	Swift API <swift>
	Admin Ops API <adminops>
	troubleshooting
	Manpage radosgw <../../man/8/radosgw>
	Manpage radosgw-admin <../../man/8/radosgw-admin>
	
