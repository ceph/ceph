======================
 Swift-compatible API
======================

RADOS Gateway provides a scalable, highly available redundant object storage 
API that is compatible with a subset of the `OpenStack Swift`_ API. The 
Swift-compatible API provides a container-based object storage, with support
for multiple users, storage containers, and access control lists (ACLs). 
This API makes it possible to use a RADOS storage cluster as a Swift-compatible
object storage system, while simultaneously supporting Ceph FS and RADOS block
devices too (*e.g.*, you can use it for your Rackspace Cloud Files).

.. note:: The popular Amazon S3 API uses the term 'bucket' to describe a data 
   container. When you hear someone refer to a 'bucket' within the Swift API, 
   the term 'bucket' may be construed as the equivalent of the term 'container.'

.. toctree::

	Tutorial Overview <tutorial>
	Tutorial-Java <java>
	Tutorial-Python <python>
	Tutorial-Ruby <ruby>
	Auth API <auth>
	Service API <serviceops>
	Container API <containerops>
	Object API <objectops>

.. _OpenStack Swift: http://docs.openstack.org/api/openstack-object-storage/1.0/content/